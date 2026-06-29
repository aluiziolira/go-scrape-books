package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aluiziolira/go-scrape-books/config"
	"github.com/aluiziolira/go-scrape-books/models"
	"github.com/aluiziolira/go-scrape-books/pipeline"
	"github.com/prometheus/client_golang/prometheus"
)

// buildCatalogPage renders a canned catalog page using the same CSS structure
// that scraper.extractBook reads (article.product_pod, h3 a[title], p.price_color,
// p.star-rating, p.instock.availability, img[src]).
func buildCatalogPage(bookCount int, hasNext bool) string {
	var b strings.Builder
	b.WriteString("<html><body><section class=\"products\">")
	for i := 1; i <= bookCount; i++ {
		b.WriteString("<article class=\"product_pod\">")
		fmt.Fprintf(&b, "<h3><a href=\"catalogue/book-%d/index.html\" title=\"Book %d\">Book %d</a></h3>", i, i, i)
		fmt.Fprintf(&b, "<p class=\"price_color\">&pound;%0.2f</p>", float64(i))
		b.WriteString("<p class=\"star-rating Two\"></p>")
		b.WriteString("<p class=\"instock availability\">In stock</p>")
		fmt.Fprintf(&b, "<img src=\"media/cache/book-%d.jpg\" />", i)
		b.WriteString("</article>")
	}
	if hasNext {
		b.WriteString("<li class=\"next\"><a href=\"page-2.html\">next</a></li>")
	}
	b.WriteString("</section></body></html>")
	return b.String()
}

// catalogServer returns an httptest.Server that responds with a canned page on
// every path.
func catalogServer(t *testing.T, bookCount int, hasNext bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, buildCatalogPage(bookCount, hasNext))
	}))
}

// freeAddr returns a host:port that is currently free (best-effort; the caller
// should bind to it quickly).
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func TestRun_EndToEnd(t *testing.T) {
	const bookCount = 4
	srv := catalogServer(t, bookCount, false)
	defer srv.Close()

	outputFile := filepath.Join(t.TempDir(), "books.csv")
	cfg := config.DefaultConfig()
	cfg.BaseURL = srv.URL
	cfg.MaxPages = 1
	cfg.Parallelism = 1
	cfg.Delay = 0
	cfg.RespectRobotsTxt = false
	cfg.RandomDelay = 0
	cfg.MaxRetries = 0
	cfg.OutputFile = outputFile
	cfg.OutputFormat = "csv"
	cfg.MetricsAddr = ""
	cfg.PipelineBufferSize = 16
	cfg.BatchSize = 1
	cfg.Timeout = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if code := run(ctx, cfg, outputFile); code != 0 {
		t.Fatalf("run exit code = %d, want 0", code)
	}

	f, err := os.Open(outputFile)
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	defer func() { _ = f.Close() }()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	if len(records) != bookCount+1 {
		t.Fatalf("record count = %d, want %d (header + %d books)", len(records), bookCount+1, bookCount)
	}
	if len(records[0]) == 0 || records[0][0] != "title" {
		t.Fatalf("first record is not the header: %v", records[0])
	}
	for i, row := range records[1:] {
		if row[0] != fmt.Sprintf("Book %d", i+1) {
			t.Fatalf("row %d title = %q, want %q", i+1, row[0], fmt.Sprintf("Book %d", i+1))
		}
	}
}

func TestRun_WithMetrics(t *testing.T) {
	srv := catalogServer(t, 2, false)
	defer srv.Close()

	outputFile := filepath.Join(t.TempDir(), "books.csv")
	cfg := config.DefaultConfig()
	cfg.BaseURL = srv.URL
	cfg.MaxPages = 1
	cfg.Parallelism = 1
	cfg.OutputFile = outputFile
	cfg.OutputFormat = "csv"
	cfg.RespectRobotsTxt = false
	cfg.MetricsAddr = freeAddr(t)
	cfg.Timeout = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if code := run(ctx, cfg, outputFile); code != 0 {
		t.Fatalf("run exit code = %d, want 0", code)
	}
}

func TestRun_CreateWriterFailure(t *testing.T) {
	// Make the output file's parent a regular file so ensureDir fails.
	parent := filepath.Join(t.TempDir(), "notadir")
	if err := os.WriteFile(parent, []byte("x"), 0o644); err != nil {
		t.Fatalf("write parent: %v", err)
	}

	cfg := config.DefaultConfig()
	cfg.BaseURL = "http://127.0.0.1:1/"
	cfg.MaxPages = 1
	cfg.OutputFile = filepath.Join(parent, "books.csv")
	cfg.OutputFormat = "csv"
	cfg.MetricsAddr = ""

	if code := run(context.Background(), cfg, cfg.OutputFile); code != 1 {
		t.Fatalf("run exit code = %d, want 1", code)
	}
}

func TestRun_ContextCancellation(t *testing.T) {
	srv := catalogServer(t, 2, true)
	defer srv.Close()

	outputFile := filepath.Join(t.TempDir(), "books.csv")
	cfg := config.DefaultConfig()
	cfg.BaseURL = srv.URL
	cfg.RespectRobotsTxt = false
	cfg.MaxPages = 10
	cfg.Parallelism = 1
	cfg.MaxRetries = 0
	cfg.OutputFile = outputFile
	cfg.OutputFormat = "csv"
	cfg.MetricsAddr = ""
	cfg.Timeout = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan int, 1)
	go func() { done <- run(ctx, cfg, outputFile) }()
	select {
	case code := <-done:
		if code != 0 && code != 1 {
			t.Fatalf("run exit code = %d, want 0 or 1", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return within 10s after context cancellation")
	}
}

func TestCreateWriter(t *testing.T) {
	tests := []struct {
		name    string
		format  string
		wantErr bool
	}{
		{name: "csv", format: "csv", wantErr: false},
		{name: "json", format: "json", wantErr: false},
		{name: "dual", format: "dual", wantErr: false},
		{name: "unsupported", format: "yaml", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "books.csv")
			w, err := createWriter(tt.format, base)
			if tt.wantErr {
				if err == nil {
					_ = w.Close()
					t.Fatalf("createWriter(%q) expected error", tt.format)
				}
				return
			}
			if err != nil {
				t.Fatalf("createWriter(%q): %v", tt.format, err)
			}
			if w == nil {
				t.Fatalf("createWriter(%q) returned nil writer", tt.format)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
		})
	}
}

func TestPrintSummary(t *testing.T) {
	result := &models.ScraperResult{
		RequestCount: 5,
		ErrorCount:   1,
		RetryCount:   2,
		FailedURLs:   []string{"http://example.com/x"},
	}
	metrics := pipeline.PipelineStats{Processed: 3}

	var buf bytes.Buffer
	printSummary(&buf, result, 250*time.Millisecond, 12.0, "/tmp/out.csv", metrics)

	got := buf.String()
	for _, want := range []string{
		"Scrape complete",
		"Total items:   3",
		"Success rate:  80.00%",
		"Errors:        1",
		"Retries:       2",
		"Failed URLs:   1",
		"Duration:      250ms",
		"Items/sec:     12.00",
		"Output file:   /tmp/out.csv",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("summary missing %q\ngot:\n%s", want, got)
		}
	}
}

func TestPrintSummary_ErrorAndValidationBreakdowns(t *testing.T) {
	result := &models.ScraperResult{
		RequestCount: 4,
		ErrorCount:   2,
		ErrorsByType: map[string]int{"timeout": 2},
	}
	metrics := pipeline.PipelineStats{
		Processed:        2,
		ValidationErrors: map[string]int{"duplicate_url": 1},
	}
	var buf bytes.Buffer
	printSummary(&buf, result, time.Second, 2.0, "out.csv", metrics)
	got := buf.String()
	for _, want := range []string{"Error types:", "timeout", "Validation:", "duplicate_url"} {
		if !strings.Contains(got, want) {
			t.Errorf("summary missing %q\ngot:\n%s", want, got)
		}
	}
}

func TestBuildConfigFromFlags(t *testing.T) {
	cfg := buildConfigFromFlags(
		"http://example.com", 3, 4, 100, 50, 2, 200, 2000,
		true, "out.csv", "DUAL", true, ":9090",
	)
	tests := []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{"BaseURL", cfg.BaseURL, "http://example.com"},
		{"MaxPages", cfg.MaxPages, 3},
		{"Parallelism", cfg.Parallelism, 4},
		{"Delay", cfg.Delay, 100 * time.Millisecond},
		{"RandomDelay", cfg.RandomDelay, 50 * time.Millisecond},
		{"MaxRetries", cfg.MaxRetries, 2},
		{"RetryBackoff", cfg.RetryBackoff, 200 * time.Millisecond},
		{"RetryBackoffMax", cfg.RetryBackoffMax, 2000 * time.Millisecond},
		{"RespectRobotsTxt", cfg.RespectRobotsTxt, true},
		{"OutputFile", cfg.OutputFile, "out.csv"},
		{"OutputFormat", cfg.OutputFormat, "dual"}, // lowercased
		{"Verbose", cfg.Verbose, true},
		{"MetricsAddr", cfg.MetricsAddr, ":9090"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if fmt.Sprint(tt.got) != fmt.Sprint(tt.want) {
				t.Fatalf("%s = %v, want %v", tt.name, tt.got, tt.want)
			}
		})
	}
}

func TestStartMetricsServer_EmptyAddr(t *testing.T) {
	if srv := startMetricsServer(context.Background(), "", prometheus.NewRegistry()); srv != nil {
		t.Fatalf("expected nil server for empty addr, got %v", srv)
	}
}

func TestShutdownMetricsServer_Nil(t *testing.T) {
	// Should not panic and should be a no-op.
	shutdownMetricsServer(nil, time.Second)
}

func TestStartShutdownMetricsServer(t *testing.T) {
	addr := freeAddr(t)
	srv := startMetricsServer(context.Background(), addr, prometheus.NewRegistry())
	if srv == nil {
		t.Fatal("expected non-nil server")
	}

	client := &http.Client{Timeout: time.Second}
	var ready bool
	for i := 0; i < 100; i++ {
		resp, err := client.Get("http://" + addr + "/metrics")
		if err == nil {
			_ = resp.Body.Close()
			ready = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ready {
		t.Fatal("metrics server never became ready")
	}

	shutdownMetricsServer(srv, 2*time.Second)
}

func TestStartMetricsServer_PortConflict(t *testing.T) {
	addr := freeAddr(t)
	// Hold the port so startMetricsServer's ListenAndServe fails and the
	// goroutine logs the failure.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("hold port: %v", err)
	}
	defer func() { _ = ln.Close() }()

	srv := startMetricsServer(context.Background(), addr, prometheus.NewRegistry())
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
	time.Sleep(100 * time.Millisecond) // allow the goroutine to observe the failure
	shutdownMetricsServer(srv, time.Second)
}

func TestNewLogger(t *testing.T) {
	for _, verbose := range []bool{true, false} {
		logger, level := newLogger(verbose)
		if logger == nil {
			t.Fatalf("nil logger for verbose=%v", verbose)
		}
		if level == nil {
			t.Fatalf("nil level for verbose=%v", verbose)
		}
	}
}

func TestFlagDefaults(t *testing.T) {
	keys := []string{"SCRAPER_PAGES", "SCRAPER_PARALLEL", "SCRAPER_OUTPUT", "SCRAPER_METRICS_ADDR"}
	for _, k := range keys {
		old, had := os.LookupEnv(k)
		_ = os.Unsetenv(k)
		t.Cleanup(func() {
			if had {
				_ = os.Setenv(k, old)
			}
		})
	}

	t.Run("defaults", func(t *testing.T) {
		def := config.DefaultConfig()
		pages, parallel, output, metrics, err := flagDefaults()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pages != def.MaxPages || parallel != def.Parallelism ||
			output != def.OutputFile || metrics != def.MetricsAddr {
			t.Fatalf("got pages=%d parallel=%d output=%q metrics=%q", pages, parallel, output, metrics)
		}
	})

	t.Run("overrides", func(t *testing.T) {
		t.Setenv("SCRAPER_PAGES", "7")
		t.Setenv("SCRAPER_PARALLEL", "3")
		t.Setenv("SCRAPER_OUTPUT", "/tmp/over.csv")
		t.Setenv("SCRAPER_METRICS_ADDR", ":1234")
		pages, parallel, output, metrics, err := flagDefaults()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pages != 7 || parallel != 3 || output != "/tmp/over.csv" || metrics != ":1234" {
			t.Fatalf("got pages=%d parallel=%d output=%q metrics=%q", pages, parallel, output, metrics)
		}
	})

	t.Run("invalid_pages", func(t *testing.T) {
		t.Setenv("SCRAPER_PAGES", "nope")
		_, _, _, _, err := flagDefaults()
		if err == nil {
			t.Fatal("expected error for invalid SCRAPER_PAGES")
		}
		if !strings.Contains(err.Error(), "invalid SCRAPER_PAGES") {
			t.Fatalf("error = %v, want message containing invalid SCRAPER_PAGES", err)
		}
	})

	t.Run("invalid_parallel", func(t *testing.T) {
		t.Setenv("SCRAPER_PARALLEL", "nope")
		_, _, _, _, err := flagDefaults()
		if err == nil {
			t.Fatal("expected error for invalid SCRAPER_PARALLEL")
		}
		if !strings.Contains(err.Error(), "invalid SCRAPER_PARALLEL") {
			t.Fatalf("error = %v, want message containing invalid SCRAPER_PARALLEL", err)
		}
	})
}
