package scraper

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aluiziolira/go-scrape-books/config"
	"github.com/aluiziolira/go-scrape-books/models"
	"github.com/aluiziolira/go-scrape-books/pipeline"
	"github.com/gocolly/colly/v2"
	"github.com/jarcoal/httpmock"
)

func TestRetryManagerScheduleRespectsLimit(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.MaxRetries = 2
	cfg.RetryBackoff = time.Hour
	cfg.RetryBackoffMax = time.Hour

	rm := newRetryManager(colly.NewCollector(), cfg, NewMetrics())

	if !rm.Schedule("http://example.com/page") {
		t.Fatalf("first retry should be scheduled")
	}
	if !rm.Schedule("http://example.com/page") {
		t.Fatalf("second retry should be scheduled")
	}
	if rm.Schedule("http://example.com/page") {
		t.Fatalf("third retry should not be scheduled")
	}

	rm.Stop()
	if got := rm.TotalRetries(); got != 2 {
		t.Fatalf("total retries = %d, want 2", got)
	}
}

func TestRetryManagerBackoffCapped(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.RetryBackoff = 200 * time.Millisecond
	cfg.RetryBackoffMax = 500 * time.Millisecond

	rm := newRetryManager(colly.NewCollector(), cfg, NewMetrics())

	delay := rm.backoff(4)
	if delay > cfg.RetryBackoffMax {
		t.Fatalf("delay %v exceeds max %v", delay, cfg.RetryBackoffMax)
	}
}

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		statusCode int
		expected   string
	}{
		{name: "nil", err: nil, statusCode: 0, expected: "unknown"},
		{name: "context timeout", err: context.DeadlineExceeded, statusCode: 0, expected: "timeout"},
		{name: "net timeout", err: &net.DNSError{IsTimeout: true}, statusCode: 0, expected: "timeout"},
		{name: "connection", err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, statusCode: 0, expected: "connection"},
		{name: "forbidden", err: nil, statusCode: http.StatusForbidden, expected: "forbidden"},
		{name: "not found", err: nil, statusCode: http.StatusNotFound, expected: "not_found"},
		{name: "rate limited", err: nil, statusCode: http.StatusTooManyRequests, expected: "rate_limited"},
		{name: "other", err: errors.New("some other error"), statusCode: 0, expected: "other"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errorTypeLabel(classifyError(tt.err, tt.statusCode)); got != tt.expected {
				t.Fatalf("classifyError(%v, %d) = %q, want %q", tt.err, tt.statusCode, got, tt.expected)
			}
		})
	}
}

func TestScraperHTTPStatusClassification(t *testing.T) {
	tests := []struct {
		status   int
		expected string
	}{
		{status: http.StatusTooManyRequests, expected: "rate_limited"},
		{status: http.StatusForbidden, expected: "forbidden"},
		{status: http.StatusNotFound, expected: "not_found"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("status_%d", tt.status), func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.BaseURL = "http://example.test/"
			cfg.MaxPages = 1
			cfg.Parallelism = 1
			cfg.MaxRetries = 0
			cfg.PipelineBufferSize = 16
			cfg.BatchSize = 1

			transport := httpmock.NewMockTransport()
			responder := httpmock.NewStringResponder(tt.status, "")
			transport.RegisterResponder("GET", cfg.BaseURL, responder)
			transport.RegisterResponder("GET", strings.TrimSuffix(cfg.BaseURL, "/"), responder)

			s, err := NewScraper(cfg)
			if err != nil {
				t.Fatalf("new scraper: %v", err)
			}
			s.collector.WithTransport(transport)

			writer := &collectingWriter{}
			p := pipeline.NewPipeline(context.Background(), writer, cfg)
			p.Start(1)

			result, err := s.Run(context.Background(), p)
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("close pipeline: %v", err)
			}

			if got := result.ErrorsByType[tt.expected]; got == 0 {
				t.Fatalf("expected %q classification for status %d", tt.expected, tt.status)
			}
		})
	}
}

type collectingWriter struct {
	mu    sync.Mutex
	books []*models.Book
}

func (cw *collectingWriter) Write(books []*models.Book) error {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.books = append(cw.books, books...)
	return nil
}

func (cw *collectingWriter) Close() error {
	return nil
}

func (cw *collectingWriter) Validate() error {
	return nil
}

func (cw *collectingWriter) Count() int {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return len(cw.books)
}

func (cw *collectingWriter) All() []*models.Book {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	out := make([]*models.Book, len(cw.books))
	copy(out, cw.books)
	return out
}

func TestScraper_Integration(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.BaseURL = "http://example.test/"
	cfg.MaxPages = 3
	cfg.Parallelism = 4
	cfg.PipelineBufferSize = 128
	cfg.BatchSize = 64
	cfg.DedupeMaxSize = 1000

	page1 := buildCatalogPage(1, true)
	page2 := buildCatalogPage(2, true)
	page3 := buildCatalogPage(3, false)

	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", cfg.BaseURL, htmlResponder(page1))
	transport.RegisterResponder("GET", strings.TrimSuffix(cfg.BaseURL, "/"), htmlResponder(page1))
	transport.RegisterResponder("GET", cfg.BaseURL+"page-2.html", htmlResponder(page2))
	transport.RegisterResponder("GET", cfg.BaseURL+"page-3.html", htmlResponder(page3))

	s, err := NewScraper(cfg)
	if err != nil {
		t.Fatalf("new scraper: %v", err)
	}
	s.collector.WithTransport(transport)

	writer := &collectingWriter{}
	p := pipeline.NewPipeline(context.Background(), writer, cfg)
	p.Start(2)

	result, err := s.Run(context.Background(), p)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close pipeline: %v", err)
	}

	if got := writer.Count(); got != 60 {
		t.Fatalf("books=%d, want 60 (requests=%d errors=%d failed=%v)", got, result.RequestCount, result.ErrorCount, result.FailedURLs)
	}

	books := writer.All()
	expectedURL := "http://example.test/catalogue/book-1/index.html"
	var sample *models.Book
	for _, book := range books {
		if book.URL == expectedURL {
			sample = book
			break
		}
	}
	if sample == nil {
		t.Fatalf("expected book with URL %s", expectedURL)
	}
	if sample.Title != "Book 1" {
		t.Fatalf("title=%q, want %q", sample.Title, "Book 1")
	}
	if sample.Price != "1.00" {
		t.Fatalf("price=%q, want %q", sample.Price, "1.00")
	}
	if sample.RatingText != "Two" || sample.RatingNumeric != 2 {
		t.Fatalf("rating=%q/%d, want Two/2", sample.RatingText, sample.RatingNumeric)
	}
	if sample.Availability == "" {
		t.Fatalf("availability should not be empty")
	}
}

type benchWriter struct {
	mu    sync.Mutex
	count int
}

func (bw *benchWriter) Write(books []*models.Book) error {
	bw.mu.Lock()
	bw.count += len(books)
	bw.mu.Unlock()
	return nil
}

func (bw *benchWriter) Close() error {
	return nil
}

func (bw *benchWriter) Validate() error {
	return nil
}

func BenchmarkPipeline_Throughput(b *testing.B) {
	cfg := config.DefaultConfig()
	cfg.PipelineBufferSize = 1024
	cfg.BatchSize = 64
	cfg.DedupeMaxSize = 5000000

	for _, workers := range []int{4, 8, 16, 32} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			writer := &benchWriter{}
			p := pipeline.NewPipeline(context.Background(), writer, cfg)
			p.Start(workers)

			scrapedAt := time.Unix(0, 0)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				book := &models.Book{
					Title:        "Benchmark Book",
					Price:        "10.00",
					RatingText:   "Two",
					Availability: "In stock",
					URL:          fmt.Sprintf("http://example.test/book/%d", i),
					ScrapedAt:    scrapedAt,
				}
				if err := p.Process(book); err != nil {
					b.Fatalf("process: %v", err)
				}
			}
			b.StopTimer()
			if err := p.Close(); err != nil {
				b.Fatalf("close: %v", err)
			}
			elapsed := b.Elapsed().Seconds()
			if elapsed > 0 {
				b.ReportMetric(float64(b.N)/elapsed, "items/sec")
			}
		})
	}
}

func htmlResponder(body string) httpmock.Responder {
	resp := httpmock.NewStringResponse(200, body)
	resp.Header.Set("Content-Type", "text/html")
	return httpmock.ResponderFromResponse(resp)
}

func buildCatalogPage(page int, hasNext bool) string {
	var builder strings.Builder
	builder.WriteString("<html><body><section class=\"products\">")

	for i := 1; i <= 20; i++ {
		id := (page-1)*20 + i
		fmt.Fprintf(&builder, "<article class=\"product_pod\">")
		fmt.Fprintf(&builder, "<h3><a href=\"catalogue/book-%d/index.html\" title=\"Book %d\">Book %d</a></h3>", id, id, id)
		fmt.Fprintf(&builder, "<p class=\"price_color\">&pound;%0.2f</p>", float64(id))
		builder.WriteString("<p class=\"star-rating Two\"></p>")
		builder.WriteString("<p class=\"instock availability\">In stock</p>")
		fmt.Fprintf(&builder, "<img src=\"media/cache/book-%d.jpg\" />", id)
		builder.WriteString("</article>")
	}

	if hasNext {
		next := page + 1
		fmt.Fprintf(&builder, "<li class=\"next\"><a href=\"page-%d.html\">next</a></li>", next)
	}

	builder.WriteString("</section></body></html>")
	return builder.String()
}
