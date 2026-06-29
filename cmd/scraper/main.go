package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aluiziolira/go-scrape-books/config"
	"github.com/aluiziolira/go-scrape-books/models"
	"github.com/aluiziolira/go-scrape-books/pipeline"
	"github.com/aluiziolira/go-scrape-books/scraper"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	pagesDefault, parallelDefault, outputDefault, metricsDefault, err := flagDefaults()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	maxPages := flag.Int("pages", pagesDefault, "Maximum catalog pages to scrape")
	parallelism := flag.Int("parallel", parallelDefault, "Number of concurrent requests")
	delayMs := flag.Int("delay", 0, "Delay between requests (milliseconds)")
	randomDelayMs := flag.Int("random-delay", 0, "Random jitter added to delay (milliseconds)")
	maxRetries := flag.Int("max-retries", 2, "Maximum retry attempts per URL")
	retryBackoffMs := flag.Int("retry-backoff", 200, "Initial retry backoff (milliseconds)")
	retryBackoffMaxMs := flag.Int("retry-backoff-max", 2000, "Maximum retry backoff (milliseconds)")
	respectRobots := flag.Bool("respect-robots", true, "Respect robots.txt directives (enabled by default; pass -respect-robots=false to disable)")
	outputFile := flag.String("output", outputDefault, "Output file path")
	outputFormat := flag.String("format", "csv", "Output format: csv, json, or dual")
	verbose := flag.Bool("v", false, "Enable verbose logging")
	baseURL := flag.String("base-url", "https://books.toscrape.com", "Base URL to crawl")
	metricsAddr := flag.String("metrics-addr", metricsDefault, "Prometheus metrics listen address (e.g. :9090)")

	flag.Parse()

	logger, level := newLogger(*verbose)
	slog.SetDefault(logger)
	slog.SetLogLoggerLevel(level.Level())

	cfg := buildConfigFromFlags(*baseURL, *maxPages, *parallelism, *delayMs, *randomDelayMs, *maxRetries, *retryBackoffMs, *retryBackoffMaxMs, *respectRobots, *outputFile, *outputFormat, *verbose, *metricsAddr)
	if err := cfg.Validate(); err != nil {
		slog.Error("invalid configuration", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	os.Exit(run(ctx, cfg, *outputFile))
}

// run executes the scrape and returns a process exit code.
func run(ctx context.Context, cfg *config.Config, outputFile string) int {
	logger, level := newLogger(cfg.Verbose)
	slog.SetDefault(logger)
	slog.SetLogLoggerLevel(level.Level())

	slog.Info("starting scrape",
		slog.String("base_url", cfg.BaseURL),
		slog.Int("pages", cfg.MaxPages),
		slog.Int("workers", cfg.Parallelism),
	)

	s, err := scraper.NewScraper(cfg)
	if err != nil {
		slog.Error("initialising scraper", slog.Any("error", err))
		return 1
	}

	writer, err := createWriter(cfg.OutputFormat, outputFile)
	if err != nil {
		slog.Error("creating writer", slog.Any("error", err))
		return 1
	}
	defer func() {
		if err := writer.Close(); err != nil {
			slog.Error("close writer", slog.Any("error", err))
		}
	}()

	go func() {
		<-ctx.Done()
		slog.Info("shutdown signal received, waiting for in-flight work to finish")
	}()

	var metricsServer *http.Server
	if cfg.MetricsAddr != "" && s.Metrics != nil {
		metricsServer = startMetricsServer(ctx, cfg.MetricsAddr, s.Metrics.Registry)
	}

	p := pipeline.NewPipeline(ctx, writer, cfg)
	p.Start(cfg.Parallelism)
	if cfg.Verbose {
		p.StartMetricsReporting(10 * time.Second)
	}

	startTime := time.Now()
	result, err := s.Run(ctx, p)
	if err != nil {
		slog.Error("scraping failed", slog.Any("error", err))
		shutdownMetricsServer(metricsServer, 5*time.Second)
		return 1
	}

	if err := p.Close(); err != nil {
		slog.Error("pipeline shutdown failed", slog.Any("error", err))
		shutdownMetricsServer(metricsServer, 5*time.Second)
		return 1
	}

	if err := writer.Validate(); err != nil {
		slog.Error("output validation failed", slog.Any("error", err))
		shutdownMetricsServer(metricsServer, 5*time.Second)
		return 1
	}

	shutdownMetricsServer(metricsServer, 5*time.Second)

	metrics := p.GetMetrics()
	duration := time.Since(startTime)
	totalItems := metrics.Processed
	itemsPerSec := 0.0
	if duration.Seconds() > 0 {
		itemsPerSec = float64(totalItems) / duration.Seconds()
	}

	printSummary(os.Stdout, result, duration, itemsPerSec, outputFile, metrics)
	return 0
}

// startMetricsServer launches the Prometheus metrics HTTP server.
// It returns nil when addr is empty (metrics disabled).
func startMetricsServer(ctx context.Context, addr string, registry *prometheus.Registry) *http.Server {
	_ = ctx
	if addr == "" {
		return nil
	}
	srv := &http.Server{
		Addr:    addr,
		Handler: promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server failed", slog.Any("error", err))
		}
	}()
	slog.Info("metrics server enabled", slog.String("addr", addr))
	return srv
}

// shutdownMetricsServer gracefully shuts down the metrics server.
// It is a no-op when srv is nil.
func shutdownMetricsServer(srv *http.Server, timeout time.Duration) {
	if srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("metrics server shutdown failed", slog.Any("error", err))
	}
}

// flagDefaults computes flag default values, applying environment overrides.
// It returns an error (matching the original stderr message) when an env var is
// present but cannot be parsed.
func flagDefaults() (pagesDefault, parallelDefault int, outputDefault, metricsDefault string, err error) {
	defaultCfg := config.DefaultConfig()
	pagesDefault = defaultCfg.MaxPages
	if value, ok, parseErr := config.EnvInt("SCRAPER_PAGES"); parseErr != nil {
		return 0, 0, "", "", fmt.Errorf("invalid SCRAPER_PAGES: %v", parseErr)
	} else if ok {
		pagesDefault = value
	}
	parallelDefault = defaultCfg.Parallelism
	if value, ok, parseErr := config.EnvInt("SCRAPER_PARALLEL"); parseErr != nil {
		return 0, 0, "", "", fmt.Errorf("invalid SCRAPER_PARALLEL: %v", parseErr)
	} else if ok {
		parallelDefault = value
	}
	outputDefault = defaultCfg.OutputFile
	if value, ok := config.EnvString("SCRAPER_OUTPUT"); ok {
		outputDefault = value
	}
	metricsDefault = defaultCfg.MetricsAddr
	if value, ok := config.EnvString("SCRAPER_METRICS_ADDR"); ok {
		metricsDefault = value
	}
	return pagesDefault, parallelDefault, outputDefault, metricsDefault, nil
}

func buildConfigFromFlags(baseURL string, maxPages, parallelism, delayMs, randomDelayMs, maxRetries, retryBackoffMs, retryBackoffMaxMs int, respectRobots bool, outputFile, outputFormat string, verbose bool, metricsAddr string) *config.Config {
	cfg := config.DefaultConfig()
	cfg.BaseURL = baseURL
	cfg.MaxPages = maxPages
	cfg.Parallelism = parallelism
	cfg.Delay = time.Duration(delayMs) * time.Millisecond
	cfg.RandomDelay = time.Duration(randomDelayMs) * time.Millisecond
	cfg.MaxRetries = maxRetries
	cfg.RetryBackoff = time.Duration(retryBackoffMs) * time.Millisecond
	cfg.RetryBackoffMax = time.Duration(retryBackoffMaxMs) * time.Millisecond
	cfg.RespectRobotsTxt = respectRobots
	cfg.OutputFile = outputFile
	cfg.OutputFormat = strings.ToLower(outputFormat)
	cfg.Verbose = verbose
	cfg.MetricsAddr = metricsAddr
	return cfg
}

func createWriter(format, filename string) (pipeline.OutputWriter, error) {
	switch format {
	case "json":
		return pipeline.NewJSONWriter(filename)
	case "csv":
		return pipeline.NewCSVWriter(filename)
	case "dual":
		jsonFilename := strings.TrimSuffix(filename, ".csv") + ".json"
		return pipeline.NewDualWriter(filename, jsonFilename)
	default:
		return nil, fmt.Errorf("unsupported format: %s", format)
	}
}

func printSummary(w io.Writer, result *models.ScraperResult, duration time.Duration, itemsPerSec float64, outputFile string, metrics pipeline.PipelineStats) {
	separator := "--------------------------------------------------"
	fmt.Fprintln(w, "\n"+separator)
	fmt.Fprintln(w, "Scrape complete")

	totalItems := metrics.Processed

	fmt.Fprintf(w, "  Total items:   %d\n", totalItems)
	successRate := 0.0
	if result.RequestCount > 0 {
		successRate = float64(result.RequestCount-result.ErrorCount) / float64(result.RequestCount) * 100
	}
	fmt.Fprintf(w, "  Success rate:  %.2f%%\n", successRate)
	fmt.Fprintf(w, "  Errors:        %d\n", result.ErrorCount)
	fmt.Fprintf(w, "  Retries:       %d\n", result.RetryCount)
	fmt.Fprintf(w, "  Failed URLs:   %d\n", len(result.FailedURLs))
	if len(result.ErrorsByType) > 0 {
		fmt.Fprintf(w, "  Error types:   %v\n", result.ErrorsByType)
	}
	if valErrors := metrics.ValidationErrors; len(valErrors) > 0 {
		fmt.Fprintf(w, "  Validation:    %v\n", valErrors)
	}
	fmt.Fprintf(w, "  Duration:      %v\n", duration)
	fmt.Fprintf(w, "  Items/sec:     %.2f\n", itemsPerSec)
	fmt.Fprintf(w, "  Output file:   %s\n", outputFile)
	fmt.Fprintln(w, separator)
}

func newLogger(verbose bool) (*slog.Logger, *slog.LevelVar) {
	level := &slog.LevelVar{}
	if verbose {
		level.Set(slog.LevelDebug)
	} else {
		level.Set(slog.LevelInfo)
	}

	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if isTerminal(os.Stdout) {
		handler = slog.NewTextHandler(os.Stdout, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}

	return slog.New(handler), level
}

func isTerminal(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}
