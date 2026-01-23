package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
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
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	defaultCfg := config.DefaultConfig()
	pagesDefault := defaultCfg.MaxPages
	if value, ok, err := config.EnvInt("SCRAPER_PAGES"); err != nil {
		fmt.Fprintf(os.Stderr, "invalid SCRAPER_PAGES: %v\n", err)
		os.Exit(1)
	} else if ok {
		pagesDefault = value
	}
	parallelDefault := defaultCfg.Parallelism
	if value, ok, err := config.EnvInt("SCRAPER_PARALLEL"); err != nil {
		fmt.Fprintf(os.Stderr, "invalid SCRAPER_PARALLEL: %v\n", err)
		os.Exit(1)
	} else if ok {
		parallelDefault = value
	}
	outputDefault := defaultCfg.OutputFile
	if value, ok := config.EnvString("SCRAPER_OUTPUT"); ok {
		outputDefault = value
	}
	metricsDefault := defaultCfg.MetricsAddr
	if value, ok := config.EnvString("SCRAPER_METRICS_ADDR"); ok {
		metricsDefault = value
	}

	maxPages := flag.Int("pages", pagesDefault, "Maximum catalog pages to scrape")
	parallelism := flag.Int("parallel", parallelDefault, "Number of concurrent requests")
	delayMs := flag.Int("delay", 0, "Delay between requests (milliseconds)")
	randomDelayMs := flag.Int("random-delay", 0, "Random jitter added to delay (milliseconds)")
	maxRetries := flag.Int("max-retries", 2, "Maximum retry attempts per URL")
	retryBackoffMs := flag.Int("retry-backoff", 200, "Initial retry backoff (milliseconds)")
	retryBackoffMaxMs := flag.Int("retry-backoff-max", 2000, "Maximum retry backoff (milliseconds)")
	respectRobots := flag.Bool("respect-robots", false, "Respect robots.txt directives")
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

	slog.Info("starting scrape",
		slog.String("base_url", cfg.BaseURL),
		slog.Int("pages", cfg.MaxPages),
		slog.Int("workers", cfg.Parallelism),
	)

	s, err := scraper.NewScraper(cfg)
	if err != nil {
		slog.Error("initialising scraper", slog.Any("error", err))
		os.Exit(1)
	}

	writer, err := createWriter(cfg.OutputFormat, cfg.OutputFile)
	if err != nil {
		slog.Error("creating writer", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() {
		if err := writer.Close(); err != nil {
			slog.Error("close writer", slog.Any("error", err))
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		slog.Info("shutdown signal received, waiting for in-flight work to finish")
	}()

	var metricsServer *http.Server
	if cfg.MetricsAddr != "" && s.Metrics != nil {
		metricsServer = &http.Server{
			Addr:    cfg.MetricsAddr,
			Handler: promhttp.HandlerFor(s.Metrics.Registry, promhttp.HandlerOpts{}),
		}
		go func() {
			if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("metrics server failed", slog.Any("error", err))
			}
		}()
		slog.Info("metrics server enabled", slog.String("addr", cfg.MetricsAddr))
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
		os.Exit(1)
	}

	if err := p.Close(); err != nil {
		slog.Error("pipeline shutdown failed", slog.Any("error", err))
		os.Exit(1)
	}

	if err := writer.Validate(); err != nil {
		slog.Error("output validation failed", slog.Any("error", err))
		os.Exit(1)
	}

	if metricsServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			slog.Error("metrics server shutdown failed", slog.Any("error", err))
		}
		cancel()
	}

	metrics := p.GetMetrics()
	duration := time.Since(startTime)
	totalItems := int64(0)
	if processed, ok := metrics["processed_books"].(int64); ok {
		totalItems = processed
	}
	itemsPerSec := 0.0
	if duration.Seconds() > 0 {
		itemsPerSec = float64(totalItems) / duration.Seconds()
	}

	printSummary(result, duration, itemsPerSec, cfg.OutputFile, metrics)
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

func printSummary(result *models.ScraperResult, duration time.Duration, itemsPerSec float64, outputFile string, metrics map[string]interface{}) {
	separator := "--------------------------------------------------"
	fmt.Println("\n" + separator)
	fmt.Println("Scrape complete")

	totalItems := int64(0)
	if processed, ok := metrics["processed_books"].(int64); ok {
		totalItems = processed
	}

	fmt.Printf("  Total items:   %d\n", totalItems)
	successRate := 0.0
	if result.RequestCount > 0 {
		successRate = float64(result.RequestCount-result.ErrorCount) / float64(result.RequestCount) * 100
	}
	fmt.Printf("  Success rate:  %.2f%%\n", successRate)
	fmt.Printf("  Errors:        %d\n", result.ErrorCount)
	fmt.Printf("  Retries:       %d\n", result.RetryCount)
	fmt.Printf("  Failed URLs:   %d\n", len(result.FailedURLs))
	if len(result.ErrorsByType) > 0 {
		fmt.Printf("  Error types:   %v\n", result.ErrorsByType)
	}
	if valErrors, ok := metrics["validation_errors"].(map[string]int); ok && len(valErrors) > 0 {
		fmt.Printf("  Validation:    %v\n", valErrors)
	}
	fmt.Printf("  Duration:      %v\n", duration)
	fmt.Printf("  Items/sec:     %.2f\n", itemsPerSec)
	fmt.Printf("  Output file:   %s\n", outputFile)
	fmt.Println(separator)
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
