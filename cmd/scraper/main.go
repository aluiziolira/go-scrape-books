package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aluiziolira/go-scrape-books/config"
	"github.com/aluiziolira/go-scrape-books/models"
	"github.com/aluiziolira/go-scrape-books/pipeline"
	"github.com/aluiziolira/go-scrape-books/scraper"
)

func main() {
	maxPages := flag.Int("pages", 50, "Maximum catalog pages to scrape")
	parallelism := flag.Int("parallel", 16, "Number of concurrent requests")
	delayMs := flag.Int("delay", 0, "Delay between requests (milliseconds)")
	randomDelayMs := flag.Int("random-delay", 0, "Random jitter added to delay (milliseconds)")
	maxRetries := flag.Int("max-retries", 2, "Maximum retry attempts per URL")
	retryBackoffMs := flag.Int("retry-backoff", 200, "Initial retry backoff (milliseconds)")
	retryBackoffMaxMs := flag.Int("retry-backoff-max", 2000, "Maximum retry backoff (milliseconds)")
	respectRobots := flag.Bool("respect-robots", false, "Respect robots.txt directives")
	outputFile := flag.String("output", "output/books.csv", "Output file path")
	outputFormat := flag.String("format", "csv", "Output format: csv, json, or dual")
	verbose := flag.Bool("v", false, "Enable verbose logging")
	baseURL := flag.String("base-url", "https://books.toscrape.com", "Base URL to crawl")

	flag.Parse()

	cfg := buildConfigFromFlags(*baseURL, *maxPages, *parallelism, *delayMs, *randomDelayMs, *maxRetries, *retryBackoffMs, *retryBackoffMaxMs, *respectRobots, *outputFile, *outputFormat, *verbose)
	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}

	log.Printf("Scraping %s (pages=%d, workers=%d)", cfg.BaseURL, cfg.MaxPages, cfg.Parallelism)

	s, err := scraper.NewScraper(cfg)
	if err != nil {
		log.Fatalf("initialising scraper: %v", err)
	}

	writer, err := createWriter(cfg.OutputFormat, cfg.OutputFile)
	if err != nil {
		log.Fatalf("creating writer: %v", err)
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("close writer: %v", err)
		}
	}()

	p := pipeline.NewPipeline(writer)
	p.Start(cfg.Parallelism)
	if cfg.Verbose {
		p.StartMetricsReporting(10 * time.Second)
	}

	startTime := time.Now()
	result, err := s.Scrape(p)
	if err != nil {
		log.Fatalf("scraping failed: %v", err)
	}

	if err := p.Close(); err != nil {
		log.Fatalf("pipeline shutdown failed: %v", err)
	}

	if err := writer.Validate(); err != nil {
		log.Fatalf("output validation failed: %v", err)
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

func buildConfigFromFlags(baseURL string, maxPages, parallelism, delayMs, randomDelayMs, maxRetries, retryBackoffMs, retryBackoffMaxMs int, respectRobots bool, outputFile, outputFormat string, verbose bool) *config.Config {
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
