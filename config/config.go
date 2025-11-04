package config

import (
	"fmt"
	"net/url"
	"time"
)

// Config holds scraper configuration.
type Config struct {
	BaseURL          string
	MaxPages         int
	Parallelism      int
	Delay            time.Duration
	RandomDelay      time.Duration
	Timeout          time.Duration
	MaxRetries       int
	RetryBackoff     time.Duration
	RetryBackoffMax  time.Duration
	OutputFile       string
	OutputFormat     string // csv, json, or dual
	UserAgent        string
	Verbose          bool
	RespectRobotsTxt bool
}

// DefaultConfig returns conservative defaults for the demo target.
func DefaultConfig() *Config {
	return &Config{
		BaseURL:          "https://books.toscrape.com",
		MaxPages:         50,
		Parallelism:      16,
		Delay:            0,
		RandomDelay:      0,
		Timeout:          10 * time.Second,
		MaxRetries:       2,
		RetryBackoff:     200 * time.Millisecond,
		RetryBackoffMax:  2 * time.Second,
		OutputFile:       "output/books.csv",
		OutputFormat:     "csv",
		UserAgent:        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
		Verbose:          false,
		RespectRobotsTxt: false,
	}
}

// Validate ensures all configuration values are coherent.
func (c *Config) Validate() error {
	if c.BaseURL == "" {
		return fmt.Errorf("base URL cannot be empty")
	}

	parsedURL, err := url.Parse(c.BaseURL)
	if err != nil {
		return fmt.Errorf("invalid base URL: %w", err)
	}
	if parsedURL.Host == "" {
		return fmt.Errorf("base URL must include a host")
	}

	if c.MaxPages <= 0 {
		return fmt.Errorf("max pages must be positive")
	}
	if c.Parallelism <= 0 {
		return fmt.Errorf("parallelism must be positive")
	}
	if c.Delay < 0 {
		return fmt.Errorf("delay cannot be negative")
	}
	if c.RandomDelay < 0 {
		return fmt.Errorf("random delay cannot be negative")
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("max retries cannot be negative")
	}
	if c.RetryBackoff < 0 {
		return fmt.Errorf("retry backoff cannot be negative")
	}
	if c.RetryBackoffMax < 0 {
		return fmt.Errorf("retry backoff max cannot be negative")
	}
	if c.RetryBackoffMax > 0 && c.RetryBackoff > c.RetryBackoffMax {
		return fmt.Errorf("retry backoff (%s) cannot exceed retry backoff max (%s)", c.RetryBackoff, c.RetryBackoffMax)
	}
	if c.OutputFile == "" {
		return fmt.Errorf("output file cannot be empty")
	}
	if c.OutputFormat != "csv" && c.OutputFormat != "json" && c.OutputFormat != "dual" {
		return fmt.Errorf("output format must be csv, json, or dual")
	}
	if c.UserAgent == "" {
		return fmt.Errorf("user agent cannot be empty")
	}

	return nil
}
