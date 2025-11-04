package scraper

import (
	"errors"
	"testing"
	"time"

	"github.com/aluiziolira/go-scrape-books/config"
	"github.com/gocolly/colly/v2"
)

func TestRetryManagerScheduleRespectsLimit(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.MaxRetries = 2
	cfg.RetryBackoff = time.Hour
	cfg.RetryBackoffMax = time.Hour

	rm := newRetryManager(colly.NewCollector(), cfg)

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

	rm := newRetryManager(colly.NewCollector(), cfg)

	delay := rm.backoff(4)
	if delay > cfg.RetryBackoffMax {
		t.Fatalf("delay %v exceeds max %v", delay, cfg.RetryBackoffMax)
	}
}

func TestClassifyError(t *testing.T) {
	tests := []struct {
		err      error
		expected string
	}{
		{nil, "unknown"},
		{errors.New("request timeout"), "timeout"},
		{errors.New("connection reset by peer"), "connection"},
		{errors.New("connection refused"), "connection"},
		{errors.New("unexpected EOF"), "eof"},
		{errors.New("403 Forbidden"), "forbidden"},
		{errors.New("404 Not Found"), "not_found"},
		{errors.New("some other error"), "other"},
	}

	for _, tt := range tests {
		if got := classifyError(tt.err); got != tt.expected {
			t.Fatalf("classifyError(%v) = %q, want %q", tt.err, got, tt.expected)
		}
	}
}
