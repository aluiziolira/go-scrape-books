package scraper

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/aluiziolira/go-scrape-books/config"
	"github.com/gocolly/colly/v2"
)

// retryManager schedules delayed re-visits for URLs that failed, honoring
// cfg.MaxRetries and an exponential backoff capped at cfg.RetryBackoffMax.
type retryManager struct {
	collector *colly.Collector
	cfg       *config.Config
	metrics   *Metrics
	ctx       context.Context

	mu           sync.Mutex
	attempts     map[string]int
	timers       map[string]*time.Timer
	totalRetries int
	stopped      bool
}

func newRetryManager(collector *colly.Collector, cfg *config.Config, metrics *Metrics) *retryManager {
	return &retryManager{
		collector: collector,
		cfg:       cfg,
		attempts:  make(map[string]int),
		timers:    make(map[string]*time.Timer),
		metrics:   metrics,
		ctx:       context.Background(),
	}
}

// Schedule queues a retry for url after a backoff delay, returning false if
// the URL has exhausted its retry budget or the manager is stopped.
func (rm *retryManager) Schedule(url string) bool {
	if rm.cfg.MaxRetries == 0 {
		return false
	}

	rm.mu.Lock()

	if rm.stopped {
		rm.mu.Unlock()
		return false
	}
	if rm.ctx != nil && rm.ctx.Err() != nil {
		rm.mu.Unlock()
		return false
	}

	attempt := rm.attempts[url]
	if attempt >= rm.cfg.MaxRetries {
		rm.mu.Unlock()
		return false
	}

	attempt++
	rm.attempts[url] = attempt
	rm.totalRetries++
	if rm.metrics != nil {
		rm.metrics.IncRetries()
	}

	delay := rm.backoff(attempt)
	rm.resetTimerLocked(url)
	rm.timers[url] = time.AfterFunc(delay, func() {
		rm.fireRetry(url)
	})
	rm.mu.Unlock()
	return true
}

func (rm *retryManager) backoff(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	base := rm.cfg.RetryBackoff
	if base <= 0 {
		base = 100 * time.Millisecond
	}

	delay := base * time.Duration(1<<(attempt-1))
	if max := rm.cfg.RetryBackoffMax; max > 0 && delay > max {
		delay = max
	}
	return delay
}

func (rm *retryManager) resetTimerLocked(url string) {
	if timer, ok := rm.timers[url]; ok {
		timer.Stop()
		delete(rm.timers, url)
	}
}

func (rm *retryManager) fireRetry(url string) {
	rm.mu.Lock()
	if rm.stopped {
		rm.mu.Unlock()
		return
	}
	ctx := rm.ctx
	rm.mu.Unlock()

	if ctx != nil && ctx.Err() != nil {
		return
	}
	if err := rm.collector.Visit(url); err != nil {
		slog.Debug("retry visit failed", slog.String("url", url), slog.Any("error", err))
	}

	rm.mu.Lock()
	delete(rm.timers, url)
	rm.mu.Unlock()
}

func (rm *retryManager) Stop() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.stopped {
		return
	}

	rm.stopped = true
	for url, timer := range rm.timers {
		timer.Stop()
		delete(rm.timers, url)
	}
}

func (rm *retryManager) TotalRetries() int {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.totalRetries
}

func (rm *retryManager) SetContext(ctx context.Context) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if ctx == nil {
		rm.ctx = context.Background()
		return
	}
	rm.ctx = ctx
}
