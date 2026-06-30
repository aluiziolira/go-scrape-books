package scraper

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aluiziolira/go-scrape-books/config"
	"github.com/aluiziolira/go-scrape-books/models"
	"github.com/gocolly/colly/v2"
)

// Sink receives books extracted from the page as they are scraped. It is
// implemented by pipeline.Pipeline; the scraper depends only on this narrow
// interface so it never needs to import the pipeline package.
type Sink interface {
	Process(books ...*models.Book) error
}

// Scraper wraps the colly collector and retry logic for the demo target.
type Scraper struct {
	cfg       *config.Config
	collector *colly.Collector
	retry     *retryManager
	Metrics   *Metrics

	requestCount int64
	pageCount    int64
	errorCount   int64

	mu           sync.Mutex
	failedURLs   []string
	errorsByType map[string]int

	sinkErrLogged atomic.Bool
	handlersOnce  sync.Once
}

// NewScraper builds a scraper instance configured from cfg.
func NewScraper(cfg *config.Config) (*Scraper, error) {
	parsed, err := url.Parse(cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse base url: %w", err)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("base url must include a host")
	}

	collector := colly.NewCollector(
		colly.Async(true),
		colly.AllowedDomains(parsed.Hostname()),
		colly.UserAgent(cfg.UserAgent),
	)

	collector.SetRequestTimeout(cfg.Timeout)
	collector.IgnoreRobotsTxt = !cfg.RespectRobotsTxt
	collector.WithTransport(&http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   cfg.Timeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	})

	if err := collector.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: cfg.Parallelism,
		Delay:       cfg.Delay,
		RandomDelay: cfg.RandomDelay,
	}); err != nil {
		return nil, fmt.Errorf("configure rate limits: %w", err)
	}

	s := &Scraper{
		cfg:          cfg,
		collector:    collector,
		errorsByType: make(map[string]int),
		Metrics:      NewMetrics(),
	}
	s.retry = newRetryManager(collector, cfg, s.Metrics)
	return s, nil
}

// Run starts the crawl and streams extracted books into sink.
func (s *Scraper) Run(ctx context.Context, sink Sink) (*models.ScraperResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.retry.SetContext(ctx)
	s.configureHandlers(ctx, sink)

	start := time.Now()
	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			s.collector.Wait()
			s.retry.Stop()
		case <-done:
		}
	}()

	if err := s.collector.Visit(s.cfg.BaseURL); err != nil {
		return nil, fmt.Errorf("initial visit: %w", err)
	}

	s.collector.Wait()
	s.retry.Stop()

	result := &models.ScraperResult{
		StartTime:    start,
		EndTime:      time.Now(),
		ErrorCount:   int(atomic.LoadInt64(&s.errorCount)),
		FailedURLs:   s.snapshotFailedURLs(),
		ErrorsByType: s.snapshotErrors(),
		RetryCount:   s.retry.TotalRetries(),
		RequestCount: int(atomic.LoadInt64(&s.requestCount)),
		PageCount:    int(atomic.LoadInt64(&s.pageCount)),
	}

	return result, nil
}

func (s *Scraper) configureHandlers(ctx context.Context, sink Sink) { //nolint:gocyclo // registers one branch per colly lifecycle callback
	s.handlersOnce.Do(func() {
		s.collector.OnRequest(func(r *colly.Request) {
			r.Ctx.Put("start", time.Now())
			current := atomic.AddInt64(&s.requestCount, 1)
			if s.Metrics != nil {
				s.Metrics.IncRequest("started")
			}
			if current%50 == 0 {
				slog.Debug("scraper request progress",
					slog.Int64("requests", current),
					slog.Int64("pages", atomic.LoadInt64(&s.pageCount)),
					slog.String("url", r.URL.String()),
				)
			}
		})

		s.collector.OnResponse(func(r *colly.Response) {
			if r.StatusCode >= http.StatusBadRequest {
				slog.Error("non-200 response",
					slog.Int("status", r.StatusCode),
					slog.String("url", r.Request.URL.String()),
				)
			}
			if s.Metrics != nil {
				if r.StatusCode < http.StatusBadRequest {
					s.Metrics.IncRequest("success")
				} else {
					s.Metrics.IncRequest("error")
				}
				if start, ok := r.Request.Ctx.GetAny("start").(time.Time); ok {
					s.Metrics.ObserveDuration(time.Since(start))
				}
			}
		})

		s.collector.OnError(func(r *colly.Response, err error) {
			atomic.AddInt64(&s.errorCount, 1)
			statusCode := 0
			if r != nil {
				statusCode = r.StatusCode
			}
			classified := classifyError(err, statusCode)
			category := errorTypeLabel(classified)

			s.mu.Lock()
			s.errorsByType[category]++
			s.mu.Unlock()

			url := ""
			if r != nil && r.Request != nil && r.Request.URL != nil {
				url = r.Request.URL.String()
			}
			slog.Error("request error",
				slog.String("url", url),
				slog.String("category", category),
				slog.Any("error", err),
			)
			if s.Metrics != nil {
				if r == nil || r.StatusCode == 0 {
					s.Metrics.IncRequest("error")
				}
				s.Metrics.IncError(category)
			}

			if !s.retry.Schedule(url) {
				s.mu.Lock()
				s.failedURLs = append(s.failedURLs, url)
				s.mu.Unlock()
			}
		})

		s.collector.OnHTML("article.product_pod", func(e *colly.HTMLElement) {
			book := extractBook(e)
			if book == nil {
				return
			}
			if s.Metrics != nil {
				s.Metrics.IncItems()
			}
			if err := sink.Process(book); err != nil {
				// Sink is an opaque interface, so the scraper can't tell a benign
				// "shutting down" rejection from a genuine failure (e.g. a write
				// error). Surface the first occurrence loudly and the rest at
				// debug, instead of guessing from ctx state and risking either a
				// real failure going unlogged or a burst of expected shutdown
				// rejections flooding the logs.
				if s.sinkErrLogged.CompareAndSwap(false, true) {
					slog.Error("sink rejected book; further occurrences logged at debug", slog.Any("error", err))
				} else {
					slog.Debug("sink process error", slog.Any("error", err))
				}
			}
		})

		s.collector.OnHTML("li.next a", func(e *colly.HTMLElement) {
			currentPage := atomic.AddInt64(&s.pageCount, 1)
			if currentPage >= int64(s.cfg.MaxPages) {
				return
			}
			if ctx.Err() != nil {
				return
			}
			link := e.Attr("href")
			abs := e.Request.AbsoluteURL(link)
			if err := s.collector.Visit(abs); err != nil {
				slog.Debug("visit failed", slog.String("url", abs), slog.Any("error", err))
			}
		})
	})
}

func (s *Scraper) snapshotFailedURLs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.failedURLs))
	copy(out, s.failedURLs)
	return out
}

func (s *Scraper) snapshotErrors() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(s.errorsByType))
	for k, v := range s.errorsByType {
		out[k] = v
	}
	return out
}
