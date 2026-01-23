package scraper

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aluiziolira/go-scrape-books/config"
	"github.com/aluiziolira/go-scrape-books/models"
	"github.com/aluiziolira/go-scrape-books/pipeline"
	"github.com/gocolly/colly/v2"
)

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

	handlersOnce sync.Once
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
		colly.AllowedDomains(parsed.Host),
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

// Run starts the crawl and streams items through the pipeline.
func (s *Scraper) Run(ctx context.Context, p *pipeline.Pipeline) (*models.ScraperResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.retry.SetContext(ctx)
	s.configureHandlers(ctx, p)

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

	if metrics := p.GetMetrics(); metrics != nil {
		if processed, ok := metrics["processed_books"].(int64); ok {
			result.TotalCount = int(processed)
		}
	}

	return result, nil
}

// Scrape is a compatibility wrapper for older callers.
func (s *Scraper) Scrape(p *pipeline.Pipeline) (*models.ScraperResult, error) {
	return s.Run(context.Background(), p)
}

func (s *Scraper) configureHandlers(ctx context.Context, p *pipeline.Pipeline) {
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
			if err := p.Process(book); err != nil && err != pipeline.ErrPipelineClosed {
				slog.Error("pipeline process error", slog.Any("error", err))
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
			s.collector.Visit(abs)
		})
	})
}

func extractBook(e *colly.HTMLElement) *models.Book {
	title := strings.TrimSpace(e.ChildAttr("h3 a", "title"))
	if title == "" {
		return nil
	}

	href := e.ChildAttr("h3 a", "href")
	if href == "" {
		return nil
	}

	bookURL := e.Request.AbsoluteURL(href)
	priceText := strings.TrimSpace(e.ChildText("p.price_color"))

	ratingClass := e.ChildAttr("p.star-rating", "class")
	ratingText := ""
	if ratingClass != "" {
		parts := strings.Fields(ratingClass)
		if len(parts) > 1 {
			ratingText = parts[1]
		}
	}

	availability := strings.TrimSpace(e.ChildText("p.instock.availability"))
	if availability == "" {
		availability = strings.TrimSpace(e.ChildText("p.availability"))
	}

	imageURL := e.Request.AbsoluteURL(e.ChildAttr("img", "src"))

	return &models.Book{
		Title:        title,
		Price:        priceText,
		RatingText:   ratingText,
		Availability: availability,
		ImageURL:     imageURL,
		URL:          bookURL,
		ScrapedAt:    time.Now(),
	}
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

func classifyError(err error, statusCode int) error {
	if err == nil && statusCode == 0 {
		return nil
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return ErrTimeout{Err: err}
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ErrTimeout{Err: err}
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return ErrConnection{Err: err}
	}

	if statusCode != 0 {
		wrapped := err
		if wrapped == nil {
			wrapped = fmt.Errorf("http status %d", statusCode)
		}
		switch statusCode {
		case http.StatusForbidden:
			return ErrForbidden{Err: wrapped}
		case http.StatusNotFound:
			return ErrNotFound{Err: wrapped}
		case http.StatusTooManyRequests:
			return ErrRateLimited{Err: wrapped}
		}
	}

	if err == nil {
		return nil
	}
	return err
}

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

func (rm *retryManager) Schedule(url string) bool {
	if rm.cfg.MaxRetries == 0 {
		return false
	}

	if rm.ctx != nil {
		select {
		case <-rm.ctx.Done():
			return false
		default:
		}
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
