package scraper

import (
	"fmt"
	"log"
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
	}
	s.retry = newRetryManager(collector, cfg)
	return s, nil
}

// Scrape starts the crawl and streams items through the pipeline.
func (s *Scraper) Scrape(p *pipeline.Pipeline) (*models.ScraperResult, error) {
	s.configureHandlers(p)

	start := time.Now()
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
	}

	if metrics := p.GetMetrics(); metrics != nil {
		if processed, ok := metrics["processed_books"].(int64); ok {
			result.TotalCount = int(processed)
		}
	}

	return result, nil
}

func (s *Scraper) configureHandlers(p *pipeline.Pipeline) {
	s.handlersOnce.Do(func() {
		s.collector.OnRequest(func(r *colly.Request) {
			current := atomic.AddInt64(&s.requestCount, 1)
			if s.cfg.Verbose && current%50 == 0 {
				log.Printf("requests=%d pages=%d url=%s", current, atomic.LoadInt64(&s.pageCount), r.URL)
			}
		})

		s.collector.OnResponse(func(r *colly.Response) {
			if r.StatusCode >= http.StatusBadRequest {
				log.Printf("non-200 response (%d): %s", r.StatusCode, r.Request.URL)
			}
		})

		s.collector.OnError(func(r *colly.Response, err error) {
			atomic.AddInt64(&s.errorCount, 1)
			category := classifyError(err)

			s.mu.Lock()
			s.errorsByType[category]++
			s.mu.Unlock()

			url := r.Request.URL.String()
			if s.cfg.Verbose {
				log.Printf("request error (%s): %v", url, err)
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
			if err := p.Process([]*models.Book{book}); err != nil && err != pipeline.ErrPipelineClosed {
				log.Printf("pipeline process error: %v", err)
			}
		})

		s.collector.OnHTML("li.next a", func(e *colly.HTMLElement) {
			currentPage := atomic.AddInt64(&s.pageCount, 1)
			if currentPage >= int64(s.cfg.MaxPages) {
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

func classifyError(err error) string {
	if err == nil {
		return "unknown"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "timeout"):
		return "timeout"
	case strings.Contains(msg, "connection"):
		return "connection"
	case strings.Contains(msg, "refused"):
		return "refused"
	case strings.Contains(msg, "EOF"):
		return "eof"
	case strings.Contains(msg, "403"):
		return "forbidden"
	case strings.Contains(msg, "404"):
		return "not_found"
	default:
		return "other"
	}
}

type retryManager struct {
	collector *colly.Collector
	cfg       *config.Config

	mu           sync.Mutex
	attempts     map[string]int
	timers       map[string]*time.Timer
	totalRetries int
	stopped      bool
}

func newRetryManager(collector *colly.Collector, cfg *config.Config) *retryManager {
	return &retryManager{
		collector: collector,
		cfg:       cfg,
		attempts:  make(map[string]int),
		timers:    make(map[string]*time.Timer),
	}
}

func (rm *retryManager) Schedule(url string) bool {
	if rm.cfg.MaxRetries == 0 {
		return false
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.stopped {
		return false
	}

	attempt := rm.attempts[url]
	if attempt >= rm.cfg.MaxRetries {
		return false
	}

	attempt++
	rm.attempts[url] = attempt
	rm.totalRetries++

	delay := rm.backoff(attempt)
	rm.resetTimer(url)
	rm.timers[url] = time.AfterFunc(delay, func() {
		rm.collector.Visit(url)
	})
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

func (rm *retryManager) resetTimer(url string) {
	if timer, ok := rm.timers[url]; ok {
		timer.Stop()
	}
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
