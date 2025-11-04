package pipeline

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aluiziolira/go-scrape-books/models"
	"github.com/aluiziolira/go-scrape-books/parser"
)

var (
	// ErrPipelineClosed is returned when Process is called after shutdown.
	ErrPipelineClosed = errors.New("pipeline: closed")
)

// OutputWriter defines the interface for data output.
type OutputWriter interface {
	Write(books []*models.Book) error
	Close() error
	Validate() error
}

// Pipeline coordinates validation, de-duplication, and output writing.
type Pipeline struct {
	writer    OutputWriter
	bookCh    chan *models.Book
	batchSize int

	wg sync.WaitGroup

	seen   map[string]struct{}
	seenMu sync.Mutex

	metrics metrics

	mu     sync.Mutex // guards closed/err
	closed bool
	err    error

	closeOnce    sync.Once
	shutdown     chan struct{}
	shutdownOnce sync.Once
}

// NewPipeline builds a pipeline with a modest in-memory buffer.
func NewPipeline(writer OutputWriter) *Pipeline {
	return &Pipeline{
		writer:    writer,
		bookCh:    make(chan *models.Book, 512),
		batchSize: 64,
		seen:      make(map[string]struct{}),
		metrics:   newMetrics(),
		shutdown:  make(chan struct{}),
	}
}

// Start launches worker goroutines.
func (p *Pipeline) Start(workers int) {
	if workers <= 0 {
		workers = 1
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

// Process enqueues books for downstream processing.
func (p *Pipeline) Process(books []*models.Book) error {
	if len(books) == 0 {
		return nil
	}

	closed, err := p.state()
	if err != nil {
		return err
	}
	if closed {
		return ErrPipelineClosed
	}

	for _, book := range books {
		if book == nil {
			continue
		}
		if err := p.enqueue(book); err != nil {
			return err
		}
	}
	return nil
}

// Close waits for workers to finish and prevents more submissions.
func (p *Pipeline) Close() error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
	}
	p.mu.Unlock()

	p.signalShutdown()
	p.closeOnce.Do(func() {
		close(p.bookCh)
	})

	p.wg.Wait()
	return p.Err()
}

// Err returns the first error encountered during processing.
func (p *Pipeline) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

// GetMetrics returns a snapshot of the internal counters.
func (p *Pipeline) GetMetrics() map[string]interface{} {
	return p.metrics.snapshot()
}

// StartMetricsReporting emits periodic progress logs.
func (p *Pipeline) StartMetricsReporting(interval time.Duration) {
	if interval <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				metrics := p.GetMetrics()
				processed := metrics["processed_books"].(int64)
				validation := metrics["validation_errors"].(map[string]int)
				log.Printf("pipeline: processed=%d validation_errors=%d", processed, len(validation))
			case <-p.shutdown:
				return
			}
		}
	}()
}

func (p *Pipeline) worker() {
	defer p.wg.Done()

	batch := make([]*models.Book, 0, p.batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := p.writer.Write(batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	for book := range p.bookCh {
		prepared := p.prepare(book)
		if prepared == nil {
			continue
		}
		batch = append(batch, prepared)
		if len(batch) >= p.batchSize {
			if err := flush(); err != nil {
				p.setErr(fmt.Errorf("write batch: %w", err))
				return
			}
		}
	}

	if err := flush(); err != nil {
		p.setErr(fmt.Errorf("write batch: %w", err))
	}
}

func (p *Pipeline) prepare(book *models.Book) *models.Book {
	if err := parser.ValidateBook(book); err != nil {
		p.metrics.addValidation("invalid_record")
		return nil
	}

	p.seenMu.Lock()
	if _, ok := p.seen[book.URL]; ok {
		p.seenMu.Unlock()
		p.metrics.addValidation("duplicate_url")
		return nil
	}
	p.seen[book.URL] = struct{}{}
	p.seenMu.Unlock()

	book.Price = parser.NormalizePrice(book.Price)
	book.Availability = parser.NormalizeAvailability(book.Availability)
	book.RatingNumeric = parser.RatingToNumeric(book.RatingText)

	p.metrics.incrementProcessed()
	return book
}

func (p *Pipeline) enqueue(book *models.Book) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = ErrPipelineClosed
		}
	}()

	select {
	case <-p.shutdown:
		return ErrPipelineClosed
	case p.bookCh <- book:
		return nil
	}
}

func (p *Pipeline) setErr(err error) {
	if err == nil {
		return
	}

	p.mu.Lock()
	if p.err != nil {
		p.mu.Unlock()
		return
	}
	p.err = err
	p.closed = true
	p.mu.Unlock()

	p.signalShutdown()
	p.closeOnce.Do(func() {
		close(p.bookCh)
	})
}

func (p *Pipeline) state() (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed, p.err
}

func (p *Pipeline) signalShutdown() {
	p.shutdownOnce.Do(func() {
		close(p.shutdown)
	})
}

type metrics struct {
	mu         sync.Mutex
	processed  int64
	validation map[string]int
}

func newMetrics() metrics {
	return metrics{
		validation: make(map[string]int),
	}
}

func (m *metrics) incrementProcessed() {
	m.mu.Lock()
	m.processed++
	m.mu.Unlock()
}

func (m *metrics) addValidation(kind string) {
	m.mu.Lock()
	m.validation[kind]++
	m.mu.Unlock()
}

func (m *metrics) snapshot() map[string]interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()

	copyValidation := make(map[string]int, len(m.validation))
	for k, v := range m.validation {
		copyValidation[k] = v
	}

	return map[string]interface{}{
		"processed_books":   m.processed,
		"validation_errors": copyValidation,
	}
}
