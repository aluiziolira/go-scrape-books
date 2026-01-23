package pipeline

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aluiziolira/go-scrape-books/config"
	"github.com/aluiziolira/go-scrape-books/models"
)

type mockWriter struct {
	mu          sync.Mutex
	batches     [][]*models.Book
	closed      bool
	validateErr error
}

func (mw *mockWriter) Write(books []*models.Book) error {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	copyBatch := make([]*models.Book, len(books))
	copy(copyBatch, books)
	mw.batches = append(mw.batches, copyBatch)
	return nil
}

func (mw *mockWriter) Close() error {
	mw.mu.Lock()
	mw.closed = true
	mw.mu.Unlock()
	return nil
}

func (mw *mockWriter) Validate() error {
	return mw.validateErr
}

func (mw *mockWriter) totalWritten() int {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	total := 0
	for _, batch := range mw.batches {
		total += len(batch)
	}
	return total
}

func (mw *mockWriter) batchSizes() []int {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	sizes := make([]int, 0, len(mw.batches))
	for _, batch := range mw.batches {
		sizes = append(sizes, len(batch))
	}
	return sizes
}

type blockingWriter struct {
	blockCh chan struct{}
}

func (bw *blockingWriter) Write(books []*models.Book) error {
	<-bw.blockCh
	return nil
}

func (bw *blockingWriter) Close() error {
	return nil
}

func (bw *blockingWriter) Validate() error {
	return nil
}

func TestPipelineProcessValidationAndDedup(t *testing.T) {
	cfg := config.DefaultConfig()
	writer := &mockWriter{}
	p := NewPipeline(context.Background(), writer, cfg)
	p.Start(1)

	valid := &models.Book{
		Title:        "Clean Architecture",
		Price:        "10.00",
		RatingText:   "Two",
		Availability: "In stock",
		URL:          "http://example.test/book/1",
		ScrapedAt:    time.Now(),
	}
	invalid := &models.Book{
		Title:        "",
		Price:        "12.00",
		RatingText:   "Three",
		Availability: "In stock",
		URL:          "http://example.test/book/2",
		ScrapedAt:    time.Now(),
	}
	duplicate := &models.Book{
		Title:        "Clean Architecture",
		Price:        "10.00",
		RatingText:   "Two",
		Availability: "In stock",
		URL:          "http://example.test/book/1",
		ScrapedAt:    time.Now(),
	}

	if err := p.Process(valid, invalid, duplicate); err != nil {
		t.Fatalf("process: %v", err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if got := writer.totalWritten(); got != 1 {
		t.Fatalf("written books = %d, want 1", got)
	}

	metrics := p.GetMetrics()
	validation, ok := metrics["validation_errors"].(map[string]int)
	if !ok {
		t.Fatalf("expected validation errors map")
	}
	if validation["invalid_record"] == 0 {
		t.Fatalf("expected invalid_record validation error")
	}
	if validation["duplicate_url"] == 0 {
		t.Fatalf("expected duplicate_url validation error")
	}
}

func TestPipelineBatchFlushThreshold(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.BatchSize = 64
	writer := &mockWriter{}
	p := NewPipeline(context.Background(), writer, cfg)
	p.Start(1)

	for i := 0; i < 65; i++ {
		book := &models.Book{
			Title:        "Book",
			Price:        "12.00",
			RatingText:   "Three",
			Availability: "In stock",
			URL:          "http://example.test/book/" + strconv.Itoa(i),
			ScrapedAt:    time.Now(),
		}
		if err := p.Process(book); err != nil {
			t.Fatalf("process: %v", err)
		}
	}

	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	sizes := writer.batchSizes()
	if len(sizes) != 2 {
		t.Fatalf("batch writes = %d, want 2", len(sizes))
	}
	if sizes[0] != 64 || sizes[1] != 1 {
		t.Fatalf("batch sizes = %v, want [64 1]", sizes)
	}
}

func TestPipelineCloseDrainsPendingItems(t *testing.T) {
	cfg := config.DefaultConfig()
	writer := &mockWriter{}
	p := NewPipeline(context.Background(), writer, cfg)
	p.Start(2)

	for i := 0; i < 100; i++ {
		book := &models.Book{
			Title:        "Book",
			Price:        "12.00",
			RatingText:   "Three",
			Availability: "In stock",
			URL:          "http://example.test/book/" + strconv.Itoa(i+200),
			ScrapedAt:    time.Now(),
		}
		if err := p.Process(book); err != nil {
			t.Fatalf("process: %v", err)
		}
	}

	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if got := writer.totalWritten(); got != 100 {
		t.Fatalf("written books = %d, want 100", got)
	}
}

func TestPipelineCloseTimeout(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.BatchSize = 1

	writer := &blockingWriter{blockCh: make(chan struct{})}
	p := NewPipeline(context.Background(), writer, cfg)
	p.Start(1)

	book := &models.Book{
		Title:        "Blocked Book",
		Price:        "10.00",
		RatingText:   "Two",
		Availability: "In stock",
		URL:          "http://example.test/book/blocked",
		ScrapedAt:    time.Now(),
	}
	if err := p.Process(book); err != nil {
		t.Fatalf("process: %v", err)
	}

	previousTimeout := drainTimeout
	drainTimeout = 25 * time.Millisecond
	t.Cleanup(func() {
		drainTimeout = previousTimeout
		close(writer.blockCh)
	})

	if err := p.Close(); err == nil || !errors.Is(err, ErrPipelineCloseTimeout) {
		t.Fatalf("expected close timeout error, got %v", err)
	}
}
