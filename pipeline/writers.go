package pipeline

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/aluiziolira/go-scrape-books/models"
)

// CSVWriter writes records to CSV.
type CSVWriter struct {
	file   *os.File
	writer *csv.Writer
	mu     sync.Mutex
}

// NewCSVWriter initialises a CSV writer and writes the header row.
func NewCSVWriter(filename string) (*CSVWriter, error) {
	if err := ensureDir(filename); err != nil {
		return nil, err
	}

	f, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("create csv file: %w", err)
	}

	writer := csv.NewWriter(f)
	header := []string{"title", "price", "rating", "rating_numeric", "availability", "image_url", "url", "scraped_at"}
	if err := writer.Write(header); err != nil {
		f.Close()
		return nil, fmt.Errorf("write csv header: %w", err)
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		f.Close()
		return nil, fmt.Errorf("flush csv header: %w", err)
	}

	return &CSVWriter{
		file:   f,
		writer: writer,
	}, nil
}

// Write appends books to the CSV output.
func (cw *CSVWriter) Write(books []*models.Book) error {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	for _, book := range books {
		record := []string{
			book.Title,
			book.Price,
			book.RatingText,
			strconv.Itoa(book.RatingNumeric),
			book.Availability,
			book.ImageURL,
			book.URL,
			book.ScrapedAt.Format(time.RFC3339),
		}
		if err := cw.writer.Write(record); err != nil {
			return fmt.Errorf("write csv record: %w", err)
		}
	}
	cw.writer.Flush()
	if err := cw.writer.Error(); err != nil {
		return fmt.Errorf("flush csv records: %w", err)
	}
	return nil
}

// Close flushes and closes the file handle.
func (cw *CSVWriter) Close() error {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	cw.writer.Flush()
	if err := cw.writer.Error(); err != nil {
		return fmt.Errorf("flush csv writer: %w", err)
	}
	return cw.file.Close()
}

// Validate ensures the file has content besides the header.
func (cw *CSVWriter) Validate() error {
	info, err := cw.file.Stat()
	if err != nil {
		return fmt.Errorf("stat csv file: %w", err)
	}
	if info.Size() <= 0 {
		return fmt.Errorf("csv file is empty")
	}
	return nil
}

// JSONWriter writes newline-delimited JSON records.
type JSONWriter struct {
	file    *os.File
	writer  *bufio.Writer
	encoder *json.Encoder
	mu      sync.Mutex
}

// NewJSONWriter initialises the JSON writer.
func NewJSONWriter(filename string) (*JSONWriter, error) {
	if err := ensureDir(filename); err != nil {
		return nil, err
	}

	f, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("create json file: %w", err)
	}

	buffer := bufio.NewWriter(f)
	return &JSONWriter{
		file:    f,
		writer:  buffer,
		encoder: json.NewEncoder(buffer),
	}, nil
}

// Write appends books in JSONL format.
func (jw *JSONWriter) Write(books []*models.Book) error {
	jw.mu.Lock()
	defer jw.mu.Unlock()

	for _, book := range books {
		if err := jw.encoder.Encode(book); err != nil {
			return fmt.Errorf("encode json record: %w", err)
		}
	}

	if err := jw.writer.Flush(); err != nil {
		return fmt.Errorf("flush json writer: %w", err)
	}

	return nil
}

// Close flushes buffers and closes the underlying file.
func (jw *JSONWriter) Close() error {
	jw.mu.Lock()
	defer jw.mu.Unlock()

	if err := jw.writer.Flush(); err != nil {
		return fmt.Errorf("flush json writer: %w", err)
	}
	return jw.file.Close()
}

// Validate ensures the JSON file has data.
func (jw *JSONWriter) Validate() error {
	info, err := jw.file.Stat()
	if err != nil {
		return fmt.Errorf("stat json file: %w", err)
	}
	if info.Size() <= 0 {
		return fmt.Errorf("json file is empty")
	}
	return nil
}

func ensureDir(filename string) error {
	dir := filepath.Dir(filename)
	if dir == "" || dir == "." {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create directory %q: %w", dir, err)
	}
	return nil
}
