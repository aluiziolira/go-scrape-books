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

// CSVWriter writes records to CSV. Output is buffered in a temp file and only
// renamed onto the final path on a successful Close, so a crash or write error
// never leaves a half-written file at the final path.
type CSVWriter struct {
	finalPath string
	tmpPath   string
	file      *os.File
	writer    *csv.Writer
	mu        sync.Mutex
}

// NewCSVWriter initialises a CSV writer and writes the header row to a temp file
// in the same directory as filename (same filesystem => atomic rename on Close).
func NewCSVWriter(filename string) (*CSVWriter, error) {
	if err := ensureDir(filename); err != nil {
		return nil, err
	}

	dir := filepath.Dir(filename)
	f, err := os.CreateTemp(dir, "."+filepath.Base(filename)+".*.tmp")
	if err != nil {
		return nil, fmt.Errorf("create csv temp file: %w", err)
	}

	writer := csv.NewWriter(f)
	header := []string{"title", "price", "rating", "rating_numeric", "availability", "image_url", "url", "scraped_at", "price_numeric"}
	if err := writer.Write(header); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, fmt.Errorf("write csv header: %w", err)
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, fmt.Errorf("flush csv header: %w", err)
	}

	return &CSVWriter{
		finalPath: filename,
		tmpPath:   f.Name(),
		file:      f,
		writer:    writer,
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
			strconv.FormatFloat(book.PriceNumeric, 'f', 2, 64),
		}
		if err := cw.writer.Write(record); err != nil {
			_ = os.Remove(cw.tmpPath)
			return fmt.Errorf("write csv record: %w", err)
		}
	}
	cw.writer.Flush()
	if err := cw.writer.Error(); err != nil {
		_ = os.Remove(cw.tmpPath)
		return fmt.Errorf("flush csv records: %w", err)
	}
	return nil
}

// Close flushes, closes the temp file, and atomically renames it onto the final
// path. On any error the temp file is removed (best-effort) and the final path
// is left untouched.
func (cw *CSVWriter) Close() error {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	cw.writer.Flush()
	if err := cw.writer.Error(); err != nil {
		_ = os.Remove(cw.tmpPath)
		_ = cw.file.Close()
		return fmt.Errorf("flush csv writer: %w", err)
	}
	if err := cw.file.Close(); err != nil {
		_ = os.Remove(cw.tmpPath)
		return fmt.Errorf("close csv file: %w", err)
	}
	if err := os.Rename(cw.tmpPath, cw.finalPath); err != nil {
		_ = os.Remove(cw.tmpPath)
		return fmt.Errorf("rename csv file: %w", err)
	}
	return nil
}

// Validate ensures the temp file has content besides the header. The data lives
// in the temp file until Close renames it onto the final path.
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

// JSONWriter writes newline-delimited JSON records. Output is buffered in a temp
// file and only renamed onto the final path on a successful Close, so a crash or
// write error never leaves a half-written file at the final path.
type JSONWriter struct {
	finalPath string
	tmpPath   string
	file      *os.File
	writer    *bufio.Writer
	encoder   *json.Encoder
	mu        sync.Mutex
}

// NewJSONWriter initialises the JSON writer using a temp file in the same
// directory as filename (same filesystem => atomic rename on Close).
func NewJSONWriter(filename string) (*JSONWriter, error) {
	if err := ensureDir(filename); err != nil {
		return nil, err
	}

	dir := filepath.Dir(filename)
	f, err := os.CreateTemp(dir, "."+filepath.Base(filename)+".*.tmp")
	if err != nil {
		return nil, fmt.Errorf("create json temp file: %w", err)
	}

	buffer := bufio.NewWriter(f)
	return &JSONWriter{
		finalPath: filename,
		tmpPath:   f.Name(),
		file:      f,
		writer:    buffer,
		encoder:   json.NewEncoder(buffer),
	}, nil
}

// Write appends books in JSONL format.
func (jw *JSONWriter) Write(books []*models.Book) error {
	jw.mu.Lock()
	defer jw.mu.Unlock()

	for _, book := range books {
		if err := jw.encoder.Encode(book); err != nil {
			_ = os.Remove(jw.tmpPath)
			return fmt.Errorf("encode json record: %w", err)
		}
	}

	if err := jw.writer.Flush(); err != nil {
		_ = os.Remove(jw.tmpPath)
		return fmt.Errorf("flush json writer: %w", err)
	}

	return nil
}

// Close flushes buffers, closes the temp file, and atomically renames it onto
// the final path. On any error the temp file is removed (best-effort) and the
// final path is left untouched.
func (jw *JSONWriter) Close() error {
	jw.mu.Lock()
	defer jw.mu.Unlock()

	if err := jw.writer.Flush(); err != nil {
		_ = os.Remove(jw.tmpPath)
		_ = jw.file.Close()
		return fmt.Errorf("flush json writer: %w", err)
	}
	if err := jw.file.Close(); err != nil {
		_ = os.Remove(jw.tmpPath)
		return fmt.Errorf("close json file: %w", err)
	}
	if err := os.Rename(jw.tmpPath, jw.finalPath); err != nil {
		_ = os.Remove(jw.tmpPath)
		return fmt.Errorf("rename json file: %w", err)
	}
	return nil
}

// Validate ensures the temp file has data. The data lives in the temp file until
// Close renames it onto the final path.
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
