// Package pipeline provides dual output writer for CSV and JSON formats.
package pipeline

import (
	"fmt"
	"sync"

	"github.com/aluiziolira/go-scrape-books/models"
)

// DualWriter outputs to both CSV and JSON formats simultaneously
type DualWriter struct {
	csvWriter  *CSVWriter
	jsonWriter *JSONWriter
	mu         sync.Mutex
}

// NewDualWriter creates a new dual writer for both CSV and JSON output
func NewDualWriter(csvFilename, jsonFilename string) (*DualWriter, error) {
	// Create CSV writer
	csvWriter, err := NewCSVWriter(csvFilename)
	if err != nil {
		return nil, fmt.Errorf("failed to create CSV writer: %w", err)
	}

	// Create JSON writer
	jsonWriter, err := NewJSONWriter(jsonFilename)
	if err != nil {
		csvWriter.Close()
		return nil, fmt.Errorf("failed to create JSON writer: %w", err)
	}

	return &DualWriter{
		csvWriter:  csvWriter,
		jsonWriter: jsonWriter,
	}, nil
}

// Write writes books to both CSV and JSON formats
func (dw *DualWriter) Write(books []*models.Book) error {
	dw.mu.Lock()
	defer dw.mu.Unlock()

	// Write to CSV
	if err := dw.csvWriter.Write(books); err != nil {
		return fmt.Errorf("CSV write failed: %w", err)
	}

	// Write to JSON (JSONL format)
	if err := dw.jsonWriter.Write(books); err != nil {
		return fmt.Errorf("JSON write failed: %w", err)
	}

	return nil
}

// Close closes both writers
func (dw *DualWriter) Close() error {
	dw.mu.Lock()
	defer dw.mu.Unlock()

	var errs []error

	if err := dw.csvWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("CSV close failed: %w", err))
	}

	if err := dw.jsonWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("JSON close failed: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("multiple errors: %v", errs)
	}

	return nil
}

// Validate validates both output files
func (dw *DualWriter) Validate() error {
	var errs []error

	if err := dw.csvWriter.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("CSV validation failed: %w", err))
	}

	if err := dw.jsonWriter.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("JSON validation failed: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("validation errors: %v", errs)
	}

	return nil
}
