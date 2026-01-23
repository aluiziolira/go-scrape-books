package pipeline

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aluiziolira/go-scrape-books/models"
)

func TestCSVWriterWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "books.csv")

	writer, err := NewCSVWriter(path)
	if err != nil {
		t.Fatalf("create csv writer: %v", err)
	}

	book := &models.Book{
		Title:         "Test Book",
		Price:         "10.00",
		RatingText:    "Two",
		RatingNumeric: 2,
		Availability:  "In stock",
		ImageURL:      "http://example.test/img.png",
		URL:           "http://example.test/book/1",
		ScrapedAt:     time.Date(2025, 11, 4, 13, 9, 13, 0, time.UTC),
	}

	if err := writer.Write([]*models.Book{book}); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close csv: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open csv: %v", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("records=%d, want 2", len(records))
	}
	if records[0][0] != "title" || records[0][1] != "price" {
		t.Fatalf("unexpected header: %v", records[0])
	}
}

func TestJSONWriterWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "books.jsonl")

	writer, err := NewJSONWriter(path)
	if err != nil {
		t.Fatalf("create json writer: %v", err)
	}

	book := &models.Book{
		Title:         "Test Book",
		Price:         "10.00",
		RatingText:    "Two",
		RatingNumeric: 2,
		Availability:  "In stock",
		ImageURL:      "http://example.test/img.png",
		URL:           "http://example.test/book/1",
		ScrapedAt:     time.Date(2025, 11, 4, 13, 9, 13, 0, time.UTC),
	}

	if err := writer.Write([]*models.Book{book}); err != nil {
		t.Fatalf("write json: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close json: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open json: %v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	count := 0
	for scanner.Scan() {
		var decoded models.Book
		if err := json.Unmarshal(scanner.Bytes(), &decoded); err != nil {
			t.Fatalf("invalid json line: %v", err)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan json: %v", err)
	}
	if count != 1 {
		t.Fatalf("json lines=%d, want 1", count)
	}
}

func TestDualWriterWrite(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "books.csv")
	jsonPath := filepath.Join(dir, "books.jsonl")

	writer, err := NewDualWriter(csvPath, jsonPath)
	if err != nil {
		t.Fatalf("create dual writer: %v", err)
	}

	book := &models.Book{
		Title:         "Test Book",
		Price:         "10.00",
		RatingText:    "Two",
		RatingNumeric: 2,
		Availability:  "In stock",
		ImageURL:      "http://example.test/img.png",
		URL:           "http://example.test/book/1",
		ScrapedAt:     time.Date(2025, 11, 4, 13, 9, 13, 0, time.UTC),
	}

	if err := writer.Write([]*models.Book{book}); err != nil {
		t.Fatalf("write dual: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close dual: %v", err)
	}

	if info, err := os.Stat(csvPath); err != nil || info.Size() == 0 {
		t.Fatalf("csv file missing or empty")
	}
	if info, err := os.Stat(jsonPath); err != nil || info.Size() == 0 {
		t.Fatalf("json file missing or empty")
	}
}
