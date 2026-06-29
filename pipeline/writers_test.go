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
	defer func() { _ = f.Close() }()

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
	defer func() { _ = f.Close() }()

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

// TestCSVWriterAtomicOnFailure proves the write-temp-then-rename contract: when
// Close cannot complete the rename, the final path keeps its prior good content
// and is never left half-written. We simulate the failure by removing the temp
// file before Close so os.Rename fails with ENOENT (deterministic on Linux
// regardless of the running user; chmod-based tricks are bypassed by root).
func TestCSVWriterAtomicOnFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "books.csv")

	original := "title,price\nOld Book,1.00\n"
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatalf("seed original csv: %v", err)
	}

	writer, err := NewCSVWriter(path)
	if err != nil {
		t.Fatalf("create csv writer: %v", err)
	}

	book := &models.Book{
		Title:         "New Book",
		Price:         "42.00",
		RatingText:    "Five",
		RatingNumeric: 5,
		Availability:  "In stock",
		ImageURL:      "http://example.test/img.png",
		URL:           "http://example.test/book/2",
		ScrapedAt:     time.Date(2025, 11, 4, 13, 9, 13, 0, time.UTC),
		PriceNumeric:  42.00,
	}
	if err := writer.Write([]*models.Book{book}); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	// Vanish the temp file so the atomic rename in Close cannot succeed.
	if err := os.Remove(writer.tmpPath); err != nil {
		t.Fatalf("remove temp file: %v", err)
	}

	if err := writer.Close(); err == nil {
		t.Fatalf("expected Close to fail when rename source vanished")
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read final csv: %v", err)
	}
	if string(got) != original {
		t.Fatalf("final path mutated on failure; want %q, got %q", original, string(got))
	}
}

// TestCSVWriterAtomicRenameOnSuccess confirms that a successful Close renames
// the temp file onto the final path with correct content and leaves no .tmp
// debris behind.
func TestCSVWriterAtomicRenameOnSuccess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "books.csv")

	writer, err := NewCSVWriter(path)
	if err != nil {
		t.Fatalf("create csv writer: %v", err)
	}

	book := &models.Book{
		Title:         "Success Book",
		Price:         "7.50",
		RatingText:    "Three",
		RatingNumeric: 3,
		Availability:  "In stock",
		ImageURL:      "http://example.test/img.png",
		URL:           "http://example.test/book/3",
		ScrapedAt:     time.Date(2025, 11, 4, 13, 9, 13, 0, time.UTC),
		PriceNumeric:  7.50,
	}
	if err := writer.Write([]*models.Book{book}); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close csv: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open final csv: %v", err)
	}
	defer func() { _ = f.Close() }()
	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	if len(records) != 2 || records[1][0] != "Success Book" {
		t.Fatalf("unexpected csv content: %v", records)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "*.tmp"))
	if err != nil {
		t.Fatalf("glob tmp: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("temp file debris left behind: %v", matches)
	}
}

// TestJSONWriterAtomicOnFailure is the JSON equivalent of the CSV atomicity
// test: when Close cannot rename, the final path is preserved untouched.
func TestJSONWriterAtomicOnFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "books.jsonl")

	original := []byte(`{"title":"Old Book","price":"1.00"}
`)
	if err := os.WriteFile(path, original, 0o644); err != nil {
		t.Fatalf("seed original json: %v", err)
	}

	writer, err := NewJSONWriter(path)
	if err != nil {
		t.Fatalf("create json writer: %v", err)
	}

	book := &models.Book{
		Title:         "New Book",
		Price:         "42.00",
		RatingText:    "Five",
		RatingNumeric: 5,
		Availability:  "In stock",
		ImageURL:      "http://example.test/img.png",
		URL:           "http://example.test/book/2",
		ScrapedAt:     time.Date(2025, 11, 4, 13, 9, 13, 0, time.UTC),
		PriceNumeric:  42.00,
	}
	if err := writer.Write([]*models.Book{book}); err != nil {
		t.Fatalf("write json: %v", err)
	}

	// Vanish the temp file so the atomic rename in Close cannot succeed.
	if err := os.Remove(writer.tmpPath); err != nil {
		t.Fatalf("remove temp file: %v", err)
	}

	if err := writer.Close(); err == nil {
		t.Fatalf("expected Close to fail when rename source vanished")
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read final json: %v", err)
	}
	if string(got) != string(original) {
		t.Fatalf("final path mutated on failure; want %q, got %q", string(original), string(got))
	}
}
