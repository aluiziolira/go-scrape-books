package parser

import (
	"testing"
	"time"

	"github.com/aluiziolira/go-scrape-books/models"
)

func TestValidateBook(t *testing.T) {
	tests := []struct {
		name    string
		book    *models.Book
		wantErr bool
	}{
		{
			name: "valid book",
			book: &models.Book{
				Title:        "Test Book",
				Price:        "£10.00",
				RatingText:   "Five",
				Availability: "In stock",
				URL:          "http://example.com",
				ScrapedAt:    time.Now(),
			},
			wantErr: false,
		},
		{
			name: "missing title",
			book: &models.Book{
				Title:        "",
				Price:        "£10.00",
				RatingText:   "Five",
				Availability: "In stock",
			},
			wantErr: true,
		},
		{
			name: "missing price",
			book: &models.Book{
				Title:        "Test Book",
				Price:        "",
				RatingText:   "Five",
				Availability: "In stock",
			},
			wantErr: true,
		},
		{
			name: "missing rating",
			book: &models.Book{
				Title:        "Test Book",
				Price:        "£10.00",
				RatingText:   "",
				Availability: "In stock",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBook(tt.book)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateBook() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNormalizePrice(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "with currency symbol",
			input:    "£51.77",
			expected: "51.77",
		},
		{
			name:     "with whitespace",
			input:    "  £10.50  ",
			expected: "10.50",
		},
		{
			name:     "already clean",
			input:    "25.99",
			expected: "25.99",
		},
		{
			name:     "multiple symbols",
			input:    "£ 99.99 £",
			expected: "99.99",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizePrice(tt.input)
			if result != tt.expected {
				t.Errorf("NormalizePrice(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestRatingToNumeric(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{
			name:     "Zero",
			input:    "Zero",
			expected: 0,
		},
		{
			name:     "One",
			input:    "One",
			expected: 1,
		},
		{
			name:     "Two",
			input:    "Two",
			expected: 2,
		},
		{
			name:     "Three",
			input:    "Three",
			expected: 3,
		},
		{
			name:     "Four",
			input:    "Four",
			expected: 4,
		},
		{
			name:     "Five",
			input:    "Five",
			expected: 5,
		},
		{
			name:     "invalid rating",
			input:    "Invalid",
			expected: 0,
		},
		{
			name:     "empty string",
			input:    "",
			expected: 0,
		},
		{
			name:     "lowercase",
			input:    "three",
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RatingToNumeric(tt.input)
			if result != tt.expected {
				t.Errorf("RatingToNumeric(%q) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestNormalizeAvailability(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "with whitespace",
			input:    "  In stock (22 available)  ",
			expected: "In stock (22 available)",
		},
		{
			name:     "no whitespace",
			input:    "In stock",
			expected: "In stock",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeAvailability(tt.input)
			if result != tt.expected {
				t.Errorf("NormalizeAvailability(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
