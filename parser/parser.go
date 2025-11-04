package parser

import (
	"fmt"
	"strings"

	"github.com/aluiziolira/go-scrape-books/models"
)

// ratingWordToValue maps textual rating descriptors to their numeric values.
var ratingWordToValue = map[string]int{
	"Zero":  0,
	"One":   1,
	"Two":   2,
	"Three": 3,
	"Four":  4,
	"Five":  5,
}

// ValidateBook ensures the scraper captured the required fields.
func ValidateBook(b *models.Book) error {
	if b == nil {
		return fmt.Errorf("book is nil")
	}
	if strings.TrimSpace(b.Title) == "" {
		return fmt.Errorf("book missing title")
	}
	if strings.TrimSpace(b.Price) == "" {
		return fmt.Errorf("book missing price for %s", b.Title)
	}
	if strings.TrimSpace(b.RatingText) == "" {
		return fmt.Errorf("book missing rating for %s", b.Title)
	}
	return nil
}

// NormalizePrice removes the currency symbol and surrounding whitespace.
func NormalizePrice(price string) string {
	price = strings.TrimSpace(price)
	price = strings.ReplaceAll(price, "£", "")
	return strings.TrimSpace(price)
}

// NormalizeAvailability trims spacing from the availability text.
func NormalizeAvailability(text string) string {
	return strings.TrimSpace(text)
}

// RatingToNumeric converts the textual rating to a numeric scale.
func RatingToNumeric(rating string) int {
	normalized := strings.TrimSpace(rating)
	if v, ok := ratingWordToValue[normalized]; ok {
		return v
	}
	return 0
}
