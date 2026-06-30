package scraper

import (
	"strings"
	"time"

	"github.com/aluiziolira/go-scrape-books/models"
	"github.com/gocolly/colly/v2"
)

// extractBook parses a single product listing element into a Book. It
// returns nil when the element is missing the fields required to identify
// the book (title, link).
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
