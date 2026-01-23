// Package models defines data structures for the scraper.
package models

import "time"

// Book represents a book item from the scraper.
type Book struct {
	Title         string    `csv:"title" json:"title"`
	Price         string    `csv:"price" json:"price"`
	RatingText    string    `csv:"rating" json:"rating"`
	RatingNumeric int       `csv:"rating_numeric" json:"rating_numeric"`
	Availability  string    `csv:"availability" json:"availability"`
	ImageURL      string    `csv:"image_url" json:"image_url"`
	URL           string    `csv:"url" json:"url"`
	ScrapedAt     time.Time `csv:"scraped_at" json:"scraped_at"`
}

// ScraperResult holds the overall result of a scraping operation
type ScraperResult struct {
	Books        []*Book
	StartTime    time.Time
	EndTime      time.Time
	TotalCount   int
	ErrorCount   int
	FailedURLs   []string
	ErrorsByType map[string]int
	RetryCount   int
	RequestCount int
	PageCount    int
}
