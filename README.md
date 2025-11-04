# Go Books Scraper

A compact Go example that demonstrates how to scrape [books.toscrape.com](https://books.toscrape.com) with bounded concurrency and a streaming pipeline. The project is tuned for clarity: the scraper collects listing pages with `colly`, validates and normalises each book record, then writes to CSV, JSONL, or both in one pass.

## Implementation Notes

- **Collector** – `colly` runs asynchronously with a configurable worker count, polite delays, retry backoff, and optional robots.txt compliance.
- **Pipeline** – books flow through a buffered channel where we validate required fields, discard duplicates, normalise price/availability text, and batch writes to disk.
- **Output** – supports CSV (`output/books.csv`), JSONL (`output/books.json`), or a dual-writer that emits both files simultaneously.
- **No proxy layer** – intentionally omitted; this is a demo scraper, not an evasion toolkit.

All configuration is exposed through CLI flags; see `go run ./cmd/scraper -h` for details.

## Running the Scraper

```bash
# Direct execution (writes to ./output/)
go run ./cmd/scraper \
  -pages 50 \
  -parallel 16 \
  -format dual \
  -output output/books.csv

# Using Docker
make run

# Override the defaults
make run ARGS="-pages 20 -parallel 8"
```

With the defaults the scraper captures 50 catalog pages (~1000 books) in roughly 9–10 seconds on a modern laptop (≈100 items/second). Increase `-parallel` for higher throughput, but be mindful of the demo site's rate limits.

## Sample Log Excerpt

```
2025/11/04 05:04:56 Scraping https://books.toscrape.com (pages=50, workers=16)

--------------------------------------------------
Scrape complete
  Total items:   1000
  Errors:        0
  Retries:       0
  Failed URLs:   0
  Duration:      9.230154486s
  Items/sec:     108.34
  Output file:   output/books.csv
--------------------------------------------------
```

## Testing

```bash
make test
```

The test suite covers the retry/backoff utility and the parser helpers used by the pipeline.
