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

With the defaults the scraper captures 50 catalog pages (~1000 books) quickly; adjust `-parallel` for higher throughput, but be mindful of the demo site's rate limits.

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

## Sample Output

```text
title,price,rating,rating_numeric,availability,image_url,url,scraped_at
Soumission,50.10,One,1,In stock,https://books.toscrape.com/media/cache/3e/ef/3eef99c9d9adef34639f510662022830.jpg,https://books.toscrape.com/catalogue/soumission_998/index.html,2025-11-04T13:09:13Z
It's Only the Himalayas,45.17,Two,2,In stock,https://books.toscrape.com/media/cache/27/a5/27a53d0bb95bdd88288eaf66c9230d7e.jpg,https://books.toscrape.com/catalogue/its-only-the-himalayas_981/index.html,2025-11-04T13:09:13Z
```

```json
{"title":"Soumission","price":"50.10","rating":"One","rating_numeric":1,"availability":"In stock","image_url":"https://books.toscrape.com/media/cache/3e/ef/3eef99c9d9adef34639f510662022830.jpg","url":"https://books.toscrape.com/catalogue/soumission_998/index.html","scraped_at":"2025-11-04T13:09:13.103170645Z"}
{"title":"It's Only the Himalayas","price":"45.17","rating":"Two","rating_numeric":2,"availability":"In stock","image_url":"https://books.toscrape.com/media/cache/27/a5/27a53d0bb95bdd88288eaf66c9230d7e.jpg","url":"https://books.toscrape.com/catalogue/its-only-the-himalayas_981/index.html","scraped_at":"2025-11-04T13:09:13.103335404Z"}
{"title":"Penny Maybe","price":"33.29","rating":"Three","rating_numeric":3,"availability":"In stock","image_url":"https://books.toscrape.com/media/cache/12/53/1253c21c5ef3c6d075c5fa3f5fecee6a.jpg","url":"https://books.toscrape.com/catalogue/penny-maybe_965/index.html","scraped_at":"2025-11-04T13:09:13.271742395Z"}
```

## Testing

```bash
make test
```

The test suite covers the retry/backoff utility and the parser helpers used by the pipeline.
