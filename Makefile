# =============================================================================
# Go Books Scraper - Makefile
# =============================================================================

.PHONY: help build run scrape test bench clean
.DEFAULT_GOAL := help

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
IMAGE      ?= go-scrape-books
OUTPUT_DIR ?= $(PWD)/output
PAGES      ?= 50
PARALLEL   ?= 16
FORMAT     ?= dual
ARGS       ?=

# -----------------------------------------------------------------------------
# Help
# -----------------------------------------------------------------------------
help: ## Show this help message
	@echo "Usage: make [target] [VARIABLE=value]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Variables:"
	@echo "  PAGES=$(PAGES)        Max catalog pages to scrape"
	@echo "  PARALLEL=$(PARALLEL)      Concurrent request limit"
	@echo "  FORMAT=$(FORMAT)        Output format (csv|json|dual)"
	@echo "  ARGS=                 Additional CLI arguments"
	@echo ""
	@echo "Examples:"
	@echo "  make scrape                      # Default: 50 pages, 16 workers, dual output"
	@echo "  make scrape PAGES=10 PARALLEL=4  # Quick test run"
	@echo "  make scrape ARGS='-metrics-addr :9090'"

# -----------------------------------------------------------------------------
# Docker
# -----------------------------------------------------------------------------
build: ## Build the Docker image
	@docker build -t $(IMAGE) .

# -----------------------------------------------------------------------------
# Scraping
# -----------------------------------------------------------------------------
scrape: build ## Run the scraper (configurable via PAGES, PARALLEL, FORMAT)
	@mkdir -p $(OUTPUT_DIR)
	@docker run --rm \
		-v $(OUTPUT_DIR):/app/output \
		$(IMAGE) \
		-pages $(PAGES) \
		-parallel $(PARALLEL) \
		-format $(FORMAT) \
		$(ARGS)

run: scrape ## Alias for 'scrape'

# -----------------------------------------------------------------------------
# Testing & Benchmarks
# -----------------------------------------------------------------------------
test: ## Run the test suite in Docker
	@docker build --target tester -t $(IMAGE)-tester .
	@docker run --rm $(IMAGE)-tester

bench: ## Run pipeline throughput benchmarks
	@docker run --rm -v $(PWD):/app -w /app golang:1.25 \
		go test ./scraper -bench BenchmarkPipeline_Throughput -benchmem -count=1

# -----------------------------------------------------------------------------
# Cleanup
# -----------------------------------------------------------------------------
clean: ## Remove output directory
	@rm -rf $(OUTPUT_DIR)
