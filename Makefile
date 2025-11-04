.PHONY: build run test clean

IMAGE ?= go-scrape-books
OUTPUT_DIR ?= $(PWD)/output
RUN_ARGS ?= -format dual -parallel 16 -pages 50
ARGS ?=

build:
	docker build -t $(IMAGE) .

run: build
	mkdir -p $(OUTPUT_DIR)
	docker run --rm -v $(OUTPUT_DIR):/app/output $(IMAGE) $(RUN_ARGS) $(ARGS)

test:
	docker build --target tester -t $(IMAGE)-tester .
	docker run --rm $(IMAGE)-tester

clean:
	rm -rf $(OUTPUT_DIR)
