# syntax=docker/dockerfile:1

FROM golang:1.21 AS deps
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

FROM deps AS build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/scraper ./cmd/scraper

FROM deps AS tester
COPY . .
CMD ["go", "test", "./..."]

FROM alpine:3.20 AS runtime
RUN apk add --no-cache ca-certificates && adduser -D -h /app scraper
WORKDIR /app
COPY --from=build /out/scraper /app/scraper
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && mkdir -p /app/output && chown -R scraper:scraper /app
USER scraper
VOLUME ["/app/output"]
ENTRYPOINT ["/entrypoint.sh"]
