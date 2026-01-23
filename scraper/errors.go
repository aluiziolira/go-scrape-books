package scraper

import (
	"errors"
	"fmt"
)

// ErrTimeout indicates a timeout while issuing a request.
type ErrTimeout struct {
	Err error
}

func (e ErrTimeout) Error() string {
	return fmt.Errorf("timeout: %w", e.Err).Error()
}

func (e ErrTimeout) Unwrap() error {
	return e.Err
}

// ErrConnection indicates a network connectivity failure.
type ErrConnection struct {
	Err error
}

func (e ErrConnection) Error() string {
	return fmt.Errorf("connection: %w", e.Err).Error()
}

func (e ErrConnection) Unwrap() error {
	return e.Err
}

// ErrForbidden indicates a forbidden response (HTTP 403).
type ErrForbidden struct {
	Err error
}

func (e ErrForbidden) Error() string {
	return fmt.Errorf("forbidden: %w", e.Err).Error()
}

func (e ErrForbidden) Unwrap() error {
	return e.Err
}

// ErrNotFound indicates a missing resource (HTTP 404).
type ErrNotFound struct {
	Err error
}

func (e ErrNotFound) Error() string {
	return fmt.Errorf("not_found: %w", e.Err).Error()
}

func (e ErrNotFound) Unwrap() error {
	return e.Err
}

// ErrRateLimited indicates the target rate-limited the request.
type ErrRateLimited struct {
	Err error
}

func (e ErrRateLimited) Error() string {
	return fmt.Errorf("rate_limited: %w", e.Err).Error()
}

func (e ErrRateLimited) Unwrap() error {
	return e.Err
}

func errorTypeLabel(err error) string {
	if err == nil {
		return "unknown"
	}
	var timeout ErrTimeout
	if errors.As(err, &timeout) {
		return "timeout"
	}
	var conn ErrConnection
	if errors.As(err, &conn) {
		return "connection"
	}
	var forbidden ErrForbidden
	if errors.As(err, &forbidden) {
		return "forbidden"
	}
	var notFound ErrNotFound
	if errors.As(err, &notFound) {
		return "not_found"
	}
	var rateLimited ErrRateLimited
	if errors.As(err, &rateLimited) {
		return "rate_limited"
	}
	return "other"
}
