// Package apperr defines API errors with stable codes. The frontend picks the
// user-facing text by code, so codes must never change meaning.
package apperr

import (
	"errors"
	"fmt"
	"net/http"
)

// Error is an error that maps to an HTTP response.
type Error struct {
	Status  int            `json:"-"`
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func (e *Error) Error() string { return fmt.Sprintf("%d %s: %s", e.Status, e.Code, e.Message) }

// With attaches details and returns the same error.
func (e *Error) With(k string, v any) *Error {
	if e.Details == nil {
		e.Details = map[string]any{}
	}
	e.Details[k] = v
	return e
}

// New creates an error.
func New(status int, code, msg string) *Error {
	return &Error{Status: status, Code: code, Message: msg}
}

func BadRequest(code, msg string) *Error    { return New(http.StatusBadRequest, code, msg) }
func Unauthorized(code, msg string) *Error  { return New(http.StatusUnauthorized, code, msg) }
func Forbidden(code, msg string) *Error     { return New(http.StatusForbidden, code, msg) }
func NotFound(code, msg string) *Error      { return New(http.StatusNotFound, code, msg) }
func Conflict(code, msg string) *Error      { return New(http.StatusConflict, code, msg) }
func Gone(code, msg string) *Error          { return New(http.StatusGone, code, msg) }
func TooLarge(code, msg string) *Error      { return New(http.StatusRequestEntityTooLarge, code, msg) }
func Unsupported(code, msg string) *Error   { return New(http.StatusUnsupportedMediaType, code, msg) }
func Unprocessable(code, msg string) *Error { return New(http.StatusUnprocessableEntity, code, msg) }
func Locked(code, msg string) *Error        { return New(http.StatusLocked, code, msg) }
func Unavailable(code, msg string) *Error   { return New(http.StatusServiceUnavailable, code, msg) }

// Common errors.
var (
	ErrNoSession = Unauthorized("unauthenticated", "sign in required")
	ErrCSRF      = Forbidden("csrf_invalid", "missing or invalid CSRF token")
)

// NoRole returns the standard 403 for a missing role in an area.
func NoRole(role, area string) *Error {
	e := Forbidden("forbidden", fmt.Sprintf("role %s required", role))
	e.With("role", role)
	if area != "" {
		e.With("area", area)
	}
	return e
}

// As extracts an *Error from err.
func As(err error) (*Error, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}
