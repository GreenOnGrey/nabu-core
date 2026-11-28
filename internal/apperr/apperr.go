// Package apperr defines API errors with stable codes. The frontend picks the
// user-facing text by code, so codes must never change meaning.
package apperr

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
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

// CodeOf is the stable code of an error: the code of *Error, else the code of
// its first line when it is shaped "422 code: message" or "code: message".
// An error without a code gives "".
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	if e, ok := As(err); ok {
		return e.Code
	}
	return CodeIn(err.Error())
}

// CodeIn is the code a tool refusal starts with: the answer of a handler of
// the built-in MCP reaches an agent as text, so the code is read from the
// first line — "422 invalid_same_as: …" of *Error or "note_missing: …" of a
// refusal that carries no status (FTR.NAB.CMN-0001 R28). "" when the text has
// no code.
func CodeIn(text string) string {
	text = strings.TrimSpace(text)
	if i := strings.IndexByte(text, '\n'); i >= 0 {
		text = text[:i]
	}
	rest := text
	coded := false
	if len(text) > 4 && text[3] == ' ' && isDigits(text[:3]) {
		rest, coded = text[4:], true
	}
	code, msg, ok := strings.Cut(rest, ":")
	if !ok || len(code) < 3 || len(code) > 64 {
		return ""
	}
	// a bare prefix is a code of a refusal only with an underscore: "http:", "open /x:" and the like are ordinary
	// messages, while the codes of the refusals of the tools are underscored
	if !coded && !strings.ContainsRune(code, '_') {
		return ""
	}
	for i := 0; i < len(code); i++ {
		c := code[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' {
			return ""
		}
	}
	if code[0] < 'a' || code[0] > 'z' || msg == "" {
		return ""
	}
	return code
}

func isDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return s != ""
}
