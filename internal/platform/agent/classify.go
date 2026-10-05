package agent

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
)

var (
	leadingStatus = regexp.MustCompile(`^\s*(\d{3})\b`)
	anyStatus     = regexp.MustCompile(`\b([45]\d\d)\b`)
	contextText   = []string{"context length", "maximum context", "too many tokens", "context window", "context_length_exceeded", "prompt is too long"}
	timeoutText   = []string{"timeout", "timed out", "econnreset", "econnrefused", "socket hang up", "terminated", "network error", "fetch failed", "connection reset", "eof"}
)

// Classified is a provider error after classification (arch §8).
type Classified struct {
	Class      ErrorClass
	HTTPStatus int
	// Message is the provider's own message, for administrators and logs.
	Message string
}

// Classify classifies a provider error as Pi reports it in errorMessage, for
// example `402: {"message":"Insufficient Balance",...}`. The status is taken
// from the start of the message (Pi's format) or, failing that, from the text.
func Classify(errorMessage string) Classified {
	msg := strings.TrimSpace(errorMessage)
	c := Classified{Message: providerMessage(msg)}
	if m := leadingStatus.FindStringSubmatch(msg); m != nil {
		c.HTTPStatus, _ = strconv.Atoi(m[1])
	} else if m := anyStatus.FindStringSubmatch(msg); m != nil {
		c.HTTPStatus, _ = strconv.Atoi(m[1])
	}
	low := strings.ToLower(msg)
	switch {
	case c.HTTPStatus == 402 || strings.Contains(low, "insufficient balance") || strings.Contains(low, "insufficient_quota"):
		c.Class = ErrInsufficientBalance
	case c.HTTPStatus == 401 || c.HTTPStatus == 403:
		c.Class = ErrAuth
	case c.HTTPStatus == 429:
		c.Class = ErrRateLimit
	case containsAny(low, contextText):
		c.Class = ErrContextOverflow
	case c.HTTPStatus >= 500 && c.HTTPStatus <= 504:
		c.Class = ErrUnavailable
	case c.HTTPStatus == 400 || c.HTTPStatus == 422:
		c.Class = ErrBadRequest
	case c.HTTPStatus == 0 && containsAny(low, timeoutText):
		c.Class = ErrUnavailable
	default:
		c.Class = ErrBadRequest
	}
	return c
}

// providerMessage extracts "message" from Pi's `NNN: {json}` format, keeping
// the raw text otherwise.
func providerMessage(msg string) string {
	body := msg
	if m := leadingStatus.FindStringSubmatchIndex(msg); m != nil {
		body = strings.TrimSpace(strings.TrimPrefix(msg[m[1]:], ":"))
	}
	var v struct {
		Message string `json:"message"`
		Error   *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(body), &v) == nil {
		if v.Message != "" {
			return v.Message
		}
		if v.Error != nil && v.Error.Message != "" {
			return v.Error.Message
		}
	}
	if len(body) > 500 {
		body = body[:500] + "…"
	}
	return body
}

func containsAny(s string, subs []string) bool {
	for _, x := range subs {
		if strings.Contains(s, x) {
			return true
		}
	}
	return false
}
