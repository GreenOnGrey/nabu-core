package httpx

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCORS(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	h := CORS([]string{"https://web.example.org/"})(ok)

	t.Run("allowed origin gets credentials", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil)
		r.Header.Set("Origin", "https://web.example.org")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if got := w.Header().Get("Access-Control-Allow-Origin"); got != "https://web.example.org" {
			t.Fatalf("Allow-Origin = %q", got)
		}
		if w.Header().Get("Access-Control-Allow-Credentials") != "true" || w.Code != http.StatusOK {
			t.Fatalf("credentials or status: %v %d", w.Header(), w.Code)
		}
	})

	t.Run("preflight answered without auth", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodOptions, "/api/v1/features", nil)
		r.Header.Set("Origin", "https://web.example.org")
		r.Header.Set("Access-Control-Request-Method", "POST")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusNoContent || w.Header().Get("Access-Control-Allow-Headers") == "" {
			t.Fatalf("preflight: %d %v", w.Code, w.Header())
		}
	})

	t.Run("other origin gets no CORS headers", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil)
		r.Header.Set("Origin", "https://evil.example")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Fatal("foreign origin must not be allowed")
		}
	})

	t.Run("no origins configured — passthrough", func(t *testing.T) {
		w := httptest.NewRecorder()
		CORS(nil)(ok).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
		if w.Header().Get("Vary") != "" {
			t.Fatal("unexpected headers")
		}
	})
}
