package whisper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// VOC-12: the language of the user, unless the recognizer is sure of another one.
func TestLanguage(t *testing.T) {
	for _, c := range []struct {
		name, detect string
		status       int
		want         string
	}{
		{"another language, sure", `{"detected_language":"english","language_code":"en","confidence":0.93}`, 200, "en"},
		{"another language, unsure", `{"detected_language":"english","language_code":"en","confidence":0.31}`, 200, "ru"},
		{"no confidence in the answer", `{"detected_language":"german","language_code":"de"}`, 200, "de"},
		{"detection failed", `boom`, 500, "ru"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var asked string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/detect-language") {
					w.WriteHeader(c.status)
					_, _ = w.Write([]byte(c.detect))
					return
				}
				asked = r.URL.Query().Get("language")
				_, _ = w.Write([]byte(`{"text":" привет "}`))
			}))
			defer srv.Close()
			text, err := New(srv.URL).Transcribe(context.Background(), strings.NewReader("ogg"), "v.ogg", "ru")
			if err != nil || text != "привет" || asked != c.want {
				t.Fatalf("text %q err %v language %q, want %q", text, err, asked, c.want)
			}
		})
	}
}

// VOC-11: the three states of the recognizer in the settings.
func TestStatus(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer up.Close()
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(502) }))
	defer down.Close()
	for _, c := range []struct {
		name, url string
		client    *Client
		want      string
	}{
		{"not configured", "", nil, `"configured":false,"ok":false`},
		{"available", up.URL, New(up.URL), `"configured":true,"ok":true`},
		{"not answering", down.URL, New(down.URL), `"error":"whisper: 502 Bad Gateway"`},
	} {
		rec := httptest.NewRecorder()
		StatusHandler(c.url, c.client)(rec, httptest.NewRequest("GET", "/voice", nil))
		if !strings.Contains(rec.Body.String(), c.want) {
			t.Errorf("%s: %s", c.name, rec.Body.String())
		}
	}
}
