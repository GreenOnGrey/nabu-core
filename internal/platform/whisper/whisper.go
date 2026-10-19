// Package whisper transcribes voice messages with a Whisper ASR web service.
package whisper

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Transcriber turns audio into text. language is the language of the user
// (en, ru, …): a hint — another language is still detected
// (FTR.NAB.CHAT-0001 R20). The caller bounds the time with ctx.
type Transcriber interface {
	Transcribe(ctx context.Context, audio io.Reader, filename, language string) (string, error)
}

// Client talks to onerahmet/openai-whisper-asr-webservice (POST /asr).
type Client struct {
	url  string
	http *http.Client
}

// New creates a client for the service at baseURL.
func New(baseURL string) *Client {
	return &Client{url: baseURL, http: &http.Client{Timeout: 2 * time.Minute, Transport: otelhttp.NewTransport(http.DefaultTransport)}}
}

// Transcribe sends the audio and returns the recognized text. The language
// of the user is used unless the recognizer is sure the speech is in another
// one (FTR.NAB.CHAT-0001 R20).
func (c *Client) Transcribe(ctx context.Context, audio io.Reader, filename, language string) (string, error) {
	if c.url == "" {
		return "", fmt.Errorf("whisper: WHISPER_URL is not configured")
	}
	data, err := io.ReadAll(audio)
	if err != nil {
		return "", err
	}
	raw, err := c.post(ctx, "/asr?"+query(c.language(ctx, data, filename, language)), data, filename)
	if err != nil {
		return "", err
	}
	var out struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		// Some versions return plain text.
		return strings.TrimSpace(string(raw)), nil
	}
	return strings.TrimSpace(out.Text), nil
}

// detectSure is the confidence from which a detected language wins over the
// language of the user.
const detectSure = 0.5

// language is the language to recognize in: the detected one when the
// recognizer is sure, otherwise the hint. A failed detection is not an error.
func (c *Client) language(ctx context.Context, data []byte, filename, hint string) string {
	raw, err := c.post(ctx, "/detect-language?encode=true", data, filename)
	if err != nil {
		return hint
	}
	var d struct {
		Code       string   `json:"language_code"`
		Confidence *float64 `json:"confidence"`
	}
	if json.Unmarshal(raw, &d) != nil || d.Code == "" || (d.Confidence != nil && *d.Confidence < detectSure) {
		return hint
	}
	return d.Code
}

// post sends the audio as the form field audio_file.
func (c *Client) post(ctx context.Context, path string, data []byte, filename string) ([]byte, error) {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, err := mw.CreateFormFile("audio_file", filename)
	if err != nil {
		return nil, err
	}
	if _, err := fw.Write(data); err != nil {
		return nil, err
	}
	if err := mw.Close(); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+path, &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("whisper: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("whisper: %s: %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	return raw, nil
}

// query is the query of /asr: transcription to JSON with ffmpeg decoding and
// the voice activity filter (silence is not recognized as words).
func query(language string) string {
	q := url.Values{"task": {"transcribe"}, "output": {"json"}, "encode": {"true"}, "vad_filter": {"true"}}
	if language != "" && len(language) <= 3 {
		q.Set("language", language)
	}
	return q.Encode()
}

// Ping checks that the recognizer answers (FTR.NAB.CHAT-0001 R22): its page of
// the API documentation.
func (c *Client) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url+"/docs", nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("whisper: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("whisper: %s", resp.Status)
	}
	return nil
}

// StatusHandler serves the state of the recognizer for the settings of the
// administration (FTR.NAB.CHAT-0001 R22): not configured, available, or not
// answering with the reason.
func StatusHandler(url string, c *Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		out := map[string]any{"configured": url != "" && c != nil, "ok": false}
		if url != "" && c != nil {
			out["url"] = url
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()
			if err := c.Ping(ctx); err != nil {
				out["error"] = err.Error()
			} else {
				out["ok"] = true
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}
}
