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
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Transcriber turns audio into text.
type Transcriber interface {
	Transcribe(ctx context.Context, audio io.Reader, filename string) (string, error)
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

// Transcribe sends the audio and returns the recognized text.
func (c *Client) Transcribe(ctx context.Context, audio io.Reader, filename string) (string, error) {
	if c.url == "" {
		return "", fmt.Errorf("whisper: WHISPER_URL is not configured")
	}
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, err := mw.CreateFormFile("audio_file", filename)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(fw, audio); err != nil {
		return "", err
	}
	if err := mw.Close(); err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+"/asr?task=transcribe&output=json&encode=true", &body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("whisper: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("whisper: %s: %s", resp.Status, strings.TrimSpace(string(raw)))
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
