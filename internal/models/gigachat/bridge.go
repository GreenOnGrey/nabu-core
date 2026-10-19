package gigachat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
)

// Bridge is the OpenAI-compatible endpoint Pi uses for a GigaChat connection
// (arch §3.5): /internal/llm/gigachat/{conn}/v1/chat/completions and /models.
type Bridge struct {
	Client *Client
	// Lookup loads the connection with its authorization key; nil, nil when
	// it is unknown or switched off.
	Lookup func(ctx context.Context, id uuid.UUID) (*Conn, error)
	// Verify admits the bridge token of the connection (GC-07).
	Verify func(token string, conn uuid.UUID) bool
}

// Prefix is the path of the bridge under the internal address of api.
const Prefix = "/internal/llm/gigachat"

// Routes mounts the bridge.
func (b *Bridge) Routes(r chi.Router) {
	r.Post(Prefix+"/{conn}/v1/chat/completions", b.completions)
	r.Get(Prefix+"/{conn}/v1/models", b.models)
	r.Post(Prefix+"/{conn}/v1/embeddings", b.embeddings)
}

// embeddings passes a request of vectors: the format of GigaChat is OpenAI's
// (FTR.NAB.CMN-0003 R20 on a GigaChat model of embeddings).
func (b *Bridge) embeddings(w http.ResponseWriter, r *http.Request) {
	c, ok := b.conn(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<20))
	if err != nil {
		oaError(w, http.StatusBadRequest, "400 "+err.Error())
		return
	}
	resp, err := b.Client.do(r.Context(), *c, func(tok string) (*http.Request, error) {
		hr, err := http.NewRequestWithContext(r.Context(), http.MethodPost, c.apiURL()+"/embeddings", bytes.NewReader(body))
		if err == nil {
			hr.Header.Set("Authorization", "Bearer "+tok)
			hr.Header.Set("Content-Type", "application/json")
		}
		return hr, err
	})
	if err != nil {
		upstreamError(w, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		upstreamError(w, readError(resp))
		return
	}
	metrics.GigaChatBridge.WithLabelValues("200").Inc()
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.Copy(w, io.LimitReader(resp.Body, 64<<20))
}

// oaError writes an error in OpenAI's form with the status of GigaChat: Pi
// classifies it as it classifies any provider (R21).
func oaError(w http.ResponseWriter, status int, msg string) {
	metrics.GigaChatBridge.WithLabelValues(strconv.Itoa(status)).Inc()
	typ := "api_error"
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		typ = "authentication_error"
	case http.StatusPaymentRequired:
		typ = "insufficient_quota"
	case http.StatusTooManyRequests:
		typ = "rate_limit_error"
	case http.StatusBadRequest, http.StatusUnprocessableEntity, http.StatusRequestEntityTooLarge:
		typ = "invalid_request_error"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": msg, "type": typ, "code": status}})
}

func (b *Bridge) conn(w http.ResponseWriter, r *http.Request) (*Conn, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "conn"))
	if err != nil {
		oaError(w, http.StatusNotFound, "unknown connection")
		return nil, false
	}
	tok, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if tok == "" || b.Verify == nil || !b.Verify(strings.TrimSpace(tok), id) {
		oaError(w, http.StatusUnauthorized, "401 invalid bridge token")
		return nil, false
	}
	c, err := b.Lookup(r.Context(), id)
	if err != nil {
		slog.ErrorContext(r.Context(), "gigachat bridge lookup", "err", err)
		oaError(w, http.StatusServiceUnavailable, "the connection is not available")
		return nil, false
	}
	if c == nil {
		oaError(w, http.StatusNotFound, "the connection is switched off or deleted")
		return nil, false
	}
	return c, true
}

// upstreamError answers with the error of GigaChat or of the OAuth service.
func upstreamError(w http.ResponseWriter, err error) {
	var ge *Error
	if errors.As(err, &ge) {
		status := ge.Status
		if status < 400 {
			status = http.StatusBadGateway
		}
		oaError(w, status, strconv.Itoa(ge.Status)+": "+ge.Message)
		return
	}
	oaError(w, http.StatusServiceUnavailable, "503 service unavailable: "+err.Error())
}

func (b *Bridge) models(w http.ResponseWriter, r *http.Request) {
	c, ok := b.conn(w, r)
	if !ok {
		return
	}
	list, err := b.Client.Models(r.Context(), *c)
	if err != nil {
		upstreamError(w, err)
		return
	}
	data := make([]map[string]any, 0, len(list))
	for _, m := range list {
		data = append(data, map[string]any{"id": m.ID, "object": "model", "owned_by": "gigachat", "type": m.Type})
	}
	metrics.GigaChatBridge.WithLabelValues("200").Inc()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data})
}

func (b *Bridge) completions(w http.ResponseWriter, r *http.Request) {
	c, ok := b.conn(w, r)
	if !ok {
		return
	}
	var in oaRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<20)).Decode(&in); err != nil {
		oaError(w, http.StatusBadRequest, "400 invalid request: "+err.Error())
		return
	}
	req, images, err := translateRequest(in)
	if err != nil {
		oaError(w, http.StatusBadRequest, "400 "+err.Error())
		return
	}
	ctx := r.Context()
	// GC-06: images go to GigaChat as files of the request and are removed after it.
	var files []string
	defer func() {
		if len(files) > 0 {
			cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			b.Client.remove(cctx, *c, files)
		}
	}()
	for _, im := range images {
		id, err := b.Client.upload(ctx, *c, im.mime, im.data)
		if err != nil {
			upstreamError(w, err)
			return
		}
		files = append(files, id)
		req.Messages[im.msg].Attachments = append(req.Messages[im.msg].Attachments, id)
	}
	body, _ := json.Marshal(req)
	resp, err := b.Client.do(ctx, *c, func(tok string) (*http.Request, error) {
		hr, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiURL()+"/chat/completions", bytes.NewReader(body))
		if err == nil {
			hr.Header.Set("Authorization", "Bearer "+tok)
			hr.Header.Set("Content-Type", "application/json")
			if req.Stream {
				hr.Header.Set("Accept", "text/event-stream")
			} else {
				hr.Header.Set("Accept", "application/json")
			}
		}
		return hr, err
	})
	if err != nil {
		upstreamError(w, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		upstreamError(w, readError(resp))
		return
	}
	if !req.Stream {
		raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		if err != nil {
			oaError(w, http.StatusBadGateway, "502 "+err.Error())
			return
		}
		out, err := translateResponse(raw, in.Model)
		if err != nil {
			oaError(w, http.StatusBadGateway, "502 "+err.Error())
			return
		}
		metrics.GigaChatBridge.WithLabelValues("200").Inc()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(out)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	fl, _ := w.(http.Flusher)
	flush := func() {
		if fl != nil {
			fl.Flush()
		}
	}
	// the status is sent before the stream: a stream broken halfway counts as 502
	if err := translateStream(resp.Body, w, flush, in.Model); err != nil && ctx.Err() == nil {
		metrics.GigaChatBridge.WithLabelValues("502").Inc()
		slog.WarnContext(ctx, "gigachat bridge stream", "err", err)
		return
	}
	metrics.GigaChatBridge.WithLabelValues("200").Inc()
}
