// Package k8s is a minimal in-cluster client of the Kubernetes API: the
// worker creates and deletes the pods of personal sandboxes with it
// (FTR.NAB.CMN-0001 arch §5.2). Ported from the runner executor of Hammurapi.
package k8s

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const saDir = "/var/run/secrets/kubernetes.io/serviceaccount/"

// Client calls the API server with the pod's service account.
type Client struct {
	APIServer string
	TokenFile string
	http      *http.Client
}

// NewInCluster creates a client with in-cluster credentials.
func NewInCluster() *Client {
	pool := x509.NewCertPool()
	if ca, err := os.ReadFile(saDir + "ca.crt"); err == nil {
		pool.AppendCertsFromPEM(ca)
	}
	return &Client{APIServer: "https://kubernetes.default.svc", TokenFile: saDir + "token",
		http: &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}}
}

// APIError is a non-2xx answer.
type APIError struct {
	Status int
	Body   string
}

func (e *APIError) Error() string { return fmt.Sprintf("kubernetes %d: %s", e.Status, e.Body) }

// NotFound reports a 404.
func NotFound(err error) bool {
	ae, ok := err.(*APIError)
	return ok && ae.Status == http.StatusNotFound
}

// Conflict reports a 409 (already exists).
func Conflict(err error) bool {
	ae, ok := err.(*APIError)
	return ok && ae.Status == http.StatusConflict
}

// Do performs a request; out may be nil.
func (c *Client) Do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.APIServer+path, body)
	if err != nil {
		return err
	}
	tok, err := os.ReadFile(c.TokenFile)
	if err != nil {
		return fmt.Errorf("service account token: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tok)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		s := string(b)
		if len(s) > 500 {
			s = s[:500]
		}
		return &APIError{Status: resp.StatusCode, Body: s}
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}

// Pod is the part of a pod the worker reads.
type Pod struct {
	Metadata struct {
		Name              string            `json:"name"`
		Labels            map[string]string `json:"labels"`
		DeletionTimestamp *time.Time        `json:"deletionTimestamp"`
	} `json:"metadata"`
	Status struct {
		Phase string `json:"phase"`
	} `json:"status"`
}

// CreatePod creates a pod from a manifest.
func (c *Client) CreatePod(ctx context.Context, ns string, manifest map[string]any) error {
	return c.Do(ctx, http.MethodPost, "/api/v1/namespaces/"+ns+"/pods", manifest, nil)
}

// DeletePod deletes a pod with a grace period.
func (c *Client) DeletePod(ctx context.Context, ns, name string, grace int) error {
	err := c.Do(ctx, http.MethodDelete, "/api/v1/namespaces/"+ns+"/pods/"+name, map[string]any{"gracePeriodSeconds": grace}, nil)
	if NotFound(err) {
		return nil
	}
	return err
}

// GetPod loads a pod; nil when it does not exist.
func (c *Client) GetPod(ctx context.Context, ns, name string) (*Pod, error) {
	var p Pod
	err := c.Do(ctx, http.MethodGet, "/api/v1/namespaces/"+ns+"/pods/"+name, nil, &p)
	if NotFound(err) {
		return nil, nil
	}
	return &p, err
}

// ListPods lists pods by label selector.
func (c *Client) ListPods(ctx context.Context, ns, selector string) ([]Pod, error) {
	var l struct {
		Items []Pod `json:"items"`
	}
	err := c.Do(ctx, http.MethodGet, "/api/v1/namespaces/"+ns+"/pods?labelSelector="+selector, nil, &l)
	return l.Items, err
}
