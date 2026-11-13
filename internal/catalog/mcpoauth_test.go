package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// FTR.NAB.ADM-0001 qa

// fakeMCPAuth is a server shaped like VK WorkSpace (arch §5): the MCP answers
// 401 with resource_metadata, the authorization server registers clients.
type fakeMCPAuth struct {
	srv          *httptest.Server
	challenge    bool
	registration bool
	authMethods  []string
	registered   map[string]any
	asOverride   string
}

func newFakeMCPAuth(t *testing.T) *fakeMCPAuth {
	f := &fakeMCPAuth{challenge: true, registration: true, authMethods: []string{"none"}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /mcp", func(w http.ResponseWriter, r *http.Request) {
		if f.challenge {
			w.Header().Set("WWW-Authenticate", `Bearer resource_metadata="`+f.srv.URL+`/.well-known/oauth-protected-resource/mcp"`)
		}
		http.Error(w, "missing or invalid Authorization header", http.StatusUnauthorized)
	})
	mux.HandleFunc("GET /.well-known/oauth-protected-resource/mcp", func(w http.ResponseWriter, r *http.Request) {
		as := f.srv.URL
		if f.asOverride != "" {
			as = f.asOverride
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"resource": f.srv.URL + "/mcp", "authorization_servers": []string{as}, "bearer_methods_supported": []string{"header"}})
	})
	mux.HandleFunc("GET /.well-known/oauth-authorization-server", func(w http.ResponseWriter, r *http.Request) {
		m := map[string]any{"issuer": f.srv.URL, "authorization_endpoint": f.srv.URL + "/oauth/authorize", "token_endpoint": f.srv.URL + "/oauth/token",
			"response_types_supported": []string{"code"}, "code_challenge_methods_supported": []string{"S256"},
			"token_endpoint_auth_methods_supported": f.authMethods}
		if f.registration {
			m["registration_endpoint"] = f.srv.URL + "/oauth/register"
		}
		_ = json.NewEncoder(w).Encode(m)
	})
	mux.HandleFunc("POST /oauth/register", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&f.registered)
		out := map[string]any{"client_id": "client-1"}
		if f.registered["token_endpoint_auth_method"] == "client_secret_post" {
			out["client_secret"] = "secret-1"
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(out)
	})
	f.srv = httptest.NewTLSServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeMCPAuth) service() *Service {
	return &Service{HTTP: f.srv.Client(), PublicAPIURL: "https://nabu.example"}
}

func TestOAUTH01DiscoverAndRegister(t *testing.T) {
	f := newFakeMCPAuth(t)
	s := f.service()
	meta, err := s.discoverMCPOAuth(context.Background(), f.srv.URL+"/mcp")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Issuer != f.srv.URL || meta.AuthorizeURL != f.srv.URL+"/oauth/authorize" || meta.TokenURL != f.srv.URL+"/oauth/token" || meta.Resource != f.srv.URL+"/mcp" {
		t.Fatalf("meta = %+v", meta)
	}
	id, secret, err := s.registerClient(context.Background(), meta, "https://nabu.example/api/v1/oauth/callback/x")
	if err != nil || id != "client-1" || secret != "" {
		t.Fatalf("register = %q %q %v", id, secret, err)
	}
	if f.registered["token_endpoint_auth_method"] != "none" || f.registered["client_name"] != "Nabu" {
		t.Fatalf("registered = %v", f.registered)
	}
	if uris, _ := f.registered["redirect_uris"].([]any); len(uris) != 1 || uris[0] != "https://nabu.example/api/v1/oauth/callback/x" {
		t.Fatalf("redirect_uris = %v", f.registered["redirect_uris"])
	}
}

func TestOAUTH02WellKnownWithoutChallenge(t *testing.T) {
	f := newFakeMCPAuth(t)
	f.challenge = false
	meta, err := f.service().discoverMCPOAuth(context.Background(), f.srv.URL+"/mcp")
	if err != nil || meta.TokenURL == "" {
		t.Fatalf("meta = %+v, err = %v", meta, err)
	}
}

func TestOAUTH03NoRegistration(t *testing.T) {
	f := newFakeMCPAuth(t)
	f.registration = false
	_, err := f.service().discoverMCPOAuth(context.Background(), f.srv.URL+"/mcp")
	var de *discoveryError
	if !errors.As(err, &de) || de.code != codeRegistrationUnsupp {
		t.Fatalf("err = %v", err)
	}
}

func TestOAUTH04RefusesInsecureOrInternalServers(t *testing.T) {
	for _, as := range []string{"http://auth.example", "https://10.0.0.5", "https://localhost:8443", "https://[::1]"} {
		f := newFakeMCPAuth(t)
		f.asOverride = as
		_, err := f.service().discoverMCPOAuth(context.Background(), f.srv.URL+"/mcp")
		var de *discoveryError
		if !errors.As(err, &de) || de.code != codeDiscoveryFailed {
			t.Fatalf("%s: err = %v", as, err)
		}
	}
	_, err := (&Service{}).discoverMCPOAuth(context.Background(), "http://mcp.example/mcp")
	if err == nil || !strings.Contains(err.Error(), "https") {
		t.Fatalf("http MCP: err = %v", err)
	}
}

func TestOAUTH05ClientSecretPost(t *testing.T) {
	f := newFakeMCPAuth(t)
	f.authMethods = []string{"client_secret_post"}
	s := f.service()
	meta, err := s.discoverMCPOAuth(context.Background(), f.srv.URL+"/mcp")
	if err != nil {
		t.Fatal(err)
	}
	_, secret, err := s.registerClient(context.Background(), meta, "https://nabu.example/cb")
	if err != nil || secret != "secret-1" || f.registered["token_endpoint_auth_method"] != "client_secret_post" {
		t.Fatalf("secret = %q, err = %v, registered = %v", secret, err, f.registered)
	}
}

func TestPrivateHost(t *testing.T) {
	for h, want := range map[string]bool{"localhost": true, "127.0.0.1": true, "192.168.1.1": true, "169.254.169.254": true,
		"ai.workspace.vk.ru": false, "8.8.8.8": false} {
		if privateHost(h) != want {
			t.Errorf("privateHost(%s) != %v", h, want)
		}
	}
}
