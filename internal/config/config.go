// Package config reads every environment variable of Nabu
// (FTR.NAB.CMN-0001 tech §11). A new variable is added here and to the
// documentation of the nabu repository (docs/configuration.md).
package config

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the configuration of the api, worker, relay, cleaner and migrate modes.
type Config struct {
	LogLevel     string
	OTLPEndpoint string

	HTTPAddr     string // public API (:8080)
	InternalAddr string // internal API: MCP, MCP proxy, sandbox sync (:8081)
	ServiceAddr  string // health and metrics (:9100)
	RelayAddr    string // relay mode (:8085)

	PublicWebURL       string
	PublicAPIURL       string
	InternalURL        string // http://api-internal.<ns>.svc:8081 — agent sessions reach the built-in MCP here
	RelayInternalURL   string // http://relay.<ns>.svc:8085 — the agent's calls to workspaces
	RelayPublicWSURL   string // wss://nabu-api.<domain>/v1/workspaces/connect — external workspaces
	RelaySandboxWSURL  string // ws://relay.<ns>.svc:8085/v1/workspaces/connect — sandboxes inside the cluster
	CORSAllowedOrigins []string
	CookieDomain       string

	// Sign-in (tech §2).
	AuthProvider       string // oidc | github
	OIDCIssuer         string
	OIDCClientID       string
	OIDCClientSecret   string
	OIDCScopes         string
	OIDCProviderName   string // shown on the button: "Keycloak"
	GitHubClientID     string
	GitHubClientSecret string
	GitHubAllowedOrg   string
	GitHubBaseURL      string // https://github.com (OAuth) — fakes in tests
	GitHubAPIURL       string // https://api.github.com
	BootstrapAdmins    []string

	SecretsKey []byte

	DatabaseURL  string
	KafkaBrokers []string
	S3Endpoint   string
	S3AccessKey  string
	S3SecretKey  string
	S3Bucket     string
	S3UseSSL     bool

	TelegramBotToken      string
	TelegramWebhookSecret string
	TelegramAPIURL        string
	VKWSEnabled           bool

	AgentAddr         string // http://agent.<ns>.svc:8090
	AgentServiceToken string
	AgentIdleTimeout  time.Duration
	TurnTimeout       time.Duration
	InboundWorkers    int

	SandboxExecutor    string // k8s | none
	SandboxNamespace   string
	SandboxImage       string
	SandboxIdleTimeout time.Duration
	SandboxCPU         string
	SandboxMemory      string
	SpaceQuota         int64
	SandboxEgressAllow []string
	SandboxExclude     []string

	AuditRetention    time.Duration
	TopicArchiveAfter time.Duration

	TaskTick          time.Duration
	TaskRunTimeout    time.Duration
	TaskMinInterval   time.Duration
	TaskMaxActive     int
	TaskMaxFailures   int
	TaskCatchupWindow time.Duration
	DefaultTimezone   string
	DefaultLanguage   string

	RunWorkspaceWait time.Duration
	UploadMaxBytes   int64
	WhisperURL       string
	PiVersion        string

	// BootstrapLLM: an OpenAI-compatible connection created on the first
	// start when there is none (the stand gets its DeepSeek key this way).
	BootstrapLLMKey  string
	BootstrapLLMURL  string
	BootstrapLLMType string
}

func env(k, def string) string {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		return v
	}
	return def
}

func list(k string) []string {
	var out []string
	for _, f := range strings.FieldsFunc(os.Getenv(k), func(r rune) bool { return r == ',' || r == ' ' || r == '\n' || r == ';' }) {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}

type parser struct{ errs []error }

func (p *parser) dur(k, def string) time.Duration {
	v := env(k, def)
	if strings.HasSuffix(v, "d") {
		if n, err := strconv.Atoi(strings.TrimSuffix(v, "d")); err == nil {
			return time.Duration(n) * 24 * time.Hour
		}
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: %w", k, err))
	}
	return d
}

func (p *parser) int(k string, def int) int {
	v := env(k, strconv.Itoa(def))
	n, err := strconv.Atoi(v)
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: %w", k, err))
	}
	return n
}

func (p *parser) bool(k string, def bool) bool {
	v := env(k, strconv.FormatBool(def))
	b, err := strconv.ParseBool(v)
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: %w", k, err))
	}
	return b
}

func (p *parser) bytes(k, def string) int64 {
	n, err := ParseBytes(env(k, def))
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: %w", k, err))
	}
	return n
}

// ParseBytes reads 5GB, 512MB, 1GiB or a number of bytes.
func ParseBytes(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	mult := int64(1)
	for _, u := range []struct {
		suf string
		m   int64
	}{{"GIB", 1 << 30}, {"MIB", 1 << 20}, {"KIB", 1 << 10}, {"GB", 1 << 30}, {"MB", 1 << 20}, {"KB", 1 << 10}, {"G", 1 << 30}, {"M", 1 << 20}, {"B", 1}} {
		if strings.HasSuffix(s, u.suf) {
			mult, s = u.m, strings.TrimSuffix(s, u.suf)
			break
		}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return n * mult, err
}

// DecodeKey reads a 32-byte key given as hex, base64 or 32 raw characters.
func DecodeKey(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if b, err := hex.DecodeString(s); err == nil && len(b) == 32 {
		return b, nil
	}
	if b, err := base64.StdEncoding.DecodeString(s); err == nil && len(b) == 32 {
		return b, nil
	}
	if len(s) == 32 {
		return []byte(s), nil
	}
	return nil, errors.New("SECRETS_KEY must be 32 bytes: 64 hex characters, base64 or 32 characters")
}

// Load reads the environment.
func Load() (*Config, error) {
	p := &parser{}
	c := &Config{
		LogLevel: env("LOG_LEVEL", "info"), OTLPEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		HTTPAddr: env("HTTP_ADDR", ":8080"), InternalAddr: env("INTERNAL_ADDR", ":8081"),
		ServiceAddr: env("SERVICE_ADDR", ":9100"), RelayAddr: env("RELAY_ADDR", ":8085"),
		PublicWebURL: strings.TrimRight(env("PUBLIC_WEB_URL", "http://localhost:5173"), "/"),
		PublicAPIURL: strings.TrimRight(env("PUBLIC_API_URL", "http://localhost:8080"), "/"),
		InternalURL:  strings.TrimRight(env("INTERNAL_URL", "http://localhost:8081"), "/"),
		CookieDomain: os.Getenv("COOKIE_DOMAIN"), CORSAllowedOrigins: list("CORS_ALLOWED_ORIGINS"),

		AuthProvider: env("AUTH_PROVIDER", "github"),
		OIDCIssuer:   strings.TrimRight(os.Getenv("OIDC_ISSUER"), "/"), OIDCClientID: os.Getenv("OIDC_CLIENT_ID"),
		OIDCClientSecret: os.Getenv("OIDC_CLIENT_SECRET"), OIDCScopes: env("OIDC_SCOPES", "openid email profile"),
		OIDCProviderName: env("OIDC_PROVIDER_NAME", "Keycloak"),
		GitHubClientID:   os.Getenv("GITHUB_CLIENT_ID"), GitHubClientSecret: os.Getenv("GITHUB_CLIENT_SECRET"),
		GitHubAllowedOrg: os.Getenv("GITHUB_ALLOWED_ORG"),
		GitHubBaseURL:    strings.TrimRight(env("GITHUB_BASE_URL", "https://github.com"), "/"),
		GitHubAPIURL:     strings.TrimRight(env("GITHUB_API_URL", "https://api.github.com"), "/"),
		BootstrapAdmins:  list("BOOTSTRAP_ADMINS"),

		DatabaseURL: os.Getenv("DATABASE_URL"), KafkaBrokers: list("KAFKA_BROKERS"),
		S3Endpoint: os.Getenv("S3_ENDPOINT"), S3AccessKey: os.Getenv("S3_ACCESS_KEY"), S3SecretKey: os.Getenv("S3_SECRET_KEY"),
		S3Bucket: env("S3_BUCKET", "nabu"), S3UseSSL: p.bool("S3_USE_SSL", false),

		TelegramBotToken: os.Getenv("TELEGRAM_BOT_TOKEN"), TelegramWebhookSecret: os.Getenv("TELEGRAM_WEBHOOK_SECRET"),
		TelegramAPIURL: strings.TrimRight(env("TELEGRAM_API_URL", "https://api.telegram.org"), "/"),
		VKWSEnabled:    p.bool("VKWS_ENABLED", false),

		AgentAddr: strings.TrimRight(env("AGENT_ADDR", "http://localhost:8090"), "/"), AgentServiceToken: os.Getenv("AGENT_SERVICE_TOKEN"),
		AgentIdleTimeout: p.dur("AGENT_IDLE_TIMEOUT", "15m"), TurnTimeout: p.dur("TURN_TIMEOUT", "30m"),
		InboundWorkers: p.int("INBOUND_WORKERS", 4),

		SandboxExecutor: env("SANDBOX_EXECUTOR", "none"), SandboxNamespace: env("SANDBOX_NAMESPACE", "nabu-sandboxes"),
		SandboxImage: os.Getenv("SANDBOX_IMAGE"), SandboxIdleTimeout: p.dur("SANDBOX_IDLE_TIMEOUT", "30m"),
		SandboxCPU: env("SANDBOX_CPU", "1"), SandboxMemory: env("SANDBOX_MEMORY", "1Gi"),
		SpaceQuota: p.bytes("SPACE_QUOTA", "5GB"), SandboxEgressAllow: list("SANDBOX_EGRESS_ALLOW"),
		SandboxExclude: append([]string{"node_modules/", ".venv/", "__pycache__/", "*.tmp"}, list("SANDBOX_EXCLUDE")...),

		AuditRetention: p.dur("AUDIT_RETENTION", "365d"), TopicArchiveAfter: p.dur("TOPIC_ARCHIVE_AFTER", "14d"),

		TaskTick: p.dur("TASK_TICK", "15s"), TaskRunTimeout: p.dur("TASK_RUN_TIMEOUT", "30m"),
		TaskMinInterval: p.dur("TASK_MIN_INTERVAL", "15m"), TaskMaxActive: p.int("TASK_MAX_ACTIVE", 20),
		TaskMaxFailures: p.int("TASK_MAX_FAILURES", 3), TaskCatchupWindow: p.dur("TASK_CATCHUP_WINDOW", "1h"),
		DefaultTimezone: env("DEFAULT_TIMEZONE", "Europe/Moscow"), DefaultLanguage: env("DEFAULT_LANGUAGE", "en"),

		RunWorkspaceWait: p.dur("RUN_WORKSPACE_WAIT", "120s"), UploadMaxBytes: p.bytes("UPLOAD_MAX_BYTES", "50MB"),
		WhisperURL: os.Getenv("WHISPER_URL"), PiVersion: env("PI_VERSION", "dev"),

		BootstrapLLMKey: os.Getenv("BOOTSTRAP_LLM_API_KEY"), BootstrapLLMURL: env("BOOTSTRAP_LLM_BASE_URL", "https://api.deepseek.com"),
		BootstrapLLMType: env("BOOTSTRAP_LLM_TYPE", "deepseek"),
	}
	c.RelayInternalURL = strings.TrimRight(env("RELAY_INTERNAL_URL", "http://localhost"+c.RelayAddr), "/")
	c.RelaySandboxWSURL = env("RELAY_SANDBOX_WS_URL", strings.Replace(c.RelayInternalURL, "http", "ws", 1)+"/v1/workspaces/connect")
	c.RelayPublicWSURL = env("RELAY_PUBLIC_WS_URL", strings.Replace(c.PublicAPIURL, "http", "ws", 1)+"/v1/workspaces/connect")
	if k := os.Getenv("SECRETS_KEY"); k != "" {
		key, err := DecodeKey(k)
		if err != nil {
			p.errs = append(p.errs, err)
		}
		c.SecretsKey = key
	}
	return c, errors.Join(p.errs...)
}

// Validate checks what the mode needs.
func (c *Config) Validate(mode string) error {
	var errs []error
	need := func(k, v string) {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", k))
		}
	}
	need("DATABASE_URL", c.DatabaseURL)
	if mode == "migrate" {
		return errors.Join(errs...)
	}
	if len(c.SecretsKey) == 0 {
		errs = append(errs, errors.New("SECRETS_KEY is required"))
	}
	if mode == "relay" {
		return errors.Join(errs...)
	}
	if len(c.KafkaBrokers) == 0 && mode != "cleaner" {
		errs = append(errs, errors.New("KAFKA_BROKERS is required"))
	}
	need("S3_ENDPOINT", c.S3Endpoint)
	if mode == "api" {
		switch c.AuthProvider {
		case "oidc":
			need("OIDC_ISSUER", c.OIDCIssuer)
			need("OIDC_CLIENT_ID", c.OIDCClientID)
			need("OIDC_CLIENT_SECRET", c.OIDCClientSecret)
		case "github":
			need("GITHUB_CLIENT_ID", c.GitHubClientID)
			need("GITHUB_CLIENT_SECRET", c.GitHubClientSecret)
		default:
			errs = append(errs, fmt.Errorf("AUTH_PROVIDER must be oidc or github, not %q", c.AuthProvider))
		}
	}
	if mode == "worker" {
		need("AGENT_SERVICE_TOKEN", c.AgentServiceToken)
		if c.SandboxExecutor == "k8s" {
			need("SANDBOX_IMAGE", c.SandboxImage)
		} else if c.SandboxExecutor != "none" {
			errs = append(errs, fmt.Errorf("SANDBOX_EXECUTOR must be k8s or none"))
		}
	}
	return errors.Join(errs...)
}
