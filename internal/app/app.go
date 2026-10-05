// Package app assembles dependencies and runs the modes of the nabu binary
// (FTR.NAB.CMN-0001 arch §3): api, worker, agent, relay, sandbox, migrate,
// cleaner.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/sync/errgroup"

	"github.com/GreenOnGrey/nabu-core/internal/admin"
	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/auth"
	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/channels"
	"github.com/GreenOnGrey/nabu-core/internal/chat"
	"github.com/GreenOnGrey/nabu-core/internal/clients"
	"github.com/GreenOnGrey/nabu-core/internal/config"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/engine"
	"github.com/GreenOnGrey/nabu-core/internal/importer"
	"github.com/GreenOnGrey/nabu-core/internal/ledger"
	"github.com/GreenOnGrey/nabu-core/internal/memory"
	"github.com/GreenOnGrey/nabu-core/internal/models"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/operator"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/k8s"
	"github.com/GreenOnGrey/nabu-core/internal/platform/kafka"
	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
	"github.com/GreenOnGrey/nabu-core/internal/platform/storage"
	"github.com/GreenOnGrey/nabu-core/internal/platform/whisper"
	"github.com/GreenOnGrey/nabu-core/internal/relay"
	"github.com/GreenOnGrey/nabu-core/internal/services"
	"github.com/GreenOnGrey/nabu-core/internal/space"
	"github.com/GreenOnGrey/nabu-core/internal/tasks"
	"github.com/GreenOnGrey/nabu-core/internal/users"
)

// core holds the dependencies shared by api and worker.
type core struct {
	cfg      *config.Config
	pool     *pgxpool.Pool
	s3       storage.Storage
	box      *crypto.Box
	signer   *jwt.Signer
	events   *events.PGPublisher
	operator *agent.Client
	users    *users.Repo
	models   *models.Service
	catalog  *catalog.Service
	memory   *memory.Service
	tasks    *tasks.Service
	space    *space.Service
	chat     *chat.Store
	services *services.Service
	ledger   *ledger.Ledger
	links    *channels.Links
	clients  *clients.Service
	producer *kafka.Producer
}

func newCore(ctx context.Context, cfg *config.Config) (*core, error) {
	pool, err := postgres.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	box, err := crypto.NewBox(cfg.SecretsKey)
	if err != nil {
		return nil, err
	}
	s3, err := storage.NewS3(ctx, cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Bucket, cfg.S3UseSSL)
	if err != nil {
		return nil, fmt.Errorf("s3: %w", err)
	}
	signer := jwt.NewSigner(cfg.PublicAPIURL, cfg.SecretsKey)
	pub := events.NewPGPublisher(pool)
	producer := kafka.NewProducer(cfg.KafkaBrokers)
	op := &agent.Client{BaseURL: cfg.AgentAddr, Token: cfg.AgentServiceToken}
	c := &core{cfg: cfg, pool: pool, s3: s3, box: box, signer: signer, events: pub, operator: op, producer: producer}
	c.users = &users.Repo{Pool: pool, DefaultLanguage: cfg.DefaultLanguage, DefaultTimezone: cfg.DefaultTimezone, SpaceQuota: cfg.SpaceQuota}
	c.models = &models.Service{Pool: pool, Box: box, Check: func(ctx context.Context, req agent.LLMCheckRequest) (*agent.LLMCheckResponse, error) {
		r, err := op.CheckLLM(ctx, req)
		return &r, err
	}}
	c.catalog = &catalog.Service{Pool: pool, Box: box, S3: s3, Signer: signer, Events: pub, InternalURL: cfg.InternalURL,
		PublicAPIURL: cfg.PublicAPIURL, WebURL: cfg.PublicWebURL, GitToken: os.Getenv("SKILLS_GIT_TOKEN"),
		CheckMCP: func(ctx context.Context, req agent.MCPCheckRequest) (*agent.MCPCheckResponse, error) {
			r, err := op.CheckMCP(ctx, req)
			return &r, err
		}}
	c.memory = &memory.Service{Pool: pool, Events: pub}
	c.tasks = &tasks.Service{Pool: pool, Events: pub, Bus: producer, Rules: tasks.Rules{MinInterval: cfg.TaskMinInterval, MaxActive: cfg.TaskMaxActive},
		Catchup: cfg.TaskCatchupWindow, Tick: cfg.TaskTick, MaxFailures: cfg.TaskMaxFailures, DefaultTimezone: cfg.DefaultTimezone, VKWS: cfg.VKWSEnabled}
	c.space = &space.Service{Pool: pool, S3: s3, Signer: signer, Events: pub, Quota: cfg.SpaceQuota, MaxFile: cfg.UploadMaxBytes,
		Enabled: cfg.SandboxExecutor == "k8s"}
	c.chat = &chat.Store{Pool: pool}
	c.services = &services.Service{Pool: pool, Models: c.models, Signer: signer, Bus: producer, Events: pub,
		RelayPublicURL: cfg.RelayPublicWSURL, PublicAPIURL: cfg.PublicAPIURL, WorkspaceTTL: 12 * time.Hour,
		Harnesses: []services.Harness{{Name: "pi", Version: cfg.PiVersion, Status: "ok"}}}
	c.ledger = &ledger.Ledger{Pool: pool}
	enabled := map[string]bool{domain.ChannelTelegram: cfg.TelegramBotToken != "", domain.ChannelVKWS: cfg.VKWSEnabled}
	c.links = &channels.Links{Pool: pool, Events: pub, Enabled: enabled, BotNames: map[string]string{}}
	c.clients = &clients.Service{Pool: pool, Signer: signer}
	return c, nil
}

func (c *core) close() {
	_ = c.producer.Close()
	c.pool.Close()
}

func (c *core) ready(ctx context.Context) error {
	if err := c.pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	if err := kafka.Ping(ctx, c.cfg.KafkaBrokers); err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	return nil
}

// serviceServer exposes /healthz, /readyz and /metrics on the service port.
func serviceServer(addr string, ready func(context.Context) error) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := ready(ctx); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
}

// serve runs servers until ctx is done, then shuts them down gracefully.
func serve(ctx context.Context, g *errgroup.Group, servers ...*http.Server) {
	for _, s := range servers {
		s := s
		g.Go(func() error {
			slog.Info("listening", "addr", s.Addr)
			if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		})
		g.Go(func() error {
			<-ctx.Done()
			sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			return s.Shutdown(sctx)
		})
	}
}

func (c *core) provider() auth.Provider {
	if c.cfg.AuthProvider == "oidc" {
		return &auth.OIDC{Issuer: c.cfg.OIDCIssuer, ClientID: c.cfg.OIDCClientID, Secret: c.cfg.OIDCClientSecret, Scopes: c.cfg.OIDCScopes, Name: c.cfg.OIDCProviderName}
	}
	return &auth.GitHub{BaseURL: c.cfg.GitHubBaseURL, APIURL: c.cfg.GitHubAPIURL, ClientID: c.cfg.GitHubClientID, Secret: c.cfg.GitHubClientSecret,
		AllowedOrg: c.cfg.GitHubAllowedOrg}
}

// PublicConfig is GET /api/v1/config.
type PublicConfig struct {
	AuthProvider    string          `json:"authProvider"`
	ProviderLabel   string          `json:"providerLabel"`
	AllowedOrg      string          `json:"allowedOrg,omitempty"`
	Languages       []string        `json:"languages"`
	DefaultLanguage string          `json:"defaultLanguage"`
	UploadMaxBytes  int64           `json:"uploadMaxBytes"`
	Voice           bool            `json:"voice"`
	Spaces          bool            `json:"spaces"`
	Channels        map[string]bool `json:"channels"`
	Version         string          `json:"version"`
	// BootstrapAdminsConfigured lets deploy checks confirm the first administrator.
	BootstrapAdminsConfigured bool `json:"bootstrapAdminsConfigured"`
}

// RunAPI serves the public API (:8080) and the internal API (:8081).
func RunAPI(ctx context.Context, cfg *config.Config, version string) error {
	c, err := newCore(ctx, cfg)
	if err != nil {
		return err
	}
	defer c.close()
	if err := kafka.EnsureTopics(ctx, cfg.KafkaBrokers, kafka.AllTopics...); err != nil {
		slog.Warn("could not ensure kafka topics (auto-creation will be used)", "err", err)
	}
	hub := events.NewHub()
	go hub.Listen(ctx, c.pool)
	if err := c.models.Bootstrap(ctx, cfg.BootstrapLLMType, cfg.BootstrapLLMURL, cfg.BootstrapLLMKey); err != nil {
		slog.Error("bootstrap model connection", "err", err)
	}
	if err := c.catalog.StarterCatalog(ctx); err != nil {
		slog.Error("starter catalog", "err", err)
	}

	var transcriber whisper.Transcriber
	if cfg.WhisperURL != "" {
		transcriber = whisper.New(cfg.WhisperURL)
	}
	chatAPI := &chat.API{Store: c.chat, Pool: c.pool, Users: c.users, Models: c.models, Links: c.links, Bus: c.producer, Events: c.events,
		S3: c.s3, Whisper: transcriber, MaxFile: cfg.UploadMaxBytes}

	var tg *channels.Telegram
	if cfg.TelegramBotToken != "" {
		tg = channels.NewTelegram(cfg.TelegramAPIURL, cfg.TelegramBotToken, cfg.TelegramWebhookSecret)
		sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := tg.Setup(sctx, cfg.PublicAPIURL); err != nil {
			slog.Error("telegram webhook setup failed", "err", err)
		} else {
			c.links.BotNames[domain.ChannelTelegram] = tg.Username()
			slog.Info("telegram webhook registered", "bot", tg.Username())
		}
		cancel()
	}

	bootstrap := map[string]bool{}
	for _, e := range cfg.BootstrapAdmins {
		bootstrap[domain.NormalizeEmail(e)] = true
	}
	prov := c.provider()
	authH := &auth.Handlers{Provider: prov, Users: c.users, PublicAPIURL: cfg.PublicAPIURL, WebURL: cfg.PublicWebURL,
		CookieDomain: cfg.CookieDomain, Secure: strings.HasPrefix(cfg.PublicAPIURL, "https://"), BootstrapAdmins: bootstrap}
	pubCfg := PublicConfig{AuthProvider: prov.Kind(), ProviderLabel: prov.Label(), AllowedOrg: cfg.GitHubAllowedOrg, Languages: domain.Languages,
		DefaultLanguage: cfg.DefaultLanguage, UploadMaxBytes: cfg.UploadMaxBytes, Voice: transcriber != nil, Spaces: c.space.Enabled,
		Channels: c.links.Enabled, Version: version, BootstrapAdminsConfigured: len(cfg.BootstrapAdmins) > 0}
	if prov.Kind() == "oidc" {
		pubCfg.AllowedOrg = ""
	}
	adminUsers := &admin.Users{Repo: c.users}
	imp := &importer.Importer{Models: c.models, Catalog: c.catalog, Services: c.services}

	r := chi.NewRouter()
	r.Use(httpx.Observe)
	r.Use(httpx.CORS(cfg.CORSAllowedOrigins))
	authH.Routes(r)
	r.Get("/.well-known/jwks.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=600")
		httpx.JSON(w, 200, c.signer.JWKS())
	})
	c.clients.TokenRoute(r)
	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/config", func(w http.ResponseWriter, _ *http.Request) { httpx.JSON(w, 200, pubCfg) })
		c.catalog.CallbackRoute(r)
		r.Group(func(r chi.Router) {
			r.Use(authH.Authenticate, auth.RequireSession)
			r.Get("/session", func(w http.ResponseWriter, r *http.Request) { httpx.JSON(w, 200, map[string]bool{"ok": true}) })
			r.Group(func(r chi.Router) {
				r.Use(auth.RequireActive)
				chatAPI.Routes(r, true)
				c.memory.Routes(r)
				c.tasks.Routes(r)
				c.space.Routes(r)
				c.catalog.Routes(r)
				c.links.Routes(r)
				c.ledger.UserRoutes(r)
				r.Get("/events", events.SSEHandler(hub))
			})
		})
	})
	r.Route("/admin/api/v1", func(r chi.Router) {
		r.Use(authH.Authenticate, auth.RequireSession, auth.RequireAdmin)
		adminUsers.Routes(r)
		c.models.AdminRoutes(r)
		c.catalog.AdminRoutes(r, cfg.SecretsKey)
		c.services.AdminRoutes(r)
		c.clients.AdminRoutes(r)
		c.ledger.AdminRoutes(r)
	})
	c.services.EventsRoute(r, hub, func(r *http.Request) (uuid.UUID, bool) {
		cl, err := c.signer.Verify(httpx.Bearer(r), jwt.AudClient)
		if err != nil {
			return uuid.Nil, false
		}
		id, err := uuid.Parse(strings.TrimPrefix(cl.Subject, "client:"))
		return id, err == nil
	})
	r.Route("/client/v1", func(r chi.Router) {
		r.Use(auth.ClientAuth(c.signer, c.clients.Load))
		c.services.ClientRoutes(r)
		imp.Route(r)
		r.Route("/me", func(r chi.Router) {
			r.Use(auth.Delegation(c.users))
			chatAPI.Routes(r, false)
			r.Get("/events", events.SSEHandler(hub))
		})
	})
	if tg != nil {
		wh := &channels.Webhook{Bot: tg, Links: c.links, Inbox: chatAPI, Attachments: chatAPI.Attachments(), MaxFile: cfg.UploadMaxBytes, WebURL: cfg.PublicWebURL}
		wh.Route(r)
	}
	c.catalog.HookRoute(r, cfg.SecretsKey)
	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		httpx.Error(w, r, apperr.NotFound("not_found", "not found"))
	})

	builtin := mcp.NewServer(c.signer)
	builtin.Register(c.memory.Tools()...)
	builtin.Register(c.space.Tool())
	builtin.Register(c.tasks.Tools()...)
	builtin.Allowed = c.users.Allowed
	builtin.ChannelOf = c.chat.LastUserChannel
	internal := chi.NewRouter()
	internal.Use(httpx.Observe)
	internal.Handle("/internal/v1/mcp", builtin)
	c.catalog.ProxyRoutes(internal, func(r *http.Request, uid uuid.UUID) (string, bool) {
		u, err := c.users.Get(r.Context(), uid)
		if err != nil || u == nil || u.Status == "blocked" {
			return "", false
		}
		return u.Email, true
	})
	c.space.InternalRoutes(internal)

	api := &http.Server{Addr: cfg.HTTPAddr, Handler: otelhttp.NewHandler(r, "http"), ReadHeaderTimeout: 15 * time.Second}
	internalHTTP := &http.Server{Addr: cfg.InternalAddr, Handler: otelhttp.NewHandler(internal, "internal"), ReadHeaderTimeout: 15 * time.Second}
	g, gctx := errgroup.WithContext(ctx)
	serve(gctx, g, api, internalHTTP, serviceServer(cfg.ServiceAddr, c.ready))
	return g.Wait()
}

// RunWorker consumes the topics, runs the scheduler, the sandbox reaper and
// the periodic maintenance.
func RunWorker(ctx context.Context, cfg *config.Config) error {
	c, err := newCore(ctx, cfg)
	if err != nil {
		return err
	}
	defer c.close()
	if err := kafka.EnsureTopics(ctx, cfg.KafkaBrokers, kafka.AllTopics...); err != nil {
		slog.Warn("could not ensure kafka topics", "err", err)
	}
	relayStore := &space.RelayStore{Pool: c.pool, Events: c.events}
	var sandboxes *space.Sandboxes
	if cfg.SandboxExecutor == "k8s" {
		sandboxes = &space.Sandboxes{Pool: c.pool, K8s: k8s.NewInCluster(), Space: c.space, Events: c.events, Cfg: space.SandboxConfig{
			Namespace: cfg.SandboxNamespace, Image: cfg.SandboxImage, CPU: cfg.SandboxCPU, Memory: cfg.SandboxMemory, Quota: cfg.SpaceQuota,
			IdleTimeout: cfg.SandboxIdleTimeout, RelayURL: cfg.RelaySandboxWSURL, APIURL: cfg.InternalURL, Exclude: cfg.SandboxExclude, TokenTTL: 7 * 24 * time.Hour}}
	}
	adapters := map[string]channels.Adapter{}
	if cfg.TelegramBotToken != "" {
		adapters[domain.ChannelTelegram] = channels.NewTelegram(cfg.TelegramAPIURL, cfg.TelegramBotToken, cfg.TelegramWebhookSecret)
	}
	eng := &engine.Engine{
		Cfg: engine.Config{InternalURL: cfg.InternalURL, RelayInternalURL: cfg.RelayInternalURL, TurnTimeout: cfg.TurnTimeout,
			TaskRunTimeout: cfg.TaskRunTimeout, RunWorkspaceWait: cfg.RunWorkspaceWait, SessionMaxAge: 12 * time.Hour, PiVersion: cfg.PiVersion},
		Pool: c.pool, Op: c.operator, Users: c.users, Chat: c.chat, Models: c.models, Catalog: c.catalog, Memory: c.memory, Tasks: c.tasks,
		Space: c.space, Sandboxes: sandboxes, Services: c.services, Ledger: c.ledger, Signer: c.signer, S3: c.s3, Events: c.events,
		Bus: c.producer, Links: c.links, Adapters: adapters,
		Connected: func(ctx context.Context, id string) bool {
			_, ok, _ := relayStore.Lookup(ctx, id)
			return ok
		},
	}
	g, gctx := errgroup.WithContext(ctx)
	serve(gctx, g, serviceServer(cfg.ServiceAddr, c.ready))
	g.Go(func() error {
		return kafka.ConsumeParallel(gctx, cfg.KafkaBrokers, "nabu-worker", kafka.TopicInbound, cfg.InboundWorkers, eng.HandleInbound)
	})
	g.Go(func() error {
		return kafka.Consume(gctx, cfg.KafkaBrokers, "nabu-worker", kafka.TopicOutbound, eng.HandleOutbound)
	})
	g.Go(func() error {
		return kafka.ConsumeParallel(gctx, cfg.KafkaBrokers, "nabu-worker", kafka.TopicTaskRun, 2, eng.HandleTaskRun)
	})
	g.Go(func() error {
		return kafka.ConsumeParallel(gctx, cfg.KafkaBrokers, "nabu-worker", kafka.TopicRuns, 2, eng.HandleRun)
	})
	g.Go(func() error { c.tasks.Run(gctx); return nil })
	if sandboxes != nil {
		g.Go(func() error { sandboxes.Run(gctx); return nil })
	}
	g.Go(func() error {
		maintenance := func() {
			if ids, err := c.chat.ArchiveIdle(gctx, cfg.TopicArchiveAfter); err != nil {
				slog.Warn("archive topics", "err", err)
			} else {
				for _, uid := range ids {
					uid := uid
					c.events.Publish(gctx, events.Event{Type: events.ConversationUpdated, UserID: &uid, Data: map[string]any{}})
				}
			}
			if err := c.ledger.Partitions(gctx, cfg.AuditRetention); err != nil {
				slog.Warn("audit partitions", "err", err)
			}
		}
		maintenance()
		t := time.NewTicker(time.Hour)
		defer t.Stop()
		n := 0
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-t.C:
				maintenance()
				if n++; n%6 == 0 {
					c.catalog.SyncAll(gctx) // git skill sources, besides the push webhook
				}
			}
		}
	})
	return g.Wait()
}

// RunRelay serves the relay (:8085): the WebSocket of workspaces and the
// calls of the agent operator.
func RunRelay(ctx context.Context, cfg *config.Config) error {
	pool, err := postgres.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	pod := os.Getenv("POD_IP")
	if pod == "" {
		pod, _ = os.Hostname()
	}
	port := cfg.RelayAddr
	if i := strings.LastIndex(port, ":"); i >= 0 {
		port = port[i:]
	}
	srv := relay.NewServer(relay.Config{Pod: pod + port}, jwt.NewSigner(cfg.PublicAPIURL, cfg.SecretsKey),
		&space.RelayStore{Pool: pool, Events: events.NewPGPublisher(pool)})
	r := chi.NewRouter()
	r.Use(httpx.Observe)
	srv.Routes(r)
	g, gctx := errgroup.WithContext(ctx)
	serve(gctx, g, &http.Server{Addr: cfg.RelayAddr, Handler: r, ReadHeaderTimeout: 15 * time.Second},
		serviceServer(cfg.ServiceAddr, func(ctx context.Context) error { return pool.Ping(ctx) }))
	return g.Wait()
}

// RunOperator serves the agent operator: the internal API and health.
func RunOperator(ctx context.Context, op *operator.Operator, listenAddr, serviceAddr string, piCommand []string) error {
	ready := func(context.Context) error {
		if len(piCommand) == 0 {
			return errors.New("PI_BINARY is empty")
		}
		if _, err := exec.LookPath(piCommand[0]); err != nil {
			return fmt.Errorf("pi: %w", err)
		}
		return nil
	}
	srv := &http.Server{Addr: listenAddr, Handler: otelhttp.NewHandler(op.Handler(), "agent"), ReadHeaderTimeout: 15 * time.Second}
	g, gctx := errgroup.WithContext(ctx)
	serve(gctx, g, srv, serviceServer(serviceAddr, ready))
	g.Go(func() error { op.Run(gctx); return nil })
	return g.Wait()
}

// RunMigrate applies database migrations.
func RunMigrate(ctx context.Context, cfg *config.Config) error {
	return postgres.Migrate(ctx, cfg.DatabaseURL)
}

// RunCleaner removes expired data in one pass (a CronJob).
func RunCleaner(ctx context.Context, cfg *config.Config) error {
	pool, err := postgres.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	for name, q := range map[string]string{
		"sessions":    `DELETE FROM user_sessions WHERE expires_at < now()`,
		"link codes":  `DELETE FROM link_codes WHERE expires_at < now() - interval '1 day'`,
		"oauth":       `DELETE FROM oauth_states WHERE expires_at < now()`,
		"run events":  `DELETE FROM run_events WHERE created_at < now() - interval '30 days'`,
		"connections": `DELETE FROM workspace_connections WHERE last_ping_at < now() - interval '10 minutes'`,
		"harness":     `DELETE FROM harness_sessions WHERE closed_at < now() - interval '30 days'`,
	} {
		tag, err := pool.Exec(ctx, q)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		slog.Info("cleaned", "what", name, "rows", tag.RowsAffected())
	}
	return (&ledger.Ledger{Pool: pool}).Partitions(ctx, cfg.AuditRetention)
}
