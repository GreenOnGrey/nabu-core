// Package app assembles dependencies and runs the modes of the nabu binary
// (FTR.NAB.CMN-0001 arch §3): api, worker, agent, relay, sandbox, migrate,
// cleaner.
package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/robfig/cron/v3"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/sync/errgroup"

	"github.com/GreenOnGrey/nabu-core/internal/access"
	"github.com/GreenOnGrey/nabu-core/internal/accounts"
	"github.com/GreenOnGrey/nabu-core/internal/admin"
	"github.com/GreenOnGrey/nabu-core/internal/agentpods"
	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/auth"
	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/channels"
	"github.com/GreenOnGrey/nabu-core/internal/channels/email"
	"github.com/GreenOnGrey/nabu-core/internal/chat"
	"github.com/GreenOnGrey/nabu-core/internal/clients"
	"github.com/GreenOnGrey/nabu-core/internal/collect"
	"github.com/GreenOnGrey/nabu-core/internal/comm"
	"github.com/GreenOnGrey/nabu-core/internal/config"
	"github.com/GreenOnGrey/nabu-core/internal/confirm"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/engine"
	"github.com/GreenOnGrey/nabu-core/internal/groups"
	"github.com/GreenOnGrey/nabu-core/internal/importer"
	"github.com/GreenOnGrey/nabu-core/internal/ledger"
	"github.com/GreenOnGrey/nabu-core/internal/memory"
	"github.com/GreenOnGrey/nabu-core/internal/models"
	"github.com/GreenOnGrey/nabu-core/internal/models/gigachat"
	"github.com/GreenOnGrey/nabu-core/internal/pkgsignals"
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
	"github.com/GreenOnGrey/nabu-core/internal/preview"
	"github.com/GreenOnGrey/nabu-core/internal/relay"
	"github.com/GreenOnGrey/nabu-core/internal/sandboxprofile"
	"github.com/GreenOnGrey/nabu-core/internal/services"
	"github.com/GreenOnGrey/nabu-core/internal/space"
	"github.com/GreenOnGrey/nabu-core/internal/tasks"
	"github.com/GreenOnGrey/nabu-core/internal/users"
)

// core holds the dependencies shared by api and worker.
type core struct {
	cfg       *config.Config
	pool      *pgxpool.Pool
	s3        storage.Storage
	box       *crypto.Box
	signer    *jwt.Signer
	events    *events.PGPublisher
	operator  *agent.Client
	users     *users.Repo
	models    *models.Service
	catalog   *catalog.Service
	memory    *memory.Service
	comm      *comm.Service
	tasks     *tasks.Service
	space     *space.Service
	snapshots *space.Snapshots
	// FTR.NAB.CMN-0008: sandbox profiles and requests of libraries.
	profiles *sandboxprofile.Service
	pkgs     *pkgsignals.Service
	chat     *chat.Store
	services *services.Service
	ledger   *ledger.Ledger
	clients  *clients.Service
	producer *kafka.Producer
	// FTR.NAB.CMN-0002: channels, group agents, accounts.
	registry *channels.Registry
	access   *access.Service
	keys     *channels.Keys
	chanAPI  *channels.API
	tg       *channels.Telegram
	vk       *channels.VKTeams
	mail     *email.Mail
	groups   *groups.Service
	accounts *accounts.Service
	chatAPI  *chat.API
	// api: this process registers the Telegram webhook.
	api bool
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
	}, Signer: signer, InternalURL: cfg.InternalURL, AutoRefresh: true}
	// FTR.NAB.CHAT-0001 R13: GigaChat through the bridge of api
	if gc, err := gigachat.New(cfg.GigaChatCAFile); err != nil {
		return nil, err
	} else {
		c.models.GigaChat = gc
	}
	c.catalog = &catalog.Service{Pool: pool, Box: box, S3: s3, Signer: signer, Events: pub, InternalURL: cfg.InternalURL,
		PublicAPIURL: cfg.PublicAPIURL, WebURL: cfg.PublicWebURL, GitToken: os.Getenv("SKILLS_GIT_TOKEN"),
		CheckMCP: func(ctx context.Context, req agent.MCPCheckRequest) (*agent.MCPCheckResponse, error) {
			r, err := op.CheckMCP(ctx, req)
			return &r, err
		}}
	c.memory = &memory.Service{Pool: pool, Events: pub}
	c.comm = &comm.Service{Pool: pool, Events: pub}
	c.comm.Embedder = func(ctx context.Context) *comm.Embedder {
		// FTR.NAB.CMN-0003 R20: the model of embeddings is a model of a connection
		st, err := c.comm.LoadSettings(ctx)
		if err != nil || st.Embeddings == nil {
			return nil
		}
		id, err := uuid.Parse(st.Embeddings.ConnectionID)
		if err != nil {
			return nil
		}
		spec, key, err := c.models.Resolve(ctx, models.Choice{ConnectionID: id, Model: st.Embeddings.Model})
		if err != nil {
			return nil
		}
		return &comm.Embedder{BaseURL: spec.BaseURL, Key: key, Model: spec.ModelID}
	}
	c.models.Embed = func(ctx context.Context, baseURL, key, model string) error {
		_, err := (&comm.Embedder{BaseURL: baseURL, Key: key, Model: model}).Embed(ctx, []string{"ping"})
		return err
	}
	c.comm.EnsureVectors(ctx)
	c.catalog.SetUserEmail(c.userEmail)
	c.catalog.MemoryRun = c.comm.ProxyGuard // FTR.NAB.CMN-0003 tech §6.4
	c.tasks = &tasks.Service{Pool: pool, Events: pub, Bus: producer, Rules: tasks.Rules{MinInterval: cfg.TaskMinInterval, MaxActive: cfg.TaskMaxActive},
		Catchup: cfg.TaskCatchupWindow, Tick: cfg.TaskTick, MaxFailures: cfg.TaskMaxFailures, DefaultTimezone: cfg.DefaultTimezone}
	// FTR.NAB.CMN-0003 R13, R21: the collection needs the consent and connected sources
	c.tasks.MemoryGate = func(ctx context.Context, uid uuid.UUID, kind string, settings []byte) error {
		if kind != tasks.KindCollect {
			return nil
		}
		return c.comm.CheckCollect(ctx, uid, settings)
	}
	c.space = &space.Service{Pool: pool, S3: s3, Signer: signer, Events: pub, Quota: cfg.SpaceQuota, MaxFile: cfg.UploadMaxBytes,
		Enabled: cfg.SandboxExecutor == "k8s"}
	c.profiles = &sandboxprofile.Service{Pool: pool, Cfg: sandboxprofile.Config{Profiles: cfg.SandboxProfiles, Defaults: cfg.SandboxDefaults,
		LegacyImage: cfg.SandboxImage, CustomImages: cfg.SandboxCustomImages, Prefixes: cfg.SandboxImagePrefixes, ProfilesDir: "/opt/nabu/profiles"}}
	c.pkgs = &pkgsignals.Service{Pool: pool, Profiles: c.profiles, Signer: signer}
	if c.space.Enabled {
		c.space.Profiles = c.profiles
	}
	// FTR.NAB.CMN-0006 R14: file arguments of catalog MCP servers
	c.snapshots = &space.Snapshots{Space: c.space, Relay: &space.RelayStore{Pool: pool, Events: pub}}
	if c.space.Enabled {
		c.snapshots.RelayURL = cfg.RelayInternalURL
	}
	c.catalog.Files = c.snapshots.Snapshot
	c.catalog.Results = c.snapshots // FTR.NAB.CHAT-0003 R10
	c.chat = &chat.Store{Pool: pool}
	c.services = &services.Service{Pool: pool, Models: c.models, Signer: signer, Bus: producer, Events: pub,
		RelayPublicURL: cfg.RelayPublicWSURL, PublicAPIURL: cfg.PublicAPIURL, WorkspaceTTL: 12 * time.Hour,
		Harnesses: []services.Harness{{Name: "pi", Version: cfg.PiVersion, Status: "ok"}}, ProfileReady: c.profiles.Ready}
	c.ledger = &ledger.Ledger{Pool: pool}
	c.clients = &clients.Service{Pool: pool, Signer: signer}

	pepper := cfg.KeyPepper
	if len(pepper) == 0 {
		pepper = channels.DerivePepper(cfg.SecretsKey)
	}
	c.registry = &channels.Registry{Pool: pool, Box: box, Events: pub}
	// FTR.NAB.CMN-0007: the access of users by groups
	c.access = &access.Service{Pool: pool, Events: pub}
	c.registry.Granted = c.access.Channels
	c.models.Access = c.access.Effective
	c.catalog.Access = c.access
	c.registry.Changed = c.configureChannel
	c.keys = &channels.Keys{Pool: pool, Pepper: pepper, Events: pub}
	c.tg = channels.NewTelegram(cfg.TelegramAPIURL, "", "")
	c.tg.TableMaxCols = cfg.TGTableMaxCols
	c.vk = channels.NewVKTeams()
	c.chatAPI = &chat.API{Store: c.chat, Pool: pool, Users: c.users, Models: c.models, Bus: producer, Events: pub, S3: s3, MaxFile: cfg.UploadMaxBytes,
		// FTR.NAB.CHAT-0001 R20: 5 minutes of opus at up to 192 kbit/s
		MaxVoiceSeconds: cfg.TranscribeMaxSeconds}
	if cfg.WhisperURL != "" { // the site (api) and the voice messages of messengers (api, worker)
		c.chatAPI.Whisper = whisper.New(cfg.WhisperURL)
	}
	c.chatAPI.Skills = func(ctx context.Context, uid uuid.UUID) ([]chat.Skill, error) {
		return ChatSkills(ctx, c.catalog, c.groups, uid)
	}
	c.accounts = &accounts.Service{Pool: pool, S3: s3, Events: pub, Ledger: c.ledger}
	c.groups = &groups.Service{Pool: pool, Registry: c.registry, Users: c.users, Inbox: c.chatAPI,
		RetentionDays: c.accounts.Retention, DeleteData: c.accounts.DeleteUserData}
	c.accounts.Groups = c.groups
	c.mail = &email.Mail{Pool: pool, Registry: c.registry, Inbox: c.chatAPI, Attachments: c.chatAPI.Attachments(), Events: pub,
		WebURL: cfg.PublicWebURL, ReplyLimit: cfg.EmailReplyLimit, AttachMax: cfg.EmailAttachMax, InboundMax: cfg.EmailInboundMax,
		Accounts: func(ctx context.Context, addr string) (*email.Account, error) {
			u, err := c.users.ByEmail(ctx, addr)
			if err != nil || u == nil {
				return nil, err
			}
			return &email.Account{ID: u.ID, Status: u.Status, Language: u.Language}, nil
		}}
	c.chanAPI = &channels.API{Registry: c.registry, Keys: c.keys,
		Validators: map[string]channels.Validator{domain.ChannelTelegram: channels.ValidateTelegram, domain.ChannelVKTeams: channels.ValidateVKTeams,
			domain.ChannelEmail: email.Validate},
		Checkers: map[string]channels.Checker{
			domain.ChannelTelegram: c.checkTelegram,
			domain.ChannelVKTeams:  c.checkVKTeams,
			domain.ChannelEmail:    c.mail.Check,
		}}
	c.chatAPI.Channels = c.chanAPI.Mine
	// results of tasks go only to channels that can deliver now (R5)
	c.tasks.Deliverable = (&engine.Engine{Pool: pool, Users: c.users, Registry: c.registry, Keys: c.keys, Groups: c.groups}).Deliverable
	return c, nil
}

// seedChannels moves the Telegram configuration of FTR.NAB.CMN-0001 from the
// environment into the channel (once) and configures the adapters.
func (c *core) seedChannels(ctx context.Context) {
	if c.cfg.TelegramBotToken != "" {
		if err := c.registry.SeedSecrets(ctx, domain.ChannelTelegram, map[string]string{"botToken": c.cfg.TelegramBotToken,
			"webhookSecret": c.cfg.TelegramWebhookSecret}); err != nil {
			slog.Error("seed telegram channel", "err", err)
		}
	}
	for _, k := range []string{domain.ChannelTelegram, domain.ChannelVKTeams} {
		c.configureChannel(ctx, k)
	}
}

func randomSecret() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// configureChannel applies the settings of a channel to its adapter; it runs
// at start and whenever an administrator changes the channel (any pod).
func (c *core) configureChannel(ctx context.Context, kind string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	switch kind {
	case domain.ChannelTelegram:
		sec, enabled, err := c.registry.Config(ctx, kind, nil)
		if err != nil {
			slog.Error("telegram channel settings", "err", err)
			return
		}
		token := sec["botToken"]
		if token != "" && sec["webhookSecret"] == "" && c.api {
			// a bot set in the administration gets its webhook secret here
			if _, err := c.registry.Update(ctx, kind, channels.Patch{Secrets: map[string]string{"webhookSecret": randomSecret()}}, nil); err != nil {
				slog.Error("telegram webhook secret", "err", err)
			}
			return // the update configures the channel again
		}
		c.tg.Configure(token, sec["webhookSecret"])
		if token == "" || !enabled || !c.api {
			return
		}
		if err := c.tg.Setup(ctx, c.cfg.PublicAPIURL); err != nil {
			slog.Error("telegram webhook setup failed", "err", err)
			c.registry.SetStatus(ctx, kind, "error", err.Error())
			return
		}
		c.registry.SetSetting(ctx, kind, "username", c.tg.Username())
		c.registry.SetStatus(ctx, kind, "ok", "")
		slog.Info("telegram webhook registered", "bot", c.tg.Username())
	case domain.ChannelVKTeams:
		var st channels.VKTeamsSettings
		sec, enabled, err := c.registry.Config(ctx, kind, &st)
		if err != nil {
			slog.Error("vkteams channel settings", "err", err)
			return
		}
		c.vk.Configure(st.APIURL, sec["token"], st.BotID)
		if st.APIURL == "" || sec["token"] == "" || !enabled {
			return
		}
		info, err := c.vk.Me(ctx)
		if err != nil {
			c.registry.SetStatus(ctx, kind, "error", err.Error())
			return
		}
		c.registry.SetSetting(ctx, kind, "botNick", info.Nick)
		c.registry.SetSetting(ctx, kind, "botId", info.ID)
		c.registry.SetStatus(ctx, kind, "ok", "")
	case domain.ChannelEmail:
		c.mail.Reset()
	}
}

// checkTelegram checks the bot with the stored token: the adapter of this
// pod may not have re-read the channel yet (the change arrives as an event).
func (c *core) checkTelegram(ctx context.Context) error {
	sec, _, err := c.registry.Config(ctx, domain.ChannelTelegram, nil)
	if err != nil {
		return err
	}
	c.tg.Configure(sec["botToken"], sec["webhookSecret"])
	_, _, err = c.tg.Me(ctx)
	return err
}

// checkVKTeams checks the bot with the stored settings, like checkTelegram.
func (c *core) checkVKTeams(ctx context.Context) error {
	var st channels.VKTeamsSettings
	sec, _, err := c.registry.Config(ctx, domain.ChannelVKTeams, &st)
	if err != nil {
		return err
	}
	c.vk.Configure(st.APIURL, sec["token"], st.BotID)
	_, err = c.vk.Me(ctx)
	return err
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
	AuthProvider    string   `json:"authProvider"`
	ProviderLabel   string   `json:"providerLabel"`
	AllowedOrg      string   `json:"allowedOrg,omitempty"`
	Languages       []string `json:"languages"`
	DefaultLanguage string   `json:"defaultLanguage"`
	UploadMaxBytes  int64    `json:"uploadMaxBytes"`
	Voice           bool     `json:"voice"`
	// VoiceMaxSeconds bounds a recording of the site (FTR.NAB.CHAT-0001 R20).
	VoiceMaxSeconds int  `json:"voiceMaxSeconds"`
	Spaces          bool `json:"spaces"`
	// PreviewOffice: documents and spreadsheets open in the preview (FTR.NAB.CHAT-0003 R22).
	PreviewOffice bool            `json:"previewOffice"`
	Channels      map[string]bool `json:"channels"`
	Version       string          `json:"version"`
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
	c.api = true
	hub := events.NewHub()
	go hub.Listen(ctx, c.pool)
	go c.registry.Listen(ctx, hub)
	c.seedChannels(ctx)
	if err := c.models.Bootstrap(ctx, cfg.BootstrapLLMType, cfg.BootstrapLLMURL, cfg.BootstrapLLMKey); err != nil {
		slog.Error("bootstrap model connection", "err", err)
	}
	// FTR.NAB.CMN-0008 tech §5.2: the built-in profiles of this version.
	c.profiles.Cfg.Version = version
	if ms, err := c.profiles.SyncBuiltins(ctx); err != nil {
		slog.Error("sync sandbox profiles", "err", err)
	} else {
		for _, m := range ms {
			if err := c.pkgs.MarkAdded(ctx, m); err != nil {
				slog.Warn("mark added packages", "profile", m.Name, "err", err)
			}
		}
	}
	if err := c.catalog.StarterCatalog(ctx); err != nil {
		slog.Error("starter catalog", "err", err)
	}
	// FTR.NAB.CHAT-0003 tech §6: previews of office files
	previews := &preview.Service{Pool: c.pool, S3: c.s3, Cfg: preview.Config{ConvertURL: cfg.PreviewConvertURL, ConvertUser: cfg.PreviewConvertUser,
		ConvertPassword: cfg.PreviewConvertPassword, LegacyURL: cfg.PreviewLegacyURL, DocgenURL: cfg.PreviewDocgenURL, DocgenKey: cfg.PreviewDocgenKey,
		MaxBytes: cfg.PreviewMaxBytes, Parallel: cfg.PreviewParallel}}
	go func() {
		t := time.NewTicker(time.Hour)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if n, err := previews.Sweep(ctx); err != nil {
					slog.Warn("sweep previews", "err", err)
				} else if n > 0 {
					slog.Info("cleaned", "what", "previews", "objects", n)
				}
			}
		}
	}()
	// FTR.NAB.CMN-0006 tech §3.6: copies of files for MCP servers live an hour
	go func() {
		t := time.NewTicker(10 * time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if n, err := c.snapshots.Sweep(ctx); err != nil {
					slog.Warn("sweep copies of space files", "err", err)
				} else if n > 0 {
					slog.Info("cleaned", "what", "copies of space files", "objects", n)
				}
			}
		}
	}()

	chatAPI := c.chatAPI
	allowedModel := func(ctx context.Context, owner uuid.UUID, m groups.ModelChoice) bool {
		avail, err := c.models.AvailableFor(ctx, owner)
		if err != nil {
			return false
		}
		for _, a := range avail {
			if a.ConnectionID == m.ConnectionID && a.Model == m.Model {
				return true
			}
		}
		return false
	}

	bootstrap := map[string]bool{}
	for _, e := range cfg.BootstrapAdmins {
		bootstrap[domain.NormalizeEmail(e)] = true
	}
	prov := c.provider()
	authH := &auth.Handlers{Provider: prov, Users: c.users, PublicAPIURL: cfg.PublicAPIURL, WebURL: cfg.PublicWebURL,
		CookieDomain: cfg.CookieDomain, Secure: strings.HasPrefix(cfg.PublicAPIURL, "https://"), BootstrapAdmins: bootstrap,
		OnArchived: func(ctx context.Context, u *users.User, id *users.Identity) {
			var ni *accounts.Identity
			if id != nil {
				ni = &accounts.Identity{Issuer: id.Issuer, Subject: id.Subject}
			}
			if err := c.accounts.RequestRestore(ctx, u.ID, ni); err != nil {
				slog.ErrorContext(ctx, "restore request", "err", err)
			}
		}}
	pubCfg := PublicConfig{AuthProvider: prov.Kind(), ProviderLabel: prov.Label(), AllowedOrg: cfg.GitHubAllowedOrg, Languages: domain.Languages,
		DefaultLanguage: cfg.DefaultLanguage, UploadMaxBytes: cfg.UploadMaxBytes, Voice: c.chatAPI.Whisper != nil, PreviewOffice: previews.Cfg.Enabled(),
		VoiceMaxSeconds: c.chatAPI.MaxVoiceSeconds, Spaces: c.space.Enabled,
		Version: version, BootstrapAdminsConfigured: len(cfg.BootstrapAdmins) > 0}
	if prov.Kind() == "oidc" {
		pubCfg.AllowedOrg = ""
	}
	actorOf := func(ctx context.Context, uid uuid.UUID) access.Actor {
		email, _ := c.userEmail(ctx, uid)
		return access.Actor{ID: uid, Email: email}
	}
	adminUsers := &admin.Users{Repo: c.users,
		Channels:      func(ctx context.Context, uid uuid.UUID) (any, error) { return c.registry.OfUser(ctx, uid) },
		GroupAgents:   func(ctx context.Context, uid uuid.UUID) (any, error) { return c.groups.List(ctx, "", &uid) },
		AccessChanged: c.access.Changed}
	c.users.RoleChanged = func(ctx context.Context, tx pgx.Tx, actor, uid uuid.UUID, admin bool) error {
		return c.access.AdminChanged(ctx, tx, actorOf(ctx, actor), uid, admin)
	}
	builtin := mcp.NewServer(c.signer)
	confirms := &confirm.Service{Pool: c.pool, Box: c.box, Events: c.events, TTL: cfg.ConfirmationTTL,
		Execute: func(ctx context.Context, p *confirm.Pending, args json.RawMessage) (string, bool, error) {
			if p.ItemID == nil {
				return builtin.Execute(ctx, mcp.Grant{UserID: p.UserID, Conversation: p.ConversationID, Channel: domain.ChannelWeb}, p.Tool, args)
			}
			return c.catalog.CallTool(ctx, p.UserID, *p.ItemID, p.Tool, args)
		},
		FollowUp: func(ctx context.Context, uid, conv uuid.UUID, text string, meta json.RawMessage) error {
			_, err := chatAPI.Post(ctx, uid, &conv, text, domain.ChannelWeb, nil, nil, meta)
			return err
		}}
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
		r.Get("/config", func(w http.ResponseWriter, r *http.Request) {
			cfg := pubCfg
			cfg.Channels = map[string]bool{}
			for _, k := range []string{domain.ChannelTelegram, domain.ChannelVKTeams, domain.ChannelEmail} {
				cfg.Channels[k] = c.registry.Enabled(r.Context(), k)
			}
			httpx.JSON(w, 200, cfg)
		})
		c.catalog.CallbackRoute(r)
		r.Group(func(r chi.Router) {
			r.Use(authH.Authenticate, auth.RequireSession)
			r.Get("/session", func(w http.ResponseWriter, r *http.Request) { httpx.JSON(w, 200, map[string]bool{"ok": true}) })
			r.Group(func(r chi.Router) {
				r.Use(auth.RequireActive)
				chatAPI.Routes(r, true)
				previews.Routes(r)
				c.memory.Routes(r)
				c.commAPI(cfg).Routes(r)
				c.tasks.Routes(r)
				c.tasks.MemoryRoutes(r, func(ctx context.Context) error {
					_, err := c.comm.Allowed(ctx)
					return err
				})
				c.space.Routes(r)
				c.catalog.Routes(r)
				c.chanAPI.Routes(r)
				c.groups.Routes(r, allowedModel)
				confirms.Routes(r)
				c.ledger.UserRoutes(r)
				r.Get("/events", events.SSEHandler(hub))
			})
		})
	})
	r.Route("/admin/api/v1", func(r chi.Router) {
		r.Use(authH.Authenticate, auth.RequireSession, auth.RequireAdmin)
		adminUsers.Routes(r)
		c.models.AdminRoutes(r)
		c.access.Routes(r, func(r *http.Request) access.Actor { // FTR.NAB.CMN-0007 tech §2
			p, _ := httpx.MustPrincipal(r)
			if p == nil {
				return access.Actor{}
			}
			return actorOf(r.Context(), p.UserID)
		}, c.models.ModelInfo)
		// FTR.NAB.CHAT-0001 R22: the state of the recognizer of speech
		wc, _ := c.chatAPI.Whisper.(*whisper.Client)
		r.Get("/voice", whisper.StatusHandler(cfg.WhisperURL, wc))
		c.catalog.AdminRoutes(r, cfg.SecretsKey)
		c.services.AdminRoutes(r)
		c.profiles.AdminRoutes(r)
		c.pkgs.AdminRoutes(r)
		c.clients.AdminRoutes(r)
		c.ledger.AdminRoutes(r)
		c.chanAPI.AdminRoutes(r)
		c.mail.LogRoute(r)
		c.accounts.AdminRoutes(r)
		c.groups.AdminRoutes(r, allowedModel)
		c.comm.AdminRoutes(r)
		(&agentpods.Admin{Pool: c.pool, Local: cfg.AgentExecutor != "k8s", Audit: func(ctx context.Context, admin, owner uuid.UUID, title string) {
			args, _ := json.Marshal(map[string]any{"owner": owner, "title": title})
			c.ledger.Audit(ctx, ledger.Entry{AgentKind: "personal", Agent: "nabu", UserID: &admin, Channel: "admin", Tool: "agent_pod.stop", Result: "ok"}, string(args), nil)
		}}).AdminRoutes(r)
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
		c.accounts.ClientRoutes(r)
		r.Route("/me", func(r chi.Router) {
			r.Use(auth.Delegation(c.users, c.registry.Open))
			chatAPI.Routes(r, false)
			r.Get("/events", events.SSEHandler(hub))
		})
	})
	// the webhook is always mounted: the bot may be set in the administration later
	wh := &channels.Webhook{Bot: c.tg, Keys: c.keys, Registry: c.registry, Users: c.users, Inbox: chatAPI, Groups: c.groups,
		Attachments: chatAPI.Attachments(), MaxFile: cfg.UploadMaxBytes, WebURL: cfg.PublicWebURL}
	wh.Route(r)
	c.catalog.HookRoute(r, cfg.SecretsKey)
	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		httpx.Error(w, r, apperr.NotFound("not_found", "not found"))
	})

	builtin.Register(c.memory.Tools()...)
	builtin.Register(c.space.Tool())
	if c.space.Enabled {
		builtin.Register(c.pkgs.Tool()) // FTR.NAB.CMN-0008 R10
	}
	builtin.Register(chatAPI.AttachTool(func(ctx context.Context, uid uuid.UUID, p string, max int64) ([]byte, error) {
		data, err := c.snapshots.Read(ctx, uid, p, max)
		switch {
		case errors.Is(err, catalog.ErrFileNotFound), errors.Is(err, catalog.ErrFileInvalid):
			return nil, &mcp.ToolError{Msg: "no such file in your space: " + p}
		case errors.Is(err, catalog.ErrFileTooLarge):
			return nil, &mcp.ToolError{Msg: fmt.Sprintf("the file is larger than %d MB", max>>20)}
		}
		return data, err
	}))
	builtin.Register(c.tasks.Tools()...)
	builtin.Register(c.comm.Tools(func(id uuid.UUID) string {
		return strings.TrimRight(cfg.PublicWebURL, "/") + "/memory?tab=comm&note=" + id.String()
	}, c.userToday)...)
	builtin.Register(c.comm.CollectTools(func(ctx context.Context, uid uuid.UUID) comm.UserInfo {
		info := comm.UserInfo{Timezone: c.userLocation(ctx, uid).String()}
		info.Email, _ = c.userEmail(ctx, uid)
		return info
	})...)
	builtin.Allowed = c.users.Allowed
	builtin.ChannelOf = c.chat.LastUserChannel
	c.catalog.ChannelOf = c.chat.LastUserChannel // FTR.NAB.CHAT-0002 R6
	// R9: in mail topics tools that change data wait for the user
	builtin.Hold = func(ctx context.Context, g mcp.Grant, tool string, args json.RawMessage) (string, bool) {
		return confirms.Hold(ctx, g.Conversation, g.UserID, nil, confirm.Builtin, tool, args)
	}
	internal := chi.NewRouter()
	internal.Use(httpx.Observe)
	internal.Handle("/internal/v1/mcp", builtin)
	(&gigachat.Bridge{Client: c.models.GigaChat, Lookup: c.models.BridgeConn, Verify: c.models.VerifyBridge}).Routes(internal)
	c.catalog.ProxyRoutes(internal, c.userEmail, func(ctx context.Context, conv, uid, item uuid.UUID, server, tool string, args json.RawMessage) (string, bool) {
		return confirms.Hold(ctx, conv, uid, &item, server, tool, args)
	})
	c.space.InternalRoutes(internal)
	c.profiles.InternalRoutes(internal, c.signer)
	c.pkgs.InternalRoutes(internal)

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
			IdleTimeout: cfg.SandboxIdleTimeout, RelayURL: cfg.RelaySandboxWSURL, APIURL: cfg.InternalURL, Exclude: cfg.SandboxExclude, TokenTTL: 7 * 24 * time.Hour,
			InitImage: cfg.SandboxInitImage, PullSecrets: cfg.SandboxPullSecrets}, Profiles: c.profiles}
	}
	hub := events.NewHub()
	go hub.Listen(ctx, c.pool)
	go c.registry.Listen(ctx, hub)
	c.seedChannels(ctx)
	adapters := map[string]channels.Adapter{domain.ChannelTelegram: c.tg, domain.ChannelVKTeams: c.vk}
	confirms := &confirm.Service{Pool: c.pool, Box: c.box, Events: c.events, TTL: cfg.ConfirmationTTL}
	c.mail.Pending = confirms.PendingSince
	eng := &engine.Engine{
		Cfg: engine.Config{InternalURL: cfg.InternalURL, RelayInternalURL: cfg.RelayInternalURL, TurnTimeout: cfg.TurnTimeout,
			TaskRunTimeout: cfg.TaskRunTimeout, RunWorkspaceWait: cfg.RunWorkspaceWait, SessionMaxAge: 12 * time.Hour, PiVersion: cfg.PiVersion},
		Pool: c.pool, Op: c.operator, Users: c.users, Chat: c.chat, Models: c.models, Catalog: c.catalog, Memory: c.memory, Tasks: c.tasks, Comm: c.comm,
		Space: c.space, Sandboxes: sandboxes, Services: c.services, Ledger: c.ledger, Signer: c.signer, S3: c.s3, Events: c.events,
		Bus: c.producer, Adapters: adapters, Registry: c.registry, Keys: c.keys, Groups: c.groups, Mail: c.mail,
		Connected: func(ctx context.Context, id string) bool {
			_, ok, _ := relayStore.Lookup(ctx, id)
			return ok
		},
	}
	c.accounts.Sandboxes = sandboxStopper{sandboxes}
	c.accounts.CloseSessions = eng.CloseSessions
	// FTR.NAB.CMN-0004: conversations and tasks run in the pod of their owner.
	eng.Collector = eng
	collect.SetGmailOverlap(cfg.MemoryGmailOverlap)
	eng.CollectCfg = engine.CollectConfig{BatchThreads: cfg.MemoryBatchThreads, RunTimeout: cfg.MemoryRunTimeout, BackfillDays: cfg.MemoryBackfillDays,
		BackfillSlots: max(1, int(float64(cfg.MemoryCollectConcurrency)*cfg.MemoryBackfillPoolShare)),
		BotAddresses:  c.botAddresses}
	if cfg.MemoryAgentAddr != "" {
		eng.MemoryOp = &agent.Client{BaseURL: cfg.MemoryAgentAddr, Token: cfg.AgentServiceToken}
	}
	pods := &agentpods.Manager{Cfg: podsConfig(cfg), Pool: c.pool, Signer: c.signer, Bus: c.producer, Events: c.events, Shared: c.operator,
		Expire: eng.QueueExpired, Notify: eng.QueueNotify, Waiting: eng.QueueTyping}
	if !pods.Cfg.Local {
		pods.Kube = k8s.NewInCluster()
	}
	eng.Pods = pods
	vkPoller := &channels.VKPoller{Bot: c.vk, Pool: c.pool, Registry: c.registry, Users: c.users, Inbox: c.chatAPI, Groups: c.groups,
		Attachments: c.chatAPI.Attachments(), MaxFile: cfg.UploadMaxBytes,
		ByEmail: func(ctx context.Context, addr string) (uuid.UUID, bool) {
			u, err := c.users.ByEmail(ctx, addr)
			if err != nil || u == nil {
				return uuid.Nil, false
			}
			return u.ID, true
		}}
	g, gctx := errgroup.WithContext(ctx)
	serve(gctx, g, serviceServer(cfg.ServiceAddr, c.ready))
	g.Go(func() error { c.mail.Run(gctx); return nil })         // one IMAP receiver per instance (advisory lock)
	g.Go(func() error { vkPoller.Run(gctx); return nil })       // one VK Teams poller per instance
	g.Go(func() error { c.accounts.RunJobs(gctx); return nil }) // the steps of archiving
	g.Go(func() error { return runPurge(gctx, cfg, c.accounts) })
	g.Go(func() error { return runModelsRefresh(gctx, cfg, c.models) })
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
	g.Go(func() error {
		return kafka.ConsumeParallel(gctx, cfg.KafkaBrokers, "nabu-worker", kafka.TopicMemoryCollect, cfg.MemoryCollectConcurrency, eng.HandleCollectBatch)
	})
	g.Go(func() error { // FTR.NAB.CMN-0003 R17: resume the collections whose pause is over
		t := time.NewTicker(cfg.MemoryHousekeepingEvery)
		defer t.Stop()
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-t.C:
				eng.MemoryHousekeeping(gctx)
			}
		}
	})
	g.Go(func() error { // the vectors of new and changed notes, and the recount after a change of the model
		for {
			n, err := c.comm.EmbedPending(gctx, cfg.MemoryEmbedBatch)
			if err != nil && gctx.Err() == nil {
				slog.Warn("embed notes", "err", err)
			}
			if pending, err := c.comm.PendingVectors(gctx); err == nil {
				metrics.MemoryEmbedPending.Set(float64(pending))
			}
			wait := 15 * time.Second
			if n > 0 && err == nil {
				wait = 200 * time.Millisecond // more may be waiting
			}
			select {
			case <-gctx.Done():
				return nil
			case <-time.After(wait):
			}
		}
	})
	g.Go(func() error { c.tasks.Run(gctx); return nil })
	g.Go(func() error { pods.Run(gctx); return nil }) // one manager of agent pods per instance (advisory lock)
	if sandboxes != nil {
		g.Go(func() error { sandboxes.Run(gctx); return nil })
		// FTR.NAB.CMN-0008 tech §5.6, §5.7: checks of custom images and the
		// preloading of the images of assigned profiles.
		profiles := &sandboxprofile.Worker{Profiles: c.profiles, K8s: sandboxes.K8s, Signer: c.signer,
			Cfg: sandboxprofile.WorkerConfig{Namespace: cfg.SandboxNamespace, InitImage: cfg.SandboxInitImage, PullSecrets: cfg.SandboxPullSecrets,
				ProbeTimeout: cfg.SandboxProbeTimeout, APIURL: cfg.InternalURL},
			OnReady: func(ctx context.Context, m sandboxprofile.Manifest) {
				if err := c.pkgs.MarkAdded(ctx, m); err != nil {
					slog.Warn("mark added packages", "profile", m.Name, "err", err)
				}
			}}
		g.Go(func() error {
			postgres.RunLocked(gctx, c.pool, "sandbox-profiles", time.Minute, profiles.Run)
			return nil
		})
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
			confirms.Expire(gctx)
			c.accounts.CountUsers(gctx)
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

// sandboxStopper stops sandboxes when they are enabled.
type sandboxStopper struct{ s *space.Sandboxes }

func (s sandboxStopper) Stop(ctx context.Context, uid uuid.UUID) error {
	if s.s == nil {
		return nil
	}
	return s.s.Stop(ctx, uid)
}

// runPurge deletes archived accounts after their retention by PURGE_SCHEDULE
// (tech §10).
func runPurge(ctx context.Context, cfg *config.Config, acc *accounts.Service) error {
	loc, err := time.LoadLocation(cfg.DefaultTimezone)
	if err != nil {
		loc = time.UTC
	}
	sched, err := cron.ParseStandard(cfg.PurgeSchedule)
	if err != nil {
		return fmt.Errorf("PURGE_SCHEDULE: %w", err)
	}
	for {
		next := sched.Next(time.Now().In(loc))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Until(next)):
		}
		n, err := acc.Purge(ctx) // idempotent: two workers delete the same rows once
		if err != nil {
			slog.Error("purge", "err", err)
			continue
		}
		slog.Info("purge finished", "accounts", n)
	}
}

// runModelsRefresh asks the providers for their models by MODELS_REFRESH_CRON
// (FTR.NAB.CHAT-0001 R10): new models are marked, gone ones too.
func runModelsRefresh(ctx context.Context, cfg *config.Config, ms *models.Service) error {
	sched, err := cron.ParseStandard(cfg.ModelsRefreshCron)
	if err != nil {
		return fmt.Errorf("MODELS_REFRESH_CRON: %w", err)
	}
	for {
		next := sched.Next(time.Now())
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Until(next)):
		}
		ms.RefreshAll(ctx)
	}
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
		"tg attempts": `DELETE FROM telegram_bind_attempts WHERE at < now() - interval '1 day'`,
		"mail rate":   `DELETE FROM email_rate WHERE window_start < now() - interval '2 days'`,
		"jobs":        `DELETE FROM account_jobs WHERE finished_at < now() - interval '30 days'`,
		"oauth":       `DELETE FROM oauth_states WHERE expires_at < now()`,
		"run events":  `DELETE FROM run_events WHERE created_at < now() - interval '30 days'`,
		// FTR.NAB.CMN-0008 R12
		"package signals": `DELETE FROM package_signals WHERE day < current_date - 180`,
		"connections":     `DELETE FROM workspace_connections WHERE last_ping_at < now() - interval '10 minutes'`,
		"harness":         `DELETE FROM harness_sessions WHERE closed_at < now() - interval '30 days'`,
	} {
		tag, err := pool.Exec(ctx, q)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		slog.Info("cleaned", "what", name, "rows", tag.RowsAffected())
	}
	// FTR.NAB.CMN-0003 R23: the retention of the notes of communications
	if purged, err := (&comm.Service{Pool: pool}).Purge(ctx); err != nil {
		return fmt.Errorf("communications: %w", err)
	} else {
		slog.Info("cleaned", "what", "communications", "notes", purged.Notes, "people", purged.People, "topics", purged.Topics)
	}
	// the mail log is kept as long as the audit (arch §8)
	if _, err := pool.Exec(ctx, `DELETE FROM email_log WHERE at < now() - make_interval(secs => $1)`, cfg.AuditRetention.Seconds()); err != nil {
		return fmt.Errorf("mail log: %w", err)
	}
	return (&ledger.Ledger{Pool: pool}).Partitions(ctx, cfg.AuditRetention)
}

func podsConfig(cfg *config.Config) agentpods.Config {
	return agentpods.Config{Local: cfg.AgentExecutor != "k8s", Namespace: cfg.AgentNamespace, Image: cfg.AgentImage,
		CPURequest: cfg.AgentPodCPURequest, CPU: cfg.AgentPodCPU, MemoryRequest: cfg.AgentPodMemRequest, Memory: cfg.AgentPodMemory,
		Work: cfg.AgentPodWork, MaxSessions: cfg.AgentPodMaxSessions, SessionIdle: cfg.AgentIdleTimeout,
		IdleTimeout: cfg.AgentPodIdleTimeout, MinIdle: cfg.AgentPodMinIdle, PodsMax: cfg.AgentPodsMax, StartParallel: cfg.AgentStartParallel,
		StartTimeout: cfg.AgentStartTimeout, ScheduleTimeout: cfg.AgentScheduleTimeout, QueueTimeout: cfg.AgentQueueTimeout,
		TurnTimeout: max(cfg.TurnTimeout, cfg.TaskRunTimeout), WarmWindow: cfg.AgentWarmWindow, WarmShare: cfg.AgentWarmShare, WarmMax: cfg.AgentWarmMax}
}

// userLocation is the time zone of a user: "today" of their commitments.
func (c *core) userLocation(ctx context.Context, uid uuid.UUID) *time.Location {
	if u, err := c.users.Get(ctx, uid); err == nil && u != nil && u.Timezone != "" {
		if loc, err := time.LoadLocation(u.Timezone); err == nil {
			return loc
		}
	}
	return time.UTC
}

func (c *core) userToday(ctx context.Context, uid uuid.UUID) time.Time {
	return time.Now().In(c.userLocation(ctx, uid))
}

// commAPI is the API of the memory of communications (FTR.NAB.CMN-0003 tech §3).
func (c *core) commAPI(cfg *config.Config) *comm.API {
	return &comm.API{Store: c.comm, Sources: c.commSources, Timezone: c.userLocation, GraphMaxNodes: cfg.MemoryGraphMaxNodes, Semantic: c.comm.Semantic,
		Collect: func(ctx context.Context, uid uuid.UUID) (comm.CollectState, error) {
			st := comm.CollectState{Status: "off"}
			mt, err := c.tasks.MemoryTaskOf(ctx, uid, tasks.KindCollect)
			if err != nil {
				return st, err
			}
			st.Status, st.PauseKind, st.NextRunAt = mt.Status, mt.PauseKind, mt.NextRunAt
			if st.Backfill, err = c.comm.Backfill(ctx, uid); err != nil {
				return st, err
			}
			st.BudgetUsedPct, err = c.comm.BudgetUsedPct(ctx, uid)
			return st, err
		},
		OnDeleteAll: func(ctx context.Context, uid uuid.UUID) {
			if err := c.tasks.DisableMemoryTasks(ctx, uid); err != nil {
				slog.ErrorContext(ctx, "switch the memory tasks off", "user", uid, "err", err)
			}
		}}
}

// commSources marks the sources the collection of the user reads.
func (c *core) commSources(ctx context.Context, uid uuid.UUID) ([]comm.SourceState, error) {
	src, err := c.comm.Sources(ctx, uid)
	if err != nil {
		return nil, err
	}
	mt, err := c.tasks.MemoryTaskOf(ctx, uid, tasks.KindCollect)
	if err != nil {
		return nil, err
	}
	var set tasks.CollectSettings
	_ = json.Unmarshal(mt.Settings, &set)
	for i := range src {
		for _, on := range set.Sources {
			if mt.Enabled && on.ItemID == src[i].ItemID && on.Source == src[i].Source {
				src[i].Enabled = true
			}
		}
	}
	return src, nil
}

// userEmail is the email of a user who may use the agent.
func (c *core) userEmail(ctx context.Context, uid uuid.UUID) (string, bool) {
	u, err := c.users.Get(ctx, uid)
	if err != nil || u == nil || u.Status == "blocked" || u.Status == "archived" {
		return "", false
	}
	return u.Email, true
}

// botAddresses are the mailbox of the Nabu bot and its aliases: the letters
// to and from the bot are not collected (FTR.NAB.CMN-0003 R4).
func (c *core) botAddresses(ctx context.Context) []string {
	var raw []byte
	if err := c.pool.QueryRow(ctx, `SELECT settings FROM channels WHERE kind = 'email'`).Scan(&raw); err != nil {
		return nil
	}
	var set struct {
		Mailbox string   `json:"mailbox"`
		Aliases []string `json:"aliases"`
	}
	_ = json.Unmarshal(raw, &set)
	out := []string{}
	for _, a := range append([]string{set.Mailbox}, set.Aliases...) {
		if a != "" {
			out = append(out, a)
		}
	}
	return out
}

// ChatSkills are the skills a user may call (FTR.NAB.CHAT-0001 R4); for the
// technical user of a group, those of the group agent (R6): the skills its
// owner chose within the owner's access (FTR.NAB.CMN-0007 R12).
func ChatSkills(ctx context.Context, cat *catalog.Service, gs *groups.Service, uid uuid.UUID) ([]chat.Skill, error) {
	var list []catalog.Skill
	var err error
	if ga, gerr := gs.ByDataUser(ctx, uid); gerr == nil && ga != nil {
		owner := uuid.Nil
		if ga.OwnerID != nil {
			owner = *ga.OwnerID
		}
		list, err = cat.GroupSkills(ctx, owner, ga.Skills)
	} else {
		list, err = cat.SkillsFor(ctx, uid)
	}
	out := make([]chat.Skill, 0, len(list))
	for _, s := range list {
		out = append(out, chat.Skill(s))
	}
	return out, err
}
