// Command nabu is the single Nabu binary (FTR.NAB.CMN-0001 arch §3). The first
// argument selects the mode: api, worker, agent (the agent operator), relay,
// sandbox (a personal space), migrate or cleaner.
package main

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/app"
	"github.com/GreenOnGrey/nabu-core/internal/config"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/operator"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/pi"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/logging"
	"github.com/GreenOnGrey/nabu-core/internal/platform/telemetry"
	"github.com/GreenOnGrey/nabu-core/internal/sandbox"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const usage = `usage: nabu <mode>

modes:
  api       public API (:8080): site, administration, clients, webhooks; internal API (:8081)
  worker    agent turns, scheduled tasks, service agent runs, sandboxes, delivery to messengers
  agent     the agent operator: Pi sessions behind the internal API (:8090)
  relay     channels of workspaces and the agent's tool calls (:8085)
  sandbox   a personal space of one user (started by the worker)
  migrate   apply database migrations
  cleaner   one maintenance pass (a CronJob)
  version   print the version`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	mode := os.Args[1]
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch mode {
	case "version":
		fmt.Println(version)
		return
	case "agent":
		exit(runOperator(ctx))
		return
	case "sandbox":
		exit(runSandbox(ctx))
		return
	case "api", "worker", "relay", "cleaner", "migrate":
	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	cfg, err := config.Load()
	if err == nil {
		err = cfg.Validate(mode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "configuration error:", err)
		os.Exit(1)
	}
	slog.SetDefault(logging.New(os.Stdout, cfg.LogLevel).With("mode", mode, "version", version))
	shutdown, err := telemetry.Setup(ctx, cfg.OTLPEndpoint, mode)
	if err != nil {
		slog.Error("telemetry setup failed", "err", err)
		os.Exit(1)
	}
	defer shutdown(context.Background()) //nolint:errcheck
	slog.Info("starting")
	switch mode {
	case "api":
		err = app.RunAPI(ctx, cfg, version)
	case "worker":
		err = app.RunWorker(ctx, cfg)
	case "relay":
		err = app.RunRelay(ctx, cfg)
	case "cleaner":
		err = app.RunCleaner(ctx, cfg)
	case "migrate":
		err = app.RunMigrate(ctx, cfg)
	}
	if err != nil {
		slog.Error("stopped with error", "err", err)
		os.Exit(1)
	}
	slog.Info("stopped")
}

func exit(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// runOperator runs the agent operator. Its configuration comes from its own
// environment only: the pod has no database, Kafka, object storage or
// secrets of Nabu besides its service token.
func runOperator(ctx context.Context) error {
	slog.SetDefault(logging.New(os.Stdout, envOr("LOG_LEVEL", "info")).With("mode", "agent", "version", version))
	maxS, err := strconv.Atoi(envOr("AGENT_MAX_SESSIONS", "100"))
	if err != nil {
		return fmt.Errorf("AGENT_MAX_SESSIONS: %w", err)
	}
	maxR, err := strconv.Atoi(envOr("AGENT_MAX_RUNS", "10"))
	if err != nil {
		return fmt.Errorf("AGENT_MAX_RUNS: %w", err)
	}
	idle, err := time.ParseDuration(envOr("AGENT_IDLE_TIMEOUT", "15m"))
	if err != nil {
		return fmt.Errorf("AGENT_IDLE_TIMEOUT: %w", err)
	}
	mode := envOr("AGENT_MODE", operator.ModeAll)
	var gen int64
	var pub ed25519.PublicKey
	if mode == operator.ModeOwner {
		// The pod of one owner (FTR.NAB.CMN-0004): the worker passes the owner,
		// the generation and the public key — no secret in the environment.
		if gen, err = strconv.ParseInt(os.Getenv("AGENT_GENERATION"), 10, 64); err != nil {
			return fmt.Errorf("AGENT_GENERATION: %w", err)
		}
		if pub, err = jwt.ParsePublicKey(os.Getenv("AGENT_JWT_PUBLIC_KEY")); err != nil {
			return fmt.Errorf("AGENT_JWT_PUBLIC_KEY: %w", err)
		}
	}
	op, err := operator.New(operator.Config{
		Mode: mode, Owner: os.Getenv("AGENT_OWNER"), Generation: gen, PublicKey: pub, Version: version,
		Runtime: pi.Runtime{Command: strings.Fields(envOr("PI_BINARY", "/usr/local/bin/pi")), Options: pi.Options{
			ExtensionDir: envOr("PI_EXTENSION_DIR", "/opt/nabu/pi-extensions/nabu-workspace"),
			Path:         envOr("PATH", "/usr/local/bin:/usr/bin:/bin"), Lang: os.Getenv("LANG"),
			ExtraEnv: strings.Fields(os.Getenv("PI_EXTRA_ENV")),
		}},
		WorkDir: envOr("AGENT_WORKDIR", "/work"), ServiceToken: os.Getenv("AGENT_SERVICE_TOKEN"),
		MaxSessions: maxS, MaxRuns: maxR, IdleTimeout: idle,
	})
	if err != nil {
		return err
	}
	return app.RunOperator(ctx, op, envOr("AGENT_LISTEN_ADDR", ":8090"), envOr("SERVICE_ADDR", ":9100"),
		strings.Fields(envOr("PI_BINARY", "/usr/local/bin/pi")))
}

// runSandbox runs a personal space; the worker passes everything in the
// environment of the pod (tech §7).
func runSandbox(ctx context.Context) error {
	slog.SetDefault(logging.New(os.Stdout, envOr("LOG_LEVEL", "info")).With("mode", "sandbox", "version", version))
	cfg := sandbox.Config{UserID: os.Getenv("NABU_SANDBOX_USER"), Root: envOr("NABU_SANDBOX_ROOT", "/work"),
		SandboxToken: os.Getenv("NABU_SANDBOX_TOKEN"), WorkspaceToken: os.Getenv("NABU_WORKSPACE_TOKEN"),
		RelayURL: os.Getenv("NABU_RELAY_URL"), APIURL: strings.TrimRight(os.Getenv("NABU_API_INTERNAL_URL"), "/"),
		Exclude: strings.Split(os.Getenv("NABU_SANDBOX_EXCLUDE"), ","), Ephemeral: os.Getenv("NABU_SANDBOX_EPHEMERAL") == "1",
		WorkspaceID: os.Getenv("NABU_SANDBOX_WORKSPACE")}
	if cfg.WorkspaceToken == "" || cfg.RelayURL == "" || (!cfg.Ephemeral && (cfg.UserID == "" || cfg.SandboxToken == "" || cfg.APIURL == "")) {
		return fmt.Errorf("sandbox: NABU_SANDBOX_USER, NABU_SANDBOX_TOKEN, NABU_WORKSPACE_TOKEN, NABU_RELAY_URL and NABU_API_INTERNAL_URL are required")
	}
	return sandbox.Run(ctx, cfg)
}
