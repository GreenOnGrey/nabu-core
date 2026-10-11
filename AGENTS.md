# AGENTS.md — nabu-core

Guide for coding agents and developers working in this repository.

## Workspace

Nabu is three repositories checked out side by side, next to Hammurapi:

| Repository | What it is |
| --- | --- |
| `nabu-core` (this one) | Go backend: one binary with modes, migrations, the agent operator, `relay`, sandboxes |
| `nabu-web` | React SPA |
| `nabu` | Docs and the reusable deploy workflow `deploy-component.yml` |
| `hammurapi-specs` | Source of truth: `specs/NAB/CMN/FTR.NAB.CMN-0001`, `FTR.NAB.CMN-0002` (channels and accounts), `FTR.NAB.CMN-0003` (memory of communications) and `FTR.HMR.CMN-0006` for Hammurapi |
| `hammurapi-infra` | The stand: charts `nabu-core`/`nabu-web` and `bin/nabu-deploy` next to those of Hammurapi |

- Every change implements a feature specification from `hammurapi-specs/specs/NAB/<GROUP>/FTR.NAB.<GROUP>-NNNN/`.
  Read all five areas (product, design, arch, tech, qa) before coding. Test IDs (`MEM-02`, `RLY-04`,
  …) come from the qa spec; name tests and comments after them.
- Refer to specs in comments as `FTR.NAB.CMN-0001 R23`, `FTR.NAB.CMN-0001 tech §6`.
- When the implementation deviates from a spec, write the deviation into the spec (version bump,
  "Принятые решения") in the same piece of work.
- Do not commit, tag or push unless asked: the maintainer reviews and releases by tags.

## Stack and layout

Go 1.27, Postgres (pgx, goose), Kafka (segmentio/kafka-go), S3 (minio-go), chi, coder/websocket,
robfig/cron, goldmark (answers → channel formats), go-imap v2 and go-message (mail), bot-golang (VK Teams). JWTs (Ed25519) are Nabu's own: `internal/platform/jwt`, keys from `SECRETS_KEY`, JWKS at
`/.well-known/jwks.json`.

```text
cmd/nabu/             entry point; modes: api | worker | agent | relay | sandbox | migrate | cleaner
pkg/pirpc/            client of Pi's RPC mode — no imports from internal/
pi-extensions/        nabu-workspace: Pi tools routed through relay to the workspace server
migrations/           goose migrations, embedded
internal/app/         wiring per mode (RunAPI, RunWorker, RunRelay, RunOperator, …)
internal/config/      every environment variable
internal/engine/      turns of personal agents, runs of service agents, persona, snapshots
internal/agentpods/   a pod per owner for conversations and tasks (FTR.NAB.CMN-0004): leases for the engine, the manager
                      loop (capacity, warm reserve, queue of turns), the Agents section of the administration
internal/relay/       the WebSocket channel of workspaces (server and client; frames in proto.go)
internal/workspace/   the workspace server (fs/*, grep, exec) served in sandboxes and runners
internal/space, sandbox/   personal spaces: sandbox pods, S3 sync
internal/catalog/     catalog, MCP proxy with credentials, skills
internal/channels/    registry and availability of channels, Telegram (keys, rich messages), VK Teams; email/ — the mail channel
internal/render/      the agent's Markdown → Telegram rich blocks, messenger HTML, letters
internal/groups/      group agents of Telegram and VK Teams chats (data owned by a technical user, created_via = 'group')
internal/accounts/    archiving, restoring and the purge of accounts
internal/confirm/     tools that change data wait for the user in mail topics
internal/collect/     adapters of the sources of communications (Gmail): what changed, the read tools, links — never the text
internal/comm/        the memory of communications (FTR.NAB.CMN-0003): notes, people, topics, commitments, the graph, the
                      digest, masking of secrets; every query runs in postgres.AsUser — the row policies hide other users
internal/auth, users, clients, services, importer, tasks, memory, chat, ledger, models, admin
internal/platform/*   adapters: postgres, kafka, storage, jwt, mcp, k8s, agent (operator, Pi files), …
internal/itest/       integration tests (build tag integration)
deploy/versions.env   DEPLOY_WORKFLOW_REF, CHART_VERSION, PI_VERSION — changed only by PR
```

`relay/proto.go` and `relay/client.go` are copied into `hammurapi-core/internal/platform/relay`
(the runner of Hammurapi is a client). Change the protocol in both places.

## Commands

```sh
make build                                               # bin/nabu
go test ./...                                            # unit tests
go test -tags integration -count=1 ./internal/itest/...  # Postgres: dockertest, or
                                                         # (the search by meaning of FTR.NAB.CMN-0003 needs pgvector, else it is skipped)
NABU_TEST_DATABASE_URL=postgres://… go test -tags integration ./internal/itest/   # an existing server (fresh DB per run);
                                                         # without Docker: github.com/fergusstrange/embedded-postgres
PIRPC_PI_CMD=pi NABU_PI_CMD=pi go test ./pkg/pirpc/ ./internal/platform/agent/... ./internal/relay/ ./internal/workspace/   # real Pi
PIRPC_PI_CMD=pi NABU_PI_CMD=pi go test -tags integration -run TestEngineWithPi ./internal/itest/   # the engine with real Pi
go test -tags 'integration load' -run TestCommLoad ./internal/itest/       # FTR.NAB.CMN-0003 PERF-01, -02, -05
NABU_PI_CMD=pi NABU_QUALITY_LLM_URL=… NABU_QUALITY_LLM_MODEL=… go test -tags 'integration quality' -run TestCollectQuality ./internal/itest/   # QLT-01…06, real model
golangci-lint run ./... && golangci-lint run --build-tags integration ./internal/itest/
```

CI (`.github/workflows/ci.yml`) runs lint, unit and integration tests, the Pi contract tests against
`PI_VERSION` (`pi-contract` job), `deploy/sync-ref.sh --check` and actionlint. Run the same before
handing work over.

## Rules

- **Migrations** are expand-only: new files with the next number; never change an applied migration
  except its comments. Every migration has a `Down`. Update the expected version in the itest.
- **The tables of the memory of communications that have row policies** (`comm_notes`, `comm_note_messages`,
  `comm_note_tags`, `comm_note_vectors`, `comm_people`, `comm_topics`, `commitments`, `comm_tombstones`,
  `comm_skipped`, `comm_cursors`, `comm_batches`) **are read and written only inside `postgres.AsUser`**
  (FTR.NAB.CMN-0003 R22): a plain query sees no rows. `comm_settings` and `comm_usage` hold no content and have no
  policy. `postgres.AsMaintenance` is for background work only, never for a handler of HTTP or of an MCP tool;
  its callers are listed in `TestMaintenanceCallers`. Integration tests of these tables use a role that is not a superuser (`appPool`).
- **Do not run generic formatters over SQL, YAML or shell.** Go: `gofmt` only.
- Every error returned to clients has a stable code (`apperr`); a new code needs a translation in
  `nabu-web/locales/*.json` (`errors.<code>`).
- A new environment variable goes to `internal/config/config.go`, `nabu/docs/configuration.md` and,
  if the stand needs it, the chart `hammurapi-infra/charts/nabu-core` and `hammurapi-infra/bin/nabu-deploy`.
- The web source never names a client product (`nabu-web` tests it): a product channel is data — the
  name of its service client — not a literal in the interface.
- A channel adapter knows nothing of conversations; messages of every channel pass the availability
  rule (`channels.Registry.Available`) on the way in and on delivery. One IMAP receiver and one VK
  Teams poller per instance run under `postgres.RunLocked`.
- Secrets never go to argv, logs, files or the environment of Pi: LLM keys and MCP credentials stay in
  `api`/`worker` (the MCP proxy adds them per call). Audit entries are masked.
- The release image must not contain Pi extensions that schedule work or run in the background;
  the Dockerfile checks it.
- Image scanning (Trivy) blocks fixable HIGH/CRITICAL; accepted findings go to `.trivyignore.yaml`
  with a reason and an `expired_at`.

## Releases

A tag `vX.Y.Z` runs `release.yml`: image `ghcr.io/greenongrey/nabu-core` (with Pi), SBOM, cosign
signature, Trivy, then the reusable `deploy-component.yml` of the `nabu` repo pinned by
`DEPLOY_WORKFLOW_REF`; `CHART_VERSION` is the `hammurapi-infra` release whose charts are installed
(as in hammurapi-core). When a change needs a new chart: tag `hammurapi-infra`; a new deploy
workflow: tag `nabu`; then bump `deploy/versions.env` here (`deploy/sync-ref.sh`) → tag core.
