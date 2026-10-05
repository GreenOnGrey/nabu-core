# nabu-core

Backend of [Nabu](../nabu), the on-premises platform of AI agents: one Go binary with the modes
`api`, `worker`, `agent` (operator of Pi processes), `relay` (WebSocket channel of workspaces),
`sandbox`, `migrate` and `cleaner`. The release image carries the Pi coding agent and the
`nabu-workspace` extension.

- Specification: `hammurapi-specs/specs/NAB/CMN/FTR.NAB.CMN-0001`
- Configuration: [`nabu/docs/configuration.md`](../nabu/docs/configuration.md)
- Development guide: [AGENTS.md](AGENTS.md)

```sh
make build && go test ./...
```
