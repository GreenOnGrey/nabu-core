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

## Sandbox images

Sandboxes run the image of their profile (`hammurapi-specs/specs/NAB/CMN/FTR.NAB.CMN-0008`). The release
builds two built-in profiles from this repository's `Dockerfile`:

| Profile | Image | Contents |
| --- | --- | --- |
| `base` | `ghcr.io/greenongrey/nabu-sandbox-base:<version>` | archives, jq, ripgrep, sqlite3, Node.js 24, git, Python 3 with `sandbox/base/uv.lock`, fonts with Cyrillic |
| `documents` | `ghcr.io/greenongrey/nabu-sandbox-documents:<version>` | `base` + `sandbox/documents/uv.lock` (python-docx, python-pptx, pandas, matplotlib, WeasyPrint…), Typst, poppler, qpdf, Chart.js, ECharts, Mermaid, KaTeX in `/opt/nabu/assets` |

`make sandbox-images` builds and checks them (`sandbox/test.sh`), `make sandbox-lock` updates the Python
libraries, `sandbox/documents/assets.lock` pins the tools and assets by sha256. System packages are pinned by
`DEBIAN_IMAGE` (a dated `debian:trixie-<date>-slim`) and `DEBIAN_SNAPSHOT` (a day of the apt archive on
snapshot.debian.org, possibly later than the image: `apt-get upgrade` brings its security fixes) at the top of the
`Dockerfile`. When the scan of a sandbox image finds a fixed Debian package, `make debian-snapshot` moves the image to
the latest dated one and the archive to today.

### Custom images

The team that runs an installation can add its own image as a profile in the administration (Sandbox
images → New profile), once the stand allows it (`SANDBOX_CUSTOM_IMAGES=true`, `SANDBOX_IMAGE_PREFIXES`).
Nabu puts its current binary into every sandbox through an init container, so a custom image needs
nothing of Nabu and keeps working after an update of Nabu. Requirements:

- any Linux image with `bash`;
- it works as user 1000 with a read-only root filesystem; `/tmp` and `/work` are writable;
- it is published under an allowed prefix; Nabu pins the profile to the digest of the tag at the check
  and moves to a new digest only on «Refresh image»;
- packages cannot be installed at run time: there is no network but the services of Nabu.

The image's `ENTRYPOINT`, `CMD` and `USER` are ignored. Its environment (`PATH`, `LD_LIBRARY_PATH`,
`ORACLE_HOME`…) reaches the agent's commands. Nabu does not scan custom images for vulnerabilities.

On a built-in profile — the standard tools plus yours:

```dockerfile
FROM ghcr.io/greenongrey/nabu-sandbox-documents:v0.5.0
USER root
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*
COPY corp-ca.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates
USER 1000
```

On a corporate base image — only what the team puts there:

```dockerfile
FROM registry.company.local/base/ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends bash python3 libaio1t64 \
 && rm -rf /var/lib/apt/lists/*
COPY instantclient/ /opt/oracle/
ENV LD_LIBRARY_PATH=/opt/oracle ORACLE_HOME=/opt/oracle PATH=/opt/oracle:$PATH
USER 1000
```
