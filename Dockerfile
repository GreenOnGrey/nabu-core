# Nabu core image: one binary with modes api | worker | agent | relay |
# sandbox | migrate | cleaner (FTR.NAB.CMN-0001 arch §3), with Pi for the
# agent operator and git for skill sources. The targets sandbox-base and
# sandbox-documents are the images of the built-in sandbox profiles
# (FTR.NAB.CMN-0008).

# The Debian of sandboxes (FTR.NAB.CMN-0008 R4): a dated image and the apt
# archive of the same day from snapshot.debian.org. The nabu-core build (the
# manifests of profiles) and the build of the published sandbox images get the
# same system packages, whenever each of them runs. `make debian-snapshot`
# moves both to the latest dated image.
ARG DEBIAN_IMAGE=debian:trixie-20261020-slim
ARG DEBIAN_SNAPSHOT=20261020T000000Z

FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/nabu ./cmd/nabu && mkdir -p /out/work

# ─── Sandbox profiles (FTR.NAB.CMN-0008) ─────────────────────────────
# Images of sandboxes: built-in profiles base and documents. They carry no
# Nabu binary: the init container of a sandbox puts the current one into
# /nabu. Python libraries come from sandbox/<profile>/uv.lock with hashes into
# the venv /opt/py without pip; tools and HTML assets from assets.lock with
# sha256, downloaded in a stage of their own so curl stays out of the images.
FROM ghcr.io/astral-sh/uv:0.9.5 AS uv

FROM debian:trixie-slim AS sandbox-assets
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl xz-utils \
 && rm -rf /var/lib/apt/lists/*
COPY sandbox/install-assets.sh sandbox/documents/assets.lock /tmp/
RUN mkdir -p /out/opt/nabu && sh /tmp/install-assets.sh /tmp/assets.lock /out

FROM ${DEBIAN_IMAGE} AS sandbox-base
ARG VERSION=dev
ARG DEBIAN_SNAPSHOT
COPY --from=node:24-trixie-slim /usr/local/bin/node /usr/local/bin/node
# apt reads the archive of the snapshot day only: upgrade brings the security
# fixes of that day, the same on every build
RUN sed -i -e "s|^URIs: http://deb.debian.org/debian-security$|URIs: http://snapshot.debian.org/archive/debian-security/${DEBIAN_SNAPSHOT}|" \
      -e "s|^URIs: http://deb.debian.org/debian$|URIs: http://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT}|" \
      /etc/apt/sources.list.d/debian.sources \
 && grep -q "snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT}" /etc/apt/sources.list.d/debian.sources \
 && echo 'Acquire::Check-Valid-Until "false";' >/etc/apt/apt.conf.d/99snapshot \
 && apt-get update && apt-get -y upgrade && apt-get install -y --no-install-recommends \
      bash ca-certificates tzdata git libstdc++6 \
      zip unzip xz-utils bzip2 jq ripgrep file tree patch less bc sqlite3 \
      python3 python3-venv \
      fontconfig fonts-dejavu-core fonts-liberation2 fonts-paratype \
 && rm -rf /var/lib/apt/lists/* /usr/lib/python3*/ensurepip \
 && fc-cache -f \
 && mkdir -p /work /opt/nabu && chown 1000:1000 /work
COPY sandbox/manifest.py /opt/nabu/manifest.py
COPY sandbox/base /tmp/sandbox
RUN --mount=from=uv,source=/uv,target=/usr/local/bin/uv \
    python3 -m venv --without-pip /opt/py \
 && cd /tmp/sandbox && uv export --frozen --no-emit-project --format requirements-txt >/tmp/requirements.txt \
 && uv pip install --python /opt/py/bin/python --require-hashes --no-deps --no-cache -r /tmp/requirements.txt \
 && rm -rf /tmp/sandbox /tmp/requirements.txt \
 && test ! -e /opt/py/bin/pip
ENV PATH=/opt/py/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    LANG=C.UTF-8 HOME=/work \
    MPLCONFIGDIR=/tmp/mpl XDG_CACHE_HOME=/tmp/cache PYTHONPYCACHEPREFIX=/tmp/pycache PYTHONDONTWRITEBYTECODE=1 \
    NABU_BASE_VERSION=${VERSION}
LABEL org.opencontainers.image.title="nabu-sandbox-base" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.source="https://github.com/GreenOnGrey/nabu-core"
USER 1000:1000
WORKDIR /work

FROM sandbox-base AS sandbox-documents
ARG VERSION=dev
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      poppler-utils qpdf libpango-1.0-0 libpangoft2-1.0-0 libharfbuzz-subset0 \
 && rm -rf /var/lib/apt/lists/*
COPY sandbox/documents /tmp/sandbox
RUN --mount=from=uv,source=/uv,target=/usr/local/bin/uv \
    cd /tmp/sandbox && uv export --frozen --no-emit-project --format requirements-txt >/tmp/requirements.txt \
 && uv pip install --python /opt/py/bin/python --require-hashes --no-deps --no-cache -r /tmp/requirements.txt \
 && rm -rf /tmp/sandbox /tmp/requirements.txt \
 && test ! -e /opt/py/bin/pip
COPY --from=sandbox-assets /out/ /
# The font cache of matplotlib, copied to MPLCONFIGDIR when a sandbox starts.
RUN MPLCONFIGDIR=/opt/nabu/mpl-cache python3 -c "import matplotlib.font_manager as f; f._load_fontmanager(try_read_cache=False)" \
 && chmod -R a+rX /opt/nabu
LABEL org.opencontainers.image.title="nabu-sandbox-documents"
USER 1000:1000

# Manifests of the built-in profiles for the api (space_info, the status of
# requested packages) without starting a sandbox.
FROM sandbox-base AS manifest-base
RUN python3 /opt/nabu/manifest.py base >/tmp/base.json
FROM sandbox-documents AS manifest-documents
RUN python3 /opt/nabu/manifest.py documents >/tmp/documents.json

# Release image (the default target): Nabu and the Pi agent. The Pi version
# comes from deploy/versions.env (PI_VERSION) and changes only by PR. The
# nabu-workspace extension routes Pi's file and shell tools to workspaces
# through the relay. Pi's scheduling and background-task extensions are not
# installed (FTR.NAB.CMN-0001 arch §9a, TSK-13). npm, corepack and yarn are
# removed after the install: nothing uses them at runtime.
FROM node:24-trixie-slim AS release
ARG VERSION=dev
ARG PI_VERSION
ARG PI_PACKAGE=@earendil-works/pi-coding-agent
RUN test -n "$PI_VERSION" \
 && apt-get update && apt-get -y upgrade && apt-get install -y --no-install-recommends ca-certificates git \
 && npm install -g --no-audit --no-fund "${PI_PACKAGE}@${PI_VERSION}" \
 && npm cache clean --force && rm -rf /var/lib/apt/lists/* /root/.npm \
 && rm -rf /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/corepack /opt/yarn-* \
           /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack /usr/local/bin/yarn /usr/local/bin/yarnpkg \
 && test -x /usr/local/bin/pi && test ! -e /usr/local/bin/npm \
 && ! ls /usr/local/lib/node_modules | grep -E 'pi-(scheduler|schedule-prompt|background-tasks)' \
 && mkdir -p /work && chown 1000:1000 /work
COPY pi-extensions/ /opt/nabu/pi-extensions/
COPY --from=build /out/nabu /usr/local/bin/nabu
COPY --from=manifest-base /tmp/base.json /opt/nabu/profiles/base.json
COPY --from=manifest-documents /tmp/documents.json /opt/nabu/profiles/documents.json
LABEL org.opencontainers.image.title="nabu-core" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.source="https://github.com/GreenOnGrey/nabu-core" \
      io.nabu.pi.package="${PI_PACKAGE}" \
      io.nabu.pi.version="${PI_VERSION}"
ENV HOME=/home/node PI_BINARY=/usr/local/bin/pi PI_EXTENSION_DIR=/opt/nabu/pi-extensions/nabu-workspace \
    AGENT_WORKDIR=/work PI_VERSION=${PI_VERSION}
# Numeric user (node): Kubernetes checks runAsNonRoot only for numeric users.
USER 1000:1000
WORKDIR /home/node
EXPOSE 8080 8081 8085 8090 9100
ENTRYPOINT ["/usr/local/bin/nabu"]
CMD ["api"]
