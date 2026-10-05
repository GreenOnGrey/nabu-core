# Nabu core image: one binary with modes api | worker | agent | relay |
# sandbox | migrate | cleaner (FTR.NAB.CMN-0001 arch §3), with Pi for the
# agent operator and git for skill sources.

FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/nabu ./cmd/nabu && mkdir -p /out/work

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
