# ===================
# ===== Builder =====
# ===================

ARG GO_VERSION

FROM --platform=${BUILDPLATFORM} golang:${GO_VERSION:-latest} AS builder

ARG TARGETOS
ARG TARGETARCH
ARG BUILDARCH=amd64
ARG GCFLAGS
ARG GOFIPS140

# Install required packages
RUN apt-get update && \
    apt-get install -y apt-utils && \
    apt-get install -y gettext-base wget xz-utils
RUN wget -O /usr/bin/yq --progress=bar:force:noscroll "https://github.com/mikefarah/yq/releases/latest/download/yq_linux_${BUILDARCH}" && \
    chmod +x /usr/bin/yq
RUN PLATFORM="x86_64" && if [ "${TARGETARCH}" = "arm64" ]; then PLATFORM="aarch64"; fi && \
    wget -O /tmp/bash --progress=bar:force:noscroll "https://github.com/robxu9/bash-static/releases/latest/download/bash-linux-${PLATFORM}" && \
    chmod +x /tmp/bash
RUN PLATFORM="x86_64" && if [ "${TARGETARCH}" = "arm64" ]; then PLATFORM="aarch64"; fi && \
    CURL_VERSION=$(wget -qO- https://github.com/stunnel/static-curl/releases/latest 2>&1 | grep -oE 'tag/[0-9.]+' | head -n1 | cut -d'/' -f2) && \
    wget -O /tmp/curl.tar.xz --progress=bar:force:noscroll "https://github.com/stunnel/static-curl/releases/download/${CURL_VERSION}/curl-linux-${PLATFORM}-glibc-${CURL_VERSION}.tar.xz" && \
    tar -xf /tmp/curl.tar.xz -C /tmp/ && \
    chmod +x /tmp/curl && \
    rm -f /tmp/curl.tar.xz

# Reconstruct source tree inside docker
WORKDIR /clickhouse-operator
ADD . .
ENV GOOS="${TARGETOS}"
ENV GOARCH="${TARGETARCH}"
ENV GCFLAGS="${GCFLAGS}"
ENV GOFIPS140="${GOFIPS140}"

# Build operator binary with explicitly specified output
RUN METRICS_EXPORTER_BIN=/tmp/metrics-exporter bash -xe ./dev/go_build_metrics_exporter.sh

# ===================
# === ACVP builder ==
# ===================
# Compiles the metrics-exporter binary with `-tags acvp_wrapper`. The wrapper
# inspects argv[0] at process start: when invoked as `*-acvp` the embedded
# NIST ACVP test responder is dispatched instead of the normal exporter entry
# point. This stage is consumed only by `image-acvp`, NOT by `image-prod`.
FROM --platform=${BUILDPLATFORM} golang:${GO_VERSION:-latest} AS acvp-builder

ARG TARGETOS
ARG TARGETARCH
ARG GCFLAGS
ARG GOFIPS140

WORKDIR /clickhouse-operator
ADD . .
ENV GOOS="${TARGETOS}"
ENV GOARCH="${TARGETARCH}"
ENV GCFLAGS="${GCFLAGS}"
ENV GOFIPS140="${GOFIPS140}"

# Build metrics-exporter binary with acvp_wrapper build tag and create the
# argv0 dispatch symlink in this builder stage (distroless has no shell to
# do it).
RUN CGO_ENABLED=0 GO111MODULE=on GOOS="${GOOS}" GOARCH="${GOARCH}" GOFIPS140="${GOFIPS140}" \
    go build \
        -mod=vendor \
        -a \
        -tags acvp_wrapper \
        ${GCFLAGS:+-gcflags "${GCFLAGS}"} \
        -o /tmp/metrics-exporter \
        ./cmd/metrics_exporter/main.go && \
    ln -s metrics-exporter /tmp/metrics-exporter-acvp

# ===================
# == Delve builder ==
# ===================
FROM --platform=${BUILDPLATFORM} golang:${GO_VERSION:-latest} AS delve-builder
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GO111MODULE=on GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go install -ldflags "-s -w -extldflags '-static'" github.com/go-delve/delve/cmd/dlv@latest && \
    rm -rf /root/.cache/go-build/ /go/pkg/mod/

# ======================
# ===== Image Base =====
# ======================
FROM gcr.io/distroless/static-debian13:latest       AS image-base-amd64
FROM gcr.io/distroless/static-debian13:latest-arm64 AS image-base-arm64
ARG TARGETARCH
FROM image-base-${TARGETARCH}                       AS image-base
# Re-declare TARGETARCH inside this stage so the LABEL below substitutes it.
# ARGs declared before a FROM are consumed by the FROM and reset in the new stage.
ARG TARGETARCH
ARG VERSION
ARG GO_VERSION
ARG GOFIPS140
ARG GODEBUG_FIPS140=off

LABEL name="ClickHouse operator. Metrics exporter" \
      maintainer="Altinity <support@altinity.com>" \
      vendor="Altinity" \
      version="${VERSION:-unspecified_version}" \
      summary="Metrics exporter" \
      description="Metrics exporter for Altinity ClickHouse operator" \
      altinity.fips.module="${GOFIPS140}" \
      altinity.fips.module.status="CMVP-in-review" \
      altinity.fips.build="GOFIPS140=${GOFIPS140}" \
      altinity.fips.runtime="GODEBUG=fips140=${GODEBUG_FIPS140}" \
      altinity.go.version="${GO_VERSION}" \
      altinity.image.base="gcr.io/distroless/static-debian13" \
      altinity.image.arch="${TARGETARCH}"

ADD LICENSE /licenses/

WORKDIR /

# Add config files from local source dir into image
ADD config /etc/clickhouse-operator/

# Copy clickhouse-operator binary into operator image from builder
COPY --from=builder /tmp/metrics-exporter .
COPY --from=builder /tmp/bash /bin/bash
COPY --from=builder /tmp/bash /bin/sh
COPY --from=builder /tmp/curl /bin/curl

# =======================
# ===== Image Debug =====
# =======================
FROM image-base AS image-debug
RUN echo "Building DEBUG image"
WORKDIR /
COPY --from=delve-builder /go/bin/dlv /go/bin/dlv
CMD ["/go/bin/dlv", "--listen=:40002", "--headless=true", "--api-version=2", "exec", "/metrics-exporter", "--", "-logtostderr=true", "-v=5"]

# ======================
# ===== Image ACVP =====
# ======================
# ACVP-wrapper variant: ships the same metrics-exporter binary plus a sibling
# at /metrics-exporter-acvp that argv0-dispatches into the embedded NIST ACVP
# test responder (compiled in via `-tags acvp_wrapper`). Use only in CI/audit
# contexts; the production image (image-prod) is the default target and does
# NOT contain the ACVP responder (attack surface).
#
# Note: image-acvp is placed BEFORE image-prod so that image-prod remains the
# last stage, which is what `docker buildx build` selects when no --target is
# passed (see dev/image_build_universal.sh — it never sets --target). Builders
# that want the ACVP variant must pass --target image-acvp explicitly.
FROM image-base AS image-acvp
# Re-declare ARGs consumed by FROM so the LABEL below substitutes them.
ARG TARGETARCH
ARG VERSION
ARG GO_VERSION
ARG GOFIPS140
ARG GODEBUG_FIPS140=off
RUN echo "Building ACVP image"
WORKDIR /
USER nobody

# Advertise the ACVP responder build via an extra OCI label; all other labels
# match image-prod (inherited from image-base).
LABEL altinity.fips.acvp.responder="true" \
      altinity.fips.acvp.binary="/metrics-exporter-acvp" \
      altinity.image.variant="acvp"

# FIPS runtime mode for the ACVP responder. Inherits image-prod default (off).
# ACVP runs typically override via --build-arg GODEBUG_FIPS140=only or `on` so
# the responder exercises the FIPS module; the default-off image is non-FIPS-
# active and inappropriate for ACVP evidence runs without an override.
ENV GODEBUG=fips140=${GODEBUG_FIPS140}

# Overwrite the production binary with the acvp_wrapper-tagged build and copy
# the argv0 dispatch symlink (created in acvp-builder, since distroless has
# no shell to create symlinks at image-assembly time).
COPY --from=acvp-builder /tmp/metrics-exporter      /metrics-exporter
COPY --from=acvp-builder /tmp/metrics-exporter-acvp /metrics-exporter-acvp

ENTRYPOINT ["/metrics-exporter"]
CMD ["-logtostderr=true", "-v=1"]

# ======================
# ===== Image Prod =====
# ======================
# Default target (last stage in this file — buildx picks the last stage when
# --target is not specified). MUST remain the final stage so production builds
# stay free of the ACVP responder.
FROM image-base AS image-prod
ARG GODEBUG_FIPS140=off
RUN echo "Building PROD image"
WORKDIR /
USER nobody

# FIPS runtime mode (paired with GOFIPS140 build-time linking). Build-arg
# GODEBUG_FIPS140 selects the runtime enforcement level. Accepted values:
# off (shipped default — module dormant), on (module active + TLS filtering),
# only (strict; non-FIPS calls panic), debug (on + stderr logging). See
# dockerfile/operator/Dockerfile and docs/security_hardening_fips.md § "Knobs"
# for the full rationale. Customers can override at runtime via Pod env
# without rebuilding.
ENV GODEBUG=fips140=${GODEBUG_FIPS140}

# Run /metrics-exporter -alsologtostderr=true -v=1
# We can specify additional options, such as:
#   --config=/path/to/config
#   --kube-config=/path/to/kubeconf
ENTRYPOINT ["/metrics-exporter"]
CMD ["-logtostderr=true", "-v=1"]
#CMD ["-alsologtostderr=true", "-v=1"]
