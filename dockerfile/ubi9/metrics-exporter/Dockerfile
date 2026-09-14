# ===================
# ===== Builder =====
# ===================

ARG GO_VERSION
ARG GO_TOOLSET_IMAGE=registry.access.redhat.com/ubi9/go-toolset:1.26.5
ARG UBI_IMAGE=registry.access.redhat.com/ubi9/ubi:9.8

FROM --platform=${BUILDPLATFORM} ${GO_TOOLSET_IMAGE} AS builder

USER 0

ARG TARGETOS
ARG TARGETARCH
ARG GCFLAGS
ARG GOFIPS140

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
FROM --platform=${BUILDPLATFORM} ${GO_TOOLSET_IMAGE} AS acvp-builder

USER 0

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
# argv0 dispatch symlink before assembling the runtime image.
RUN go mod tidy && \
    go mod vendor && \
    CGO_ENABLED=0 GO111MODULE=on GOOS="${GOOS}" GOARCH="${GOARCH}" GOFIPS140="${GOFIPS140}" go build \
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
FROM --platform=${BUILDPLATFORM} ${GO_TOOLSET_IMAGE} AS delve-builder
USER 0
ARG TARGETOS
ARG TARGETARCH
RUN GOBIN=/tmp CGO_ENABLED=0 GO111MODULE=on GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go install -ldflags "-s -w -extldflags '-static'" github.com/go-delve/delve/cmd/dlv@latest && \
    rm -rf /opt/app-root/src/.cache/go-build/ /opt/app-root/src/go/pkg/mod/

# ======================
# ===== Image Base =====
# ======================
FROM ${UBI_IMAGE} AS image-base
# Re-declare TARGETARCH inside this stage so the LABEL below substitutes it.
# ARGs declared before a FROM are consumed by the FROM and reset in the new stage.
ARG TARGETARCH
ARG UBI_IMAGE
ARG VERSION
ARG RELEASE=1
ARG GO_VERSION
ARG GOFIPS140
ARG GODEBUG_FIPS140=off

LABEL name="ClickHouse operator. Metrics exporter" \
      maintainer="Altinity <support@altinity.com>" \
      vendor="Altinity" \
      version="${VERSION:-unspecified_version}" \
      release="${RELEASE}" \
      summary="Metrics exporter" \
      description="Metrics exporter for Altinity ClickHouse operator" \
      altinity.fips.module="${GOFIPS140}" \
      altinity.fips.module.status="CMVP-in-review" \
      altinity.fips.build="GOFIPS140=${GOFIPS140}" \
      altinity.fips.runtime="GODEBUG=fips140=${GODEBUG_FIPS140}" \
      altinity.go.version="${GO_VERSION}" \
      altinity.image.base="${UBI_IMAGE}" \
      altinity.image.arch="${TARGETARCH}"

COPY LICENSE THIRD_PARTY_NOTICES.md /licenses/

WORKDIR /

# Add config files from local source dir into image
ADD config /etc/clickhouse-operator/

# Copy clickhouse-operator binary into operator image from builder
COPY --from=builder /tmp/metrics-exporter .

# =======================
# ===== Image Debug =====
# =======================
FROM image-base AS image-debug
WORKDIR /
COPY --from=delve-builder /tmp/dlv /go/bin/dlv
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
# the argv0 dispatch symlink created in acvp-builder.
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
