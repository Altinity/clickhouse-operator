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
RUN OPERATOR_BIN=/tmp/clickhouse-operator bash -xe ./dev/go_build_operator.sh

# ===================
# === ACVP builder ==
# ===================
# Compiles the operator binary with `-tags acvp_wrapper`. The wrapper inspects
# argv[0] at process start: when invoked as `*-acvp` the embedded NIST ACVP
# test responder is dispatched instead of the normal operator entrypoint.
# This stage is consumed only by `image-acvp`, NOT by `image-prod`.
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

# Build operator binary with acvp_wrapper build tag and create the argv0
# dispatch symlink before assembling the runtime image.
RUN go mod tidy && \
    go mod vendor && \
    CGO_ENABLED=0 GO111MODULE=on GOOS="${GOOS}" GOARCH="${GOARCH}" GOFIPS140="${GOFIPS140}" go build \
        -mod=vendor \
        -a \
        -tags acvp_wrapper \
        ${GCFLAGS:+-gcflags "${GCFLAGS}"} \
        -o /tmp/clickhouse-operator \
        ./cmd/operator/main.go && \
    ln -s clickhouse-operator /tmp/clickhouse-operator-acvp

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

LABEL name="ClickHouse operator" \
      maintainer="Altinity <support@altinity.com>" \
      vendor="Altinity" \
      version="${VERSION:-unspecified_version}" \
      release="${RELEASE}" \
      summary="ClickHouse operator" \
      description="ClickHouse operator operates ClickHouse clusters in kubernetes" \
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
COPY --from=builder /tmp/clickhouse-operator .

# =======================
# ===== Image Debug =====
# =======================
FROM image-base AS image-debug
WORKDIR /
COPY --from=delve-builder /tmp/dlv /go/bin/dlv
CMD ["/go/bin/dlv", "--listen=:40001", "--headless=true", "--api-version=2", "exec", "/clickhouse-operator", "--", "-logtostderr=true", "-v=5"]

# ======================
# ===== Image ACVP =====
# ======================
# ACVP-wrapper variant: ships the same operator binary plus a sibling at
# /clickhouse-operator-acvp that argv0-dispatches into the embedded NIST ACVP
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
      altinity.fips.acvp.binary="/clickhouse-operator-acvp" \
      altinity.image.variant="acvp"

# FIPS runtime mode for the ACVP responder. Inherits image-prod default (off).
# ACVP runs typically override via --build-arg GODEBUG_FIPS140=only (strict) or
# `on` (permissive) at the dispatch level so the responder exercises the FIPS
# module; the default-off image is non-FIPS-active and inappropriate for ACVP
# evidence runs without an override.
ENV GODEBUG=fips140=${GODEBUG_FIPS140}

# Overwrite the production binary with the acvp_wrapper-tagged build and copy
# the argv0 dispatch symlink created in acvp-builder.
COPY --from=acvp-builder /tmp/clickhouse-operator     /clickhouse-operator
COPY --from=acvp-builder /tmp/clickhouse-operator-acvp /clickhouse-operator-acvp

ENTRYPOINT ["/clickhouse-operator"]
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
#   off   — FIPS module not active; standard stdlib crypto. SHIPPED DEFAULT.
#           Preserves pre-0.27.1 runtime behavior even on the GOFIPS140-linked
#           binary. Opt in to on/only at runtime via Pod env (no rebuild).
#   on    — FIPS module active. TLS / cipher / signature / cert-chain
#           selection filtered to FIPS-approved primitives. Direct calls into
#           non-approved primitives outside the TLS layer remain callable.
#   only  — Strictest. Same as `on` plus any direct invocation of a
#           non-FIPS-approved primitive panics at call time. Best-effort
#           per Go upstream; documented as not for general production but
#           useful for CI gating.
#   debug — Same as `on` plus per-call stderr logging. Verbose, diagnostic only.
# Non-security SHA-1/MD5 usage in deterministic identifier hashing
# (pkg/util/{hash,shell}.go) is implemented as inline pure-Go and does NOT
# invoke crypto/sha1 or crypto/md5, so the operator runs cleanly under all
# four modes including `only`. See docs/security_hardening_fips.md §
# "Non-security hash exclusions".
# Customers can override at runtime via Pod env (e.g. kubectl set env
# deploy/clickhouse-operator GODEBUG=fips140=on) without rebuilding.
ENV GODEBUG=fips140=${GODEBUG_FIPS140}

# Run /clickhouse-operator -alsologtostderr=true -v=1
# We can specify additional options, such as:
#   --config=/path/to/config
#   --kube-config=/path/to/kubeconf
ENTRYPOINT ["/clickhouse-operator"]
CMD ["-logtostderr=true", "-v=1"]
#CMD ["-alsologtostderr=true", "-v=1"]
