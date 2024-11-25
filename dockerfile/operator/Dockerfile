# ===================
# ===== Builder =====
# ===================

FROM --platform=${BUILDPLATFORM} golang:1.23 AS builder

ARG TARGETOS
ARG TARGETARCH
ARG GCFLAGS

# Install required packages
RUN apt-get update && \
    apt-get install -y apt-utils && \
    apt-get install -y gettext-base wget
RUN wget -O /usr/bin/yq --progress=bar:force:noscroll "https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64" && \
    chmod +x /usr/bin/yq
RUN PLATFORM="x86_64" && if [ "${TARGETARCH}" = "arm64" ]; then PLATFORM="aarch64"; fi && \
    wget -O /tmp/bash --progress=bar:force:noscroll "https://github.com/robxu9/bash-static/releases/latest/download/bash-linux-${PLATFORM}" && \
    chmod +x /tmp/bash
RUN PLATFORM="amd64" && if [ "${TARGETARCH}" = "arm64" ]; then PLATFORM="aarch64"; fi && \
    wget -O /tmp/curl --progress=bar:force:noscroll "https://github.com/moparisthebest/static-curl/releases/latest/download/curl-${PLATFORM}" && \
    chmod +x /tmp/curl

# Reconstruct source tree inside docker
WORKDIR /clickhouse-operator
ADD . .
ENV GOOS="${TARGETOS}"
ENV GOARCH="${TARGETARCH}"
ENV GCFLAGS="${GCFLAGS}"

# Build operator binary with explicitly specified output
RUN OPERATOR_BIN=/tmp/clickhouse-operator bash -xe ./dev/go_build_operator.sh

# ===================
# == Delve builder ==
# ===================
FROM --platform=${BUILDPLATFORM} golang:1.23 AS delve-builder
RUN CGO_ENABLED=0 GO111MODULE=on GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go install -ldflags "-s -w -extldflags '-static'" github.com/go-delve/delve/cmd/dlv@latest && \
    rm -rf /root/.cache/go-build/ /go/pkg/mod/

# ======================
# ===== Image Base =====
# ======================
FROM gcr.io/distroless/static-debian11:latest       AS image-base-amd64
FROM gcr.io/distroless/static-debian11:latest-arm64 AS image-base-arm64
ARG TARGETARCH
FROM image-base-${TARGETARCH}                       AS image-base
ARG VERSION

LABEL name="ClickHouse operator" \
      maintainer="Altinity <support@altinity.com>" \
      vendor="Altinity" \
      version="${VERSION:-unspecified_version}" \
      summary="ClickHouse operator" \
      description="ClickHouse operator operates ClickHouse clusters in kubernetes"

ADD LICENSE /licenses/

WORKDIR /

# Add config files from local source dir into image
ADD config /etc/clickhouse-operator/

# Copy clickhouse-operator binary into operator image from builder
COPY --from=builder /tmp/clickhouse-operator .
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
CMD ["/go/bin/dlv", "--listen=:40001", "--headless=true", "--api-version=2", "exec", "/clickhouse-operator", "--", "-logtostderr=true", "-v=5"]

# ======================
# ===== Image Prod =====
# ======================
FROM image-base AS image-prod
RUN echo "Building PROD image"
WORKDIR /
USER nobody

# Run /clickhouse-operator -alsologtostderr=true -v=1
# We can specify additional options, such as:
#   --config=/path/to/config
#   --kube-config=/path/to/kubeconf
ENTRYPOINT ["/clickhouse-operator"]
CMD ["-logtostderr=true", "-v=1"]
#CMD ["-alsologtostderr=true", "-v=1"]
