# ===================
# ===== Builder =====
# ===================

ARG GO_VERSION

FROM --platform=${BUILDPLATFORM} golang:${GO_VERSION:-latest} AS builder

ARG TARGETOS
ARG TARGETARCH
ARG GCFLAGS

# Install required packages
RUN apt-get update && \
    apt-get install -y apt-utils && \
    apt-get install -y gettext-base wget xz-utils
RUN wget -O /usr/bin/yq --progress=bar:force:noscroll "https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64" && \
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

# Build operator binary with explicitly specified output
RUN METRICS_EXPORTER_BIN=/tmp/metrics-exporter bash -xe ./dev/go_build_metrics_exporter.sh

# ===================
# == Delve builder ==
# ===================
FROM --platform=${BUILDPLATFORM} golang:${GO_VERSION:-latest} AS delve-builder
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

LABEL name="ClickHouse operator. Metrics exporter" \
      maintainer="Altinity <support@altinity.com>" \
      vendor="Altinity" \
      version="${VERSION:-unspecified_version}" \
      summary="Metrics exporter" \
      description="Metrics exporter for Altinity ClickHouse operator"

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
# ===== Image Prod =====
# ======================
FROM image-base AS image-prod
RUN echo "Building PROD image"
WORKDIR /
USER nobody

# Run /metrics-exporter -alsologtostderr=true -v=1
# We can specify additional options, such as:
#   --config=/path/to/config
#   --kube-config=/path/to/kubeconf
ENTRYPOINT ["/metrics-exporter"]
CMD ["-logtostderr=true", "-v=1"]
#CMD ["-alsologtostderr=true", "-v=1"]
