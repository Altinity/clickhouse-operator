# ===================
# ===== Builder =====
# ====================

FROM golang:1.17 AS builder
ARG TARGETOS
ARG TARGETARCH

# Install required packages
RUN apt-get update && apt-get install -y apt-utils && apt-get install -y gettext-base curl
RUN curl -o /usr/bin/yq -sL https://github.com/mikefarah/yq/releases/download/v4.16.2/yq_${TARGETOS}_${TARGETARCH} && \
    chmod +x /usr/bin/yq

# Reconstruct source tree inside docker
WORKDIR /clickhouse-operator
ADD . .

# ./vendor is excluded in .dockerignore, reconstruct it with 'mod' tool
RUN GO111MODULE=on go mod tidy
RUN GO111MODULE=on go mod vendor

# Build operator binary with explicitly specified output
RUN METRICS_EXPORTER_BIN=/tmp/metrics-exporter ./dev/go_build_metrics_exporter.sh

# ============================
# ===== Metrics exporter =====
# ============================

FROM registry.access.redhat.com/ubi8/ubi-minimal AS ubi8
RUN microdnf update && microdnf clean all

MAINTAINER "Altinity <support@altinity.com>"

LABEL name="ClickHouse operator. Metrics exporter" \
      maintainer="support@altinity.com" \
      vendor="Altinity" \
      version="0.18.0" \
      release="1" \
      summary="Metrics Exporter" \
      description="Metrics exporter for Altinity ClickHouse operator"

ADD LICENSE /licenses/

WORKDIR /

# Add config files from local source dir into image
ADD config/config.yaml   /etc/clickhouse-operator/
ADD config/conf.d/*      /etc/clickhouse-operator/conf.d/
ADD config/config.d/*    /etc/clickhouse-operator/config.d/
ADD config/templates.d/* /etc/clickhouse-operator/templates.d/
ADD config/users.d/*     /etc/clickhouse-operator/users.d/

# Copy clickhouse-operator binary into operator image from builder
COPY --from=builder /tmp/metrics-exporter .

USER nobody

# Run /metrics-exporter -alsologtostderr=true -v=1
# We can specify additional options, such as:
#   --config=/path/to/config
#   --kube-config=/path/to/kubeconf
ENTRYPOINT ["/metrics-exporter"]
CMD ["-logtostderr=true", "-v=1"]
#CMD ["-alsologtostderr=true", "-v=1"]
