# ===================
# ===== Builder =====
# ====================

FROM golang:1.17 AS builder

# Install required packages
RUN echo "deb http://ppa.launchpad.net/rmescandon/yq/ubuntu focal main" > /etc/apt/sources.list.d/yq.list && \
    echo "deb-src http://ppa.launchpad.net/rmescandon/yq/ubuntu focal main" >> /etc/apt/sources.list.d/yq.list && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys CC86BB64
RUN apt-get update && apt-get install -y apt-utils gettext-base yq

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

FROM registry.access.redhat.com/ubi7/ubi-minimal AS ubi7
RUN microdnf update && microdnf clean all

MAINTAINER "Altinity <support@altinity.com>"

LABEL name="ClickHouse operator. Metrics exporter" \
      maintainer="support@altinity.com" \
      vendor="Altinity" \
      version="0.17.0" \
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
