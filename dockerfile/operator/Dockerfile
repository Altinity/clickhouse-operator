# ===================
# ===== Builder =====
# ===================

FROM golang:1.13 AS builder

# Install required packages
RUN apt-get update && apt-get install -y apt-utils gettext-base

# Reconstruct source tree inside docker
WORKDIR /clickhouse-operator
ADD . .

# ./vendor is excluded in .dockerignore, reconstruct it with 'mod' tool
RUN GO111MODULE=on go mod tidy
RUN GO111MODULE=on go mod vendor

# Build operator binary with explicitly specified output
RUN OPERATOR_BIN=/tmp/clickhouse-operator ./dev/go_build_operator.sh

# ====================
# ===== Operator =====
# ====================

FROM registry.access.redhat.com/ubi7/ubi-minimal AS ubi7
RUN microdnf update && microdnf clean all

MAINTAINER "Altinity <support@altinity.com>"

LABEL name="ClickHouse operator" \
      maintainer="support@altinity.com" \
      vendor="Altinity" \
      version="0.17.0" \
      release="1" \
      summary="ClickHouse operator" \
      description="ClickHouse operator operates ClickHouse clusters in kubernetes"

ADD LICENSE /licenses/

WORKDIR /

# Add config files from local source dir into image
ADD config/config.yaml   /etc/clickhouse-operator/
ADD config/conf.d/*      /etc/clickhouse-operator/conf.d/
ADD config/config.d/*    /etc/clickhouse-operator/config.d/
ADD config/templates.d/* /etc/clickhouse-operator/templates.d/
ADD config/users.d/*     /etc/clickhouse-operator/users.d/

# Copy clickhouse-operator binary into operator image from builder
COPY --from=builder /tmp/clickhouse-operator .

USER nobody

# Run /clickhouse-operator -alsologtostderr=true -v=1
# We can specify additional options, such as:
#   --config=/path/to/confi g
#   --kube-config=/path/to/kubeconf
ENTRYPOINT ["/clickhouse-operator"]
CMD ["-logtostderr=true", "-v=1"]
#CMD ["-alsologtostderr=true", "-v=1"]
