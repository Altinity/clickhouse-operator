# ===================
# ===== Builder =====
# ===================

FROM --platform=${BUILDPLATFORM} golang:1.17 AS builder
ARG TARGETOS
ARG TARGETARCH

# Install required packages
RUN apt-get update && apt-get install -y apt-utils && apt-get install -y gettext-base curl
RUN curl -o /usr/bin/yq -sL https://github.com/mikefarah/yq/releases/download/v4.16.2/yq_${TARGETOS}_${TARGETARCH} && \
    chmod +x /usr/bin/yq

ENV GOOS=${TARGETOS}
ENV GOARCH=${TARGETARCH}

# Reconstruct source tree inside docker
WORKDIR /clickhouse-operator
ADD . .

# Build operator binary with explicitly specified output
RUN OPERATOR_BIN=/tmp/clickhouse-operator ./dev/go_build_operator.sh

FROM --platform=${BUILDPLATFORM} golang:1.17 AS delve-builder
WORKDIR /tmp/
RUN go get -v github.com/go-delve/delve/cmd/dlv && rm -rf /root/.cache/go-build/ /go/pkg/mod/

FROM ubuntu:20.04 AS delve
WORKDIR /tmp/
COPY --from=delve-builder /go/bin/dlv /go/bin/dlv

# Add config files from local source dir into image
ADD config/config.yaml   /etc/clickhouse-operator/
ADD config/conf.d/*      /etc/clickhouse-operator/conf.d/
ADD config/config.d/*    /etc/clickhouse-operator/config.d/
ADD config/templates.d/* /etc/clickhouse-operator/templates.d/
ADD config/users.d/*     /etc/clickhouse-operator/users.d/
COPY --from=builder /tmp/clickhouse-operator .
CMD ["/go/bin/dlv", "--listen=:40001", "--headless=true", "--api-version=2", "exec", "/tmp/clickhouse-operator","--","-logtostderr=true", "-v=5"]

# ====================
# ===== Operator =====
# ====================

FROM registry.access.redhat.com/ubi8/ubi-minimal AS ubi8
RUN microdnf update && microdnf clean all

MAINTAINER "Altinity <support@altinity.com>"

LABEL name="ClickHouse operator" \
      maintainer="support@altinity.com" \
      vendor="Altinity" \
      version="0.18.0" \
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
#   --config=/path/to/config
#   --kube-config=/path/to/kubeconf
ENTRYPOINT ["/clickhouse-operator"]
CMD ["-logtostderr=true", "-v=1"]
#CMD ["-alsologtostderr=true", "-v=1"]
