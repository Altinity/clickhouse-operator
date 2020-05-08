# ===================
# ===== Builder =====
# ====================

FROM golang:1.14.2 AS builder-metrics-exporter

# Install required packages
RUN apt-get update && apt-get install -y apt-utils gettext-base

# Check $GOPATH, for convenience
RUN echo "GOPATH=$GOPATH"
RUN echo "WORKDIR=$GOPATH/src/github.com/altinity/clickhouse-operator"

# Reconstruct source tree inside docker
WORKDIR $GOPATH/src/github.com/altinity/clickhouse-operator
ADD . .
# ./vendor is excluded in .dockerignore, reconstruct it with 'mod' tool
ENV GO111MODULE=on
RUN go mod tidy
RUN go mod vendor

# Build operator binary with explicitly specified output
RUN METRICS_EXPORTER_BIN=/tmp/metrics-exporter ./dev/go_build_metrics_exporter.sh

FROM builder-metrics-exporter AS delve
WORKDIR /tmp/
RUN go get -v github.com/go-delve/delve/cmd/dlv

# Add config files from local source dir into image
ADD config/config.yaml   /etc/clickhouse-operator/
ADD config/conf.d/*      /etc/clickhouse-operator/conf.d/
ADD config/config.d/*    /etc/clickhouse-operator/config.d/
ADD config/templates.d/* /etc/clickhouse-operator/templates.d/
ADD config/users.d/*     /etc/clickhouse-operator/users.d/

CMD ["/go/bin/dlv", "--listen=:40002", "--headless=true", "--api-version=2", "exec", "/tmp/metrics-exporter","--","-logtostderr=true", "-v=5"]

# ============================
# ===== Metrics exporter =====
# ============================

FROM alpine:3.10 AS metrics-exporter

RUN apk add --no-cache ca-certificates

WORKDIR /

# Add config files from local source dir into image
ADD config/config.yaml   /etc/clickhouse-operator/
ADD config/conf.d/*      /etc/clickhouse-operator/conf.d/
ADD config/config.d/*    /etc/clickhouse-operator/config.d/
ADD config/templates.d/* /etc/clickhouse-operator/templates.d/
ADD config/users.d/*     /etc/clickhouse-operator/users.d/

# Copy clickhouse-operator binary into operator image from builder
COPY --from=builder-metrics-exporter /tmp/metrics-exporter .

# Run /metrics-exporter -alsologtostderr=true -v=1
# We can specify additional options, such as:
#   --config=/path/to/config
#   --kube-config=/path/to/kubeconf
ENTRYPOINT ["/metrics-exporter"]
CMD ["-logtostderr=true", "-v=1"]
#CMD ["-alsologtostderr=true", "-v=1"]
