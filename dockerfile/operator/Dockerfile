# ===================
# ===== Builder =====
# ===================

FROM golang:1.14.3 AS builder-operator

# Install required packages
RUN apt-get update && apt-get install -y apt-utils gettext-base

# Check $GOPATH, for convenience
RUN echo "GOPATH=$GOPATH"
RUN echo "WORKDIR=$GOPATH/src/github.com/altinity/clickhouse-operator"

# Reconstruct source tree inside docker
WORKDIR $GOPATH/src/github.com/altinity/clickhouse-operator
ADD . .

# speedup docker build if vendor exists
# ./vendor is included in .dockerignore
# RUN go mod tidy
# RUN go mod vendor
ENV BUILD_IN_DOCKER=1

# Build operator binary with explicitly specified output
RUN OPERATOR_BIN=/tmp/clickhouse-operator ./dev/go_build_operator.sh

FROM builder-operator AS delve
WORKDIR /tmp/
RUN go get -v github.com/go-delve/delve/cmd/dlv

# Add config files from local source dir into image
ADD config/config.yaml   /etc/clickhouse-operator/
ADD config/conf.d/*      /etc/clickhouse-operator/conf.d/
ADD config/config.d/*    /etc/clickhouse-operator/config.d/
ADD config/templates.d/* /etc/clickhouse-operator/templates.d/
ADD config/users.d/*     /etc/clickhouse-operator/users.d/

CMD ["/go/bin/dlv", "--listen=:40001", "--headless=true", "--api-version=2", "exec", "/tmp/clickhouse-operator","--","-logtostderr=true", "-v=5"]

# ====================
# ===== Operator =====
# ====================

FROM alpine:3.10 AS operator

RUN apk add --no-cache ca-certificates

WORKDIR /

# Add config files from local source dir into image
ADD config/config.yaml   /etc/clickhouse-operator/
ADD config/conf.d/*      /etc/clickhouse-operator/conf.d/
ADD config/config.d/*    /etc/clickhouse-operator/config.d/
ADD config/templates.d/* /etc/clickhouse-operator/templates.d/
ADD config/users.d/*     /etc/clickhouse-operator/users.d/

# Copy clickhouse-operator binary into operator image from builder
COPY --from=builder-operator /tmp/clickhouse-operator .

# Run /clickhouse-operator -alsologtostderr=true -v=1
# We can specify additional options, such as:
#   --config=/path/to/config
#   --kube-config=/path/to/kubeconf
ENTRYPOINT ["/clickhouse-operator"]
CMD ["-logtostderr=true", "-v=1"]
#CMD ["-alsologtostderr=true", "-v=1"]
