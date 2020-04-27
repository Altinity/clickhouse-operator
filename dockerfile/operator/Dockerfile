# ===================
# ===== Builder =====
# ===================

FROM golang:1.12.7 AS builder-operator

# Install required packages
RUN apt-get update && apt-get install -y apt-utils gettext-base

# Check $GOPATH, for convenience
RUN echo "GOPATH=$GOPATH"
RUN echo "WORKDIR=$GOPATH/src/github.com/altinity/clickhouse-operator"

# Reconstruct source tree inside docker
WORKDIR $GOPATH/src/github.com/altinity/clickhouse-operator
ADD . .
# ./vendor is excluded in .dockerignore, reconstruct it with 'mod' tool
RUN GO111MODULE=on go mod tidy
RUN GO111MODULE=on go mod vendor

# Build operator binary with explicitly specified output
RUN OPERATOR_BIN=/tmp/clickhouse-operator ./dev/go_build_operator.sh

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
