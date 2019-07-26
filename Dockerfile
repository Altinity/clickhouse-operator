# === Builder ===

FROM golang:1.12.7 AS builder

RUN apt-get update && apt-get install -y apt-utils gettext-base

# Reconstruct source tree inside docker
WORKDIR $GOPATH/src/github.com/altinity/clickhouse-operator
ADD . .
# ./vendor is excluded in .dockerignore, reconstruct it with 'dep' tool
RUN go get -u github.com/golang/dep/cmd/dep
RUN dep ensure --vendor-only

# Build operator binary with explicitly specified output
RUN OPERATOR_BIN=/tmp/clickhouse-operator ./dev/binary_build.sh

# === Runner ===

FROM alpine:3.10 AS runner

RUN apk add --no-cache ca-certificates
WORKDIR /
COPY --from=builder /tmp/clickhouse-operator .

ENTRYPOINT ["/clickhouse-operator"]
CMD ["-alsologtostderr=true", "-v=1"]
