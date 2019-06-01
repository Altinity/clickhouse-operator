# === Builder ===

FROM golang:1.11.5 AS builder

RUN apt-get update && apt-get install -y -q apt-utils && apt-get install -y -q gettext-base
WORKDIR $GOPATH/src/github.com/altinity/clickhouse-operator

# Reconstruct source tree inside docker
ADD . .
# ./vendor is excluded in .dockerignore, reconstruct it with 'dep' tool
RUN go get -u github.com/golang/dep/cmd/dep
RUN dep ensure --vendor-only

# Build operator binary with explicitly specified output
RUN OPERATOR_BIN=/tmp/clickhouse-operator ./dev/binary_build.sh

# === Runner ===

FROM alpine:3.8 AS runner
RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*
WORKDIR /
COPY --from=builder /tmp/clickhouse-operator .
ENTRYPOINT ["./clickhouse-operator"]
CMD ["-alsologtostderr=true", "-v=1"]
