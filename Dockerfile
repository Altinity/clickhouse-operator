# === Builder ===

FROM golang:1.11.5 AS builder

WORKDIR $GOPATH/src/github.com/altinity/clickhouse-operator

ADD Gopkg.toml Gopkg.toml
ADD Gopkg.lock Gopkg.lock
RUN go get -u github.com/golang/dep/cmd/dep
RUN dep ensure --vendor-only

ADD pkg pkg
ADD cmd cmd
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /tmp/clickhouse-operator ./cmd/clickhouse-operator

# === Runner ===

FROM alpine:3.8 AS runner
RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*
WORKDIR /
COPY --from=builder /tmp/clickhouse-operator .
ENTRYPOINT ["./clickhouse-operator"]
CMD ["-alsologtostderr=true", "-v=1"]
