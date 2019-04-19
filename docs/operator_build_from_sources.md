# ClickHouse Operator Build From Sources

## Requirements 

1. `go-lang` compiler
2. `dep` Package Manager
3. Get the sources from our repository using `go` git wrapper `go get github.com/Altinity/clickhouse-operator`

## Binary Build Procedure

1. Switch working dir to `src/github.com/Altinity/clickhouse-operator`
2. Make sure all packages are linked properly by using `dep` package manager: `dep ensure --vendor-only` (you can also install `dep` with `go` like so: `go get -u github.com/golang/dep/cmd/dep`)
3. Build the sources `go build -o ./clickhouse-operator ./cmd/clickhouse-operator`. This will create `clickhouse-operator` binary which could be only used inside kubernetes environment.

## Docker Image Build and Usage Procedure

This process does not require `go-lang` compiler nor `dep` package manager. Instead it requires `kubernetes` and `docker`.

1. Switch working dir to `src/github.com/Altinity/clickhouse-operator`
2. Build docker image with `docker`: `docker build -t altinity/clickhouse-operator ./`
3. Register freshly build `docker` image inside `kubernetes` environment like so: `docker save altinity/clickhouse-operator | (eval $(minikube docker-env) && docker load)` 
4. Install `clickhouse-operator` as described here: [Install ClickHouse Operator][install] 

[install]: ./operator_installation_details.md