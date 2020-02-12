# ClickHouse Operator Build From Sources

## Requirements 

1. `go-lang` compiler
2. `mod` Package Manager
3. Get the sources from our repository using `go` git wrapper `go get github.com/altinity/clickhouse-operator`

## Binary Build Procedure

1. Switch working dir to `src/github.com/altinity/clickhouse-operator`
2. Make sure all packages are linked properly by using `mod` package manager: `go mod tidy`
3. Build the sources `go build -o ./clickhouse-operator cmd/operator/main.go`. This will create `clickhouse-operator` binary which could be only used inside kubernetes environment.

## Docker Image Build and Usage Procedure

This process does not require `go-lang` compiler nor `dep` package manager. Instead it requires `kubernetes` and `docker`.

1. Switch working dir to `src/github.com/altinity/clickhouse-operator`
2. Build docker image with `docker`: `docker build -t altinity/clickhouse-operator ./`
3. Register freshly build `docker` image inside `kubernetes` environment like so: `docker save altinity/clickhouse-operator | (eval $(minikube docker-env) && docker load)` 
4. Install `clickhouse-operator` as described here: [Install ClickHouse Operator][install] 

[install]: ./operator_installation_details.md