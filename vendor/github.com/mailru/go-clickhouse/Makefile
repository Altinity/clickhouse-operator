SHELL := /bin/bash

init:
	GO111MODULE=on go mod vendor
	GO111MODULE=off go get golang.org/x/lint/golint

up_docker_server: stop_docker_server
	docker run --rm=true -p 127.0.0.1:8123:8123 --name dbr-clickhouse-server -d yandex/clickhouse-server:19.16.14.65;

stop_docker_server:
	test -n "$$(docker ps --format {{.Names}} | grep dbr-clickhouse-server)" && docker stop dbr-clickhouse-server || true

test: up_docker_server
	test -z "$$(golint ./... | grep -v vendor | tee /dev/stderr)"
	go vet -v ./...
	test -z "$$(gofmt -d -s $$(find . -name \*.go -print | grep -v vendor) | tee /dev/stderr)"
	go test -v -covermode=count -coverprofile=coverage.out . 
	$(MAKE) stop_docker_server