SHELL := /bin/bash

init:
	dep ensure -v
	go install ./vendor/...

up_docker_server: stop_docker_server
	docker run --rm=true -p 127.0.0.1:8123:8123 --name dbr-clickhouse-server -d yandex/clickhouse-server;

stop_docker_server:
	test -n "$$(docker ps --format {{.Names}} | grep dbr-clickhouse-server)" && docker stop dbr-clickhouse-server || true

test: up_docker_server
	test -z "$$(golint ./... | grep -v vendor | tee /dev/stderr)"
	go vet -v ./...
	test -z "$$(gofmt -d -s $$(find . -name \*.go -print | grep -v vendor) | tee /dev/stderr)"
	go test -v -covermode=count -coverprofile=coverage.out . 
	$(MAKE) stop_docker_server