#!/bin/bash

# Builds a dev docker image
# Alter image tag in deployment spec in dev env in order to use it

cat Dockerfile | envsubst | docker build -t sunsingerus/clickhouse-operator:dev .

docker login -u sunsingerus
docker push sunsingerus/clickhouse-operator:dev
