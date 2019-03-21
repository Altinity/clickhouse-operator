#!/bin/bash

eval $(minikube docker-env) # comment for outside of minikube
cat Dockerfile | envsubst | docker build -t altinity/clickhouse-operator:dev .
