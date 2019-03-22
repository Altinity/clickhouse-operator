#!/bin/bash

# Builds a dev docker image
# Alter image tag in deployment spec in dev env in order to use it

eval $(minikube docker-env) # comment for outside of minikube
cat Dockerfile | envsubst | docker build -t altinity/clickhouse-operator:dev .

# In order to push images to dockehub use those commands:
#
# docker login -u altinitybuilds
# docker push altinity/clickhouse-operator:dev