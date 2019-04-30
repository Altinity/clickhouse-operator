#!/bin/bash

# Production docker image builder

# Externally configurable build-dependent options
TAG="${TAG:-altinity/clickhouse-operator:dev}"
DOCKERHUB_LOGIN="${DOCKERHUB_LOGIN:-altinitybuilds}"
DOCKERHUB_PUBLISH="${DOCKERHUB_PUBLISH:-yes}"
MINIKUBE="${MINIKUBE:-no}"

TAG="${TAG}" \
DOCKERHUB_LOGIN="${DOCKERHUB_LOGIN}" \
DOCKERHUB_PUBLISH="${DOCKERHUB_PUBLISH}" \
MINIKUBE="${MINIKUBE}" \
./image_build_universal.sh
