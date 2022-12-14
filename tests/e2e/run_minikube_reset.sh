#!/bin/bash

NODES="${NODES:-"1"}"
# The Kubernetes version that the minikube will use
# Ex: v1.2.3, 'stable' for v1.23.3, 'latest' for v1.23.4-rc.0)
KUBERNETES_VERSION="${KUBERNETES_VERSION:-"stable"}"
echo "Launching kubernetes cluster version ${KUBERNETES_VERSION} with ${NODES} nodes"
sleep 5

minikube delete && docker system prune -f && minikube start --kubernetes-version="${KUBERNETES_VERSION}" --nodes="${NODES}" && k9s -c ns
