#!/bin/bash

# The number of nodes to spin up. Defaults to 1.
NODES="${NODES:-"1"}"
# The Kubernetes version that the minikube will use
# Ex: v1.2.3, 'stable' for v1.23.3, 'latest' for v1.23.4-rc.0)
KUBERNETES_VERSION="${KUBERNETES_VERSION:-"stable"}"

PRUNE="${PRUNE:-""}"

echo "Launching kubernetes cluster."
echo "k8s version: ${KUBERNETES_VERSION}"
echo "nodes:       ${NODES}"
echo "prune:       ${PRUNE}"
sleep 5

minikube delete
if [[ ! -z "${PRUNE}" ]]; then
    docker system prune -f
fi
minikube version
minikube start --kubernetes-version="${KUBERNETES_VERSION}" --nodes="${NODES}"

if [[ -z ${SKIP_K9S} ]]; then
    k9s -c ns
fi
