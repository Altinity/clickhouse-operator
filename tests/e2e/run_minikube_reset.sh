#!/bin/bash

# The number of nodes to spin up. Defaults to 1.
NODES="${NODES:-"1"}"
# The Kubernetes version that the minikube will use
# Ex: v1.2.3, 'stable' for v1.23.3, 'latest' for v1.23.4-rc.0)
KUBERNETES_VERSION="${KUBERNETES_VERSION:-"stable"}"

# Whether to prune docker during reset process
DOCKER_PRUNE="${DOCKER_PRUNE:-""}"
DOCKER_PRUNE_ALL="${DOCKER_PRUNE_ALL:-""}"

echo "Reset kubernetes cluster."
echo "k8s version: ${KUBERNETES_VERSION}"
echo "nodes:       ${NODES}"
echo "docker prune:${DOCKER_PRUNE}"

echo "Delete cluster"
minikube delete
if [[ ! -z "${DOCKER_PRUNE}" ]]; then
    echo "System prune"
    docker system prune -f
fi
if [[ ! -z "${DOCKER_PRUNE_ALL}" ]]; then
    echo "System prune all"
    docker system prune -f --all
fi

echo "Create cluster"
echo "--------------"
echo "minikube version"
minikube version
echo "docker version"
docker version | grep Version -B1 | grep -v '\-\-'
echo "--------------"
minikube start --kubernetes-version="${KUBERNETES_VERSION}" --nodes="${NODES}"

if [[ -z "${SKIP_K9S}" ]]; then
    echo "Launching k9s"
    k9s -c ns
fi
