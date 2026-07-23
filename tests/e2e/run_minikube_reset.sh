#!/bin/bash

# The number of nodes to spin up.
# Defaults to 1.
NODES="${NODES:-"1"}"
# Number of CPUs allocated to Kubernetes.
# Use "max" to use the maximum number of CPUs.
CPUS="${CPUS:-"max"}"
# Amount of RAM to allocate to Kubernetes (format: <number>[<unit>], where unit = b, k, m or g).
# Use "max" to use the maximum amount of memory.
MEMORY="${MEMORY:-"20g"}"
# The Kubernetes version that the minikube will use
# Ex: v1.2.3, 'stable' for v1.23.3, 'latest' for v1.23.4-rc.0)
KUBERNETES_VERSION="${KUBERNETES_VERSION:-"stable"}"

# Whether to prune docker during reset process
DOCKER_PRUNE="${DOCKER_PRUNE:-""}"
DOCKER_PRUNE_ALL="${DOCKER_PRUNE_ALL:-""}"
DOCKER_VERSION="${DOCKER_VERSION:-""}"

# Whether to prune minikube during reset process
MINIKUBE_PRUNE="${MINIKUBE_PRUNE:-""}"

# Minikube profile to operate on. Defaults to "minikube" (single-cluster, unchanged).
# Set to an isolated name for dual-cluster e2e; for a non-default profile the
# destructive cross-profile prune is skipped so a sibling cluster is never nuked.
MINIKUBE_PROFILE="${MINIKUBE_PROFILE:-minikube}"

echo "Reset kubernetes cluster."
echo "k8s version:   ${KUBERNETES_VERSION}"
echo "nodes:         ${NODES}"
echo "cpus:          ${CPUS}"
echo "memory:        ${MEMORY}"
echo "docker prune:  ${DOCKER_PRUNE}"
echo "minikube prune:${MINIKUBE_PRUNE}"
echo "minikube profile:${MINIKUBE_PROFILE}"

echo "Delete cluster"
minikube delete -p "${MINIKUBE_PROFILE}"
if [[ ! -z "${DOCKER_PRUNE}" ]]; then
    echo "Docker system prune"
    docker system prune -f
fi
if [[ ! -z "${DOCKER_PRUNE_ALL}" ]]; then
    echo "Docker system prune all"
    docker system prune -f --all
fi
if [[ ! -z "${MINIKUBE_PRUNE}" && "${MINIKUBE_PROFILE}" == "minikube" ]]; then
    # `--all --purge` + `rm -rf ~/.minikube` destroy EVERY profile, so this only
    # runs for the default profile. A named (dual-cluster) profile skips it to
    # avoid nuking the sibling cluster running concurrently.
    echo "Minikube prune"
    minikube stop
    minikube delete --all --purge
    rm -rf ~/.minikube
fi

echo "Create cluster"
echo "--------------"
echo "minikube version"
minikube version
echo "docker version"
docker version | grep Version -B1 | grep -v '\-\-'

if [[ ! -z "${DOCKER_VERSION}" ]]; then
    CUR_DOCKER_VERSION=$(docker version | grep Version | head -1 | awk '{print $2}')
    # Split CUR_DOCKER_VERSION by '.' and extract major version
    arrDOCKER_VERSION=(${CUR_DOCKER_VERSION//./ })
    MAJOR_VERSION=${arrDOCKER_VERSION[0]}
    echo "cur docker version  : ${CUR_DOCKER_VERSION}"
    echo "major docker version: ${MAJOR_VERSION}"
    if [[ "${MAJOR_VERSION}" -gt "24" ]]; then
        echo "Need to downgrade docker to 24"
        VERSION_STRING=5:24.0.8-1~ubuntu.20.04~focal
        #VERSION_STRING=5:24.0.8-1~ubuntu.22.04~jammy
        sudo apt-get install -y --allow-downgrades docker-ce=${VERSION_STRING} docker-ce-cli=${VERSION_STRING} containerd.io docker-buildx-plugin docker-compose-plugin
        echo "updated docker version"
        docker version | grep Version -B1 | grep -v '\-\-'
    else
        echo "Docker version is OK"
    fi
fi

echo "-----------------------"
echo "-- Starting minikube --"
echo "-----------------------"

minikube start -p "${MINIKUBE_PROFILE}" --kubernetes-version="${KUBERNETES_VERSION}" --nodes="${NODES}" --cpus="${CPUS}" --memory="${MEMORY}"
#minikube start -p "${MINIKUBE_PROFILE}" --kubernetes-version="${KUBERNETES_VERSION}" --nodes="${NODES}" --cpus="${CPUS}" --memory="${MEMORY}" --cache-images=false

echo "Enabling metrics-server addon"
minikube addons enable metrics-server -p "${MINIKUBE_PROFILE}"

if [[ -z "${SKIP_K9S}" && "${MINIKUBE_PROFILE}" == "minikube" ]]; then
    echo "Launching k9s"
    k9s -c ns
fi
