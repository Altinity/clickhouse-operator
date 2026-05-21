#!/bin/bash

# Universal docker image builder.
# Should be called from image_build_operator_universal.sh or image_build_metrics_exporter_universal.sh

set -e
DOCKERFILE="${DOCKERFILE_DIR}/Dockerfile"

DOCKERHUB_LOGIN="${DOCKERHUB_LOGIN}"
DOCKERHUB_PUBLISH="${DOCKERHUB_PUBLISH:-"no"}"
MINIKUBE="${MINIKUBE:-"no"}"
MINIKUBE_PLATFORM="${MINIKUBE_PLATFORM:-""}"

# Source-dependent options
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
SRC_ROOT="$(realpath "${CUR_DIR}/..")"
source "${CUR_DIR}/go_build_config.sh"

source "${CUR_DIR}/build_manifests.sh"

cat << EOF
########################################
Build vars:
DOCKERHUB_LOGIN=${DOCKERHUB_LOGIN}
DOCKERHUB_PUBLISH=${DOCKERHUB_PUBLISH}
DOCKER_IMAGE=${DOCKER_IMAGE}
MINIKUBE=${MINIKUBE}
MINIKUBE_PLATFORM=${MINIKUBE_PLATFORM}
EOF

if [[ "${MINIKUBE}" == "yes" ]]; then
    echo "Going to build on minikube, not on the build host itself."
    echo "Minikube is expected to be run on build host, though"
    eval "$(minikube docker-env)"
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: docker CLI not found in PATH." >&2
    echo "Install Docker Desktop, or another engine with a client, and ensure the \`docker\` command works." >&2
    echo "Note: MINIKUBE=yes runs \`minikube docker-env\` so builds use Minikube's Docker daemon, but you still need the Docker *client* on this machine." >&2
    exit 1
fi

# BuildKit reads credsStore from config.json and runs docker-credential-<store>. If that binary is not on PATH
# (common with incomplete Docker Desktop PATH, Colima, or SSH sessions), pulls fail with:
#   exec: "docker-credential-desktop": executable file not found in $PATH
# Public base images do not need credentials; use a minimal config for this process only.
chop_docker_config_without_creds_if_helper_missing() {
    local cfg=""
    if [[ -n "${DOCKER_CONFIG:-}" && -f "${DOCKER_CONFIG}/config.json" ]]; then
        cfg="${DOCKER_CONFIG}/config.json"
    elif [[ -f "${HOME}/.docker/config.json" ]]; then
        cfg="${HOME}/.docker/config.json"
    else
        return 0
    fi
    local store
    store=$(sed -n 's/.*"credsStore"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "${cfg}" | head -1)
    [[ -n "${store}" ]] || return 0
    if command -v "docker-credential-${store}" >/dev/null 2>&1; then
        return 0
    fi
    export CHOP_SAVED_DOCKER_CONFIG="${DOCKER_CONFIG:-}"
    export DOCKER_CONFIG="$(mktemp -d)"
    echo '{}' > "${DOCKER_CONFIG}/config.json"
    echo "NOTE: ${cfg} sets credsStore=${store} but docker-credential-${store} is not in PATH." >&2
    echo "Using temporary DOCKER_CONFIG=${DOCKER_CONFIG} for this build (public image pulls only)." >&2
    echo "To fix permanently: add Docker's bin to PATH, or remove credsStore from ${cfg}, or install the matching helper." >&2
}
chop_docker_config_without_creds_if_helper_missing

normalize_image_build_architecture() {
    local arch="${1}"
    arch="${arch#linux/}"
    case "${arch}" in
        amd64|x86_64)
            echo "amd64"
            ;;
        arm64|aarch64)
            echo "arm64"
            ;;
        *)
            return 1
            ;;
    esac
}

MINIKUBE_BUILD_ARCH="amd64"
if [[ "${MINIKUBE}" == "yes" ]]; then
    if [[ -n "${MINIKUBE_PLATFORM}" ]]; then
        if ! MINIKUBE_BUILD_ARCH="$(normalize_image_build_architecture "${MINIKUBE_PLATFORM}")"; then
            echo "ERROR: unsupported MINIKUBE_PLATFORM=${MINIKUBE_PLATFORM}; expected linux/amd64 or linux/arm64." >&2
            exit 1
        fi
    else
        MINIKUBE_DOCKER_ARCH="$(docker info --format '{{.Architecture}}' 2>/dev/null || true)"
        if [[ -z "${MINIKUBE_DOCKER_ARCH}" ]]; then
            MINIKUBE_DOCKER_ARCH="$(uname -m)"
            echo "WARNING: unable to read Docker daemon architecture; falling back to host architecture ${MINIKUBE_DOCKER_ARCH}." >&2
        fi
        if ! MINIKUBE_BUILD_ARCH="$(normalize_image_build_architecture "${MINIKUBE_DOCKER_ARCH}")"; then
            echo "ERROR: unsupported Docker daemon architecture ${MINIKUBE_DOCKER_ARCH}; expected amd64/x86_64 or arm64/aarch64." >&2
            exit 1
        fi
    fi
fi

# Minikube dev images are loaded into one Docker daemon architecture; skip host qemu/binfmt setup (needs docker run).
# In case architecture of the host we are building on is arm, such as MacOS M1/M2, no need to install qemu
# We may need to install qemu otherwise
ARCHITECTURE=$(uname -m)
if [[ "${MINIKUBE}" == "yes" ]]; then
    echo "MINIKUBE=yes: skipping multiarch/qemu-user-static setup (single-platform ${MINIKUBE_BUILD_ARCH} build)."
elif [[ "${ARCHITECTURE}" =~ "arm" || "${ARCHITECTURE}" == "aarch64" ]]; then
    echo "Build host is arm and does not need qemu to be installed"
else
    echo "Need qemu to be installed on build host"
    echo "Check whether qemu is available"
    if docker run --rm --privileged multiarch/qemu-user-static --reset -p yes; then
        echo "qemu is in place, continue."
    else
        echo "qemu is not available, need to install."
        sudo apt-get install -y qemu binfmt-support qemu-user-static
        if docker run --rm --privileged multiarch/qemu-user-static --reset -p yes; then
            echo "qemu installed and available, continue."
        else
            echo "qemu is not installed and not available."
            echo "Abort."
            exit 1
        fi
    fi
fi

if docker buildx > /dev/null; then
    echo "docker buildx available, continue"
else
    echo "No docker buildx available. Abort."
    exit 1
fi

DOCKER_BUILDX_NUM=$(docker buildx ls | grep -E 'linux/arm.+\*' | grep -E 'running|inactive' | wc -l)
if [[ "${DOCKER_BUILDX_NUM}" == "0" ]]; then
    echo "Looks like there is no appropriate buildx instance available."
    echo "Create a new buildx instance."
    docker buildx create --use --name multi-platform --platform=linux/amd64,linux/arm64
else
    echo "Looks like there is an appropriate buildx instance available."
fi

#
# Build docker command
#

# Base docker build command
DOCKER_CMD="docker buildx build --progress plain"

# Append arch. Minikube platform wins over the dev tag so DOCKER_IMAGE=:dev does not force amd64.
if [[ "${MINIKUBE}" == "yes" ]]; then
    echo "Build image for Minikube Docker daemon architecture (${MINIKUBE_BUILD_ARCH}) only."
    DOCKER_CMD="${DOCKER_CMD} --platform=linux/${MINIKUBE_BUILD_ARCH} --load"
elif [[ "${DOCKER_IMAGE}" =~ ":dev" ]]; then
    echo "Build image (dev) for amd64 only, skip arm arch."
    DOCKER_CMD="${DOCKER_CMD} --platform=linux/amd64 --load"
    # Single --load into local docker (replaces invalid double --output: type=docker + type=image;
    # buildx errors: "multiple outputs currently unsupported".)
else
    echo "Build image for both amd64 and arm64."
    DOCKER_CMD="${DOCKER_CMD} --platform=linux/amd64,linux/arm64"
fi

# Append VERSION and RELEASE
DOCKER_CMD="${DOCKER_CMD} --build-arg VERSION=${VERSION:-dev} --build-arg GO_VERSION=${GO_VERSION}"

# Append GC flags if present
if [[ ! -z "${GCFLAGS}" ]]; then
    DOCKER_CMD="${DOCKER_CMD} --build-arg GCFLAGS='${GCFLAGS}'"
fi

# Append repo push
if [[ "${DOCKERHUB_PUBLISH}" == "yes" ]]; then
    DOCKER_CMD="${DOCKER_CMD} --push"
fi

DOCKER_CMD_BASE="${DOCKER_CMD}"

# Append tag, dockerfile and SRC_ROOT
DOCKER_CMD="${DOCKER_CMD} --tag ${DOCKER_IMAGE} --file ${DOCKERFILE} ${SRC_ROOT}"

if [[ "${DOCKERHUB_PUBLISH}" == "yes" ]]; then
    if [[ -n "${DOCKERHUB_LOGIN}" ]]; then
        echo "Dockerhub login specified: '${DOCKERHUB_LOGIN}', perform login"
        docker login -u "${DOCKERHUB_LOGIN}"
    fi
fi

echo "Docker build command ready:"
echo "${DOCKER_CMD}"
echo "Starting docker image build."
echo "Please, wait..."

if ${DOCKER_CMD}; then
    echo "OK. Build successful."
else
    echo "########################"
    echo "ERROR."
    echo "Docker image build has failed."
    echo "Abort"
    exit 1
fi

FIPS_BUILD="${FIPS_BUILD:-yes}"
if [[ "${FIPS_BUILD}" == "yes" ]]; then
    DOCKER_IMAGE_FIPS="${DOCKER_IMAGE}.fips"
    DOCKER_CMD_FIPS="${DOCKER_CMD_BASE} --target image-fips --tag ${DOCKER_IMAGE_FIPS} --file ${DOCKERFILE} ${SRC_ROOT}"

    echo ""
    echo "########################################"
    echo "Building FIPS variant: ${DOCKER_IMAGE_FIPS}"
    echo "########################################"
    echo "${DOCKER_CMD_FIPS}"
    echo "Please, wait..."

    if ${DOCKER_CMD_FIPS}; then
        echo "OK. FIPS build successful."
    else
        echo "########################"
        echo "ERROR."
        echo "FIPS Docker image build has failed."
        echo "Abort"
        exit 1
    fi
fi
