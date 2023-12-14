#!/bin/bash

# Start new release branch

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

CUR_RELEASE=$(cat "${SRC_ROOT}/release")
echo "Starting new release. Current release ${CUR_RELEASE}"

echo -n "Enter new release: "
read NEW_RELEASE

echo "Starting new release ${NEW_RELEASE}"

# Create release branch
git branch "${NEW_RELEASE}"
git checkout "${NEW_RELEASE}"
# Append cur release to the head of releases list
cat "${SRC_ROOT}/release" "${SRC_ROOT}/releases" > "${SRC_ROOT}/releases_tmp" && mv "${SRC_ROOT}/releases_tmp" "${SRC_ROOT}/releases"
# And write new release to release file
echo "${NEW_RELEASE}" > "${SRC_ROOT}/release"

# Commit new branch
git add ..
git commit -m "${NEW_RELEASE}"

# Some niceness
echo "Releases"
cat "${SRC_ROOT}/release"
head -n5 "${SRC_ROOT}/releases"

