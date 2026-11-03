#!/bin/bash
#
# run_go_tests.sh — runs all Go unit tests in the repository.
#
# Usage:
#   ./dev/run_go_tests.sh                                # run every test in the repo
#   ./dev/run_go_tests.sh -v                             # verbose output
#   ./dev/run_go_tests.sh -race                          # enable race detector
#   ./dev/run_go_tests.sh -cover                         # with coverage summary
#   ./dev/run_go_tests.sh -run TestFoo ./pkg/foo/...     # filter by test name and package
#   ./dev/run_go_tests.sh ./pkg/controller/chi/...       # restrict to one package tree
#
# Any args are forwarded verbatim to `go test`. With no args, all packages
# (./...) are tested.
#
# Why `-vet=off`:
#   The repo currently has pre-existing `go vet` warnings in files unrelated to
#   the packages under test (non-constant format strings in a few callers). If
#   vet is on, the test binary fails to compile even for packages that have no
#   warnings themselves. Vet is still run separately via `dev/run_vet.sh`.
#

# Exit immediately when a command fails
set -o errexit
# Error on unset variables
set -o nounset
# Only exit with zero if all commands of the pipeline exit successfully
set -o pipefail

# Source configuration (sets SRC_ROOT and friends)
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

cd "${SRC_ROOT}"

# Detect whether the caller already passed a package spec (./..., ./pkg/...,
# ../pkg/..., /absolute/path, github.com/...). If not, append ./... so plain
# flag args like `-v` or `-run TestFoo` still run against every package in
# the repo.
HAS_PKG=""
for arg in "$@"; do
    case "${arg}" in
        ./*|../*|/*|github.com/*)
            HAS_PKG="yes"
            break
            ;;
    esac
done

if [[ -n "${HAS_PKG}" ]]; then
    go test -vet=off "$@"
else
    go test -vet=off "$@" ./...
fi
