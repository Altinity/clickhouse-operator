#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail

# Run `go vet` over all packages as a local static-analysis gate.
#
# Two analyzers are disabled because in this codebase they only fire on
# deliberate, accepted patterns — not real defects — and would otherwise bury
# the actionable findings under ~50 lines of known noise:
#
#   printf    — the announcer logging API (a.V(n).M(x).F().Info(msg, args...))
#               legitimately forwards a runtime-built, non-constant format
#               string. Every printf hit is that logging pattern, not a bug.
#   copylocks — generated deepcopy/fake code (zz_generated.deepcopy.go,
#               pkg/client/**/fake) copies lock-bearing structs by value, which
#               is unavoidable in generated code, plus one mergo.Merge call.
#
# Everything else stays enabled (unreachable, struct tag, unusedresult, …), so
# real problems still fail the run. This mirrors the govet settings planned for
# .golangci.yml so the local gate and the linter agree.
#
# NOTE: the suite is run with `go test -vet=off` (see run_go_tests.sh) because
# the disabled analyzers above also trip the test-binary compile; this script is
# the dedicated place vet actually runs.

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

cd "${SRC_ROOT}"

echo "Running go vet over ${SRC_ROOT} (printf, copylocks disabled — see header)..."
if go vet -copylocks=false -printf=false ./...; then
    echo "go vet: no findings"
else
    rc=$?
    echo "go vet: found issues (exit ${rc})" >&2
    exit "${rc}"
fi
