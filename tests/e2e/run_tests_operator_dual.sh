#!/bin/bash
# Dual-cluster operator e2e: run the PARALLEL-safe scenarios and the NO_PARALLEL
# scenarios SIMULTANEOUSLY against two independent minikube clusters, then merge
# both result sets into ONE combined table.
#
#   k8s-par (parallel cluster):   E2E_PHASE=parallel, POOL_SIZE threads -> the concurrent pool
#   k8s-seq (sequential cluster): E2E_PHASE=serial,   POOL_SIZE=1       -> NO_PARALLEL scenarios
#
# PAR_ONLY=yes runs ONLY k8s-par (no k8s-seq) so the parallel cluster gets the whole
# host — used to test high parallelism (e.g. POOL_SIZE=25 on all 12 CPU).
#
# Each suite is a normal run_tests_operator.sh process whose kube comms are pinned to
# its cluster via KUBECTL_CMD=--context/--kubeconfig, emitting an untrimmed raw log;
# the logs are rendered into one combined table by merge_dual_results.py.
# The single-cluster scripts are untouched; this is a separate opt-in entry point.
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/test_common.sh"

PROFILE_PAR="${MINIKUBE_PROFILE_PAR:-k8s-par}"
PROFILE_SEQ="${MINIKUBE_PROFILE_SEQ:-k8s-seq}"
KUBECONFIG_PAR="${KUBECONFIG_PAR:-${HOME}/.kube/${PROFILE_PAR}.config}"
KUBECONFIG_SEQ="${KUBECONFIG_SEQ:-${HOME}/.kube/${PROFILE_SEQ}.config}"
RAW_PAR="${RAW_PAR:-/tmp/e2e_dual_${PROFILE_PAR}.raw}"
RAW_SEQ="${RAW_SEQ:-/tmp/e2e_dual_${PROFILE_SEQ}.raw}"
OUT_PAR="${OUT_PAR:-/tmp/e2e_dual_${PROFILE_PAR}.out}"
OUT_SEQ="${OUT_SEQ:-/tmp/e2e_dual_${PROFILE_SEQ}.out}"

# PAR_ONLY=yes: run only the parallel cluster. With k8s-seq not competing for the host,
# k8s-par gets the whole machine, so its CPU/RAM defaults jump (override as needed).
PAR_ONLY="${PAR_ONLY:-}"
if [[ -n "${PAR_ONLY}" ]]; then
    export CPUS_PAR="${CPUS_PAR:-12}"
    export MEMORY_PAR="${MEMORY_PAR:-28g}"
fi

# Active profiles drive both image preload/load and (for normal mode) the dual reset.
if [[ -n "${PAR_ONLY}" ]]; then
    ACTIVE_PROFILES="${PROFILE_PAR}"
else
    ACTIVE_PROFILES="${PROFILE_PAR} ${PROFILE_SEQ}"
fi

# Tear down the profile(s) on ANY exit (normal, early FATAL, Ctrl-C) so a failed
# reset/build or interrupt never leaks clusters. KEEP_CLUSTERS=yes opts out. Deleting
# a non-existent profile is a harmless no-op (safe on preflight exit / PAR_ONLY).
teardown_clusters() {
    [[ -n "${KEEP_CLUSTERS:-}" ]] && return
    echo "Tearing down ${ACTIVE_PROFILES} (set KEEP_CLUSTERS=yes to keep)"
    local p
    for p in ${ACTIVE_PROFILES}; do minikube delete -p "${p}" >/dev/null 2>&1; done
}
trap teardown_clusters EXIT

# PREFLIGHT: the result merge depends on the `tfs` CLI. Fail LOUD now rather than
# after ~an hour of testing.
command -v tfs >/dev/null 2>&1 || { echo "FATAL: tfs CLI not found (needed to render results)"; exit 3; }
tfs transform short --help >/dev/null 2>&1 || { echo "FATAL: 'tfs transform short' unavailable"; exit 3; }

# Reset cluster(s) unless explicitly opted out (MINIKUBE_RESET=no).
if [[ "${MINIKUBE_RESET:-yes}" != "no" ]]; then
    if [[ -n "${PAR_ONLY}" ]]; then
        SKIP_K9S=yes MINIKUBE_PROFILE="${PROFILE_PAR}" KUBECONFIG="${KUBECONFIG_PAR}" \
        CPUS="${CPUS_PAR}" MEMORY="${MEMORY_PAR}" \
            "${CUR_DIR}/run_minikube_reset.sh" || { echo "FATAL: ${PROFILE_PAR} reset failed"; exit 2; }
    else
        MINIKUBE_PROFILE_PAR="${PROFILE_PAR}" MINIKUBE_PROFILE_SEQ="${PROFILE_SEQ}" \
        KUBECONFIG_PAR="${KUBECONFIG_PAR}" KUBECONFIG_SEQ="${KUBECONFIG_SEQ}" \
            "${CUR_DIR}/run_minikube_dual_reset.sh" || { echo "FATAL: dual minikube reset failed"; exit 2; }
    fi
fi

# Preload the ClickHouse/Keeper/Zookeeper images into the cluster(s) BEFORE the run.
# Without this, ~POOL_SIZE parallel tests each pull large images from the registry
# concurrently -> network contention -> pods stuck ContainerCreating/InProgress for
# minutes (the single-cluster runner preloads these; the dual path must too).
MINIKUBE_PRELOAD_IMAGES=yes MINIKUBE_PROFILES="${ACTIVE_PROFILES}" \
    common_preload_images "${PRELOAD_IMAGES_ALL[@]}" || echo "WARNING: image preload had failures (continuing)"

# Build operator+metrics images ONCE, load into the active profile(s).
MINIKUBE_PROFILES="${ACTIVE_PROFILES}" common_build_and_load_images || { echo "FATAL: image build/load failed"; exit 2; }

# k8s-par's pool size. Defaults to 25 threads. Image preload (above) removes the
# per-test image-pull stalls that previously made high parallelism flake; the
# remaining limit is host CPU (~1 CH-server-test per core), so 25 needs ~12 cores —
# which PAR_ONLY gives by handing k8s-par the whole host. Override POOL_SIZE to tune.
POOL_SIZE_PAR="${POOL_SIZE:-25}"

# Retry failing scenarios in-process. Dual default is 5 (matches single-cluster full runs);
# merge_dual_results.py collapses a retried fail->pass to one passing entry, so retries never
# double-count. Exported so both child run_tests_operator.sh suites inherit it. RETRY_DELAY is
# seconds between attempts (run_tests_operator.sh defaults it to 30). Note: retry masks
# transient flakes but cannot rescue a deterministic resource shortage on an undersized cluster.
export RETRY_COUNT="${RETRY_COUNT:-5}"
export RETRY_DELAY="${RETRY_DELAY:-30}"

# Each suite streams to the console LIVE, line-prefixed by cluster, AND to a per-suite
# file. `> >(sed | tee file)` is process substitution: the sed|tee runs concurrently
# but is NOT $!, so `wait "${PID_PAR}"` still captures run_tests_operator.sh's status.
# IMAGE_PULL_POLICY=IfNotPresent uses the locally built/preloaded images.
KUBECTL_CMD="kubectl --context=${PROFILE_PAR} --kubeconfig=${KUBECONFIG_PAR}" \
E2E_PHASE=parallel POOL_SIZE="${POOL_SIZE_PAR}" MINIKUBE_PROFILE="${PROFILE_PAR}" \
IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-IfNotPresent}" \
TF_LOG="${RAW_PAR}" TRIM_RESULTS=off ONLY="${ONLY:-*}" \
    "${CUR_DIR}/run_tests_operator.sh" > >(sed -u "s/^/[${PROFILE_PAR}] /" | tee "${OUT_PAR}") 2>&1 &
PID_PAR=$!

if [[ -z "${PAR_ONLY}" ]]; then
    KUBECTL_CMD="kubectl --context=${PROFILE_SEQ} --kubeconfig=${KUBECONFIG_SEQ}" \
    E2E_PHASE=serial POOL_SIZE=1 MINIKUBE_PROFILE="${PROFILE_SEQ}" \
    IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-IfNotPresent}" \
    TF_LOG="${RAW_SEQ}" TRIM_RESULTS=off ONLY="${ONLY:-*}" \
        "${CUR_DIR}/run_tests_operator.sh" > >(sed -u "s/^/[${PROFILE_SEQ}] /" | tee "${OUT_SEQ}") 2>&1 &
    PID_SEQ=$!
fi

wait "${PID_PAR}"; RC_PAR=$?
RC_SEQ=0
[[ -z "${PAR_ONLY}" ]] && { wait "${PID_SEQ}"; RC_SEQ=$?; }
# Drain the process-substitution tee/sed pipelines before printing the summary.
wait 2>/dev/null

# Build the (raw, label) pairs for the merge — only the clusters that actually ran.
MERGE_ARGS=("${RAW_PAR}" "${PROFILE_PAR}")
[[ -z "${PAR_ONLY}" ]] && MERGE_ARGS+=("${RAW_SEQ}" "${PROFILE_SEQ}")
for ((i = 0; i < ${#MERGE_ARGS[@]}; i += 2)); do
    f="${MERGE_ARGS[$i]}"
    [[ -s "${f}" ]] || { echo "FATAL: raw log ${f} missing/empty — cannot render results"; exit 4; }
done

# Fuse the run(s) into ONE unified report (single Passing/Failing listing + tally).
# Not `cat *.raw | transform`: each raw log is a complete TestFlows stream whose
# test-id namespace roots at the same path, so concatenation collides ids and drops a
# run's scenarios. The merge helper renders each log separately and fuses result lines.
python3 "${CUR_DIR}/merge_dual_results.py" "${MERGE_ARGS[@]}" || true

# Combined verdict: fail if EITHER cluster failed. Teardown runs via the EXIT trap.
if [[ "${RC_PAR}" -eq 0 && "${RC_SEQ}" -eq 0 ]]; then COMBINED=0; VERDICT="PASS"; else COMBINED=1; VERDICT="FAIL"; fi
echo
echo "==================== COMBINED dual-cluster verdict: ${VERDICT} ===================="
echo "  ${PROFILE_PAR} (parallel)    exit=${RC_PAR}"
[[ -z "${PAR_ONLY}" ]] && echo "  ${PROFILE_SEQ} (no-parallel) exit=${RC_SEQ}"
echo "==================================================================================="

exit "${COMBINED}"
