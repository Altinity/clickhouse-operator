#!/bin/bash
# Reset TWO isolated minikube profiles concurrently for dual-cluster e2e.
#
# k8s-par hosts the PARALLEL workload, k8s-seq the SEQUENTIAL (NO_PARALLEL) workload.
# Each profile gets its own kubeconfig file so the two concurrent `minikube start`
# calls never race on a shared ~/.kube/config, and a split of host CPU/RAM. The
# single-cluster path (run_minikube_reset.sh with no MINIKUBE_PROFILE) is untouched;
# this wrapper just drives it twice with distinct profiles. Used by
# run_tests_operator_dual.sh.
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

PROFILE_PAR="${MINIKUBE_PROFILE_PAR:-k8s-par}"
PROFILE_SEQ="${MINIKUBE_PROFILE_SEQ:-k8s-seq}"
KUBECONFIG_PAR="${KUBECONFIG_PAR:-${HOME}/.kube/${PROFILE_PAR}.config}"
KUBECONFIG_SEQ="${KUBECONFIG_SEQ:-${HOME}/.kube/${PROFILE_SEQ}.config}"
# ASYMMETRIC resource split (override per host). k8s-par runs the full PARALLEL pool
# (POOL_SIZE scenarios at once, each spinning its own operator + ClickHouse pods), so
# it needs the BULK of the host. k8s-seq runs NO_PARALLEL tests SERIALLY (one at a
# time) and needs only a small slice. An equal split starves k8s-par: its control
# plane and operator-restart paths race under load (apiserver transients,
# pod-not-found), which fails operator-mutating tests (e.g. test_010055 chopconf
# restart) and times out others as collateral. Defaults below suit a ~12-CPU / ~31g
# host; tune for yours, and lower POOL_SIZE if k8s-par is still CPU-bound (it can't
# have the whole host like a single-cluster run does).
CPUS_PAR="${CPUS_PAR:-8}"
MEMORY_PAR="${MEMORY_PAR:-16g}"
CPUS_SEQ="${CPUS_SEQ:-4}"
MEMORY_SEQ="${MEMORY_SEQ:-8g}"

reset_one() {
    local profile="$1" kubeconfig="$2" cpus="$3" memory="$4"
    # MINIKUBE_PROFILE (non-default) makes run_minikube_reset.sh target this profile
    # AND skip the destructive cross-profile prune + k9s; a dedicated KUBECONFIG
    # isolates this cluster's context so the concurrent start does not corrupt the
    # sibling's kubeconfig.
    SKIP_K9S=yes \
    MINIKUBE_PROFILE="${profile}" \
    KUBECONFIG="${kubeconfig}" \
    CPUS="${cpus}" MEMORY="${memory}" \
    "${CUR_DIR}/run_minikube_reset.sh"
}

echo "Resetting dual minikube clusters concurrently: ${PROFILE_PAR} (${CPUS_PAR} CPU / ${MEMORY_PAR}) + ${PROFILE_SEQ} (${CPUS_SEQ} CPU / ${MEMORY_SEQ})"
reset_one "${PROFILE_PAR}" "${KUBECONFIG_PAR}" "${CPUS_PAR}" "${MEMORY_PAR}" > "/tmp/minikube_reset_${PROFILE_PAR}.log" 2>&1 &
PID_PAR=$!
reset_one "${PROFILE_SEQ}" "${KUBECONFIG_SEQ}" "${CPUS_SEQ}" "${MEMORY_SEQ}" > "/tmp/minikube_reset_${PROFILE_SEQ}.log" 2>&1 &
PID_SEQ=$!

wait "${PID_PAR}"; RC_PAR=$?
wait "${PID_SEQ}"; RC_SEQ=$?

echo "=== ${PROFILE_PAR} reset log tail ==="; tail -8 "/tmp/minikube_reset_${PROFILE_PAR}.log"
echo "=== ${PROFILE_SEQ} reset log tail ==="; tail -8 "/tmp/minikube_reset_${PROFILE_SEQ}.log"
echo "dual reset exit codes: ${PROFILE_PAR}=${RC_PAR} ${PROFILE_SEQ}=${RC_SEQ}"

[[ "${RC_PAR}" -eq 0 && "${RC_SEQ}" -eq 0 ]]
