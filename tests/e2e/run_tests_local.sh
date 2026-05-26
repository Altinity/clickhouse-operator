#!/bin/bash
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/test_common.sh"

# Test component select options:
# - operator
# - metrics
# - acvp
# - all  (acvp → metrics → operator, ordered fast→slow; bail on first failure)
# Can be set via env var for non-interactive use: WHAT=metrics ./run_tests_local.sh
WHAT="${WHAT}"

# Repeat mode options:
# - success = repeat until success
# - failure = repeat until failure
# - not specified / empty = single run
# Usage: REPEAT_UNTIL=success ./run_tests_local.sh
REPEAT_UNTIL="${REPEAT_UNTIL:-""}"

#
# Interactive menu (or non-interactive if WHAT is already set)
#
function select_test_goal() {
    local specified_goal="${1}"
    if [[ -n "${specified_goal}" ]]; then
        echo "Having specified explicitly: ${specified_goal}" >&2
        echo "${specified_goal}"
        return
    fi

    echo "What would you like to start? Possible options:" >&2
    echo "  1     - test operator" >&2
    echo "  2     - test metrics" >&2
    echo "  3     - test acvp (host-only, no minikube)" >&2
    echo "  4     - test all  (acvp → metrics → operator)" >&2
    echo -n "Enter your choice (1, 2, 3, 4): " >&2
    read COMMAND
    COMMAND=$(echo "${COMMAND}" | tr -d '\n\t\r ')
    case "${COMMAND}" in
        "1") echo "operator" ;;
        "2") echo "metrics" ;;
        "3") echo "acvp" ;;
        "4") echo "all" ;;
        *)
            echo "don't know what '${COMMAND}' is, so picking operator" >&2
            echo "operator"
            ;;
    esac
}

WHAT=$(select_test_goal "${WHAT}")

# Map test goal to one-or-more dedicated local scripts.
# `all` runs in fast→slow order (acvp ~13s, metrics ~3min, operator ~45min)
# so a regression at the bottom fails the whole "all" run early.
case "${WHAT}" in
    "operator")
        LOCAL_SCRIPTS=("run_tests_operator_local.sh")
        echo "Selected: test OPERATOR"
        ;;
    "metrics")
        LOCAL_SCRIPTS=("run_tests_metrics_local.sh")
        echo "Selected: test METRICS"
        ;;
    "acvp")
        LOCAL_SCRIPTS=("run_tests_acvp_local.sh")
        echo "Selected: test ACVP (host-only)"
        ;;
    "all")
        LOCAL_SCRIPTS=(
            "run_tests_acvp_local.sh"
            "run_tests_metrics_local.sh"
            "run_tests_operator_local.sh"
        )
        # `all` defaults to MINIKUBE_RESET=yes so the first minikube-bound suite
        # gets a clean cluster. The run loop below force-clears MINIKUBE_RESET
        # for subsequent suites so we don't re-reset between metrics → operator.
        # User can opt out with `MINIKUBE_RESET= WHAT=all ./run_tests_local.sh`.
        MINIKUBE_RESET="${MINIKUBE_RESET-yes}"
        export MINIKUBE_RESET
        echo "Selected: test ALL (acvp → metrics → operator, bail on first failure)"
        echo "  MINIKUBE_RESET=${MINIKUBE_RESET:-<unset>} (reset once before first minikube suite)"
        ;;
    *)
        echo "Unknown test type: '${WHAT}', exiting"
        exit 1
        ;;
esac

# Only wait for confirmation when running interactively (stdin is a terminal)
if [ -t 0 ]; then
    TIMEOUT=30
    echo "Press <ENTER> to start test immediately (if you agree with specified options)"
    echo "In case no input provided tests would start in ${TIMEOUT} seconds automatically"
    read -t ${TIMEOUT}
fi

# Run one round of all selected scripts in order; returns non-zero on first failure.
# MINIKUBE_RESET semantics for multi-suite runs (`WHAT=all`): the first
# minikube-bound suite honors the caller's MINIKUBE_RESET; subsequent suites
# run with MINIKUBE_RESET cleared so we don't reset the cluster mid-sequence
# (which would also drop preloaded images and rebuild from scratch — minutes
# wasted). ACVP doesn't touch minikube so it's transparent to either setting.
function run_one_round() {
    local script
    local first_minikube_suite=yes
    for script in "${LOCAL_SCRIPTS[@]}"; do
        echo
        echo "============================================="
        echo "=== Running ${script}"
        echo "============================================="
        # ACVP doesn't read MINIKUBE_RESET; pass through unchanged.
        # Other suites: first one honors caller, subsequent ones get empty.
        local run_env=()
        if [[ "${script}" != "run_tests_acvp_local.sh" ]]; then
            if [[ "${first_minikube_suite}" == "no" ]]; then
                run_env+=("MINIKUBE_RESET=")
            fi
            first_minikube_suite=no
        fi
        if ! env "${run_env[@]}" "${CUR_DIR}/${script}"; then
            echo "=== FAILED: ${script}"
            return 1
        fi
    done
    return 0
}

# Dispatch with optional repeat mode
case "${REPEAT_UNTIL}" in
    "success")
        # Repeat until tests pass
        start=$(date)
        run=1
        echo "start run ${run}"
        until run_one_round; do
            echo "run number ${run} failed"
            echo "-------------------------------------------"
            run=$((run+1))
            echo "start run ${run}"
        done
        end=$(date)
        echo "============================================="
        echo "Run number ${run} succeeded"
        echo "start time: ${start}"
        echo "end   time: ${end}"
        ;;
    "failure")
        # Repeat until tests fail
        start=$(date)
        run=1
        echo "start run ${run}"
        while run_one_round; do
            echo "run number ${run} completed successfully"
            echo "-------------------------------------------"
            run=$((run+1))
            echo "start run ${run}"
        done
        end=$(date)
        echo "============================================="
        echo "Run number ${run} failed"
        echo "start time: ${start}"
        echo "end   time: ${end}"
        ;;
    *)
        # Single run
        run_one_round
        ;;
esac
