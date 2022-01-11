#!/bin/bash

# Run clickhouse-operator
# Do not forget to update version

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

# Setup paths
LOG_DIR="${CUR_DIR}/log"

echo "Building ${OPERATOR_BIN}, please wait..."
if [[ $1 == "nobuild" ]]; then
    echo "Build step skipped, starting old binary"
else
    if "${CUR_DIR}/go_build_operator.sh"; then
        echo "Successfully built ${OPERATOR_BIN}."
    else
        echo "Unable to build ${OPERATOR_BIN}. Abort."
        exit 1
    fi
fi

if [[ ! -x "${OPERATOR_BIN}" ]]; then
    echo "Unable to start ${OPERATOR_BIN} Is not executable or not found. Abort"
    exit 2
fi

VERBOSITY="${VERBOSITY:-1}"

echo "Starting ${OPERATOR_BIN}..."

mkdir -p "${LOG_DIR}"
rm -f "${LOG_DIR}"/clickhouse-operator.*.log.*
"${OPERATOR_BIN}" \
    -config="${SRC_ROOT}/config/config-dev.yaml" \
    -alsologtostderr=true \
    -log_dir=log \
    -v=${VERBOSITY} 2>&1 | tee operator_output
#	-logtostderr=true \
#	-stderrthreshold=FATAL \

# -log_dir=log Log files will be written to this directory instead of the default temporary directory
# -alsologtostderr=true Logs are written to standard error as well as to files
# -logtostderr=true  Logs are written to standard error instead of to files
# -stderrthreshold=FATAL Log events at or above this severity are logged to standard	error as well as to files

if [[ $2 == "noclean" ]]; then
    echo "Clean step skipped"
else
    # And clean binary after run. It'll be rebuilt next time
    "${CUR_DIR}/go_build_operator_clean.sh"
fi

echo "======================"
echo "=== Logs available ==="
echo "======================"
ls "${LOG_DIR}"/*
