#!/bin/bash

# Run metrics-exporter
# Do not forget to update version

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

# Setup paths
LOG_DIR="${CUR_DIR}/log"

echo "Building ${METRICS_EXPORTER_BIN}, please wait..."
if [[ $1 == "nobuild" ]]; then
    echo "Build step skipped, starting old binary"
else
    if "${CUR_DIR}/go_build_metrics_exporter.sh"; then
        echo "Successfully built ${METRICS_EXPORTER_BIN}."
    else
        echo "Unable to build ${METRICS_EXPORTER_BIN}. Abort."
        exit 1
    fi
fi

if [[ ! -x "${METRICS_EXPORTER_BIN}" ]]; then
    echo "Unable to start ${METRICS_EXPORTER_BIN} Is not executable or not found. Abort"
    exit 2
fi

echo "Starting ${METRICS_EXPORTER_BIN}..."

mkdir -p "${LOG_DIR}"
rm -f "${LOG_DIR}"/clickhouse-operator.*.log.*
"${METRICS_EXPORTER_BIN}" \
    -alsologtostderr=true \
    -log_dir=log \
    -v=1
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
    "${CUR_DIR}/go_build_metrics_exporter_clean.sh"
fi
#    echo "======================"
#    echo "=== Logs available ==="
#    echo "======================"
#    ls "${LOG_DIR}"/*
