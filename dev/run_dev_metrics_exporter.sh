#!/bin/bash

# Run metrics-exporter
# Do not forget to update version

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"
LOG_DIR="${CUR_DIR}/log"

echo -n "Building ${METRICS_EXPORTER_BIN}, please wait..."
if "${CUR_DIR}/go_build_metrics_exporter.sh"; then
    echo "successfully built ${METRICS_EXPORTER_BIN}. Starting"

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

    # And clean binary after run. It'll be rebuilt next time
    "${CUR_DIR}/go_build_metrics_exporter_clean.sh"

#    echo "======================"
#    echo "=== Logs available ==="
#    echo "======================"
#    ls "${LOG_DIR}"/*
else
    echo "unable to build ${METRICS_EXPORTER_BIN}"
fi
