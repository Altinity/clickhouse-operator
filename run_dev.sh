#!/bin/bash

echo -n "Building binary, please wait..."
if ./build_binary.sh; then
    echo "successfully built clickhouse-operator. Starting"

    mkdir -p log
    rm -f log/clickhouse-operator.*.log.*
    ./clickhouse-operator \
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
    ./clean_binary.sh

    echo "======================"
    echo "=== Logs available ==="
    echo "======================"
    ls log/*
else
    echo "unable to build clickhouse-operator"
fi
