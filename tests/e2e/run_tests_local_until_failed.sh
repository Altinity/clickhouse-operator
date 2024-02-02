#!/bin/bash

function start_run {
    local run_to_start=${1}
    echo "start run ${run_to_start}"
}

start=$(date)
run=1
start_run ${run}
while ./run_tests_local.sh; do
    echo "run number ${run} completed successfully"
    echo "-------------------------------------------"
    echo "-------------------------------------------"
    echo "-------------------------------------------"

    run=$((run+1))
    start_run ${run}
done
end=$(date)

echo "============================================="
echo "Run number ${run} failed"
echo "start time: ${start}"
echo "end   time: ${end}"
