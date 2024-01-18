#!/bin/bash

MINIKUBE_RESET="${MINIKUBE_RESET:-""}"

function start_run {
  local run_to_start=${1}
  echo "start run ${run_to_start}"
  if [[ ! -z ${MINIKUBE_RESET} ]]; then
    SKIP_K9S=yes ./run_minikube_reset.sh
  fi
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
