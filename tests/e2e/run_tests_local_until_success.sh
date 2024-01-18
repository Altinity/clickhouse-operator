#!/bin/bash

start=$(date)
run=1
echo "start run ${run}"
until ./run_tests_local.sh; do
  echo "run number ${run} failed"
  run=$((run+1))
  echo "-------------------------------------------"
  echo "-------------------------------------------"
  echo "-------------------------------------------"
  echo "start run ${run}"
done
end=$(date)

echo "============================================="
echo "Run number ${run} succeeded"
echo "start time: ${start}"
echo "end   time: ${end}"
