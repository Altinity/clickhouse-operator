#!/bin/bash

start=$(date)
run=1
echo "start run ${run}"
while ./run_tests_local.sh; do
  echo "run number ${run} completed successfully"
  run=$((run+1))
  echo "-------------------------------------------"
  echo "-------------------------------------------"
  echo "-------------------------------------------"
  echo "start run ${run}"
done
end=$(date)

echo "============================================="
echo "Run number ${run} failed"
echo "start time: ${start}"
echo "end   time: ${end}"
