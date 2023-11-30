#!/bin/bash

start=$(date)
echo "run 1"
run=0
until ./run_tests_local.sh; do
  run=$((run+1))
  echo "run ${run} completed"
done
end=$(date)
echo "Has to run ${run} iterations before success"
echo "start time: ${start}"
echo "end   time: ${end}"
