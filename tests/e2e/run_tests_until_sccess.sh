#!/bin/bash

start=$(date)
run=1
until ./run_tests_local.sh; do
  run=$((run+1))
  echo "start run ${run}"
done
end=$(date)
echo "successful run ${run}"
echo "total time"
echo "start ${start}"
echo "end   ${end}"
