#!/bin/bash

start=`date`
runs=1
echo "start run $runs"
while ./run_tests.sh; do
  runs=$((runs+1))
  echo "---------------------"
  echo "---------------------"
  echo "---------------------"
  echo "---------------------"
  echo "start run $runs"
done
end=`date`

echo "====================="
echo "====================="
echo "====================="
echo "successful runs $((runs-1))"
echo "start $start"
echo "end   $end"
