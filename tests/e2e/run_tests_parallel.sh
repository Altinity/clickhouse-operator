#!/bin/bash
set -e
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
pip3 install -r "$CUR_DIR/../image/requirements.txt"

export NO_WAIT=1
"${CUR_DIR}/../../deploy/prometheus/create-prometheus.sh"
"${CUR_DIR}/../../deploy/minio/create-minio.sh"
ONLY="*"
for test_file in ${CUR_DIR}/test_*.py; do
  name=$(basename "$test_file" .py | sed 's/^test_//')
  run_cmd="python3 ./tests/regression.py --only=/regression/e2e?test_${name}/${ONLY} --trim-results on -o short --native --log ./tests/raw_${name}.log && "
  run_cmd+="tfs --no-colors transform compact ./tests/raw_${name}.log ./tests/compact_${name}.log && "
  run_cmd+="tfs --no-colors transform nice ./tests/raw_${name}.log ./tests/nice_${name}.log.txt && "
  run_cmd+="tfs --no-colors transform short ./tests/raw_${name}.log ./tests/short_${name}.log.txt && "
  run_cmd+="bash -xec 'tfs --no-colors report results -a 'local run' ./tests/raw_${name}.log - --confidential --copyright 'Altinity Inc.' --logo ./tests/altinity.png | ~/venv/qa/bin/tfs --debug --no-colors document convert > ./tests/report_${name}.html'"

  run_tests+=(
    "${run_cmd}"
  )
done
printf "%s\n" "${run_tests[@]}" | xargs -P 10 -I {} bash -xec '{}'
test_result=$?

date
if [[ "$test_result" == "0" ]]; then
  echo "ALL TESTS PASSED"
else
  echo "TESTS FAILED LOOK ./tests/*.log"
fi
