#!/bin/bash
set -e
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
pip3 install -r "$CUR_DIR/../image/requirements.txt"
rm -rfv /tmp/test*.log
pad="000"
MAX_PARALLEL=${MAX_PARALLEL:-5}

export IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-"Always"}"
export OPERATOR_INSTALL="${OPERATOR_INSTALL:-"yes"}"

function run_test_parallel() {
  test_names=("$@")
  run_test_cmd=""
  delete_ns_cmd=""
  create_ns_cmd=""
  is_crd_present=$(kubectl get crd -o name | grep clickhouse.altinity.com | wc -l)
  for test_name in "${test_names[@]}"; do
    ns=$(echo ${test_name} | tr '_' '-')
    if [[ "0" != "${is_crd_present}" && "0" != $(kubectl get chi -n $ns -o name | wc -l ) ]]; then
      delete_ns_cmd+="( kubectl delete -n $ns \$(kubectl get chi -n $ns -o name) ) && "
    fi
    delete_ns_cmd+=" (kubectl delete ns $ns --ignore-not-found --now --timeout=600s);"
    create_ns_cmd+="kubectl create ns $ns;"
    # TODO remove randomization, currently need to avoid same 'No such file or directory: '/tmp/testflows.x.x.x.x.log'
    run_test_cmd+="(sleep $(echo "scale=2; $((1 + $RANDOM % 100)) / 100" | bc -l) && OPERATOR_NAMESPACE=${ns} TEST_NAMESPACE=${ns} python3 $CUR_DIR/../regression.py --only=/regression/e2e.test_operator/${test_name}* --trim-results on --debug --no-color --native &>/tmp/${test_name}.log && echo ${test_name} PASS && kubectl delete ns $ns --timeout=600s) || (echo \"TEST ${test_name} FAILED EXIT_CODE=\$?\" && cat /tmp/${test_name}.log && exit 255);"
  done
  echo "${delete_ns_cmd}" | xargs -P 0 -r --verbose -d ";" -n 1 bash -ce
  if [[ "0" != "${is_crd_present}" ]]; then
    kubectl delete crd clickhouseinstallations.clickhouse.altinity.com clickhouseinstallationtemplates.clickhouse.altinity.com clickhouseoperatorconfigurations.clickhouse.altinity.com
  fi
  kubectl apply -f "${CUR_DIR}/../../deploy/operator/parts/crd.yaml"
  echo "${create_ns_cmd}" | xargs -P 0 -r --verbose -d ";" -n 1 bash -ce
  set +e
  echo "${run_test_cmd}" | xargs -P ${MAX_PARALLEL} -r --verbose -d ";" -n 1 bash -ce
  if [[ "0" != "$?" ]]; then
    echo "TEST FAILED LOOK TO LOGS ABOVE"
    if [[ "0" != $(pgrep -c -f "python.+regression") ]]; then
      kill $(pgrep -f "python.+regression")
    fi
    exit 1
  fi
  set -e
}

test_list=()
test_ids=(34 35 1 2 3 4 5 6 7 10 11 12 13 15 16 17 18 22 23 24 25 26 27 28 29 31 32 33)
for i in "${test_ids[@]}"; do
  test_list+=( "test_${pad:${#i}}${i}" )
done
run_test_parallel "${test_list[@]}"

# allow parallel long test_XXX_X
test_list=("test_019" "test_014" "test_008" "test_020" "test_021")
run_test_parallel "${test_list[@]}"

# operator upgrade require sequenced execution (upgrade operator, delete crd)
test_list=("test_009" "test_030")
MAX_PARALLEL=1
run_test_parallel "${test_list[@]}"



