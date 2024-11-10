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
  for test_name in "${test_names[@]}"; do
    ns=$(echo ${test_name} | tr '_' '-')
    delete_ns_cmd+="(kubectl delete ns $ns --ignore-not-found --now --timeout=600s);"
    create_ns_cmd+="kubectl create ns $ns;"
    # TODO randomization, currently need to avoid same 'No such file or directory: '/tmp/testflows.x.x.x.x.log'
    # sleep $(echo "scale=2; $((1 + $RANDOM % 100)) / 100" | bc -l) &&
    run_test_cmd+="( OPERATOR_NAMESPACE=${ns} TEST_NAMESPACE=${ns} python3 $CUR_DIR/../regression.py --only=/regression/e2e?test_operator/${test_name}* --no-color --native &>/tmp/${test_name}.log && date && echo ${test_name} PASS && kubectl delete ns $ns --timeout=600s) || (echo \"TEST ${test_name} FAILED EXIT_CODE=\$?\" && cat /tmp/${test_name}.log && exit 255);"
  done
  echo "${delete_ns_cmd}" | xargs -P 0 -r --verbose -d ";" -n 1 bash -ce
  echo "${create_ns_cmd}" | xargs -P 0 -r --verbose -d ";" -n 1 bash -ce
  set +e
  echo "${run_test_cmd}" | xargs -P ${MAX_PARALLEL} -r --verbose -d ";" -n 1 bash -ce
  if [[ "0" != "$?" ]]; then
    echo "TEST FAILED LOOK TO LOGS ABOVE"
    pkill -e -f "python.+regression"
    exit 1
  fi
  set -e
}

is_crd_present=$(kubectl get crd -o name | grep clickhouse.altinity.com | wc -l)
delete_chi_cmd=""
if [[ "0" != "${is_crd_present}" && "0" != $(kubectl get chi --all-namespaces -o name | wc -l ) ]]; then
  while read chi; do
    delete_chi_cmd+="kubectl delete chi -n ${chi};"
  done < <(kubectl get chi --all-namespaces -o custom-columns=name:.metadata.namespace,name:.metadata.name | tail -n +2)
  echo "${delete_chi_cmd}" | xargs -P 0 -r --verbose -d ";" -n 1 bash -ce
fi
if [[ "0" != "${is_crd_present}" ]]; then
  kubectl delete crd clickhouseinstallations.clickhouse.altinity.com clickhouseinstallationtemplates.clickhouse.altinity.com clickhouseoperatorconfigurations.clickhouse.altinity.com
fi
kubectl apply -f "${CUR_DIR}/../../deploy/operator/parts/crd.yaml"

test_list=()
test_ids=(34 6 35 11 32 1 2 3 4 5 7 10 12 13 15 17 18 22 24 25 26 27 29 33 16 23)
for i in "${test_ids[@]}"; do
  test_list+=( "test_${pad:${#i}}${i}" )
done
MAX_PARALLEL=5
run_test_parallel "${test_list[@]}"

# allow parallel long test_XXX_X
test_list=("test_019" "test_014" "test_008" "test_020" "test_021" "test_028")
MAX_PARALLEL=5
run_test_parallel "${test_list[@]}"

# following test require sequenced execution (test_009 upgrade operator, test_030 delete crd)
test_list=("test_009" "test_030" "test_031")
MAX_PARALLEL=1
run_test_parallel "${test_list[@]}"

date
echo "ALL TESTS PASSED"

