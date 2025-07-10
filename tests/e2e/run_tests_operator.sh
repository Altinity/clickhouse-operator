#!/bin/bash
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
pip3 install -r "$CUR_DIR/../image/requirements.txt"

export OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-"test"}"
export OPERATOR_INSTALL="${OPERATOR_INSTALL:-"yes"}"
export IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-"Always"}"
RUN_ALL_TESTS="${RUN_ALL_TESTS:-""}"
ONLY="${ONLY:-"*"}"

# We may want run all tests to the end ignoring failed tests in the process
if [[ ! -z "${RUN_ALL_TESTS}" ]]; then
    RUN_ALL_TESTS="--test-to-end"
fi

python3 "$CUR_DIR/../regression.py" --only="/regression/e2e.test_operator/${ONLY}" ${RUN_ALL_TESTS} -o short --trim-results on --debug --native
#python3 "$CUR_DIR/../regression.py" --only="/regression/e2e.test_operator/${ONLY}" --test-to-end -o short --trim-results on --debug --native
#python3 "$CUR_DIR/../regression.py" --only="/regression/e2e.test_operator/${ONLY}" --parallel-pool ${MAX_PARALLEL} -o short --trim-results on --debug --native
#python3 "$CUR_DIR/../regression.py" --only=/regression/e2e.test_operator/* -o short --trim-results on --debug --native --native
#python3 "$CUR_DIR/../regression.py" --only=/regression/e2e.test_operator/* --trim-results on --debug --native --native
#python3 "$CUR_DIR/../regression.py" --only=/regression/e2e.test_operator/test_008_2* --trim-results on --debug --native --native
#python3 "$CUR_DIR/../regression.py" --only=/regression/e2e.test_operator/test_008_2* --trim-results on --debug --native -o short --native
#python3 "$CUR_DIR/../regression.py" --only=/regression/e2e.test_operator/*32* --trim-results on --debug --native -o short --native
