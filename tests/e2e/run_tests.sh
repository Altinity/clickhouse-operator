#!/bin/bash
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
pip3 install -r "$CUR_DIR/../image/requirements.txt"

export OPERATOR_NAMESPACE=test
export OPERATOR_INSTALL=yes
python3 "$CUR_DIR/../regression.py" --only=/regression/e2e.test_operator/* -o short --native
#python3 "$CUR_DIR/../regression.py" --only=/regression/e2e.test_operator/* --native
#python3 "$CUR_DIR/../regression.py" --only=/regression/e2e.test_operator/test_008_2* --native
#python3 "$CUR_DIR/../regression.py" --only=/regression/e2e.test_operator/test_008_2* -o short --native
