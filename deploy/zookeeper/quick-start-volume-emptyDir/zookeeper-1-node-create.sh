#!/bin/bash

ZK_NAMESPACE="${ZK_NAMESPACE:-zoo1ns}"

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

kubectl create namespace "${ZK_NAMESPACE}"
cat "${CUR_DIR}/zookeeper-1-node.yaml" | kubectl --namespace="${ZK_NAMESPACE}" apply -f -
