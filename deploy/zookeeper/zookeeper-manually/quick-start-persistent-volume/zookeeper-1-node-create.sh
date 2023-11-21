#!/bin/bash

ZK_NAMESPACE="${ZK_NAMESPACE:-zoo1ns}"

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

kubectl create namespace "${ZK_NAMESPACE}"
kubectl --namespace="${ZK_NAMESPACE}" apply -f "${CUR_DIR}/zookeeper-1-node.yaml"
