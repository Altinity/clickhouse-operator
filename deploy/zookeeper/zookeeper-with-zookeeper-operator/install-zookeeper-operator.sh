#!/usr/bin/env bash
ZOOKEEPER_OPERATOR_VERSION=${ZOOKEEPER_OPERATOR_VERSION:-0.2.15}
ZOOKEEPER_OPERATOR_NAMESPACE=${ZOOKEEPER_OPERATOR_NAMESPACE:-zookeeper-operator}
kubectl create ns "${ZOOKEEPER_OPERATOR_NAMESPACE}" || true
kubectl apply -f "https://raw.githubusercontent.com/pravega/zookeeper-operator/raw/v${ZOOKEEPER_OPERATOR_VERSION}/config/crd/bases/zookeeper.pravega.io_zookeeperclusters.yaml"
kubectl apply -n "${ZOOKEEPER_OPERATOR_NAMESPACE}" -f <(
  curl -sL "https://raw.githubusercontent.com/pravega/zookeeper-operator/raw/v${ZOOKEEPER_OPERATOR_VERSION}/config/rbac/all_ns_rbac.yaml" | sed -e "s/namespace: default/namespace: ${ZOOKEEPER_OPERATOR_NAMESPACE}/g"
)
kubectl apply -n "${ZOOKEEPER_OPERATOR_NAMESPACE}" -f <(
  curl -sL "https://raw.githubusercontent.com/pravega/zookeeper-operator/raw/v${ZOOKEEPER_OPERATOR_VERSION}/config/manager/manager.yaml" | yq eval ".spec.template.spec.containers[0].image=\"pravega/zookeeper-operator:${ZOOKEEPER_OPERATOR_VERSION}\" | del(.spec.template.spec.containers[0].env[] | select(.name == \"WATCH_NAMESPACE\") | .spec.template.spec.containers[0].env[] += {\"name\": \"WATCH_NAMESPACE\",\"value\":\"\"} )" -
)
