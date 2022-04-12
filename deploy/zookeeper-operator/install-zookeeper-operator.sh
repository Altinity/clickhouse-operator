#!/usr/bin/env bash
ZOOKEEPER_OPERATOR_VERSION=${ZOOKEEPER_OPERATOR_VERSION:-v0.2.13}
ZOOKEEPER_OPERATOR_NAMESPACE=${ZOOKEEPER_OPERATOR_NAMESPACE:-zookeeper-operator}
kubectl create ns "${ZOOKEEPER_OPERATOR_NAMESPACE}" || true
kubectl apply -f "https://github.com/pravega/zookeeper-operator/raw/${ZOOKEEPER_OPERATOR_VERSION}/deploy/crds/zookeeper.pravega.io_zookeeperclusters_crd.yaml"
kubectl apply -n "${ZOOKEEPER_OPERATOR_NAMESPACE}" -f <(
  curl -sL "https://github.com/pravega/zookeeper-operator/raw/${ZOOKEEPER_OPERATOR_VERSION}/deploy/all_ns/rbac.yaml" | sed -e "s/namespace: default/namespace: ${ZOOKEEPER_OPERATOR_NAMESPACE}/g" | sed -e "s/v1beta1/v1/g"
)
kubectl apply -n "${ZOOKEEPER_OPERATOR_NAMESPACE}" -f "https://github.com/pravega/zookeeper-operator/raw/${ZOOKEEPER_OPERATOR_VERSION}/deploy/all_ns/operator.yaml"
