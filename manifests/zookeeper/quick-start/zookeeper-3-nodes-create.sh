#!/bin/bash

ZK_NAMESPACE="${ZK_NAMESPACE:-zoo3ns}"

kubectl create namespace "${ZK_NAMESPACE}"
kubectl --namespace="${ZK_NAMESPACE}" apply -f zookeeper-3-nodes.yaml

