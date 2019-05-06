#!/bin/bash

ZK_NAMESPACE="${ZK_NAMESPACE:-zoo1ns}"

kubectl create namespace "${ZK_NAMESPACE}"
kubectl --namespace="${ZK_NAMESPACE}" apply -f zookeeper-1-node.yaml
