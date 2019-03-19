#!/bin/bash

ZK_NAMESPACE="${ZK_NAMESPACE:-zoo1ns}"

echo "Delete Zookeeper namespace ${ZK_NAMESPACE}"

kubectl delete namespace "${ZK_NAMESPACE}"
