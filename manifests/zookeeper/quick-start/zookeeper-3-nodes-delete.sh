#!/bin/bash

ZK_NAMESPACE="${ZK_NAMESPACE:-zoo3ns}"

echo "Delete Zookeeper namespace ${ZK_NAMESPACE}"

kubectl delete namespace "${ZK_NAMESPACE}"
