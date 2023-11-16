#!/bin/bash

ZK_NAMESPACE="${ZK_NAMESPACE:-zoons}"

kubectl delete namespace ${ZK_NAMESPACE}
