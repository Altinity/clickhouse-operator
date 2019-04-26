#!/bin/bash

. ./dev-config.sh

if kubectl get namespace "${DEV_NAMESPACE}"; then
    echo "Delete ${DEV_NAMESPACE} namespace"
    kubectl delete namespace "${DEV_NAMESPACE}"
else
    echo "No namespace ${DEV_NAMESPACE} available"
fi
