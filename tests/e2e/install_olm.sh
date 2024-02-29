#!/bin/bash

echo "Install OLM CRDs"
kubectl apply -f https://raw.githubusercontent.com/operator-framework/operator-lifecycle-manager/master/deploy/upstream/quickstart/crds.yaml
kubectl apply -f https://raw.githubusercontent.com/operator-framework/operator-lifecycle-manager/master/deploy/upstream/quickstart/olm.yaml

NAMESPACE="${NAMESPACE:-"test"}"
echo "Ensure namespace: ${NAMESPACE}"
kubectl create ns "${NAMESPACE}"

echo "Apply subscription into namespace: ${NAMESPACE}"
kubectl apply -f <( cat << EOF
apiVersion: operators.coreos.com/v1
kind: OperatorGroup
metadata:
  name: operatorgroup
  namespace: "${NAMESPACE}"
spec:
  targetNamespaces:
    - "${NAMESPACE}"
---
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: clickhouse
  namespace: "${NAMESPACE}"
spec:
  channel: latest
  name: clickhouse
  source: operatorhubio-catalog
  sourceNamespace: olm
  installPlanApproval: Automatic
EOF
)
