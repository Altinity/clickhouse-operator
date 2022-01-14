#!/usr/bin/env bash

set -xeo pipefail
OPERATOR_NAMESPACE=${OPERATOR_NAMESPACE:-kube-system}
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

cp -fv "${CUR_DIR}/../../deploy/operator/clickhouse-operator-install-bundle.yaml" "${CUR_DIR}/clickhouse-operator-install.yaml"

yq eval -e --inplace "(select(.kind == \"Deployment\" and .metadata.name == \"clickhouse-operator\") | .spec.template.spec.containers[] | select(.name==\"clickhouse-operator\") | .image) = \"altinity/clickhouse-operator:${CLICKHOUSE_OPERATOR_TAG}\"" "${CUR_DIR}/clickhouse-operator-install.yaml"
yq eval -e --inplace '(select(.kind == "Deployment" and .metadata.name == "clickhouse-operator") | .spec.template.spec.containers[] | select(.name=="clickhouse-operator") | .imagePullPolicy) = "IfNotPresent"' "${CUR_DIR}/clickhouse-operator-install.yaml"
yq eval -e --inplace '(select(.kind == "Deployment" and .metadata.name == "clickhouse-operator") | .spec.template.spec.containers[] | select(.name=="clickhouse-operator") | .securityContext.capabilities.add) = ["SYS_PTRACE"]' "${CUR_DIR}/clickhouse-operator-install.yaml"

yq eval -e --inplace "(select(.kind == \"Deployment\" and .metadata.name == \"clickhouse-operator\") | .spec.template.spec.containers[] | select(.name==\"metrics-exporter\") | .image) = \"altinity/metrics-exporter:${METRICS_EXPORTER_TAG}\"" "${CUR_DIR}/clickhouse-operator-install.yaml"
yq eval -e --inplace '(select(.kind == "Deployment" and .metadata.name == "clickhouse-operator") | .spec.template.spec.containers[] | select(.name=="metrics-exporter") | .imagePullPolicy) = "IfNotPresent"' "${CUR_DIR}/clickhouse-operator-install.yaml"
yq eval -e --inplace '(select(.kind == "Deployment" and .metadata.name == "clickhouse-operator") | .spec.template.spec.containers[] | select(.name=="metrics-exporter") | .securityContext.capabilities.add) = ["SYS_PTRACE"]' "${CUR_DIR}/clickhouse-operator-install.yaml"

sed -i "s/namespace: kube-system/namespace: ${OPERATOR_NAMESPACE}/" "${CUR_DIR}/clickhouse-operator-install.yaml"
