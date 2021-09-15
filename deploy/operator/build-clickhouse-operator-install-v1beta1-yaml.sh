#!/bin/bash
# Build clickhouse-operator-install-v1beta1.yaml, need install https://github.com/mikefarah/yq
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

cp "${CUR_DIR}/clickhouse-operator-install.yaml" "${CUR_DIR}/clickhouse-operator-install-v1beta1.yaml"
cp "${CUR_DIR}/clickhouse-operator-install-template.yaml" "${CUR_DIR}/clickhouse-operator-install-template-v1beta1.yaml"
declare -a ALL_YAML=("${CUR_DIR}/clickhouse-operator-install-v1beta1.yaml" "${CUR_DIR}/clickhouse-operator-install-template-v1beta1.yaml")
for YAML in "${ALL_YAML[@]}"; do
  yq eval -e --inplace '(select(documentIndex == 0 or documentIndex == 1 or documentIndex == 2) | .apiVersion) = "apiextensions.k8s.io/v1beta1"' "${YAML}"
  yq eval -e --inplace '(select(documentIndex == 0 or documentIndex == 1 or documentIndex == 2) | .spec.version) = .spec.versions[0].name' "${YAML}"
  yq eval -e --inplace '(select(documentIndex == 0 or documentIndex == 1 or documentIndex == 2) | .spec.additionalPrinterColumns) = .spec.versions[0].additionalPrinterColumns' "${YAML}"
  sed -i -e 's/jsonPath/JSONPath/g' "${YAML}"
  # yq eval -e --inplace '(select(.spec.additionalPrinterColumns[].JSONPath) = .spec.additionalPrinterColumns.[].jsonPath' "${YAML}"
  yq eval -e --inplace 'del(select(documentIndex == 0 or documentIndex == 1 or documentIndex == 2) | .spec.additionalPrinterColumns[].jsonPath)' "${YAML}"
  yq eval -e --inplace '(select(documentIndex == 0 or documentIndex == 1 or documentIndex == 2) | .spec.validation ) = .spec.versions[0].schema' "${YAML}"
  yq eval -e --inplace 'del(select(documentIndex == 0 or documentIndex == 1 or documentIndex == 2) | .spec.versions)' "${YAML}"
done
