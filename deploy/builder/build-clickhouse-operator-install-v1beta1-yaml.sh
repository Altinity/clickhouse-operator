#!/bin/bash
# Build clickhouse-operator-install-bundle-v1beta1.yaml, need install https://github.com/mikefarah/yq
# sudo wget "https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64" -O /usr/bin/yq && sudo chmod +x /usr/bin/yq
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
PROJECT_ROOT="$(realpath "${CUR_DIR}/../..")"
MANIFEST_ROOT="$(realpath ${PROJECT_ROOT}/deploy)"

cp -fv "${MANIFEST_ROOT}/operator/clickhouse-operator-install-bundle.yaml"   "${MANIFEST_ROOT}/operator/clickhouse-operator-install-bundle-v1beta1.yaml"
cp -fv "${MANIFEST_ROOT}/operator/clickhouse-operator-install-template.yaml" "${MANIFEST_ROOT}/operator/clickhouse-operator-install-template-v1beta1.yaml"
declare -a ALL_YAML=("${MANIFEST_ROOT}/operator/clickhouse-operator-install-bundle-v1beta1.yaml" "${MANIFEST_ROOT}/operator/clickhouse-operator-install-template-v1beta1.yaml")
for YAML in "${ALL_YAML[@]}"; do
    yq eval -e --inplace '(select(documentIndex == 0 or documentIndex == 1 or documentIndex == 2) | .apiVersion) = "apiextensions.k8s.io/v1beta1"' "${YAML}"
    yq eval -e --inplace '(select(documentIndex == 0 or documentIndex == 1 or documentIndex == 2) | .spec.version) = .spec.versions[0].name' "${YAML}"
    yq eval -e --inplace '(select(documentIndex == 0 or documentIndex == 1 or documentIndex == 2) | .spec.additionalPrinterColumns) = .spec.versions[0].additionalPrinterColumns' "${YAML}"
    yq eval -e --inplace '(select(documentIndex == 0) | .spec.subresources) = .spec.versions[0].subresources' "${YAML}"
    # sed -i -e 's/jsonPath/JSONPath/g' "${YAML}"
    yq eval -e --inplace 'with(select(documentIndex == 0 or documentIndex == 1 or documentIndex == 2) | .spec.additionalPrinterColumns.[]; .JSONPath = .jsonPath )' "${YAML}"
    yq eval -e --inplace 'del(select(documentIndex == 0 or documentIndex == 1 or documentIndex == 2) | .spec.additionalPrinterColumns[].jsonPath)' "${YAML}"
    yq eval -e --inplace '(select(documentIndex == 0 or documentIndex == 1 or documentIndex == 2) | .spec.validation ) = .spec.versions[0].schema' "${YAML}"
    yq eval -e --inplace 'del(select(documentIndex == 0 or documentIndex == 1 or documentIndex == 2) | .spec.versions)' "${YAML}"
done
