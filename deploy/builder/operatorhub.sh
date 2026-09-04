#!/bin/bash

# Compose clickhouse-operator .yaml manifest from components

# Paths
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
SRC_ROOT="$(realpath "${CUR_DIR}/../..")"

# 0.9.3
VERSION=$(cd "${SRC_ROOT}"; cat release)
echo "=================================================================================="
echo "VERSION: ${VERSION}"
echo "=================================================================================="

# Callers that need an isolated bundle flavor (for example, the Red Hat
# certified bundle) can direct generated files to a staging directory without
# changing the existing community bundle tree.
OPERATORHUB_DIR="${OPERATORHUB_DIR:-${SRC_ROOT}/deploy/operatorhub}"
MANIFESTS_DIR="${OPERATORHUB_DIR}/${VERSION}"
mkdir -p "${MANIFESTS_DIR}"

#yq eval 'select(documentIndex == 0) | .rules' deploy/builder/templates-install-bundle/clickhouse-operator-install-yaml-template-02-section-rbac-02-role.yaml
#yq eval '.spec.install.spec.permissions[0].rules' deploy/builder/templates-operatorhub/clickhouse-operator.vVERSION.clusterserviceversion.yaml

#yq eval '.spec.install.spec.permissions[0].rules |= load("-" + .file)' deploy/builder/templates-operatorhub/clickhouse-operator.vVERSION.clusterserviceversion.yaml
#yq eval-all 'select(fileIndex==0).a = select(fileIndex==1) | select(fileIndex==0)' sample.yml another.yml


CVV_FILE_TEMPLATE="${MANIFESTS_DIR}/clickhouse-operator.v${VERSION}.clusterserviceversion-template.yaml"
CVV_FILE="${MANIFESTS_DIR}/clickhouse-operator.v${VERSION}.clusterserviceversion.yaml"

cp -f \
  "${SRC_ROOT}/deploy/builder/templates-operatorhub/clickhouse-operator.vVERSION.clusterserviceversion-template.yaml" \
  "${CVV_FILE_TEMPLATE}"

RBAC_ROLE_FILE="${SRC_ROOT}/deploy/builder/templates-install-bundle/clickhouse-operator-install-yaml-template-02-section-rbac-02-role.yaml"

#yq 'select(documentIndex == 0).rules' "${RBAC_FILE}" | yq 'select(fileIndex==0).spec.install.spec.permissions[0].rules = select(fileIndex==1)' "${CVV_FILE}" -
#yq eval --inplace 'select(documentIndex==0)' "${CVV_FILE}"

# Extract rules from the role file and insert rules into CVV file
RBAC_ROLE_RULES_FILE="qwe.yaml"
yq 'select(documentIndex == 0).rules' "${RBAC_ROLE_FILE}" > "${RBAC_ROLE_RULES_FILE}"
yq -i ".spec.install.spec.permissions[0].rules = load(\"${RBAC_ROLE_RULES_FILE}\")" "${CVV_FILE_TEMPLATE}"
rm "${RBAC_ROLE_RULES_FILE}"

# Build partial .yaml manifest(s)
#MANIFEST_PRINT_CRD="no" \
#MANIFEST_PRINT_RBAC_CLUSTERED="no" \
#MANIFEST_PRINT_RBAC_NAMESPACED="no" \
#MANIFEST_PRINT_DEPLOYMENT="yes" \
#MANIFEST_PRINT_SERVICE_METRICS="no" \
#"${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh" | yq eval 'select(.kind == "Deployment").spec' - | yq eval-all --inplace 'select(fileIndex==0).spec.install.spec.deployments[0].spec = select(fileIndex==1)' "${CVV_FILE}" -


#yq eval -o=json "${SRC_ROOT}/docs/chi-examples/01-simple-layout-01-1shard-1repl.yaml"
#yq eval -o=json "${SRC_ROOT}/docs/chi-examples/50-CHIT-10-useTemplates-and-use-templates-with-selector.yaml"
#yq eval -o=json "${SRC_ROOT}/docs/chi-examples/70-chop-config.yaml"

# Insert examples into CVV file
EXAMPLES_FILE="qwe.yaml"
F1="${SRC_ROOT}/docs/chi-examples/01-simple-layout-01-1shard-1repl.yaml"
F2="${SRC_ROOT}/docs/chi-examples/50-CHIT-02-manual-template-with-selector.yaml"
F3="${SRC_ROOT}/docs/chi-examples/70-chop-config.yaml"
F4="${SRC_ROOT}/docs/chk-examples/01-simple-1.yaml"
yq -n -I=2 -o=json ".[0] = load(\"${F1}\") | .[1] = load(\"${F2}\") | .[2] = load(\"${F3}\") | .[3] = load(\"${F4}\")" > "${EXAMPLES_FILE}"
yq -i ".metadata.annotations.alm-examples = strload(\"${EXAMPLES_FILE}\")" "${CVV_FILE_TEMPLATE}"
rm "${EXAMPLES_FILE}"

cat "${CVV_FILE_TEMPLATE}" | \
  OPERATOR_VERSION="${VERSION}" \
  OPERATOR_RELEASE_DATE=$(date +'%Y-%m-%dT%H:%M:%SZ') \
  envsubst > "${CVV_FILE}"

rm "${CVV_FILE_TEMPLATE}"

# Inject spec.skips from releases file (versions that may not be in the IIB from-index)
SKIPS_FILE="skips.yaml"
grep -v '^$' "${SRC_ROOT}/releases" | while IFS= read -r v; do
    echo "- clickhouse-operator.v${v}"
done > "${SKIPS_FILE}"
if [[ -s "${SKIPS_FILE}" ]]; then
    yq -i ".spec.skips = load(\"${SKIPS_FILE}\")" "${CVV_FILE}"
fi
rm "${SKIPS_FILE}"

#$CHIT.crd.yaml
#$CONF.crd.yaml

CHI="clickhouseinstallations.clickhouse.altinity.com"
CHIT="clickhouseinstallationtemplates.clickhouse.altinity.com"
CONF="clickhouseoperatorconfigurations.clickhouse.altinity.com"
CHK="clickhousekeeperinstallations.clickhouse-keeper.altinity.com"

# Build partial .yaml manifest(s)
MANIFEST_PRINT_CRD="yes" \
MANIFEST_PRINT_RBAC_CLUSTERED="no" \
MANIFEST_PRINT_RBAC_NAMESPACED="no" \
MANIFEST_PRINT_DEPLOYMENT="no" \
MANIFEST_PRINT_SERVICE_METRICS="no" \
"${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh" | yq "select(.metadata.name == \"${CHI}\")" > "${MANIFESTS_DIR}/${CHI}.crd.yaml"

# Build partial .yaml manifest(s)
MANIFEST_PRINT_CRD="yes" \
MANIFEST_PRINT_RBAC_CLUSTERED="no" \
MANIFEST_PRINT_RBAC_NAMESPACED="no" \
MANIFEST_PRINT_DEPLOYMENT="no" \
MANIFEST_PRINT_SERVICE_METRICS="no" \
"${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh" | yq "select(.metadata.name == \"${CHIT}\")" > "${MANIFESTS_DIR}/${CHIT}.crd.yaml"

# Build partial .yaml manifest(s)
MANIFEST_PRINT_CRD="yes" \
MANIFEST_PRINT_RBAC_CLUSTERED="no" \
MANIFEST_PRINT_RBAC_NAMESPACED="no" \
MANIFEST_PRINT_DEPLOYMENT="no" \
MANIFEST_PRINT_SERVICE_METRICS="no" \
"${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh" | yq "select(.metadata.name == \"${CONF}\")" > "${MANIFESTS_DIR}/${CONF}.crd.yaml"

# Build partial .yaml manifest(s)
MANIFEST_PRINT_CRD="yes" \
MANIFEST_PRINT_RBAC_CLUSTERED="no" \
MANIFEST_PRINT_RBAC_NAMESPACED="no" \
MANIFEST_PRINT_DEPLOYMENT="no" \
MANIFEST_PRINT_SERVICE_METRICS="no" \
"${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh" | yq "select(.metadata.name == \"${CHK}\")" > "${MANIFESTS_DIR}/${CHK}.crd.yaml"

# TODO
# Package file not used any more?
#cat <<EOF > "${OPERATORHUB_DIR}/clickhouse.package.yaml"
#channels:
#- currentCSV: clickhouse-operator.v${VERSION}
#  name: latest
#defaultChannel: latest
#packageName: clickhouse
#EOF
