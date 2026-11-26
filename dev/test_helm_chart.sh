#!/usr/bin/env bash
# Rendering tests for the operator Helm chart.
#
# Every assertion is a `helm template` against the local chart, so no cluster, no CRDs and no
# prometheus-operator are needed - this runs in a couple of seconds locally and in CI. It covers
# the parts a `helm install` with default values never reaches: the ServiceMonitor templates are
# gated off by default, so without this they are shipped unrendered and untested.
#
# Usage: dev/test_helm_chart.sh [--skip-docs]
#
#   --skip-docs  Do not run the README drift check. That check regenerates README.md in place,
#                so callers which must not touch the working tree (the e2e suite) pass this.

set -u

skip_docs="no"
for arg in "$@"; do
    case "${arg}" in
        --skip-docs) skip_docs="yes" ;;
        *) echo "unknown argument: ${arg}" >&2; exit 2 ;;
    esac
done

CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/deploy/helm/clickhouse-operator"
RELEASE="t"
NS="op"

failures=0
checks=0

# render prints the manifests for the given --set/-f arguments, or the helm error on failure.
render() {
    helm template "${RELEASE}" "${CHART}" -n "${NS}" "$@" 2>&1
}

report() {
    local name="$1" ok="$2" detail="${3:-}"
    checks=$((checks + 1))
    if [[ "${ok}" == "yes" ]]; then
        printf '  ok   %s\n' "${name}"
    else
        failures=$((failures + 1))
        printf '  FAIL %s\n' "${name}"
        [[ -n "${detail}" ]] && printf '       %s\n' "${detail}"
    fi
}

# count_kind <kind> -- <helm args...>
count_kind() {
    local kind="$1"; shift; shift
    render "$@" | grep -c "^kind: ${kind}$"
}

expect_count() {
    local name="$1" kind="$2" want="$3"; shift 3
    local got
    got="$(count_kind "${kind}" -- "$@")"
    if [[ "${got}" == "${want}" ]]; then report "${name}" yes; else report "${name}" no "expected ${want} ${kind}, got ${got}"; fi
}

expect_contains() {
    local name="$1" needle="$2"; shift 2
    if render "$@" | grep -qF -- "${needle}"; then report "${name}" yes; else report "${name}" no "missing: ${needle}"; fi
}

expect_absent() {
    local name="$1" needle="$2"; shift 2
    if render "$@" | grep -qF -- "${needle}"; then report "${name}" no "unexpected: ${needle}"; else report "${name}" yes; fi
}

expect_renders() {
    local name="$1"; shift
    local out
    out="$(render "$@")"
    if [[ $? -eq 0 ]] && ! grep -qE '^Error:' <<<"${out}"; then
        report "${name}" yes
    else
        report "${name}" no "$(grep -m1 -E '^Error:' <<<"${out}")"
    fi
}

echo "chart: ${CHART}"
echo

echo "gating"
expect_count "no ServiceMonitor by default" ServiceMonitor 0
expect_count "serviceMonitor.enabled alone does not enable keeper" ServiceMonitor 1 \
    --set serviceMonitor.enabled=true
expect_count "keeper ServiceMonitor is additive when opted in" ServiceMonitor 2 \
    --set serviceMonitor.enabled=true --set serviceMonitor.keeperMetrics.enabled=true
expect_count "keeperMetrics alone stays off - the outer gate holds" ServiceMonitor 0 \
    --set serviceMonitor.keeperMetrics.enabled=true

echo
echo "keeper ServiceMonitor content"
KEEPER_ON=(--set serviceMonitor.enabled=true --set serviceMonitor.keeperMetrics.enabled=true)
expect_contains "selects operator-managed Keeper Services" "clickhouse-keeper.altinity.com/app: chop" "${KEEPER_ON[@]}"
expect_contains "port name is quoted" 'port: "prometheus"' "${KEEPER_ON[@]}"
expect_contains "namespaceSelector is emitted" "any: true" "${KEEPER_ON[@]}"
expect_contains "named after the release" "-keeper-metrics" "${KEEPER_ON[@]}"
expect_contains "selector is overridable" "clickhouse-keeper.altinity.com/Service: chk" \
    "${KEEPER_ON[@]}" --set 'serviceMonitor.keeperMetrics.selector.clickhouse-keeper\.altinity\.com/Service=chk'

echo
echo "regressions"
# A numeric port NAME is a valid string in values.schema.json, but prometheus-operator declares
# endpoints[].port as a string: rendering it bare makes it a YAML integer and the apiserver
# rejects the whole release. It must survive as a quoted string.
expect_contains "numeric port name stays a string" 'port: "9363"' \
    "${KEEPER_ON[@]}" --set-string serviceMonitor.keeperMetrics.port=9363
# An explicitly nulled sub-block must not panic the template.
expect_renders "explicit null keeperMetrics does not panic" \
    --set serviceMonitor.enabled=true --set serviceMonitor.keeperMetrics=null
# The pre-existing endpoints must keep their gating.
expect_contains "op-metrics survives metrics.enabled=false" "port: op-metrics" \
    --set serviceMonitor.enabled=true --set metrics.enabled=false
if render --set serviceMonitor.enabled=true --set metrics.enabled=false | grep -qF "port: ch-metrics"; then
    report "ch-metrics is gated on metrics.enabled" no "ch-metrics rendered while metrics.enabled=false"
else
    report "ch-metrics is gated on metrics.enabled" yes
fi

# The operator reads its configuration once at startup and has no file watcher, so a missed
# restart strands stale confd/configd/templatesd/usersd config. These two assertions are the
# contract: nothing version-derived may sit in the pod template, and a real data change must
# still move the matching checksum. Chart version is not settable via --set, so the first one
# renders a copy of the chart with Chart.yaml edited.
pod_template() {
    render "$@" | awk '/^  template:/{t=1} t{print} t&&/^      containers:/{exit}'
}

tmp_chart="$(mktemp -d)"
cp -R "${CHART}" "${tmp_chart}/chart"
pinned=(--set operator.image.tag=pinned --set metrics.image.tag=pinned)
before="$(pod_template "${pinned[@]}")"
yq e -i '.version = "99.99.99" | .appVersion = "99.99.99"' "${tmp_chart}/chart/Chart.yaml"
after="$(helm template "${RELEASE}" "${tmp_chart}/chart" -n "${NS}" "${pinned[@]}" 2>&1 | awk '/^  template:/{t=1} t{print} t&&/^      containers:/{exit}')"
rm -rf "${tmp_chart}"
if [[ "${before}" == "${after}" ]]; then
    report "pod template is unchanged by a version bump" yes
else
    report "pod template is unchanged by a version bump" no \
        "$(diff <(printf '%s\n' "${before}") <(printf '%s\n' "${after}") | head -6 | tr '\n' ' ')"
fi

# ... but a real change to the mounted data must still roll the pods.
# watchNamespaces is rendered into config.yaml, so it must move checksum/files. Both renders must
# succeed and carry the annotation: a failed render yields an empty value, which differs from any hash.
base_files="$(render | grep -E '^[[:space:]]+checksum/files:')"
changed_out="$(render --set 'watchNamespaces={ns1,ns2}')"
changed_files="$(grep -E '^[[:space:]]+checksum/files:' <<<"${changed_out}")"
if grep -qE '^Error:' <<<"${changed_out}"; then
    report "checksum/files tracks a data change" no "$(grep -E '^Error:' <<<"${changed_out}" | head -1)"
elif [[ -n "${base_files}" && -n "${changed_files}" && "${base_files}" != "${changed_files}" ]]; then
    report "checksum/files tracks a data change" yes
else
    report "checksum/files tracks a data change" no "checksum/files did not move when the mounted data changed"
fi

echo
echo "extra volumes"
# extraVolumes/extraVolumeMounts let a Secret (for example the operator's client certificate,
# clickhouse.access.clientCertFile/clientKeyFile) be mounted into the operator and the
# metrics-exporter, which each read their own copy of the operator configuration.
# Assertions are structural (yq over the rendered Deployment), so a wrong indent fails them.
extra_values="$(mktemp)"
cat > "${extra_values}" <<'VALUES'
extraVolumes:
  - name: client-tls
    secret:
      secretName: clickhouse-operator-client-tls
operator:
  extraVolumeMounts:
    - name: client-tls
      mountPath: /etc/clickhouse-operator-client-tls
      readOnly: true
metrics:
  extraVolumeMounts:
    - name: client-tls
      mountPath: /etc/clickhouse-operator-client-tls
      readOnly: true
VALUES
# deployment_query <yq expression> <helm args...> prints the expression over the operator Deployment.
deployment_query() {
    local expr="$1"; shift
    render "$@" | yq ea '[select(.kind == "Deployment")] | .[0] | '"${expr}" -
}
expect_query() {
    local name="$1" expr="$2" want="$3"; shift 3
    local got
    got="$(deployment_query "${expr}" "$@" 2>&1)"
    if [[ "${got}" == "${want}" ]]; then report "${name}" yes; else report "${name}" no "expected ${want}, got ${got}"; fi
}
expect_query "no extra volume by default" \
    '[.spec.template.spec.volumes[] | select(.name == "client-tls")] | length' 0
expect_query "extraVolumes adds the pod volume" \
    '.spec.template.spec.volumes[] | select(.name == "client-tls") | .secret.secretName' \
    clickhouse-operator-client-tls -f "${extra_values}"
expect_query "operator.extraVolumeMounts mounts it in the operator" \
    '.spec.template.spec.containers[0].volumeMounts[] | select(.name == "client-tls") | .mountPath' \
    /etc/clickhouse-operator-client-tls -f "${extra_values}"
expect_query "metrics.extraVolumeMounts mounts it in the metrics-exporter" \
    '.spec.template.spec.containers[] | select(.name == "metrics-exporter") | .volumeMounts[] | select(.name == "client-tls") | .readOnly' \
    true -f "${extra_values}"
expect_query "the chart's own volumes are kept" \
    '[.spec.template.spec.volumes[] | select(.name == "clickhouse-operator-ipc")] | length' 1 -f "${extra_values}"
expect_query "metrics.enabled=false keeps the volume and the operator mount" \
    '[.spec.template.spec.containers[0].volumeMounts[] | select(.name == "client-tls")] | length' \
    1 -f "${extra_values}" --set metrics.enabled=false
rm -f "${extra_values}"

echo
echo "extra containers"
# extraContainers/extraInitContainers let the pod run more containers, for example a native sidecar
# (an init container with restartPolicy: Always) that writes the client certificate before the
# operator starts.
containers_values="$(mktemp)"
cat > "${containers_values}" <<'VALUES'
extraInitContainers:
  - name: cert-writer
    image: example/cert-writer:1
    restartPolicy: Always
extraContainers:
  - name: extra-sidecar
    image: example/sidecar:1
VALUES
expect_query "no initContainers by default" '.spec.template.spec | has("initContainers")' false
expect_query "only the chart's containers by default" '[.spec.template.spec.containers[].name] | join(",")' \
    "altinity-clickhouse-operator,metrics-exporter"
expect_query "extraContainers are appended after the chart's containers" \
    '[.spec.template.spec.containers[].name] | join(",")' \
    "altinity-clickhouse-operator,metrics-exporter,extra-sidecar" -f "${containers_values}"
expect_query "metrics.enabled=false does not drop extraContainers" \
    '[.spec.template.spec.containers[].name] | join(",")' \
    "altinity-clickhouse-operator,extra-sidecar" -f "${containers_values}" --set metrics.enabled=false
expect_query "extraInitContainers renders a native sidecar" \
    '.spec.template.spec.initContainers[] | select(.name == "cert-writer") | .restartPolicy' \
    Always -f "${containers_values}"
rm -f "${containers_values}"

echo
echo "crd-install hook"
# The hook runs at pre-install/pre-upgrade, so anything wrong here fails the whole release with
# only a Helm timeout to show for it. It shipped broken once (#2065): registry.k8s.io/kubectl
# publishes no `latest` tag, and the image is distroless, so a floating tag or a shell wrapper
# both make every install hang until --timeout and then fail.
expect_absent "hook image is pinned, not :latest" "kubectl:latest"
expect_contains "hook execs kubectl directly, without a shell" "- kubectl"
# `apply -f <dir>` is non-recursive by design here: kubelet keeps `..data` and `..<timestamp>`
# entries beside the projected CRDs, and -R would descend into them and apply stale duplicates.
expect_absent "hook apply is not recursive" "- -R"
expect_count "crdHook.enabled=false emits no Job" Job 0 --set crdHook.enabled=false

# One CRD per ConfigMap. Grouping them pushed a single object past the 262144-byte
# metadata.annotations ceiling, which breaks client-side `kubectl apply` (ArgoCD,
# `helm template | kubectl apply`) while leaving `helm install` working - so it escapes CI.
crd_count=0
oversized=""
for crd in "${CHART}"/crds/*.yaml; do
    crd_count=$((crd_count + 1))
    size="$(wc -c < "${crd}")"
    if [[ "${size}" -gt 250000 ]]; then oversized="${oversized} $(basename "${crd}")=${size}"; fi
done
if [[ -z "${oversized}" ]]; then
    report "every CRD fits one ConfigMap under the 262144 limit" yes
else
    report "every CRD fits one ConfigMap under the 262144 limit" no "too big to pair 1:1:${oversized}"
fi

hook_render="$(render)"
cm_count="$(grep -cE '^  name: .*-crds-[0-9]+$' <<<"${hook_render}")"
src_count="$(grep -cE '^ {14}name: .*-crds-[0-9]+$' <<<"${hook_render}")"
if [[ "${cm_count}" == "${crd_count}" ]]; then
    report "one crds ConfigMap per CRD file" yes
else
    report "one crds ConfigMap per CRD file" no "${crd_count} CRD files, ${cm_count} ConfigMaps"
fi
if [[ "${src_count}" == "${cm_count}" ]]; then
    report "every crds ConfigMap is mounted by the Job" yes
else
    report "every crds ConfigMap is mounted by the Job" no "${cm_count} ConfigMaps, ${src_count} projected sources"
fi

echo
echo "schema"
if helm template "${RELEASE}" "${CHART}" -n "${NS}" --set serviceMonitor.keeperMetrics.enabled=notabool >/dev/null 2>&1; then
    report "values.schema.json rejects a wrong type" no "notabool was accepted for a boolean"
else
    report "values.schema.json rejects a wrong type" yes
fi

echo
echo "docs"
if [[ "${skip_docs}" == "yes" ]]; then
    echo "  skip README drift check - --skip-docs"
elif command -v helm-docs >/dev/null 2>&1; then
    readme="${CHART}/README.md"
    saved="$(mktemp)"
    cp "${readme}" "${saved}"
    helm-docs --chart-search-root="${CHART}" --log-level=warning >/dev/null 2>&1
    if diff -q "${saved}" "${readme}" >/dev/null; then
        report "README.md matches values.yaml (helm-docs)" yes
    else
        report "README.md matches values.yaml (helm-docs)" no "run: helm-docs --chart-search-root=${CHART}"
        cp "${saved}" "${readme}"
    fi
    rm -f "${saved}"
else
    echo "  skip README drift check - helm-docs not installed"
fi

echo
if [[ "${failures}" -eq 0 ]]; then
    echo "PASSED ${checks} checks"
    exit 0
fi
echo "FAILED ${failures} of ${checks} checks"
exit 1
