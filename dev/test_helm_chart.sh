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
