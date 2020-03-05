#!/bin/bash

echo "External value for \$PROMETHEUS_NAMESPACE=$PROMETHEUS_NAMESPACE"
echo "External value for \$OPERATOR_NAMESPACE=$OPERATOR_NAMESPACE"
echo "External value for \$VALIDATE_YAML=$VALIDATE_YAML"

PROMETHEUS_NAMESPACE="${PROMETHEUS_NAMESPACE:-prometheus}"
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-kube-system}"
# Possible values for "validate yaml" are values from --validate=XXX kubectl option. They are true/false ATM
VALIDATE_YAML="${VALIDATE_YAML:-true}"

echo "OPTIONS"
echo "Setup Prometheus into \$PROMETHEUS_NAMESPACE=${PROMETHEUS_NAMESPACE} namespace"
echo "Expecting operator in \$OPERATOR_NAMESPACE=${OPERATOR_NAMESPACE} namespace"
echo "Validate .yaml file   \$VALIDATE_YAML=${VALIDATE_YAML}"
echo ""
echo "!!! IMPORTANT !!!"
echo "If you do not agree with specified options, press ctrl-c now"
sleep 30
echo "Apply options now..."

# Let's setup all prometheus-related stuff into dedicated namespace called "prometheus"
kubectl create namespace "${PROMETHEUS_NAMESPACE}"

# Create prometheus-operator's CRDs
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f \
    https://raw.githubusercontent.com/coreos/prometheus-operator/master/example/prometheus-operator-crd/monitoring.coreos.com_alertmanagers.yaml
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f \
    https://raw.githubusercontent.com/coreos/prometheus-operator/master/example/prometheus-operator-crd/monitoring.coreos.com_podmonitors.yaml
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f \
    https://raw.githubusercontent.com/coreos/prometheus-operator/master/example/prometheus-operator-crd/monitoring.coreos.com_prometheuses.yaml
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f \
    https://raw.githubusercontent.com/coreos/prometheus-operator/master/example/prometheus-operator-crd/monitoring.coreos.com_prometheusrules.yaml
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f \
    https://raw.githubusercontent.com/coreos/prometheus-operator/master/example/prometheus-operator-crd/monitoring.coreos.com_servicemonitors.yaml

# Setup prometheus-operator into specified namespace. Would manage prometheus instances
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f  <( \
    wget -qO- https://raw.githubusercontent.com/coreos/prometheus-operator/master/bundle.yaml | \
    PROMETHEUS_NAMESPACE=${PROMETHEUS_NAMESPACE} sed "s/namespace: default/namespace: ${PROMETHEUS_NAMESPACE}/" \
)

# Setup RBAC
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f <( \
    wget -qO- https://raw.githubusercontent.com/coreos/prometheus-operator/master/example/rbac/prometheus/prometheus-cluster-role-binding.yaml | \
    PROMETHEUS_NAMESPACE=${PROMETHEUS_NAMESPACE} sed "s/namespace: default/namespace: ${PROMETHEUS_NAMESPACE}/" \
)
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f \
    https://raw.githubusercontent.com/coreos/prometheus-operator/master/example/rbac/prometheus/prometheus-cluster-role.yaml
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f \
    https://raw.githubusercontent.com/coreos/prometheus-operator/master/example/rbac/prometheus/prometheus-service-account.yaml

# Setup Prometheus instance via prometheus-operator into dedicated namespace
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f prometheus.yaml

# Setup "Prometheus <-> clickhouse-operator" integration.
# Specify endpoint, where Prometheus can gather data from clickhouse-operator
# IMPORTANT: clickhouse-operator should be installed and running prior to this step,
# otherwise Prometheus would not be able to setup integration with clickhouse-operator

if kubectl --namespace="${OPERATOR_NAMESPACE}" get service clickhouse-operator-metrics; then
    echo "clickhouse-operator-metrics endpoint found. Configuring integration with clickhouse-operator"
    # clickhouse-operator-metrics service found, can setup integration
    kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f <( \
        cat ./prometheus-clickhouse-operator-service-monitor.yaml | \
        OPERATOR_NAMESPACE=${OPERATOR_NAMESPACE} sed "s/- kube-system/- ${OPERATOR_NAMESPACE}/" \
    )
    echo ""
    echo "DONE"
else
    echo ""
    echo "ERROR install prometheus. Unable to find service '${OPERATOR_NAMESPACE}/clickhouse-operator-metrics'."
    echo "Please setup clickhouse-operator into ${OPERATOR_NAMESPACE} namespace and restart this script."
fi
