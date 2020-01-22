#!/bin/bash

echo "External value for \$PROMETHEUS_NAMESPACE=$PROMETHEUS_NAMESPACE"
echo "External value for \$OPERATOR_NAMESPACE=$OPERATOR_NAMESPACE"

PROMETHEUS_NAMESPACE="${PROMETHEUS_NAMESPACE:-prometheus}"
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-kube-system}"

echo "Setup Prometheus into '${PROMETHEUS_NAMESPACE}' namespace"
echo "Expecting operator in '${OPERATOR_NAMESPACE}' namespace"
sleep 10

# Let's setup all prometheus-related stuff into dedicated namespace called "prometheus"
kubectl create namespace "${PROMETHEUS_NAMESPACE}"

# Create prometheus-operator's CRDs
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply -f \
    https://raw.githubusercontent.com/coreos/prometheus-operator/master/example/prometheus-operator-crd/monitoring.coreos.com_alertmanagers.yaml
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply -f \
    https://raw.githubusercontent.com/coreos/prometheus-operator/master/example/prometheus-operator-crd/monitoring.coreos.com_podmonitors.yaml
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply -f \
    https://raw.githubusercontent.com/coreos/prometheus-operator/master/example/prometheus-operator-crd/monitoring.coreos.com_prometheuses.yaml
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply -f \
    https://raw.githubusercontent.com/coreos/prometheus-operator/master/example/prometheus-operator-crd/monitoring.coreos.com_prometheusrules.yaml
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply -f \
    https://raw.githubusercontent.com/coreos/prometheus-operator/master/example/prometheus-operator-crd/monitoring.coreos.com_servicemonitors.yaml

# Setup prometheus-operator into specified namespace. Would manage prometheus instances
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply -f  <( \
    wget -qO- https://raw.githubusercontent.com/coreos/prometheus-operator/master/bundle.yaml | \
    PROMETHEUS_NAMESPACE=${PROMETHEUS_NAMESPACE} sed "s/namespace: default/namespace: ${PROMETHEUS_NAMESPACE}/" \
)

# Setup RBAC
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply -f <( \
    wget -qO- https://raw.githubusercontent.com/coreos/prometheus-operator/master/example/rbac/prometheus/prometheus-cluster-role-binding.yaml | \
    PROMETHEUS_NAMESPACE=${PROMETHEUS_NAMESPACE} sed "s/namespace: default/namespace: ${PROMETHEUS_NAMESPACE}/" \
)
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply -f \
    https://raw.githubusercontent.com/coreos/prometheus-operator/master/example/rbac/prometheus/prometheus-cluster-role.yaml
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply -f \
    https://raw.githubusercontent.com/coreos/prometheus-operator/master/example/rbac/prometheus/prometheus-service-account.yaml

# Setup Prometheus instance via prometheus-operator into dedicated namespace
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply -f prometheus.yaml

# Setup "Prometheus - clickhouse-operator" integration.
# Specify endpoint, where Prometheus can gather data from clickhouse-operator
# IMPORTANT: clickhouse-operator should be installed and running prior to this step,
# otherwise Prometheus would not be able to setup integration with clickhouse-operator

if kubectl --namespace="${OPERATOR_NAMESPACE}" get service clickhouse-operator-metrics; then
    echo "clickhouse-operator-metrics endpoint found. Configuring integration with clickhouse-operator"
    # clickhouse-operator-metrics service found, can setup integration
    kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply -f <( \
        cat ./prometheus-clickhouse-operator-service-monitor.yaml | \
        OPERATOR_NAMESPACE=${OPERATOR_NAMESPACE} sed "s/- kube-system/- ${OPERATOR_NAMESPACE}/" \
    )
else
    echo "ERROR install prometheus. Unable to find service '${OPERATOR_NAMESPACE}/clickhouse-operator-metrics'."
    echo "Please setup clickhouse-operator into ${OPERATOR_NAMESPACE} namespace and restart this script."
fi
