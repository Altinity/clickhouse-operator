#!/bin/bash

OPENEBS_NAMESPACE="${OPENEBS_NAMESPACE:-openebs}"
CLICKHOUSE_NAMESPACE="${CLICKHOUSE_NAMESPACE:-ch-test}"

echo "Delete Test ClickHouse installation"
kubectl delete --namespace="${CLICKHOUSE_NAMESPACE}" ClickhouseInstallation clickhouse-openebs

echo "Delete OpenEBS namespace ${OPENEBS_NAMESPACE}"
LVMVOLUMNE_TO_DELETE=$(kubectl get LVMVolume --namespace "${OPENEBS_NAMESPACE}" | tail -1 | cut -f1 -d ' ')
kubectl delete LVMVolume $LVMVOLUMNE_TO_DELETE --namespace "${OPENEBS_NAMESPACE}"
helm uninstall openebs --namespace ${OPENEBS_NAMESPACE}
kubectl delete namespace "${OPENEBS_NAMESPACE}"
