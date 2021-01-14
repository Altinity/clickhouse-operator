#!/bin/bash
set -e
git checkout -b fix_metrics_exporter_fetch_status
git pull origin fix_metrics_exporter_fetch_status
export OPERATOR_NAMESPACE=clickhouse
export GRAFANA_NAMESPACE=grafana
export PROMETHEUS_NAMESPACE=prometheus

kubectl delete ns ${OPERATOR_NAMESPACE} || true
kubectl create ns ${OPERATOR_NAMESPACE}
kubectl apply --namespace="${OPERATOR_NAMESPACE}" -f <(
  docker run --rm -v ${PWD}:/workdir mikefarah/yq yq w -d10 /workdir/deploy/operator/clickhouse-operator-install.yaml "spec.template.spec.containers(name==metrics-exporter).imagePullPolicy" IfNotPresent | sed -e "s/namespace: kube-system/namespace: ${OPERATOR_NAMESPACE}/"
)

kubectl delete ns ${PROMETHEUS_NAMESPACE} || true
cd ./deploy/prometheus/
bash -x create-prometheus.sh
cd ../../


kubectl delete ns ${GRAFANA_NAMESPACE} || true
cd ./deploy/grafana/grafana-with-grafana-operator/
bash -x install-grafana-operator.sh
bash -x install-grafana-with-operator.sh
cd ../../../

echo "Created ${GRAFANA_NAMESPACE}, ${PROMETHEUS_NAMESPACE}, ${OPERATOR_NAMESPACE} DONE"

kubectl apply -n ${OPERATOR_NAMESPACE} -f ./deploy/zookeeper/quick-start-persistent-volume/zookeeper-1-node-1GB-for-tests-only.yaml
kubectl apply -n ${OPERATOR_NAMESPACE} -f ./tests/configs/test-014-replication.yaml

while [[ "2" != $(kubectl -n clickhouse get pods | grep test-014 | wc -l) ]]; do
    echo "wait for creating test-014 chi statefullset"
    sleep 5
done


metrics=$(kubectl exec -n clickhouse $(kubectl get pods -n clickhouse | grep operator | cut -d " " -f 1) -c metrics-exporter wget -- -O- http://127.0.0.1:8888/metrics)
while [[ "0" == $(echo $metrics | grep chi_clickhouse_metric_fetches | wc -l) ]]; do
    echo "WAIT when chi_clickhouse_metric_fetches present"
    sleep 5
    metrics=$(kubectl exec -n clickhouse $(kubectl get pods -n clickhouse | grep operator | cut -d " " -f 1) -c metrics-exporter wget -- -O- http://127.0.0.1:8888/metrics)
done

while [[ "0" != $(echo $metrics | grep chi_clickhouse_metric_fetch_errors | wc -l) ]]; do
    echo "WAIT when chi_clickhouse_metric_fetch_errors leave"
    sleep 5
    metrics=$(kubectl exec -n clickhouse $(kubectl get pods -n clickhouse | grep operator | cut -d " " -f 1) -c metrics-exporter wget -- -O- http://127.0.0.1:8888/metrics)
    kubectl exec -n clickhouse $(kubectl get pods -n clickhouse | grep operator | cut -d " " -f 1) -c metrics-exporter wget -- -O- http://127.0.0.1:8888/metrics | grep chi_clickhouse_metric_fetch_errors || true
done


if [[ "0" != $(echo $metrics | grep chi_clickhouse_metric_fetch_errors | wc -l) ]]; then
  echo "CASE 1 METRICS-EXPORTER SHOULD NOT CONTAINS chi_clickhouse_metric_fetch_errors !!!"
  exit 1
fi

if [[ "0" == $(echo $metrics | grep chi_clickhouse_metric_fetches | wc -l) ]]; then
  echo "CASE 2 METRICS-EXPORTER SHOULD CONTAINS chi_clickhouse_metric_fetches !!!"
  exit 1
fi


docker build -f ./dockerfile/metrics-exporter/Dockerfile -t altinity/metrics-exporter:new .
kubectl set image --namespace=${OPERATOR_NAMESPACE} deployment.v1.apps/clickhouse-operator metrics-exporter=altinity/metrics-exporter:new
kubectl rollout --namespace=${OPERATOR_NAMESPACE} status deployment.v1.apps/clickhouse-operator

while [[ "1" != $(kubectl get pods -n ${OPERATOR_NAMESPACE} | grep operator | grep Running | wc -l) ]]; do
  echo "wait when old operator version will down"
  sleep 5
done


metrics=$(kubectl exec -n clickhouse $(kubectl get pods -n ${OPERATOR_NAMESPACE} | grep operator | grep Running | cut -d " " -f 1) -c metrics-exporter wget -- -O- http://127.0.0.1:8888/metrics)

# check metrics-exporter
if [[ "0" == $(echo $metrics | grep chi_clickhouse_metric_fetch_errors | wc -l) ]]; then
  echo "CASE 3 METRICS-EXPORTER SHOULD CONTAINS chi_clickhouse_metric_fetch_errors !!!"
  exit 1
fi

# check prometheus, need wait when prometheus renew metrics set
sleep 60


chi_clickhouse_metric_fetch_errors=$(kubectl exec -it -n ${PROMETHEUS_NAMESPACE} $(kubectl -n ${PROMETHEUS_NAMESPACE} get pods | grep prometheus-0 | cut -d " " -f 1) -c prometheus sh -- -c "wget -O- 'http://localhost:9090/api/v1/query?query=chi_clickhouse_metric_fetch_errors&time=$(date +%s)' 2>/dev/null" | jq ".data.result | length")
if [[ "8" != "${chi_clickhouse_metric_fetch_errors}" ]]; then
  echo "CASE 4 PROMETHEUS SHOULD CONTAINS chi_clickhouse_metric_fetch_errors !!!"
  kubectl exec -it -n ${PROMETHEUS_NAMESPACE} $(kubectl -n ${PROMETHEUS_NAMESPACE} get pods | grep prometheus-0 | cut -d " " -f 1) -c prometheus sh -- -c "wget -O- 'http://localhost:9090/api/v1/query?query=chi_clickhouse_metric_fetch_errors&time=$(date +%s)' 2>/dev/null" | jq
  exit 1
fi


chi_clickhouse_metric_fetches=$(kubectl exec -it -n ${PROMETHEUS_NAMESPACE} $(kubectl -n ${PROMETHEUS_NAMESPACE} get pods | grep prometheus-0 | cut -d " " -f 1) -c prometheus sh -- -c "wget -O- 'http://localhost:9090/api/v1/query?query=chi_clickhouse_metric_fetches&time=$(date +%s)' 2>/dev/null" | jq ".data.result | length")
if [[ "0" != "${chi_clickhouse_metric_fetches}" ]]; then
  echo "CASE 5 PROMETHEUS SHOULD NOT CONTAINS chi_clickhouse_metric_fetches !!!"
  kubectl exec -it -n ${PROMETHEUS_NAMESPACE} $(kubectl -n ${PROMETHEUS_NAMESPACE} get pods | grep prometheus-0 | cut -d " " -f 1) -c prometheus sh -- -c "wget -O- 'http://localhost:9090/api/v1/query?query=chi_clickhouse_metric_fetches&time=$(date +%s)' 2>/dev/null"
  exit 1
fi

echo "TEST PASSED OK"
exit 0