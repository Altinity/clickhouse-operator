import json
import time
import random

from testflows.core import Given, Then, And, fail, When

import kubectl
import settings
import clickhouse
import util


def check_alert_state(alert_name, prometheus_pod, alert_state="firing", labels=None, time_range="10s"):
    with Then(f"check {alert_name} for state {alert_state} and {labels} labels in {time_range}"):
        cmd = f"exec -n {settings.prometheus_namespace} {prometheus_pod} -c prometheus -- "
        cmd += "wget -qO- 'http://127.0.0.1:9090/api/v1/query?query=ALERTS{"
        if labels is None:
            labels = {}
        if not isinstance(labels, dict):
            fail(f"Invalid labels={labels}")
        labels.update({"alertname": alert_name, "alertstate": alert_state})
        cmd += ",".join([f"{name}=\"{value}\"" for name, value in labels.items()])
        cmd += f"}}[{time_range}]' 2>/dev/null"
        out = kubectl.launch(cmd)
        out = json.loads(out)
        if not ("status" in out and out["status"] == "success"):
            fail("wrong response from prometheus query API")
        if len(out["data"]["result"]) == 0:
            with And("not present, empty result"):
                return False
        result_labels = out["data"]["result"][0]["metric"].items()
        exists = all(item in result_labels for item in labels.items())
        with And("got result and contains labels" if exists else "got result, but doesn't contain labels"):
            return exists


def wait_alert_state(alert_name, alert_state, expected_state, prometheus_pod='prometheus-prometheus-0', labels=None, callback=None,
                     max_try=20, sleep_time=10, time_range="10s"):
    catched = False
    for _ in range(max_try):
        if callback is not None:
            callback()
        if expected_state == check_alert_state(alert_name, prometheus_pod, alert_state, labels, time_range):
            catched = True
            break
        with And(f"not ready, wait {sleep_time}s"):
            time.sleep(sleep_time)
    return catched


def random_pod_choice_for_callbacks(chi):
    first_idx = random.randint(0, 1)
    first_pod = chi["status"]["pods"][first_idx]
    first_svc = chi["status"]["fqdns"][first_idx]
    second_idx = 0 if first_idx == 1 else 1
    second_pod = chi["status"]["pods"][second_idx]
    second_svc = chi["status"]["fqdns"][second_idx]
    return second_pod, second_svc, first_pod, first_svc


def get_prometheus_and_alertmanager_spec():
    with Given("get information about prometheus installation"):
        prometheus_operator_spec = kubectl.get(
            "pod", ns=settings.prometheus_namespace, name="",
            label="-l app.kubernetes.io/component=controller,app.kubernetes.io/name=prometheus-operator"
        )

        alertmanager_spec = kubectl.get(
            "pod", ns=settings.prometheus_namespace, name="",
            label="-l app=alertmanager,alertmanager=alertmanager"
        )

        prometheus_spec = kubectl.get(
            "pod", ns=settings.prometheus_namespace, name="",
            label="-l app=prometheus,prometheus=prometheus"
        )
        if not ("items" in prometheus_spec and len(prometheus_spec["items"]) and "metadata" in prometheus_spec["items"][0]):
            fail("invalid prometheus_spec, please run create-prometheus.sh")
        return prometheus_operator_spec, prometheus_spec, alertmanager_spec


def install_clickhouse_and_zookeeper_for_alerts(chi_file, chi_template_file, chi_name):
    with Given("install zookeeper+clickhouse"):
        kubectl.delete_ns(settings.test_namespace, ok_to_fail=True, timeout=600)
        kubectl.create_ns(settings.test_namespace)
        util.require_zookeeper()
        kubectl.create_and_check(
            config=chi_file,
            check={
                "apply_templates": [
                    chi_template_file,
                    "templates/tpl-persistent-volume-100Mi.yaml"
                ],
                "object_counts": {
                    "statefulset": 2,
                    "pod": 2,
                    "service": 3,
                },
                "do_not_delete": 1
            }
        )
        clickhouse_operator_spec = kubectl.get(
            "pod", name="", ns=settings.operator_namespace, label="-l app=clickhouse-operator"
        )
        chi = kubectl.get("chi", ns=kubectl.namespace, name=chi_name)
        return clickhouse_operator_spec, chi


def wait_clickhouse_cluster_ready(chi):
    with Given("All expected pods present in system.clusters"):
        all_pods_ready = False
        while all_pods_ready is False:
            all_pods_ready = True
            for pod in chi['status']['pods']:
                cluster_response = clickhouse.query(
                    chi["metadata"]["name"],
                    "SYSTEM RELOAD CONFIG; SELECT host_name FROM system.clusters WHERE cluster='all-sharded'",
                    pod=pod
                )
                for host in chi['status']['fqdns']:
                    svc_short_name = host.replace(f'.{settings.test_namespace}.svc.cluster.local', '')
                    if svc_short_name not in cluster_response:
                        with Then("Not ready, sleep 5 seconds"):
                            all_pods_ready = False
                            time.sleep(5)


def initialize(chi_file, chi_template_file, chi_name):
    prometheus_operator_spec, prometheus_spec, alertmanager_spec = get_prometheus_and_alertmanager_spec()
    clickhouse_operator_spec, chi = install_clickhouse_and_zookeeper_for_alerts(chi_file, chi_template_file, chi_name)
    wait_clickhouse_cluster_ready(chi)

    return prometheus_operator_spec, prometheus_spec, alertmanager_spec, clickhouse_operator_spec, chi
