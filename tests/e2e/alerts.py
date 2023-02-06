import json
import time
import random

from testflows.core import Given, Then, And, fail, When

import e2e.kubectl as kubectl
import e2e.settings as settings
import e2e.clickhouse as clickhouse
import e2e.util as util


def check_alert_state(alert_name, prometheus_pod, alert_state="firing", labels=None, time_range="10s"):
    with Then(f"check {alert_name} for state {alert_state} and {labels} labels in {time_range}"):
        cmd = f"exec -n {settings.prometheus_namespace} {prometheus_pod} -c prometheus -- "
        cmd += "wget -qO- 'http://127.0.0.1:9090/api/v1/query?query=ALERTS{"
        if labels is None:
            labels = {}
        if not isinstance(labels, dict):
            fail(f"Invalid labels={labels}")
        labels.update({"alertname": alert_name, "alertstate": alert_state})
        cmd += ",".join([f'{name}="{value}"' for name, value in labels.items()])
        cmd += f"}}[{time_range}]' 2>/dev/null"
        out = kubectl.launch(cmd)
        out = json.loads(out)
        if not ("status" in out and out["status"] == "success"):
            fail("wrong response from prometheus query API")
        if len(out["data"]["result"]) == 0:
            with Then("not present, empty result"):
                return False
        result_labels = out["data"]["result"][0]["metric"].items()
        exists = all(item in result_labels for item in labels.items())
        with Then("got result and contains labels" if exists else "got result, but doesn't contain labels"):
            return exists


def wait_alert_state(
    alert_name,
    alert_state,
    expected_state,
    prometheus_pod="prometheus-prometheus-0",
    labels=None,
    callback=None,
    max_try=20,
    sleep_time=settings.prometheus_scrape_interval,
    time_range=f"{settings.prometheus_scrape_interval * 2}s",
):
    catched = False
    for _ in range(max_try):
        if callback is not None:
            callback()
        if expected_state == check_alert_state(alert_name, prometheus_pod, alert_state, labels, time_range):
            catched = True
            break
        with Then(f"not ready, wait {sleep_time}s"):
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
            "pod",
            ns=settings.prometheus_namespace,
            name="",
            label="-l app.kubernetes.io/component=controller,app.kubernetes.io/name=prometheus-operator",
        )

        alertmanager_spec = kubectl.get(
            "pod",
            ns=settings.prometheus_namespace,
            name="",
            label="-l app.kubernetes.io/managed-by=prometheus-operator,alertmanager=alertmanager",
        )

        prometheus_spec = kubectl.get(
            "pod",
            ns=settings.prometheus_namespace,
            name="",
            label="-l app.kubernetes.io/managed-by=prometheus-operator,prometheus=prometheus",
        )
        if not (
            "items" in prometheus_spec and len(prometheus_spec["items"]) and "metadata" in prometheus_spec["items"][0]
        ):
            fail("invalid prometheus_spec, please run create-prometheus.sh")
        return prometheus_operator_spec, prometheus_spec, alertmanager_spec


def initialize(chi_file, chi_template_file, chi_name, keeper_type):
    (
        prometheus_operator_spec,
        prometheus_spec,
        alertmanager_spec,
    ) = get_prometheus_and_alertmanager_spec()
    clickhouse_operator_spec, chi = util.install_clickhouse_and_keeper(
        chi_file, chi_template_file, chi_name, keeper_type
    )
    util.wait_clickhouse_cluster_ready(chi)

    return (
        prometheus_operator_spec,
        prometheus_spec,
        alertmanager_spec,
        clickhouse_operator_spec,
        chi,
    )
