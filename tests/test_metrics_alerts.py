import re
import time
import json
import random

from testflows.core import TestScenario, Name, When, Then, Given, And, main, run, Module
from testflows.asserts import error
import kubectl
import settings

from test_operator import set_operator_version, require_zookeeper, create_and_check
from test_metrics_exporter import set_metrics_exporter_version

prometheus_operator_spec = None
alertmanager_spec = None
prometheus_spec = None

clickhouse_operator_spec = None
clickhouse_spec = None


def check_alert_state(alert_name, alert_state="firing", labels=None, time_range="10s"):
    with Then(f"check {alert_name} for state {alert_state} and {labels} labels in {time_range}"):
        prometheus_pod = prometheus_spec["items"][0]["metadata"]["name"]
        cmd = f"exec -n {settings.prometheus_namespace} {prometheus_pod} -c prometheus -- "
        cmd += "wget -qO- 'http://127.0.0.1:9090/api/v1/query?query=ALERTS{"
        if labels is None:
            labels = {}
        assert isinstance(labels, dict), error()

        labels.update({"alertname": alert_name, "alertstate": alert_state})
        cmd += ",".join([f"{name}=\"{value}\"" for name, value in labels.items()])
        cmd += f"}}[{time_range}]' 2>/dev/null"
        out = kubectl.kubectl(cmd)
        out = json.loads(out)
        assert "status" in out and out["status"] == "success", error("wrong response from prometheus query API")
        if len(out["data"]["result"]) == 0:
            with And("not present, empty result"):
                return False
        result_labels = out["data"]["result"][0]["metric"].items()
        exists = all(item in result_labels for item in labels.items())
        with And("got result and contains labels" if exists else "got result, but doesn't contain labels"):
            return exists


def wait_alert_state(alert_name, alert_state, expected_state, labels=None, callback=None, max_try=20, sleep_time=10, time_range="10s"):
    catched = False
    for i in range(max_try):
        if not callback is None:
            callback()
        if expected_state == check_alert_state(alert_name, alert_state, labels, time_range):
            catched = True
            break
        with And(f"not ready, wait {sleep_time}s"):
            time.sleep(sleep_time)
    return catched


@TestScenario
@Name("Check clickhouse-operator/prometheus/alertmanager setup")
def test_prometheus_setup():
    with Given("clickhouse-operator is installed"):
        assert kubectl.kube_get_count("pod", ns=settings.operator_namespace,
                                      label="-l app=clickhouse-operator") > 0, error(
            "please run deploy/operator/clickhouse-operator-install.sh before run test")
        set_operator_version(settings.operator_version)
        set_metrics_exporter_version(settings.operator_version)

    with Given("prometheus-operator is installed"):
        assert kubectl.kube_get_count("pod", ns=settings.prometheus_namespace,
                                      label="-l app.kubernetes.io/component=controller,app.kubernetes.io/name=prometheus-operator") > 0, error(
            "please run deploy/promehteus/create_prometheus.sh before test run")
        assert kubectl.kube_get_count("pod", ns=settings.prometheus_namespace,
                                      label="-l app=prometheus,prometheus=prometheus") > 0, error(
            "please run deploy/promehteus/create_prometheus.sh before test run")
        assert kubectl.kube_get_count("pod", ns=settings.prometheus_namespace,
                                      label="-l app=alertmanager,alertmanager=alertmanager") > 0, error(
            "please run deploy/promehteus/create_prometheus.sh before test run")
        prometheus_operator_exptected_version = f"quay.io/coreos/prometheus-operator:v{settings.prometheus_operator_version}"
        assert prometheus_operator_exptected_version in prometheus_operator_spec["items"][0]["spec"]["containers"][0][
            "image"], error(f"require {prometheus_operator_exptected_version} image")


@TestScenario
@Name("Check MetricsExporterDown")
def test_metrics_exporter_down():
    def reboot_metrics_exporter():
        clickhouse_operator_pod = clickhouse_operator_spec["items"][0]["metadata"]["name"]
        kubectl.kubectl(
            f"exec -n {settings.operator_namespace} {clickhouse_operator_pod} -c metrics-exporter reboot",
            ok_to_fail=True,
        )

    with When("reboot metrics exporter"):
        fired = wait_alert_state("MetricsExporterDown", "firing", expected_state=True, callback=reboot_metrics_exporter)
        assert fired, error("can't get MetricsExporterDown alert in firing state")

    with Then("check MetricsExporterDown gone away"):
        pended = wait_alert_state("MetricsExporterDown", "firing", expected_state=False)
        assert pended, error("can't get MetricsExporterDown alert is gone away")


@TestScenario
@Name("Check ClickHouseServerDown, ClickHouseServerRestartRecently")
def test_clickhouse_server_reboot():
    random_idx = random.randint(0, 1)
    clickhouse_pod = clickhouse_spec["status"]["pods"][random_idx]
    clickhouse_svc = clickhouse_spec["status"]["fqdns"][random_idx]

    def reboot_clickhouse_server():
        kubectl.kubectl(
            f"exec -n {kubectl.namespace} {clickhouse_pod} -c clickhouse -- kill 1",
            ok_to_fail=True,
        )

    with When("reboot clickhouse-server pod"):
        fired = wait_alert_state("ClickHouseServerDown", "firing", True,
                                 labels={"hostname": clickhouse_svc, "chi": clickhouse_spec["metadata"]["name"]},
                                 callback=reboot_clickhouse_server)
        assert fired, error("can't get ClickHouseServerDown alert in firing state")

    with Then("check ClickHouseServerDown gone away"):
        pended = wait_alert_state("ClickHouseServerDown", "firing", False, labels={"hostname": clickhouse_svc})
        assert pended, error("can't check ClickHouseServerDown alert is gone away")

    with Then("check ClickHouseServerRestartRecently firing and gone away"):
        fired = wait_alert_state("ClickHouseServerRestartRecently", "firing", True,
                                 labels={"hostname": clickhouse_svc, "chi": clickhouse_spec["metadata"]["name"]})
        assert fired, error("after ClickHouseServerDown gone away, ClickHouseServerRestartRecently shall firing")

        pended = wait_alert_state("ClickHouseServerRestartRecently", "firing", False,
                                  labels={"hostname": clickhouse_svc})
        assert pended, error("can't check ClickHouseServerRestartRecently alert is gone away")

@TestScenario
@Name("Check ClickHouseDNSErrors")
def test_clickhouse_dns_errors():
    random_idx = random.randint(0, 1)
    clickhouse_pod = clickhouse_spec["status"]["pods"][random_idx]
    clickhouse_svc = clickhouse_spec["status"]["fqdns"][random_idx]

    old_dns = kubectl.kubectl(
        f"exec -n {kubectl.namespace} {clickhouse_pod} -c clickhouse -- cat /etc/resolv.conf",
        ok_to_fail=False,
    )
    new_dns = re.sub(r'^nameserver (.+)', 'nameserver 1.1.1.1', old_dns)

    def rewrite_dns_on_clickhouse_server(write_new=True):
        dns = new_dns if write_new else old_dns
        kubectl.kubectl(
            f"exec -n {kubectl.namespace} {clickhouse_pod} -c clickhouse -- bash -c \"printf \\\"{dns}\\\" > /etc/resolv.conf\"",
            ok_to_fail=False,
        )
        kubectl.kubectl(
            f"exec -n {kubectl.namespace} {clickhouse_pod} -c clickhouse -- clickhouse-client -q \"SYSTEM DROP DNS CACHE\"",
            ok_to_fail=False,
        )

    with When("rewrite /etc/resolv.conf in clickhouse-server pod"):
        rewrite_dns_on_clickhouse_server(write_new=True)
        fired = wait_alert_state("ClickHouseDNSErrors", "firing", True, labels={"hostname": clickhouse_svc})
        assert fired, error("can't get ClickHouseDNSErrors alert in firing state")

    with Then("check ClickHouseDNSErrors gone away"):
        rewrite_dns_on_clickhouse_server(write_new=False)
        pended = wait_alert_state("ClickHouseDNSErrors", "firing", False, labels={"hostname": clickhouse_svc})
        assert pended, error("can't check ClickHouseDNSErrors alert is gone away")


if main():
    with Module("metrics_alerts"):
        with Given("get information about prometheus installation"):
            prometheus_operator_spec = kubectl.kube_get(
                "pod", ns=settings.prometheus_namespace, name="",
                label="-l app.kubernetes.io/component=controller,app.kubernetes.io/name=prometheus-operator"
            )

            alertmanager_spec = kubectl.kube_get(
                "pod", ns=settings.prometheus_namespace, name="",
                label="-l app=alertmanager,alertmanager=alertmanager"
            )

            prometheus_spec = kubectl.kube_get(
                "pod", ns=settings.prometheus_namespace, name="",
                label="-l app=prometheus,prometheus=prometheus"
            )
        with Given("install zookeeper+clickhouse"):
            kubectl.kube_deletens(kubectl.namespace)
            kubectl.kube_createns(kubectl.namespace)
            require_zookeeper()
            create_and_check(
                "configs/test-cluster-for-alerts.yaml",
                {
                    "apply_templates": [
                        "templates/tpl-clickhouse-latest.yaml",
                        "templates/tpl-persistent-volume-100Mi.yaml"
                    ],
                    "object_counts": [2, 2, 3],
                    "do_not_delete": 1
                }
            )
            clickhouse_operator_spec = kubectl.kube_get(
                "pod", name="", ns=settings.operator_namespace, label="-l app=clickhouse-operator"
            )
            clickhouse_spec = kubectl.kube_get("chi", ns=kubectl.namespace, name="test-cluster-for-alerts")

        test_cases = [
            test_prometheus_setup,
            test_metrics_exporter_down,
            test_clickhouse_server_reboot,
            test_clickhouse_dns_errors,
        ]
        for t in test_cases:
            run(test=t)
