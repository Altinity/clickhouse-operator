import re
import time
import json
import random

from testflows.core import TestScenario, Name, When, Then, Given, And, main, run, Module
from testflows.asserts import error
import settings
import kubectl
import clickhouse

from test_operator import set_operator_version, require_zookeeper, create_and_check
from test_metrics_exporter import set_metrics_exporter_version

prometheus_operator_spec = None
alertmanager_spec = None
prometheus_spec = None

clickhouse_operator_spec = None
chi = None


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


def wait_alert_state(alert_name, alert_state, expected_state, labels=None, callback=None, max_try=20, sleep_time=10,
                     time_range="10s"):
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


def random_pod_choice_for_callbacks():
    first_idx = random.randint(0, 1)
    first_pod = chi["status"]["pods"][first_idx]
    first_svc = chi["status"]["fqdns"][first_idx]
    second_idx = 0 if first_idx == 1 else 1
    second_pod = chi["status"]["pods"][second_idx]
    second_svc = chi["status"]["fqdns"][second_idx]
    return second_pod, second_svc, first_pod, first_svc


def drop_distributed_table_on_cluster(cluster_name='all-sharded'):
    drop_distr_sql = 'DROP TABLE default.test_distr ON CLUSTER \\\"' + cluster_name + '\\\"'
    clickhouse.clickhouse_query(chi["metadata"]["name"], drop_distr_sql)
    drop_mergetree_table_on_cluster(cluster_name)


def create_distributed_table_on_cluster(cluster_name='all-sharded'):
    create_mergetree_table_on_cluster(cluster_name)
    create_distr_sql = 'CREATE TABLE default.test_distr ON CLUSTER \\\"' + cluster_name + '\\\" (event_time DateTime, test UInt64) ENGINE Distributed("all-sharded",default, test, test)'
    clickhouse.clickhouse_query(chi["metadata"]["name"], create_distr_sql)


def drop_mergetree_table_on_cluster(cluster_name='all-sharded'):
    drop_local_sql = 'DROP TABLE default.test ON CLUSTER \\\"' + cluster_name + '\\\"'
    clickhouse.clickhouse_query(chi["metadata"]["name"], drop_local_sql)


def create_mergetree_table_on_cluster(cluster_name='all-sharded'):
    create_local_sql = 'CREATE TABLE default.test ON CLUSTER \\\"' + cluster_name + '\\\" (event_time DateTime, test UInt64) ENGINE MergeTree() ORDER BY tuple()'
    clickhouse.clickhouse_query(chi["metadata"]["name"], create_local_sql)


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
        resolved = wait_alert_state("MetricsExporterDown", "firing", expected_state=False)
        assert resolved, error("can't get MetricsExporterDown alert is gone away")


@TestScenario
@Name("Check ClickHouseServerDown, ClickHouseServerRestartRecently")
def test_clickhouse_server_reboot():
    random_idx = random.randint(0, 1)
    clickhouse_pod = chi["status"]["pods"][random_idx]
    clickhouse_svc = chi["status"]["fqdns"][random_idx]

    def reboot_clickhouse_server():
        kubectl.kubectl(
            f"exec -n {kubectl.namespace} {clickhouse_pod} -c clickhouse -- kill 1",
            ok_to_fail=True,
        )

    with When("reboot clickhouse-server pod"):
        fired = wait_alert_state("ClickHouseServerDown", "firing", True,
                                 labels={"hostname": clickhouse_svc, "chi": chi["metadata"]["name"]},
                                 callback=reboot_clickhouse_server)
        assert fired, error("can't get ClickHouseServerDown alert in firing state")

    with Then("check ClickHouseServerDown gone away"):
        resolved = wait_alert_state("ClickHouseServerDown", "firing", False, labels={"hostname": clickhouse_svc})
        assert resolved, error("can't check ClickHouseServerDown alert is gone away")

    with Then("check ClickHouseServerRestartRecently firing and gone away"):
        fired = wait_alert_state("ClickHouseServerRestartRecently", "firing", True,
                                 labels={"hostname": clickhouse_svc, "chi": chi["metadata"]["name"]})
        assert fired, error("after ClickHouseServerDown gone away, ClickHouseServerRestartRecently shall firing")

        resolved = wait_alert_state("ClickHouseServerRestartRecently", "firing", False,
                                    labels={"hostname": clickhouse_svc})
        assert resolved, error("can't check ClickHouseServerRestartRecently alert is gone away")


@TestScenario
@Name("Check ClickHouseDNSErrors")
def test_clickhouse_dns_errors():
    random_idx = random.randint(0, 1)
    clickhouse_pod = chi["status"]["pods"][random_idx]
    clickhouse_svc = chi["status"]["fqdns"][random_idx]

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
        resolved = wait_alert_state("ClickHouseDNSErrors", "firing", False, labels={"hostname": clickhouse_svc})
        assert resolved, error("can't check ClickHouseDNSErrors alert is gone away")


@TestScenario
@Name("Check DistributedFilesToInsertHigh")
def test_distributed_files_to_insert():
    delayed_pod, delayed_svc, restarted_pod, restarted_svc = random_pod_choice_for_callbacks()
    create_distributed_table_on_cluster()

    # we need 70 delayed files for catch
    insert_sql = 'INSERT INTO default.test_distr(event_time, test) SELECT now(), number FROM system.numbers LIMIT 10000'
    for i in range(120):
        kubectl.kubectl(
            f"exec -n {kubectl.namespace} {restarted_pod} -c clickhouse -- kill 1",
            ok_to_fail=True,
        )
        clickhouse.clickhouse_query(chi["metadata"]["name"], insert_sql, host=delayed_pod, ns=kubectl.namespace)

    with When("reboot clickhouse-server pod"):
        fired = wait_alert_state("DistributedFilesToInsertHigh", "firing", True,
                                 labels={"hostname": delayed_svc, "chi": chi["metadata"]["name"]})
        assert fired, error("can't get DistributedFilesToInsertHigh alert in firing state")
    # @TODO remove it when  https://github.com/ClickHouse/ClickHouse/pull/11220 will merged to docker latest image
    kubectl.kube_wait_pod_status(restarted_pod, "Running", ns=kubectl.namespace)

    with Then("check DistributedFilesToInsertHigh gone away"):
        resolved = wait_alert_state("DistributedFilesToInsertHigh", "firing", False, labels={"hostname": delayed_svc})
        assert resolved, error("can't check DistributedFilesToInsertHigh alert is gone away")

    drop_distributed_table_on_cluster()


@TestScenario
@Name("Check DistributedConnectionExceptions")
def test_distributed_connection_exceptions():
    delayed_pod, delayed_svc, restarted_pod, restarted_svc = random_pod_choice_for_callbacks()
    create_distributed_table_on_cluster()

    def reboot_clickhouse_and_distributed_exection():
        # we need 70 delayed files for catch
        insert_sql = 'INSERT INTO default.test_distr(event_time, test) SELECT now(), number FROM system.numbers LIMIT 10000'
        select_sql = 'SELECT count() FROM default.test_distr'
        with Then("reboot clickhouse-server pod"):
            kubectl.kubectl(
                f"exec -n {kubectl.namespace} {restarted_pod} -c clickhouse -- kill 1",
                ok_to_fail=True,
            )
            with And("Insert to distributed table"):
                clickhouse.clickhouse_query(chi["metadata"]["name"], insert_sql, host=delayed_pod, ns=kubectl.namespace)

            with And("Select from distributed table"):
                clickhouse.clickhouse_query_with_error(chi["metadata"]["name"], select_sql, host=delayed_pod,
                                                       ns=kubectl.namespace)

    with When("check DistributedConnectionExceptions firing"):
        fired = wait_alert_state("DistributedConnectionExceptions", "firing", True,
                                 labels={"hostname": delayed_svc, "chi": chi["metadata"]["name"]},
                                 callback=reboot_clickhouse_and_distributed_exection)
        assert fired, error("can't get DistributedConnectionExceptions alert in firing state")

    with Then("check DistributedConnectionExpections gone away"):
        resolved = wait_alert_state("DistributedConnectionExceptions", "firing", False,
                                    labels={"hostname": delayed_svc})
        assert resolved, error("can't check DistributedConnectionExceptions alert is gone away")
    kubectl.kube_wait_pod_status(restarted_pod, "Running", ns=kubectl.namespace)
    kubectl.kube_wait_jsonpath("pod", restarted_pod, "{.status.containerStatuses[0].ready}", "true",
                               ns=kubectl.namespace)
    drop_distributed_table_on_cluster()


@TestScenario
@Name("Check RejectedInsert, DelayedInsertThrottling")
def test_delayed_and_rejected_insert():
    create_mergetree_table_on_cluster()
    delayed_pod, delayed_svc, rejected_pod, rejected_svc = random_pod_choice_for_callbacks()

    prometheus_scrape_interval = 30
    # default values in system.merge_tree_settings
    parts_to_throw_insert = 300
    parts_to_delay_insert = 150
    chi_name = chi["metadata"]["name"]

    parts_limits = parts_to_delay_insert
    selected_svc = delayed_svc

    def insert_many_parts_to_clickhouse():
        stop_merges = "SYSTEM STOP MERGES default.test;"
        min_block = "SET max_block_size=1; SET max_insert_block_size=1; SET min_insert_block_size_rows=1;"
        with When(f"Insert to MergeTree table {parts_limits} parts"):
            r = parts_limits
            sql = stop_merges + min_block + \
                  "INSERT INTO default.test(event_time, test) SELECT now(), number FROM system.numbers LIMIT %d;" % r
            clickhouse.clickhouse_query(chi_name, sql, host=selected_svc, ns=kubectl.namespace)

            # @TODO we need only one query after resolve https://github.com/ClickHouse/ClickHouse/issues/11384
            sql = min_block + "INSERT INTO default.test(event_time, test) SELECT now(), number FROM system.numbers LIMIT 1;"
            clickhouse.clickhouse_query_with_error(chi_name, sql, host=selected_svc, ns=kubectl.namespace)
            with And(f"wait prometheus_scrape_interval={prometheus_scrape_interval}*2 seconds"):
                time.sleep(prometheus_scrape_interval * 2)

            sql = min_block + "INSERT INTO default.test(event_time, test) SELECT now(), number FROM system.numbers LIMIT 1;"
            clickhouse.clickhouse_query_with_error(chi_name, sql, host=selected_svc, ns=kubectl.namespace)

    insert_many_parts_to_clickhouse()
    with Then("check DelayedInsertThrottling firing"):
        fired = wait_alert_state("DelayedInsertThrottling", "firing", True, labels={"hostname": delayed_svc})
        assert fired, error("can't get DelayedInsertThrottling alert in firing state")

    clickhouse.clickhouse_query(chi_name, "SYSTEM START MERGES default.test", host=selected_svc, ns=kubectl.namespace)

    with Then("check DelayedInsertThrottling gone away"):
        resolved = wait_alert_state("DelayedInsertThrottling", "firing", False, labels={"hostname": delayed_svc})
        assert resolved, error("can't check DelayedInsertThrottling alert is gone away")

    parts_limits = parts_to_throw_insert
    selected_svc = rejected_svc
    insert_many_parts_to_clickhouse()
    with Then("check RejectedInsert firing"):
        fired = wait_alert_state("RejectedInsert", "firing", True, labels={"hostname": rejected_svc})
        assert fired, error("can't get RejectedInsert alert in firing state")

    with Then("check RejectedInsert gone away"):
        resolved = wait_alert_state("RejectedInsert", "firing", False, labels={"hostname": rejected_svc})
        assert resolved, error("can't check RejectedInsert alert is gone away")

    clickhouse.clickhouse_query(chi_name, "SYSTEM START MERGES default.test", host=selected_svc, ns=kubectl.namespace)
    drop_mergetree_table_on_cluster()


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
            chi = kubectl.kube_get("chi", ns=kubectl.namespace, name="test-cluster-for-alerts")

        test_cases = [
            test_prometheus_setup,
            test_metrics_exporter_down,
            test_clickhouse_server_reboot,
            test_clickhouse_dns_errors,
            # @TODO uncomment it when  https://github.com/ClickHouse/ClickHouse/pull/11220 will merged to docker latest image
            # test_distributed_files_to_insert,
            test_distributed_connection_exceptions,
            test_delayed_and_rejected_insert,
        ]
        for t in test_cases:
            run(test=t)
