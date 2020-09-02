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


def drop_mergetree_table_on_cluster(cluster_name='all-sharded'):
    drop_local_sql = 'DROP TABLE default.test ON CLUSTER \\\"' + cluster_name + '\\\"'
    clickhouse.clickhouse_query(chi["metadata"]["name"], drop_local_sql, timeout=120)


def create_mergetree_table_on_cluster(cluster_name='all-sharded'):
    create_local_sql = 'CREATE TABLE default.test ON CLUSTER \\\"' + cluster_name + '\\\" (event_time DateTime, test UInt64) ENGINE MergeTree() ORDER BY tuple()'
    clickhouse.clickhouse_query(chi["metadata"]["name"], create_local_sql, timeout=120)


def drop_distributed_table_on_cluster(cluster_name='all-sharded'):
    drop_distr_sql = 'DROP TABLE default.test_distr ON CLUSTER \\\"' + cluster_name + '\\\"'
    clickhouse.clickhouse_query(chi["metadata"]["name"], drop_distr_sql, timeout=120)
    drop_mergetree_table_on_cluster(cluster_name)


def create_distributed_table_on_cluster(cluster_name='all-sharded'):
    create_mergetree_table_on_cluster(cluster_name)
    create_distr_sql = 'CREATE TABLE default.test_distr ON CLUSTER \\\"' + cluster_name + '\\\" (event_time DateTime, test UInt64) ENGINE Distributed("all-sharded",default, test, test)'
    clickhouse.clickhouse_query(chi["metadata"]["name"], create_distr_sql, timeout=120)


def drop_replicated_table_on_cluster(cluster_name='all-replicated'):
    drop_repl_sql = 'DROP TABLE default.test_repl ON CLUSTER \\\"' + cluster_name + '\\\"'
    clickhouse.clickhouse_query(chi["metadata"]["name"], drop_repl_sql, timeout=120)


def create_replicated_table_on_cluster(cluster_name='all-replicated'):
    create_local_sql = 'CREATE TABLE default.test_repl ON CLUSTER \\\"' + cluster_name + '\\\" (event_time DateTime, test UInt64) ENGINE ReplicatedMergeTree(\'/clickhouse/tables/{installation}-{shard}/test_repl\', \'{replica}\') ORDER BY tuple()'
    clickhouse.clickhouse_query(chi["metadata"]["name"], create_local_sql, timeout=120)

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
        assert prometheus_operator_exptected_version in prometheus_operator_spec["items"][0]["spec"]["containers"][0]["image"], error(f"require {prometheus_operator_exptected_version} image")


@TestScenario
@Name("Check ClickHouseClickHouseMetricsExporterDown")
def test_metrics_exporter_down():
    def reboot_metrics_exporter():
        clickhouse_operator_pod = clickhouse_operator_spec["items"][0]["metadata"]["name"]
        kubectl.kubectl(
            f"exec -n {settings.operator_namespace} {clickhouse_operator_pod} -c metrics-exporter -- reboot",
            ok_to_fail=True,
        )

    with When("reboot metrics exporter"):
        fired = wait_alert_state("ClickHouseClickHouseMetricsExporterDown", "firing", expected_state=True, callback=reboot_metrics_exporter, time_range='30s')
        assert fired, error("can't get ClickHouseClickHouseMetricsExporterDown alert in firing state")

    with Then("check ClickHouseMetricsExporterDown gone away"):
        resolved = wait_alert_state("ClickHouseMetricsExporterDown", "firing", expected_state=False, sleep_time=5)
        assert resolved, error("can't get ClickHouseMetricsExporterDown alert is gone away")


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
                                 callback=reboot_clickhouse_server,
                                 sleep_time=5, time_range='30s', max_try=30,
                                 )
        assert fired, error("can't get ClickHouseServerDown alert in firing state")

    with Then("check ClickHouseServerDown gone away"):
        resolved = wait_alert_state("ClickHouseServerDown", "firing", False, labels={"hostname": clickhouse_svc}, time_range='5s', sleep_time=5)
        assert resolved, error("can't check ClickHouseServerDown alert is gone away")

    with Then("check ClickHouseServerRestartRecently firing and gone away"):
        fired = wait_alert_state("ClickHouseServerRestartRecently", "firing", True,
                                 labels={"hostname": clickhouse_svc, "chi": chi["metadata"]["name"]}, time_range="30s")
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
            f"exec -n {kubectl.namespace} {clickhouse_pod} -c clickhouse -- clickhouse-client --echo -mn -q \"SYSTEM DROP DNS CACHE; SELECT count() FROM cluster('all-sharded',system,metrics)\"",
            ok_to_fail=True,
        )

    with When("rewrite /etc/resolv.conf in clickhouse-server pod"):
        fired = wait_alert_state("ClickHouseDNSErrors", "firing", True, labels={"hostname": clickhouse_svc},
                                 time_range='20s', callback=rewrite_dns_on_clickhouse_server, sleep_time=5)
        assert fired, error("can't get ClickHouseDNSErrors alert in firing state")

    with Then("check ClickHouseDNSErrors gone away"):
        rewrite_dns_on_clickhouse_server(write_new=False)
        resolved = wait_alert_state("ClickHouseDNSErrors", "firing", False, labels={"hostname": clickhouse_svc})
        assert resolved, error("can't check ClickHouseDNSErrors alert is gone away")


@TestScenario
@Name("Check ClickHouseDistributedFilesToInsertHigh")
def test_distributed_files_to_insert():
    delayed_pod, delayed_svc, restarted_pod, restarted_svc = random_pod_choice_for_callbacks()
    create_distributed_table_on_cluster()

    # we need 70 delayed files for catch
    insert_sql = 'INSERT INTO default.test_distr(event_time, test) SELECT now(), number FROM system.numbers LIMIT 10000'
    # clickhouse.clickhouse_query(
    #     chi["metadata"]["name"], 'SYSTEM STOP DISTRIBUTED SENDS default.test_distr',
    #     host=delayed_svc, ns=kubectl.namespace
    # )

    files_to_insert_from_metrics = 0
    files_to_insert_from_disk = 0
    tries = 0
    while files_to_insert_from_disk < 50 and tries < 500:
        kubectl.kubectl(
            f"exec -n {kubectl.namespace} {restarted_pod} -c clickhouse -- kill 1",
            ok_to_fail=True,
        )
        clickhouse.clickhouse_query(chi["metadata"]["name"], insert_sql, host=delayed_svc, ns=kubectl.namespace)
        files_to_insert_from_metrics = clickhouse.clickhouse_query(
            chi["metadata"]["name"], "SELECT value FROM system.metrics WHERE metric='DistributedFilesToInsert'",
            host=delayed_svc, ns=kubectl.namespace
        )
        files_to_insert_from_metrics = int(files_to_insert_from_metrics)

        files_to_insert_from_disk = int(kubectl.kubectl(
            f"exec -n {kubectl.namespace} {delayed_pod} -c clickhouse -- bash -c 'ls -la /var/lib/clickhouse/data/default/test_distr/*/*.bin 2>/dev/null | wc -l'",
            ok_to_fail=False,
        ))

    with When("reboot clickhouse-server pod"):
        fired = wait_alert_state("ClickHouseDistributedFilesToInsertHigh", "firing", True,
                                 labels={"hostname": delayed_svc, "chi": chi["metadata"]["name"]})
        assert fired, error("can't get ClickHouseDistributedFilesToInsertHigh alert in firing state")
    # @TODO remove it when  https://github.com/ClickHouse/ClickHouse/pull/11220 will merged to docker latest image
    kubectl.kube_wait_pod_status(restarted_pod, "Running", ns=kubectl.namespace)

    with Then("check ClickHouseClickHouseDistributedFilesToInsertHigh gone away"):
        resolved = wait_alert_state("ClickHouseDistributedFilesToInsertHigh", "firing", False, labels={"hostname": delayed_svc})
        assert resolved, error("can't check ClickHouseDistributedFilesToInsertHigh alert is gone away")

    drop_distributed_table_on_cluster()


@TestScenario
@Name("Check ClickHouseDistributedConnectionExceptions")
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

    with When("check ClickHouseDistributedConnectionExceptions firing"):
        fired = wait_alert_state("ClickHouseDistributedConnectionExceptions", "firing", True,
                                 labels={"hostname": delayed_svc, "chi": chi["metadata"]["name"]}, time_range='30s',
                                 callback=reboot_clickhouse_and_distributed_exection)
        assert fired, error("can't get ClickHouseDistributedConnectionExceptions alert in firing state")

    with Then("check DistributedConnectionExpections gone away"):
        resolved = wait_alert_state("ClickHouseDistributedConnectionExceptions", "firing", False,
                                    labels={"hostname": delayed_svc})
        assert resolved, error("can't check ClickHouseDistributedConnectionExceptions alert is gone away")
    kubectl.kube_wait_pod_status(restarted_pod, "Running", ns=kubectl.namespace)
    kubectl.kube_wait_jsonpath("pod", restarted_pod, "{.status.containerStatuses[0].ready}", "true",
                               ns=kubectl.namespace)
    drop_distributed_table_on_cluster()


@TestScenario
@Name("Check ClickHouseRejectedInsert, ClickHouseDelayedInsertThrottling, ClickHouseMaxPartCountForPartition, ClickHouseLowInsertedRowsPerQuery")
def test_delayed_and_rejected_insert_and_max_part_count_for_partition_and_low_inserted_rows_per_query():
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
    with Then("check ClickHouseDelayedInsertThrottling firing"):
        fired = wait_alert_state("ClickHouseDelayedInsertThrottling", "firing", True, labels={"hostname": delayed_svc}, time_range="30s", sleep_time=5)
        assert fired, error("can't get ClickHouseDelayedInsertThrottling alert in firing state")
    with Then("check ClickHouseMaxPartCountForPartition firing"):
        fired = wait_alert_state("ClickHouseMaxPartCountForPartition", "firing", True, labels={"hostname": delayed_svc}, time_range="45s", sleep_time=5)
        assert fired, error("can't get ClickHouseMaxPartCountForPartition alert in firing state")
    with Then("check ClickHouseLowInsertedRowsPerQuery firing"):
        fired = wait_alert_state("ClickHouseLowInsertedRowsPerQuery", "firing", True, labels={"hostname": delayed_svc}, time_range="60s", sleep_time=5)
        assert fired, error("can't get ClickHouseLowInsertedRowsPerQuery alert in firing state")

    clickhouse.clickhouse_query(chi_name, "SYSTEM START MERGES default.test", host=selected_svc, ns=kubectl.namespace)

    with Then("check ClickHouseDelayedInsertThrottling gone away"):
        resolved = wait_alert_state("ClickHouseDelayedInsertThrottling", "firing", False, labels={"hostname": delayed_svc}, sleep_time=5)
        assert resolved, error("can't check ClickHouseDelayedInsertThrottling alert is gone away")
    with Then("check ClickHouseMaxPartCountForPartition gone away"):
        resolved = wait_alert_state("ClickHouseMaxPartCountForPartition", "firing", False, labels={"hostname": delayed_svc}, sleep_time=5)
        assert resolved, error("can't check ClickHouseMaxPartCountForPartition alert is gone away")
    with Then("check ClickHouseLowInsertedRowsPerQuery gone away"):
        resolved = wait_alert_state("ClickHouseLowInsertedRowsPerQuery", "firing", False, labels={"hostname": delayed_svc}, sleep_time=5)
        assert resolved, error("can't check ClickHouseLowInsertedRowsPerQuery alert is gone away")

    parts_limits = parts_to_throw_insert
    selected_svc = rejected_svc
    insert_many_parts_to_clickhouse()
    with Then("check ClickHouseRejectedInsert firing"):
        fired = wait_alert_state("ClickHouseRejectedInsert", "firing", True, labels={"hostname": rejected_svc}, time_range="30s", sleep_time=5)
        assert fired, error("can't get ClickHouseRejectedInsert alert in firing state")

    with Then("check ClickHouseRejectedInsert gone away"):
        resolved = wait_alert_state("ClickHouseRejectedInsert", "firing", False, labels={"hostname": rejected_svc}, sleep_time=5)
        assert resolved, error("can't check ClickHouseRejectedInsert alert is gone away")

    clickhouse.clickhouse_query(chi_name, "SYSTEM START MERGES default.test", host=selected_svc, ns=kubectl.namespace)
    drop_mergetree_table_on_cluster()

@TestScenario
@Name("Check ClickHouseLongestRunningQuery")
def test_longest_running_query():
    long_running_pod, long_running_svc, _, _ = random_pod_choice_for_callbacks()
    # 600s trigger + 2*30s - double prometheus scraping interval
    clickhouse.clickhouse_query(chi["metadata"]["name"], "SELECT now(),sleepEachRow(1),number FROM system.numbers LIMIT 660",
                                host=long_running_svc, timeout=670)
    with Then("check ClickHouseLongestRunningQuery firing"):
        fired = wait_alert_state("ClickHouseLongestRunningQuery", "firing", True, labels={"hostname": long_running_svc},
                                 time_range='30s', sleep_time=5)
        assert fired, error("can't get ClickHouseLongestRunningQuery alert in firing state")
    with Then("check ClickHouseLongestRunningQuery gone away"):
        resolved = wait_alert_state("ClickHouseLongestRunningQuery", "firing", False, labels={"hostname": long_running_svc})
        assert resolved, error("can't check ClickHouseLongestRunningQuery alert is gone away")


@TestScenario
@Name("Check ClickHouseQueryPreempted")
def test_query_preempted():
    priority_pod, priority_svc, _, _ = random_pod_choice_for_callbacks()

    def run_queries_with_priority():
        sql = ""
        for i in range(50):
            sql += f"SET priority={i % 20};SELECT uniq(number) FROM numbers(20000000):"
        cmd = f"echo \\\"{sql} SELECT 1\\\" | xargs -i'{{}}' --no-run-if-empty -d ':' -P 20 clickhouse-client --time -m -n -q \\\"{{}}\\\""
        kubectl.kubectl(f"exec {priority_pod} -- bash -c \"{cmd}\"", timeout=120)
        clickhouse.clickhouse_query(
            chi["metadata"]["name"],
            "SELECT event_time, CurrentMetric_QueryPreempted FROM system.metric_log WHERE CurrentMetric_QueryPreempted > 0",
            host=priority_svc,
        )

    with Then("check ClickHouseQueryPreempted firing"):
        fired = wait_alert_state("ClickHouseQueryPreempted", "firing", True, labels={"hostname": priority_svc},
                                 time_range='30s', sleep_time=5, callback=run_queries_with_priority)
        assert fired, error("can't get ClickHouseQueryPreempted alert in firing state")
    with Then("check ClickHouseQueryPreempted gone away"):
        resolved = wait_alert_state("ClickHouseQueryPreempted", "firing", False, labels={"hostname": priority_svc})
        assert resolved, error("can't check ClickHouseQueryPreempted alert is gone away")

@TestScenario
@Name("Check ClickHouseReadonlyReplica")
def test_read_only_replica():
    read_only_pod, read_only_svc, other_pod, other_svc = random_pod_choice_for_callbacks()
    chi_name = chi["metadata"]["name"]
    create_replicated_table_on_cluster()

    def restart_zookeeper():
        kubectl.kubectl(
            f"exec -n {kubectl.namespace} zookeeper-0 -- sh -c \"kill 1\"",
            ok_to_fail=True,
        )
        clickhouse.clickhouse_query_with_error(chi_name, "INSERT INTO default.test_repl VALUES(now(),rand())", host=read_only_svc)

    with Then("check ClickHouseReadonlyReplica firing"):
        fired = wait_alert_state("ClickHouseReadonlyReplica", "firing", True, labels={"hostname": read_only_svc},
                                 time_range='30s', sleep_time=5, callback=restart_zookeeper)
        assert fired, error("can't get ClickHouseReadonlyReplica alert in firing state")
    with Then("check ClickHouseReadonlyReplica gone away"):
        resolved = wait_alert_state("ClickHouseReadonlyReplica", "firing", False, labels={"hostname": read_only_svc})
        assert resolved, error("can't check ClickHouseReadonlyReplica alert is gone away")

    kubectl.kube_wait_pod_status("zookeeper-0", "Running", ns=kubectl.namespace)
    kubectl.kube_wait_jsonpath("pod", "zookeeper-0", "{.status.containerStatuses[0].ready}", "true",
                               ns=kubectl.namespace)

    clickhouse.clickhouse_query_with_error(
        chi_name, "SYSTEM RESTART REPLICAS; SYSTEM SYNC REPLICA default.test_repl",
        host=read_only_svc, timeout=240
    )
    clickhouse.clickhouse_query_with_error(
        chi_name, "SYSTEM RESTART REPLICAS; SYSTEM SYNC REPLICA default.test_repl",
        host=other_svc, timeout=240
    )

    drop_replicated_table_on_cluster()

@TestScenario
@Name("Check ClickHouseReplicasMaxAbsoluteDelay")
def test_replicas_max_abosulute_delay():
    stop_replica_pod, stop_replica_svc, insert_pod, insert_svc = random_pod_choice_for_callbacks()
    create_replicated_table_on_cluster()
    prometheus_scrape_interval = 30

    def restart_clickhouse_and_insert_to_replicated_table():
        with When(f"stop replica fetches on {stop_replica_svc}"):
            sql = "SYSTEM STOP FETCHES default.test_repl"
            kubectl.kubectl(
                f"exec -n {kubectl.namespace} {stop_replica_pod} -c clickhouse -- clickhouse-client -q \"{sql}\"",
                ok_to_fail=True,
            )
            sql = "INSERT INTO default.test_repl SELECT now(), number FROM numbers(100000)"
            kubectl.kubectl(
                f"exec -n {kubectl.namespace} {insert_pod} -c clickhouse -- clickhouse-client -q \"{sql}\"",
            )

    with Then("check ClickHouseReplicasMaxAbsoluteDelay firing"):
        fired = wait_alert_state("ClickHouseReplicasMaxAbsoluteDelay", "firing", True, labels={"hostname": stop_replica_svc},
                                 time_range='60s', sleep_time=prometheus_scrape_interval*2,
                                 callback=restart_clickhouse_and_insert_to_replicated_table)
        assert fired, error("can't get ClickHouseReadonlyReplica alert in firing state")

    clickhouse.clickhouse_query(
        chi["metadata"]["name"],
        "SYSTEM START FETCHES; SYSTEM RESTART REPLICAS; SYSTEM SYNC REPLICA default.test_repl", timeout=240
    )
    with Then("check ClickHouseReplicasMaxAbsoluteDelay gone away"):
        resolved = wait_alert_state("ClickHouseReplicasMaxAbsoluteDelay", "firing", False, labels={"hostname": stop_replica_svc})
        assert resolved, error("can't check ClickHouseReplicasMaxAbsoluteDelay alert is gone away")

    drop_replicated_table_on_cluster()

@TestScenario
@Name("Check ClickHouseTooManyConnections")
def test_too_many_connections():
    too_many_connection_pod, too_many_connection_svc, _, _ = random_pod_choice_for_callbacks()
    cmd = "export DEBIAN_FRONTEND=noninteractive; apt-get update; apt-get install -y netcat"
    kubectl.kubectl(
        f"exec -n {kubectl.namespace} {too_many_connection_pod} -c clickhouse -- bash -c  \"{cmd}\"",
        ok_to_fail=True,
    )

    def make_too_many_connection():
        long_cmd = ""
        for _ in range(110):
            port = random.choice(["8123", "3306", "9000"])
            if port == "8123":
                # @TODO HTTPConnection metric increase after full parsing of HTTP Request, we can't provide pause between CONNECT and QUERY running
                # long_cmd += f"printf \"POST / HTTP/1.1\\r\\nHost: 127.0.0.1:8123\\r\\nContent-Length: 34\\r\\n\\r\\nTEST\\r\\nTEST\\r\\nTEST\\r\\nTEST\\r\\nTEST\\r\\nTEST\" | nc -q 5 -i 10 -vv 127.0.0.1 {port};"
                long_cmd += 'wget -qO- "http://127.0.0.1:8123?query=SELECT sleepEachRow(1),now() FROM numbers(60)";'
            elif port == "9000":
                long_cmd += 'clickhouse-client -q "SELECT sleepEachRow(1),now() FROM numbers(60)";'
            else:
                long_cmd += f"printf \"1\\n1\" | nc -q 5 -i 30 -vv 127.0.0.1 {port};"

        nc_cmd = f"echo '{long_cmd} exit 0' | xargs --verbose -i'{{}}' --no-run-if-empty -d ';' -P 120 bash -c '{{}}' 1>/dev/null"
        with open("/tmp/nc_cmd.sh", "w") as f:
            f.write(nc_cmd)

        kubectl.kubectl(
            f"cp /tmp/nc_cmd.sh {too_many_connection_pod}:/tmp/nc_cmd.sh -c clickhouse"
        )

        kubectl.kubectl(
            f"exec -n {kubectl.namespace} {too_many_connection_pod} -c clickhouse -- bash /tmp/nc_cmd.sh",
            timeout=120,
        )

    with Then("check ClickHouseTooManyConnections firing"):
        fired = wait_alert_state("ClickHouseTooManyConnections", "firing", True, labels={"hostname": too_many_connection_svc},
                                 time_range='90s', callback=make_too_many_connection)
        assert fired, error("can't get ClickHouseTooManyConnections alert in firing state")

    with Then("check ClickHouseTooManyConnections gone away"):
        resolved = wait_alert_state("ClickHouseTooManyConnections", "firing", False, labels={"hostname": too_many_connection_svc})
        assert resolved, error("can't check ClickHouseTooManyConnections alert is gone away")


@TestScenario
@Name("Check ClickHouseTooMuchRunningQueries")
def test_too_much_running_queries():
    _, _, too_many_queries_pod, too_many_queries_svc = random_pod_choice_for_callbacks()
    cmd = "export DEBIAN_FRONTEND=noninteractive; apt-get update; apt-get install -y mysql-client"
    kubectl.kubectl(
        f"exec -n {kubectl.namespace} {too_many_queries_pod} -c clickhouse -- bash -c  \"{cmd}\"",
        ok_to_fail=True,
    )

    def make_too_many_queries():
        long_cmd = ""
        for _ in range(90):
            port = random.choice(["8123", "3306", "9000"])
            if port == "9000":
                long_cmd += 'clickhouse-client -q "SELECT sleepEachRow(1),now() FROM numbers(60)";'
            if port == "3306":
                long_cmd += 'mysql -h 127.0.0.1 -P 3306 -u default -e "SELECT sleepEachRow(1),now() FROM numbers(60)";'
            if port == "8123":
                long_cmd += 'wget -qO- "http://127.0.0.1:8123?query=SELECT sleepEachRow(1),now() FROM numbers(60)";'

        long_cmd = f"echo '{long_cmd}' | xargs --verbose -i'{{}}' --no-run-if-empty -d ';' -P 100 bash -c '{{}}' 1>/dev/null"
        with open("/tmp/long_cmd.sh", "w") as f:
            f.write(long_cmd)

        kubectl.kubectl(
            f"cp /tmp/long_cmd.sh {too_many_queries_pod}:/tmp/long_cmd.sh -c clickhouse"
        )
        kubectl.kubectl(
            f"exec -n {kubectl.namespace} {too_many_queries_pod} -c clickhouse -- bash /tmp/long_cmd.sh",
            timeout=70,
        )

    with Then("check ClickHouseTooMuchRunningQueries firing"):
        fired = wait_alert_state("ClickHouseTooMuchRunningQueries", "firing", True, labels={"hostname": too_many_queries_svc},
                                 callback=make_too_many_queries, time_range="30s", sleep_time=5)
        assert fired, error("can't get ClickHouseTooManyConnections alert in firing state")

    with Then("check ClickHouseTooManyConnections gone away"):
        resolved = wait_alert_state("ClickHouseTooMuchRunningQueries", "firing", False, labels={"hostname": too_many_queries_svc}, sleep_time=5)
        assert resolved, error("can't check ClickHouseTooManyConnections alert is gone away")


@TestScenario
@Name("Check ClickHouseSystemSettingsChanged")
def test_system_settings_changed():
    changed_pod, changed_svc, _, _ = random_pod_choice_for_callbacks()

    with When("apply changed settings"):
        create_and_check(
            "configs/test-cluster-for-alerts-changed-settings.yaml",
            {
                "apply_templates": [
                    "templates/tpl-clickhouse-latest.yaml",
                    "templates/tpl-persistent-volume-100Mi.yaml"
                ],
                "object_counts": [2, 2, 3],
                "do_not_delete": 1
            }
        )

    with Then("check ClickHouseSystemSettingsChanged firing"):
        fired = wait_alert_state("ClickHouseSystemSettingsChanged", "firing", True, labels={"hostname": changed_svc},
                                 time_range="30s", sleep_time=5)
        assert fired, error("can't get ClickHouseTooManyConnections alert in firing state")

    with When("rollback changed settings"):
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

    with Then("check ClickHouseSystemSettingsChanged gone away"):
        resolved = wait_alert_state("ClickHouseSystemSettingsChanged", "firing", False, labels={"hostname": changed_svc}, sleep_time=30)
        assert resolved, error("can't check ClickHouseTooManyConnections alert is gone away")

@TestScenario
@Name("Check ClickHouseVersionChanged")
def test_version_changed():
    changed_pod, changed_svc, _, _ = random_pod_choice_for_callbacks()

    with When("apply changed settings"):
        create_and_check(
            "configs/test-cluster-for-alerts-changed-settings.yaml",
            {
                "apply_templates": [
                    "templates/tpl-clickhouse-20.3.yaml",
                    "templates/tpl-persistent-volume-100Mi.yaml"
                ],
                "object_counts": [2, 2, 3],
                "do_not_delete": 1
            }
        )
        prometheus_scrape_interval = 30
        with And(f"wait prometheus_scrape_interval={prometheus_scrape_interval}*2 sec"):
            time.sleep(prometheus_scrape_interval * 2)

    with Then("check ClickHouseVersionChanged firing"):
        fired = wait_alert_state("ClickHouseVersionChanged", "firing", True, labels={"hostname": changed_svc},
                                 time_range="30s", sleep_time=5)
        assert fired, error("can't get ClickHouseVersionChanged alert in firing state")

    with When("rollback changed settings"):
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

    with Then("check ClickHouseVersionChanged gone away"):
        resolved = wait_alert_state("ClickHouseVersionChanged", "firing", False, labels={"hostname": changed_svc}, sleep_time=30)
        assert resolved, error("can't check ClickHouseVersionChanged alert is gone away")

@TestScenario
@Name("Check ZooKeeperHardwareExceptions")
def test_zookeeper_hardware_exceptions():
    pod1, svc1, pod2, svc2 = random_pod_choice_for_callbacks()
    chi_name = chi["metadata"]["name"]

    def restart_zookeeper():
        kubectl.kubectl(
            f"exec -n {kubectl.namespace} zookeeper-0 -- sh -c \"kill 1\"",
            ok_to_fail=True,
        )
        clickhouse.clickhouse_query_with_error(chi_name, "SELECT name, path FROM system.zookeeper WHERE path='/'", host=svc1)
        clickhouse.clickhouse_query_with_error(chi_name, "SELECT name, path FROM system.zookeeper WHERE path='/'", host=svc2)

    with Then("check ZooKeeperHardwareExceptions firing"):
        for svc in (svc1, svc2):
            fired = wait_alert_state("ZooKeeperHardwareExceptions", "firing", True, labels={"hostname": svc},
                                     time_range='30s', sleep_time=5, callback=restart_zookeeper)
            assert fired, error("can't get ZooKeeperHardwareExceptions alert in firing state")

    kubectl.kube_wait_pod_status("zookeeper-0", "Running", ns=kubectl.namespace)
    kubectl.kube_wait_jsonpath("pod", "zookeeper-0", "{.status.containerStatuses[0].ready}", "true",
                               ns=kubectl.namespace)

    with Then("check ZooKeeperHardwareExceptions gone away"):
        for svc in (svc1, svc2):
            resolved = wait_alert_state("ZooKeeperHardwareExceptions", "firing", False, labels={"hostname": svc})
            assert resolved, error("can't check ZooKeeperHardwareExceptions alert is gone away")


if main():
    with Module("main"):
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
            assert "items" in prometheus_spec and len(prometheus_spec["items"]) > 0 and "metadata" in prometheus_spec["items"][0], "invalid prometheus_spec"

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

        with Module("metrics_alerts"):
            test_cases = [
                test_prometheus_setup,
                test_read_only_replica,
                test_metrics_exporter_down,
                test_clickhouse_server_reboot,
                test_clickhouse_dns_errors,
                test_replicas_max_abosulute_delay,
                test_distributed_connection_exceptions,
                test_delayed_and_rejected_insert_and_max_part_count_for_partition_and_low_inserted_rows_per_query,
                test_too_many_connections,
                test_too_much_running_queries,
                test_longest_running_query,
                test_system_settings_changed,
                test_version_changed,
                test_zookeeper_hardware_exceptions,
                # @TODO remove it when  https://github.com/ClickHouse/ClickHouse/pull/11220 will merged to docker latest image
                # test_distributed_files_to_insert,
            ]
            for t in test_cases:
                run(test=t)
