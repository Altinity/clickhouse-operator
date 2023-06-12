import re
import time
import random

from testflows.core import *
from testflows.asserts import error

import e2e.clickhouse as clickhouse
import e2e.kubectl as kubectl
import e2e.settings as settings
import e2e.util as util
import e2e.alerts as alerts


@TestScenario
@Name("test_prometheus_setup. Check clickhouse-operator/prometheus/alertmanager setup")
def test_prometheus_setup(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    with Given("clickhouse-operator is installed"):
        assert (
            kubectl.get_count(
                "pod",
                ns=settings.operator_namespace,
                label="-l app=clickhouse-operator",
            )
            > 0
        ), error("please run deploy/operator/clickhouse-operator-install.sh before run test")
        util.set_operator_version(settings.operator_version)
        util.set_metrics_exporter_version(settings.operator_version)

    with Given("prometheus-operator is installed"):
        assert (
            kubectl.get_count(
                "pod",
                ns=settings.prometheus_namespace,
                label="-l app.kubernetes.io/component=controller,app.kubernetes.io/name=prometheus-operator",
            )
            > 0
        ), error("please run deploy/promehteus/create_prometheus.sh before test run")
        assert (
            kubectl.get_count(
                "pod",
                ns=settings.prometheus_namespace,
                label="-l app.kubernetes.io/managed-by=prometheus-operator,prometheus=prometheus",
            )
            > 0
        ), error("please run deploy/promehteus/create_prometheus.sh before test run")
        assert (
            kubectl.get_count(
                "pod",
                ns=settings.prometheus_namespace,
                label="-l app.kubernetes.io/managed-by=prometheus-operator,alertmanager=alertmanager",
            )
            > 0
        ), error("please run deploy/promehteus/create_prometheus.sh before test run")
        prometheus_operator_exptected_version = (
            f"quay.io/prometheus-operator/prometheus-operator:v{settings.prometheus_operator_version}"
        )
        actual_image = prometheus_operator_spec["items"][0]["spec"]["containers"][0]["image"]
        assert prometheus_operator_exptected_version in actual_image, error(
            f"require {prometheus_operator_exptected_version} image"
        )


@TestScenario
@Name("test_metrics_exporter_down. Check ClickHouseMetricsExporterDown")
def test_metrics_exporter_down(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    def reboot_metrics_exporter():
        clickhouse_operator_pod = clickhouse_operator_spec["items"][0]["metadata"]["name"]
        kubectl.launch(
            f"exec -n {settings.operator_namespace} {clickhouse_operator_pod} -c metrics-exporter -- sh -c 'kill 1'",
            ok_to_fail=True,
        )

    with When("reboot metrics exporter"):
        fired = alerts.wait_alert_state(
            "ClickHouseMetricsExporterDown",
            "firing",
            expected_state=True,
            callback=reboot_metrics_exporter,
            time_range="30s",
        )
        assert fired, error("can't get ClickHouseMetricsExporterDown alert in firing state")

    with Then("check ClickHouseMetricsExporterDown gone away"):
        resolved = alerts.wait_alert_state("ClickHouseMetricsExporterDown", "firing", expected_state=False)
        assert resolved, error("can't get ClickHouseMetricsExporterDown alert is gone away")


@TestScenario
@Name("test_clickhouse_server_reboot. Check ClickHouseServerDown, ClickHouseServerRestartRecently")
def test_clickhouse_server_reboot(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    random_idx = random.randint(0, 1)
    clickhouse_pod = chi["status"]["pods"][random_idx]
    clickhouse_svc = chi["status"]["fqdns"][random_idx]

    def reboot_clickhouse_server():
        kubectl.launch(
            f"exec -n {kubectl.namespace} {clickhouse_pod} -c clickhouse-pod -- kill 1",
            ok_to_fail=True,
        )

    with When("reboot clickhouse-server pod"):
        fired = alerts.wait_alert_state(
            "ClickHouseServerDown",
            "firing",
            True,
            callback=reboot_clickhouse_server,
            labels={"hostname": clickhouse_svc, "chi": chi["metadata"]["name"]},
            sleep_time=settings.prometheus_scrape_interval,
            time_range="30s",
            max_try=30,
        )
        assert fired, error("can't get ClickHouseServerDown alert in firing state")

    with Then("check ClickHouseServerDown gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseServerDown",
            "firing",
            False,
            labels={"hostname": clickhouse_svc},
            time_range="5s",
            sleep_time=settings.prometheus_scrape_interval,
            max_try=100,
        )
        assert resolved, error("can't check ClickHouseServerDown alert is gone away")

    with Then("check ClickHouseServerRestartRecently firing and gone away"):
        fired = alerts.wait_alert_state(
            "ClickHouseServerRestartRecently",
            "firing",
            True,
            labels={"hostname": clickhouse_svc, "chi": chi["metadata"]["name"]},
            time_range="30s",
        )
        assert fired, error("after ClickHouseServerDown gone away, ClickHouseServerRestartRecently shall firing")

        resolved = alerts.wait_alert_state(
            "ClickHouseServerRestartRecently",
            "firing",
            False,
            sleep_time=10,
            max_try=100,
            time_range="5s",
            labels={"hostname": clickhouse_svc},
        )
        assert resolved, error("can't check ClickHouseServerRestartRecently alert is gone away")


@TestScenario
@Name("test_clickhouse_dns_errors. Check ClickHouseDNSErrors")
def test_clickhouse_dns_errors(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    random_idx = random.randint(0, 1)
    clickhouse_pod = chi["status"]["pods"][random_idx]
    clickhouse_svc = chi["status"]["fqdns"][random_idx]

    old_dns = kubectl.launch(
        f"exec -n {kubectl.namespace} {clickhouse_pod} -c clickhouse-pod -- cat /etc/resolv.conf",
        ok_to_fail=False,
    )
    new_dns = re.sub(r"^nameserver (.+)", "nameserver 1.1.1.1", old_dns, flags=re.MULTILINE)

    def rewrite_dns_on_clickhouse_server(write_new=True):
        dns = new_dns if write_new else old_dns
        kubectl.launch(
            f'exec -n {kubectl.namespace} {clickhouse_pod} -c clickhouse-pod -- bash -c "printf \\"{dns}\\" > /etc/resolv.conf"',
            ok_to_fail=False,
        )
        kubectl.launch(
            f"exec -n {kubectl.namespace} {clickhouse_pod} -c clickhouse-pod -- clickhouse-client --echo -mn "
            f"-q \"SYSTEM DROP DNS CACHE; SELECT count() FROM cluster('all-sharded',system.metrics)\"",
            ok_to_fail=True,
        )
        kubectl.launch(
            f"exec -n {kubectl.namespace} {clickhouse_pod} -c clickhouse-pod -- clickhouse-client --echo -mn "
            f"-q \"SELECT sum(ProfileEvent_DNSError) FROM system.metric_log;SELECT * FROM system.events WHERE event='DNSError' FORMAT Vertical; SELECT * FROM system.errors FORMAT Vertical\"",
            ok_to_fail=False,
        )

    with When("rewrite /etc/resolv.conf in clickhouse-server pod"):
        fired = alerts.wait_alert_state(
            "ClickHouseDNSErrors",
            "firing",
            True,
            labels={"hostname": clickhouse_svc},
            time_range="20s",
            callback=rewrite_dns_on_clickhouse_server,
        )
        assert fired, error("can't get ClickHouseDNSErrors alert in firing state")

    with Then("check ClickHouseDNSErrors gone away"):
        rewrite_dns_on_clickhouse_server(write_new=False)
        resolved = alerts.wait_alert_state("ClickHouseDNSErrors", "firing", False, labels={"hostname": clickhouse_svc})
        assert resolved, error("can't check ClickHouseDNSErrors alert is gone away")


@TestScenario
@Name("test_distributed_files_to_insert. Check ClickHouseDistributedFilesToInsertHigh")
def test_distributed_files_to_insert(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    (
        delayed_pod,
        delayed_svc,
        restarted_pod,
        restarted_svc,
    ) = alerts.random_pod_choice_for_callbacks(chi)
    clickhouse.create_distributed_table_on_cluster(chi)

    insert_sql = "INSERT INTO default.test_distr(event_time, test) SELECT now(), number FROM system.numbers LIMIT 1000"
    clickhouse.query(
        chi["metadata"]["name"],
        "SYSTEM STOP DISTRIBUTED SENDS default.test_distr",
        pod=delayed_pod,
        ns=kubectl.namespace,
    )

    files_to_insert_from_metrics = 0
    files_to_insert_from_disk = 0
    tries = 0
    # we need more than 50 delayed files for catch
    while files_to_insert_from_disk <= 55 and files_to_insert_from_metrics <= 55 and tries < 500:
        kubectl.launch(
            f"exec -n {kubectl.namespace} {restarted_pod} -c clickhouse-pod -- kill 1",
            ok_to_fail=True,
        )
        clickhouse.query(
            chi["metadata"]["name"],
            insert_sql,
            pod=delayed_pod,
            host=delayed_pod,
            ns=kubectl.namespace,
        )
        files_to_insert_from_metrics = clickhouse.query(
            chi["metadata"]["name"],
            "SELECT value FROM system.metrics WHERE metric='DistributedFilesToInsert'",
            pod=delayed_pod,
            ns=kubectl.namespace,
        )
        files_to_insert_from_metrics = int(files_to_insert_from_metrics)

        files_to_insert_from_disk = int(
            kubectl.launch(
                f"exec -n {kubectl.namespace} {delayed_pod} -c clickhouse-pod -- bash -c 'ls -la /var/lib/clickhouse/data/default/test_distr/*/*.bin 2>/dev/null | wc -l'",
                ok_to_fail=False,
            )
        )

    with When("reboot clickhouse-server pod"):
        fired = alerts.wait_alert_state(
            "ClickHouseDistributedFilesToInsertHigh",
            "firing",
            True,
            labels={"hostname": delayed_svc, "chi": chi["metadata"]["name"]},
        )
        assert fired, error("can't get ClickHouseDistributedFilesToInsertHigh alert in firing state")

    kubectl.wait_pod_status(restarted_pod, "Running", ns=kubectl.namespace)

    clickhouse.query(
        chi["metadata"]["name"],
        "SYSTEM START DISTRIBUTED SENDS default.test_distr",
        pod=delayed_pod,
        ns=kubectl.namespace,
    )

    with Then("check ClickHouseDistributedFilesToInsertHigh gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseDistributedFilesToInsertHigh",
            "firing",
            False,
            labels={"hostname": delayed_svc},
        )
        assert resolved, error("can't check ClickHouseDistributedFilesToInsertHigh alert is gone away")

    clickhouse.drop_distributed_table_on_cluster(chi)


@TestScenario
@Name("test_distributed_connection_exceptions. Check ClickHouseDistributedConnectionExceptions")
def test_distributed_connection_exceptions(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    (
        delayed_pod,
        delayed_svc,
        restarted_pod,
        restarted_svc,
    ) = alerts.random_pod_choice_for_callbacks(chi)
    clickhouse.create_distributed_table_on_cluster(chi)

    def reboot_clickhouse_and_distributed_exection():
        # we need 70 delayed files for catch
        insert_sql = (
            "INSERT INTO default.test_distr(event_time, test) SELECT now(), number FROM system.numbers LIMIT 10000"
        )
        select_sql = "SELECT count() FROM default.test_distr"
        with Then("reboot clickhouse-server pod"):
            kubectl.launch(
                f"exec -n {kubectl.namespace} {restarted_pod} -c clickhouse-pod -- kill 1",
                ok_to_fail=True,
            )
            with Then("Insert to distributed table"):
                clickhouse.query(
                    chi["metadata"]["name"],
                    insert_sql,
                    host=delayed_pod,
                    ns=kubectl.namespace,
                )

            with Then("Select from distributed table"):
                clickhouse.query_with_error(
                    chi["metadata"]["name"],
                    select_sql,
                    host=delayed_pod,
                    ns=kubectl.namespace,
                )

    with When("check ClickHouseDistributedConnectionExceptions firing"):
        fired = alerts.wait_alert_state(
            "ClickHouseDistributedConnectionExceptions",
            "firing",
            True,
            labels={"hostname": delayed_svc, "chi": chi["metadata"]["name"]},
            time_range="30s",
            callback=reboot_clickhouse_and_distributed_exection,
        )
        assert fired, error("can't get ClickHouseDistributedConnectionExceptions alert in firing state")

    with Then("check DistributedConnectionExpections gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseDistributedConnectionExceptions",
            "firing",
            False,
            labels={"hostname": delayed_svc},
        )
        assert resolved, error("can't check ClickHouseDistributedConnectionExceptions alert is gone away")
    kubectl.wait_pod_status(restarted_pod, "Running", ns=kubectl.namespace)
    kubectl.wait_jsonpath(
        "pod",
        restarted_pod,
        "{.status.containerStatuses[0].ready}",
        "true",
        ns=kubectl.namespace,
    )
    clickhouse.drop_distributed_table_on_cluster(chi)


@TestScenario
@Name(
    "test_insert_related_alerts. Check ClickHouseRejectedInsert, ClickHouseDelayedInsertThrottling, ClickHouseMaxPartCountForPartition, ClickHouseLowInsertedRowsPerQuery"
)
def test_insert_related_alerts(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    clickhouse.create_table_on_cluster(chi)
    (
        delayed_pod,
        delayed_svc,
        rejected_pod,
        rejected_svc,
    ) = alerts.random_pod_choice_for_callbacks(chi)

    prometheus_scrape_interval = settings.prometheus_scrape_interval
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
            sql = (
                stop_merges
                + min_block
                + f"INSERT INTO default.test(event_time, test) SELECT now(),number FROM system.numbers LIMIT {r};"
            )
            clickhouse.query(chi_name, sql, host=selected_svc, ns=kubectl.namespace)

            sql = (
                min_block
                + "INSERT INTO default.test(event_time, test) SELECT now(), number FROM system.numbers LIMIT 1;"
            )
            clickhouse.query_with_error(chi_name, sql, host=selected_svc, ns=kubectl.namespace)
            with Then(f"wait prometheus_scrape_interval={prometheus_scrape_interval}*2 sec"):
                time.sleep(prometheus_scrape_interval * 2)

            with Then("after 21.8 InsertedRows include system.* rows"):
                for i in range(35):
                    sql = (
                        min_block
                        + "INSERT INTO default.test(event_time, test) SELECT now(), number FROM system.numbers LIMIT 1;"
                    )
                    clickhouse.query_with_error(chi_name, sql, host=selected_svc, ns=kubectl.namespace)

    insert_many_parts_to_clickhouse()
    with Then("check ClickHouseDelayedInsertThrottling firing"):
        fired = alerts.wait_alert_state(
            "ClickHouseDelayedInsertThrottling",
            "firing",
            True,
            labels={"hostname": delayed_svc},
            time_range="60s",
        )
        assert fired, error("can't get ClickHouseDelayedInsertThrottling alert in firing state")
    with Then("check ClickHouseMaxPartCountForPartition firing"):
        fired = alerts.wait_alert_state(
            "ClickHouseMaxPartCountForPartition",
            "firing",
            True,
            labels={"hostname": delayed_svc},
            time_range="90s",
        )
        assert fired, error("can't get ClickHouseMaxPartCountForPartition alert in firing state")
    with Then("check ClickHouseLowInsertedRowsPerQuery firing"):
        fired = alerts.wait_alert_state(
            "ClickHouseLowInsertedRowsPerQuery",
            "firing",
            True,
            labels={"hostname": delayed_svc},
            time_range="120s",
        )
        assert fired, error("can't get ClickHouseLowInsertedRowsPerQuery alert in firing state")

    clickhouse.query(
        chi_name,
        "SYSTEM START MERGES default.test",
        host=selected_svc,
        ns=kubectl.namespace,
    )

    with Then("check ClickHouseDelayedInsertThrottling gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseDelayedInsertThrottling",
            "firing",
            False,
            labels={"hostname": delayed_svc},
        )
        assert resolved, error("can't check ClickHouseDelayedInsertThrottling alert is gone away")
    with Then("check ClickHouseMaxPartCountForPartition gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseMaxPartCountForPartition",
            "firing",
            False,
            labels={"hostname": delayed_svc},
        )
        assert resolved, error("can't check ClickHouseMaxPartCountForPartition alert is gone away")
    with Then("check ClickHouseLowInsertedRowsPerQuery gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseLowInsertedRowsPerQuery",
            "firing",
            False,
            labels={"hostname": delayed_svc},
        )
        assert resolved, error("can't check ClickHouseLowInsertedRowsPerQuery alert is gone away")

    parts_limits = parts_to_throw_insert
    selected_svc = rejected_svc
    insert_many_parts_to_clickhouse()
    with Then("check ClickHouseRejectedInsert firing"):
        fired = alerts.wait_alert_state(
            "ClickHouseRejectedInsert",
            "firing",
            True,
            labels={"hostname": rejected_svc},
            time_range="30s",
            sleep_time=settings.prometheus_scrape_interval,
        )
        assert fired, error("can't get ClickHouseRejectedInsert alert in firing state")

    with Then("check ClickHouseRejectedInsert gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseRejectedInsert",
            "firing",
            False,
            labels={"hostname": rejected_svc},
        )
        assert resolved, error("can't check ClickHouseRejectedInsert alert is gone away")

    clickhouse.query(
        chi_name,
        "SYSTEM START MERGES default.test",
        host=selected_svc,
        ns=kubectl.namespace,
    )
    clickhouse.drop_table_on_cluster(chi)


@TestScenario
@Name("test_longest_running_query. Check ClickHouseLongestRunningQuery")
def test_longest_running_query(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    long_running_pod, long_running_svc, _, _ = alerts.random_pod_choice_for_callbacks(chi)
    # 600s trigger + 2*30s - double prometheus scraping interval
    clickhouse.query(
        chi["metadata"]["name"],
        "SELECT now(),sleepEachRow(1),number FROM system.numbers LIMIT 660",
        host=long_running_svc,
        timeout=670,
    )
    with Then("check ClickHouseLongestRunningQuery firing"):
        fired = alerts.wait_alert_state(
            "ClickHouseLongestRunningQuery",
            "firing",
            True,
            labels={"hostname": long_running_svc},
            time_range="30s",
        )
        assert fired, error("can't get ClickHouseLongestRunningQuery alert in firing state")
    with Then("check ClickHouseLongestRunningQuery gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseLongestRunningQuery",
            "firing",
            False,
            labels={"hostname": long_running_svc},
        )
        assert resolved, error("can't check ClickHouseLongestRunningQuery alert is gone away")


@TestScenario
@Name("test_query_preempted. Check ClickHouseQueryPreempted")
def test_query_preempted(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    priority_pod, priority_svc, _, _ = alerts.random_pod_choice_for_callbacks(chi)

    def run_queries_with_priority():
        sql = ""
        for i in range(50):
            sql += f"SET priority={i % 20};SELECT uniq(number) FROM numbers(20000000):"
        cmd = f"echo \\\"{sql} SELECT 1\\\" | xargs -i'{{}}' --no-run-if-empty -d ':' -P 20 clickhouse-client --time -m -n -q \\\"{{}}\\\""
        kubectl.launch(f'exec {priority_pod} -- bash -c "{cmd}"', timeout=120)
        clickhouse.query(
            chi["metadata"]["name"],
            "SELECT event_time, CurrentMetric_QueryPreempted FROM system.metric_log WHERE CurrentMetric_QueryPreempted > 0",
            host=priority_svc,
        )

    with Then("check ClickHouseQueryPreempted firing"):
        fired = alerts.wait_alert_state(
            "ClickHouseQueryPreempted",
            "firing",
            True,
            labels={"hostname": priority_svc},
            time_range="30s",
            sleep_time=settings.prometheus_scrape_interval,
            callback=run_queries_with_priority,
        )
        assert fired, error("can't get ClickHouseQueryPreempted alert in firing state")
    with Then("check ClickHouseQueryPreempted gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseQueryPreempted",
            "firing",
            False,
            labels={"hostname": priority_svc},
        )
        assert resolved, error("can't check ClickHouseQueryPreempted alert is gone away")


@TestScenario
@Name("test_read_only_replica. Check ClickHouseReadonlyReplica")
def test_read_only_replica(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    (
        read_only_pod,
        read_only_svc,
        other_pod,
        other_svc,
    ) = alerts.random_pod_choice_for_callbacks(chi)
    chi_name = chi["metadata"]["name"]
    clickhouse.create_table_on_cluster(
        chi,
        "all-replicated",
        "default.test_repl",
        "(event_time DateTime, test UInt64) "
        + "ENGINE ReplicatedMergeTree('/clickhouse/tables/{installation}-{shard}/test_repl', '{replica}') ORDER BY tuple()",
    )

    def restart_keeper():
        kubectl.launch(
            f'exec -n {kubectl.namespace} {self.context.keeper_type}-0 -- sh -c "kill 1"',
            ok_to_fail=True,
        )
        clickhouse.query_with_error(
            chi_name,
            "INSERT INTO default.test_repl VALUES(now(),rand())",
            host=read_only_svc,
        )

    with Then("check ClickHouseReadonlyReplica firing"):
        fired = alerts.wait_alert_state(
            "ClickHouseReadonlyReplica",
            "firing",
            True,
            labels={"hostname": read_only_svc},
            time_range="30s",
            sleep_time=settings.prometheus_scrape_interval,
            callback=restart_keeper,
        )
        assert fired, error("can't get ClickHouseReadonlyReplica alert in firing state")
    with Then("check ClickHouseReadonlyReplica gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseReadonlyReplica",
            "firing",
            False,
            labels={"hostname": read_only_svc},
        )
        assert resolved, error("can't check ClickHouseReadonlyReplica alert is gone away")

    kubectl.wait_pod_status("zookeeper-0", "Running", ns=kubectl.namespace)
    kubectl.wait_jsonpath(
        "pod",
        "zookeeper-0",
        "{.status.containerStatuses[0].ready}",
        "true",
        ns=kubectl.namespace,
    )

    for i in range(11):
        keeper_status = kubectl.launch(
            f"exec -n {kubectl.namespace} {self.context.keeper_type}-0 -- bash -c 'exec 3<>/dev/tcp/127.0.0.1/2181 && printf \"ruok\" >&3 && cat <&3'",
            ok_to_fail=True,
        )
        if "imok" in keeper_status:
            break
        elif i == 10:
            fail(f"invalid zookeeper status after {i} retries")
        with Then(f"{self.context.keeper_type} is not ready, wait 2 seconds"):
            time.sleep(2)

    clickhouse.query_with_error(
        chi_name,
        "SYSTEM RESTART REPLICAS; SYSTEM SYNC REPLICA default.test_repl",
        host=read_only_svc,
        timeout=240,
    )
    clickhouse.query_with_error(
        chi_name,
        "SYSTEM RESTART REPLICAS; SYSTEM SYNC REPLICA default.test_repl",
        host=other_svc,
        timeout=240,
    )

    clickhouse.drop_table_on_cluster(chi, "all-replicated", "default.test_repl")


@TestScenario
@Name("test_replicas_max_absolute_delay. Check ClickHouseReplicasMaxAbsoluteDelay")
def test_replicas_max_absolute_delay(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    (
        stop_replica_pod,
        stop_replica_svc,
        insert_pod,
        insert_svc,
    ) = alerts.random_pod_choice_for_callbacks(chi)
    clickhouse.create_table_on_cluster(
        chi,
        "all-replicated",
        "default.test_repl",
        "(event_time DateTime, test UInt64) "
        + "ENGINE ReplicatedMergeTree('/clickhouse/tables/{installation}-{shard}/test_repl', '{replica}') ORDER BY tuple()",
    )
    prometheus_scrape_interval = 15

    def restart_clickhouse_and_insert_to_replicated_table():
        with When(f"stop replica fetches on {stop_replica_svc}"):
            sql = "SYSTEM STOP FETCHES default.test_repl"
            kubectl.launch(
                f'exec -n {kubectl.namespace} {stop_replica_pod} -c clickhouse-pod -- clickhouse-client -q "{sql}"',
                ok_to_fail=True,
                timeout=600,
            )
            sql = "INSERT INTO default.test_repl SELECT now(), number FROM numbers(100000)"
            kubectl.launch(
                f'exec -n {kubectl.namespace} {insert_pod} -c clickhouse-pod -- clickhouse-client -q "{sql}"',
            )

    with Then("check ClickHouseReplicasMaxAbsoluteDelay firing"):
        fired = alerts.wait_alert_state(
            "ClickHouseReplicasMaxAbsoluteDelay",
            "firing",
            True,
            labels={"hostname": stop_replica_svc},
            time_range="60s",
            sleep_time=prometheus_scrape_interval * 2,
            callback=restart_clickhouse_and_insert_to_replicated_table,
        )
        assert fired, error("can't get ClickHouseReadonlyReplica alert in firing state")

    clickhouse.query(
        chi["metadata"]["name"],
        "SYSTEM START FETCHES; SYSTEM RESTART REPLICAS; SYSTEM SYNC REPLICA default.test_repl",
        host=stop_replica_svc,
        timeout=240,
    )
    with Then("check ClickHouseReplicasMaxAbsoluteDelay gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseReplicasMaxAbsoluteDelay",
            "firing",
            False,
            labels={"hostname": stop_replica_svc},
        )
        assert resolved, error("can't check ClickHouseReplicasMaxAbsoluteDelay alert is gone away")

    clickhouse.drop_table_on_cluster(chi, "all-replicated", "default.test_repl")


@TestScenario
@Name("test_too_many_connections. Check ClickHouseTooManyConnections")
def test_too_many_connections(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    (
        too_many_connection_pod,
        too_many_connection_svc,
        _,
        _,
    ) = alerts.random_pod_choice_for_callbacks(chi)
    cmd = "export DEBIAN_FRONTEND=noninteractive; apt-get update; apt-get install -y netcat mysql-client"
    kubectl.launch(
        f'exec -n {kubectl.namespace} {too_many_connection_pod} -c clickhouse-pod -- bash -c  "{cmd}"',
        timeout=120,
    )

    def make_too_many_connection():
        long_cmd = ""
        for _ in range(120):
            port = random.choice(["8123", "3306", "3306", "3306", "9000"])
            if port == "8123":
                # HTTPConnection metric increase after full parsing of HTTP Request, we can't provide pause between CONNECT and QUERY running
                # long_cmd += f"nc -vv 127.0.0.1 {port} <( printf \"POST / HTTP/1.1\\r\\nHost: 127.0.0.1:8123\\r\\nContent-Length: 34\\r\\n\\r\\nTEST\\r\\nTEST\\r\\nTEST\\r\\nTEST\\r\\nTEST\");"
                long_cmd += (
                    'wget -qO- "http://127.0.0.1:8123?query=SELECT sleepEachRow(1),number,now() FROM numbers(30)";'
                )
            elif port == "9000":
                long_cmd += 'clickhouse-client --send_logs_level trace --idle_connection_timeout 70 --receive_timeout 70 -q "SELECT sleepEachRow(1),number,now() FROM numbers(30)";'
            # elif port == "3306":
            #     long_cmd += 'mysql -u default -h 127.0.0.1 -e "SELECT sleepEachRow(1),number, now() FROM numbers(30)";'
            else:
                long_cmd += f'printf "1\\n1" | nc -q 5 -i 30 -vv 127.0.0.1 {port};'

        nc_cmd = f"echo '{long_cmd} whereis nc; exit 0' | xargs --verbose -i'{{}}' --no-run-if-empty -d ';' -P 120 bash -c '{{}}' 1>/dev/null"
        with open("/tmp/nc_cmd.sh", "w") as f:
            f.write(nc_cmd)

        kubectl.launch(f"cp /tmp/nc_cmd.sh {too_many_connection_pod}:/tmp/nc_cmd.sh -c clickhouse-pod")

        kubectl.launch(
            f"exec -n {kubectl.namespace} {too_many_connection_pod} -c clickhouse-pod -- bash /tmp/nc_cmd.sh",
            timeout=600,
        )

    with Then("check ClickHouseTooManyConnections firing"):
        fired = alerts.wait_alert_state(
            "ClickHouseTooManyConnections",
            "firing",
            True,
            labels={"hostname": too_many_connection_svc},
            time_range="90s",
            callback=make_too_many_connection,
        )
        assert fired, error("can't get ClickHouseTooManyConnections alert in firing state")

    with Then("check ClickHouseTooManyConnections gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseTooManyConnections",
            "firing",
            False,
            labels={"hostname": too_many_connection_svc},
        )
        assert resolved, error("can't check ClickHouseTooManyConnections alert is gone away")


@TestScenario
@Name("test_too_much_running_queries. Check ClickHouseTooManyRunningQueries")
def test_too_much_running_queries(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    (
        _,
        _,
        too_many_queries_pod,
        too_many_queries_svc,
    ) = alerts.random_pod_choice_for_callbacks(chi)
    cmd = "export DEBIAN_FRONTEND=noninteractive; apt-get update; apt-get install -y mysql-client"
    kubectl.launch(
        f'exec -n {kubectl.namespace} {too_many_queries_pod} -c clickhouse-pod -- bash -c  "{cmd}"',
        timeout=120,
    )

    def make_too_many_queries():
        long_cmd = ""
        for _ in range(90):
            port = random.choice(["8123", "3306", "9000"])
            if port == "9000":
                long_cmd += (
                    'clickhouse-client --send_logs_level trace -q "SELECT sleepEachRow(1),now() FROM numbers(60)";'
                )
            if port == "3306":
                long_cmd += 'mysql -h 127.0.0.1 -P 3306 -u default -e "SELECT sleepEachRow(1),now() FROM numbers(60)";'
            if port == "8123":
                long_cmd += 'wget -qO- "http://127.0.0.1:8123?query=SELECT sleepEachRow(1),now() FROM numbers(60)";'

        long_cmd = (
            f"echo '{long_cmd}' | xargs --verbose -i'{{}}' --no-run-if-empty -d ';' -P 100 bash -c '{{}}' 1>/dev/null"
        )
        with open("/tmp/long_cmd.sh", "w") as f:
            f.write(long_cmd)

        kubectl.launch(f"cp /tmp/long_cmd.sh {too_many_queries_pod}:/tmp/long_cmd.sh -c clickhouse-pod")
        kubectl.launch(
            f"exec -n {kubectl.namespace} {too_many_queries_pod} -c clickhouse-pod -- bash /tmp/long_cmd.sh",
            timeout=90,
        )

    with Then("check ClickHouseTooManyRunningQueries firing"):
        fired = alerts.wait_alert_state(
            "ClickHouseTooManyRunningQueries",
            "firing",
            True,
            labels={"hostname": too_many_queries_svc},
            callback=make_too_many_queries,
            time_range="30s",
        )
        assert fired, error("can't get ClickHouseTooManyRunningQueries alert in firing state")

    with Then("check ClickHouseTooManyConnections gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseTooManyRunningQueries",
            "firing",
            False,
            labels={"hostname": too_many_queries_svc},
            sleep_time=settings.prometheus_scrape_interval,
        )
        assert resolved, error("can't check ClickHouseTooManyConnections alert is gone away")


@TestScenario
@Name("test_system_settings_changed. Check ClickHouseSystemSettingsChanged")
def test_system_settings_changed(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    changed_pod, changed_svc, _, _ = alerts.random_pod_choice_for_callbacks(chi)

    with When("apply changed settings"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-cluster-for-alerts-changed-settings.yaml",
            check={
                "apply_templates": [
                    "manifests/chit/tpl-clickhouse-stable.yaml",
                    "manifests/chit/tpl-persistent-volume-100Mi.yaml",
                ],
                "object_counts": {
                    "statefulset": 2,
                    "pod": 2,
                    "service": 3,
                },
                "do_not_delete": 1,
            },
        )

    with Then("check ClickHouseSystemSettingsChanged firing"):
        fired = alerts.wait_alert_state(
            "ClickHouseSystemSettingsChanged",
            "firing",
            True,
            labels={"hostname": changed_svc},
            time_range="30s",
        )
        assert fired, error("can't get ClickHouseSystemSettingsChanged alert in firing state")

    with When("rollback changed settings"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-cluster-for-alerts.yaml",
            check={
                "apply_templates": [
                    "manifests/chit/tpl-clickhouse-latest.yaml",
                    "manifests/chit/tpl-clickhouse-alerts.yaml",
                    "manifests/chit/tpl-persistent-volume-100Mi.yaml",
                ],
                "object_counts": {
                    "statefulset": 2,
                    "pod": 2,
                    "service": 3,
                },
                "do_not_delete": 1,
            },
        )

    with Then("check ClickHouseSystemSettingsChanged gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseSystemSettingsChanged",
            "firing",
            False,
            labels={"hostname": changed_svc},
            sleep_time=30,
        )
        assert resolved, error("can't check ClickHouseSystemSettingsChanged alert is gone away")


@TestScenario
@Name("test_version_changed. Check ClickHouseVersionChanged")
def test_version_changed(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    changed_pod, changed_svc, _, _ = alerts.random_pod_choice_for_callbacks(chi)

    with When("apply changed settings"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-cluster-for-alerts-changed-settings.yaml",
            check={
                "apply_templates": [
                    "manifests/chit/tpl-clickhouse-stable.yaml",
                    "manifests/chit/tpl-persistent-volume-100Mi.yaml",
                ],
                "object_counts": {
                    "statefulset": 2,
                    "pod": 2,
                    "service": 3,
                },
                "do_not_delete": 1,
            },
        )
        prometheus_scrape_interval = 15
        with Then(f"wait prometheus_scrape_interval={prometheus_scrape_interval}*2 sec"):
            time.sleep(prometheus_scrape_interval * 2)

    with Then("check ClickHouseVersionChanged firing"):
        fired = alerts.wait_alert_state(
            "ClickHouseVersionChanged",
            "firing",
            True,
            labels={"hostname": changed_svc},
            time_range="30s",
            sleep_time=settings.prometheus_scrape_interval,
        )
        assert fired, error("can't get ClickHouseVersionChanged alert in firing state")

    with When("rollback changed settings"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-cluster-for-alerts.yaml",
            check={
                "apply_templates": [
                    "manifests/chit/tpl-clickhouse-latest.yaml",
                    "manifests/chit/tpl-clickhouse-alerts.yaml",
                    "manifests/chit/tpl-persistent-volume-100Mi.yaml",
                ],
                "object_counts": {
                    "statefulset": 2,
                    "pod": 2,
                    "service": 3,
                },
                "do_not_delete": 1,
            },
        )

    with Then("check ClickHouseVersionChanged gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseVersionChanged",
            "firing",
            False,
            labels={"hostname": changed_svc},
            sleep_time=30,
        )
        assert resolved, error("can't check ClickHouseVersionChanged alert is gone away")


@TestScenario
@Name("test_zookeeper_hardware_exceptions. Check ClickHouseZooKeeperHardwareExceptions")
def test_zookeeper_hardware_exceptions(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    pod1, svc1, pod2, svc2 = alerts.random_pod_choice_for_callbacks(chi)
    chi_name = chi["metadata"]["name"]

    def restart_keeper():
        kubectl.launch(
            f'exec -n {kubectl.namespace} {self.context.keeper_type}-0 -- sh -c "kill 1"',
            ok_to_fail=True,
        )
        clickhouse.query_with_error(
            chi_name,
            "SELECT name, path FROM system.zookeeper WHERE path='/'",
            host=svc1,
        )
        clickhouse.query_with_error(
            chi_name,
            "SELECT name, path FROM system.zookeeper WHERE path='/'",
            host=svc2,
        )

    with Then("check ClickHouseZooKeeperHardwareExceptions firing"):
        for svc in (svc1, svc2):
            fired = alerts.wait_alert_state(
                "ClickHouseZooKeeperHardwareExceptions",
                "firing",
                True,
                labels={"hostname": svc},
                time_range="40s",
                sleep_time=settings.prometheus_scrape_interval,
                callback=restart_keeper,
            )
            assert fired, error("can't get ClickHouseZooKeeperHardwareExceptions alert in firing state")

    kubectl.wait_pod_status("zookeeper-0", "Running", ns=kubectl.namespace)
    kubectl.wait_jsonpath(
        "pod",
        "zookeeper-0",
        "{.status.containerStatuses[0].ready}",
        "true",
        ns=kubectl.namespace,
    )

    with Then("check ClickHouseZooKeeperHardwareExceptions gone away"):
        for svc in (svc1, svc2):
            resolved = alerts.wait_alert_state(
                "ClickHouseZooKeeperHardwareExceptions",
                "firing",
                False,
                labels={"hostname": svc},
            )
            assert resolved, error("can't check ClickHouseZooKeeperHardwareExceptions alert is gone away")


@TestScenario
@Name("test_distributed_sync_insertion_timeout. Check ClickHouseDistributedSyncInsertionTimeoutExceeded")
def test_distributed_sync_insertion_timeout(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    (
        sync_pod,
        sync_svc,
        restarted_pod,
        restarted_svc,
    ) = alerts.random_pod_choice_for_callbacks(chi)
    clickhouse.create_distributed_table_on_cluster(chi, local_engine="ENGINE Null()")

    def insert_distributed_sync():
        with When("Insert to distributed table SYNC"):
            # look to https://github.com/ClickHouse/ClickHouse/pull/14260#issuecomment-683616862
            # insert_sql = 'INSERT INTO default.test_distr SELECT now(), number FROM numbers(toUInt64(5e9))'
            insert_sql = "INSERT INTO FUNCTION remote('127.1', currentDatabase(), test_distr) SELECT now(), number FROM numbers(toUInt64(5e9))"
            insert_params = "--insert_distributed_timeout=1 --insert_distributed_sync=1"
            error = clickhouse.query_with_error(
                chi["metadata"]["name"],
                insert_sql,
                pod=sync_pod,
                host=sync_pod,
                ns=kubectl.namespace,
                advanced_params=insert_params,
            )
            assert "Code: 159" in error

    with When("check ClickHouseDistributedSyncInsertionTimeoutExceeded firing"):
        fired = alerts.wait_alert_state(
            "ClickHouseDistributedSyncInsertionTimeoutExceeded",
            "firing",
            True,
            labels={"hostname": sync_svc, "chi": chi["metadata"]["name"]},
            time_range="30s",
            callback=insert_distributed_sync,
        )
        assert fired, error("can't get ClickHouseDistributedSyncInsertionTimeoutExceeded alert in firing state")

    with Then("check ClickHouseDistributedSyncInsertionTimeoutExceeded gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseDistributedSyncInsertionTimeoutExceeded",
            "firing",
            False,
            labels={"hostname": sync_svc},
        )
        assert resolved, error("can't check ClickHouseDistributedSyncInsertionTimeoutExceeded alert is gone away")

    clickhouse.drop_distributed_table_on_cluster(chi)


@TestScenario
@Name("test_detached_parts. Check ClickHouseDetachedParts")
def test_detached_parts(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    clickhouse.create_table_on_cluster(chi)
    detached_pod, detached_svc, _, _ = alerts.random_pod_choice_for_callbacks(chi)

    def create_part_and_detach():
        clickhouse.query(
            chi["metadata"]["name"],
            "INSERT INTO default.test SELECT now(), number FROM numbers(100)",
            pod=detached_pod,
        )
        part_name = clickhouse.query(
            chi["metadata"]["name"],
            sql="SELECT name FROM system.parts WHERE database='default' AND table='test' ORDER BY modification_time DESC LIMIT 1",
            pod=detached_pod,
        )
        clickhouse.query(
            chi["metadata"]["name"],
            f"ALTER TABLE default.test DETACH PART '{part_name}'",
            pod=detached_pod,
        )

    def attach_all_parts():
        detached_parts = clickhouse.query(
            chi["metadata"]["name"],
            "SELECT name FROM system.detached_parts WHERE database='default' AND table='test' AND reason=''",
            pod=detached_pod,
        )
        all_parts = ""
        for part in detached_parts.splitlines():
            all_parts += f"ALTER TABLE default.test ATTACH PART '{part}';"
        if all_parts.strip() != "":
            clickhouse.query(chi["metadata"]["name"], all_parts, pod=detached_pod)

    with When("check ClickHouseDetachedParts firing"):
        fired = alerts.wait_alert_state(
            "ClickHouseDetachedParts",
            "firing",
            True,
            labels={"hostname": detached_svc, "chi": chi["metadata"]["name"]},
            time_range="30s",
            callback=create_part_and_detach,
        )
        assert fired, error("can't get ClickHouseDetachedParts alert in firing state")

    with Then("check ClickHouseDetachedParts gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseDetachedParts",
            "firing",
            False,
            labels={"hostname": detached_svc},
            callback=attach_all_parts,
        )
        assert resolved, error("can't check ClickHouseDetachedParts alert is gone away")

    clickhouse.drop_table_on_cluster(chi)


@TestScenario
@Name("test_clickhouse_keeper_alerts. Check ClickHouseKeeperDown")
def test_clickhouse_keeper_alerts(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    test_keeper_alerts_outline(keeper_type="clickhouse-keeper")


@TestScenario
@Name("test_zookeeper_alerts. Check ZookeeperDown, ZookeeperRestartRecently")
def test_zookeeper_alerts(self, prometheus_operator_spec, clickhouse_operator_spec, chi):
    test_keeper_alerts_outline(keeper_type="zookeeper")


@TestOutline
def test_keeper_alerts_outline(self, keeper_type):
    keeper_spec = kubectl.get("endpoints", keeper_type)
    keeper_spec = random.choice(keeper_spec["subsets"][0]["addresses"])["targetRef"]["name"]

    expected_alerts = {
        "zookeeper": {
            "down": "ZookeeperDown",
            "restartRecently": "ZookeeperRestartRecently",
        },
        "clickhouse-keeper": {
            "down": "ClickHouseKeeperDown",
        },
    }

    def restart_keeper():
        kubectl.launch(
            f'exec -n {kubectl.namespace} {keeper_spec} -- sh -c "kill 1"',
            ok_to_fail=True,
        )

    def wait_when_keeper_up():
        kubectl.wait_pod_status(keeper_spec, "Running", ns=kubectl.namespace)
        kubectl.wait_jsonpath(
            "pod",
            keeper_spec,
            "{.status.containerStatuses[0].ready}",
            "true",
            ns=kubectl.namespace,
        )

    if "down" in expected_alerts[keeper_type]:
        with Then(f"check {expected_alerts[keeper_type]['down']} firing"):
            fired = alerts.wait_alert_state(
                expected_alerts[keeper_type]["down"],
                "firing",
                True,
                labels={"pod_name": keeper_spec},
                time_range="1m",
                sleep_time=settings.prometheus_scrape_interval,
                callback=restart_keeper,
            )
            assert fired, error(f"can't get {expected_alerts[keeper_type]['down']} alert in firing state")

    wait_when_keeper_up()

    if "down" in expected_alerts[keeper_type]:
        with Then(f"check {expected_alerts[keeper_type]['down']} gone away"):
            resolved = alerts.wait_alert_state(
                expected_alerts[keeper_type]["down"],
                "firing",
                False,
                labels={"pod_name": keeper_spec},
            )
            assert resolved, error(f"can't check {expected_alerts[keeper_type]['down']} alert is gone away")

    restart_keeper()

    if "restart" in expected_alerts[keeper_type]:
        with Then(f"check {expected_alerts[keeper_type]['restart']} firing"):
            fired = alerts.wait_alert_state(
                expected_alerts[keeper_type]["restart"],
                "firing",
                True,
                labels={"pod_name": keeper_spec},
                time_range="30s",
            )
            assert fired, error(f"can't get {expected_alerts[keeper_type]['restart']} alert in firing state")

    wait_when_keeper_up()

    if "restart" in expected_alerts[keeper_type]:
        with Then(f"check {expected_alerts[keeper_type]['restart']} gone away"):
            resolved = alerts.wait_alert_state(
                "expected_alerts[keeper_type]['restart']",
                "firing",
                False,
                labels={"pod_name": keeper_spec},
                max_try=30,
            )
            assert resolved, error(f"can't check {expected_alerts[keeper_type]['restart']} alert is gone away")


@TestFeature
@Name("e2e.test_metrics_alerts")
def test(self):
    util.clean_namespace(delete_chi=True)
    util.install_operator_if_not_exist()

    (prometheus_operator_spec, prometheus_spec, alertmanager_spec, clickhouse_operator_spec, chi,) = alerts.initialize(
        chi_file="manifests/chi/test-cluster-for-alerts.yaml",
        chi_template_file="manifests/chit/tpl-clickhouse-alerts.yaml",
        chi_name="test-cluster-for-alerts",
        keeper_type="clickhouse-keeper",
    )
    Scenario(test=test_clickhouse_keeper_alerts)(
        prometheus_operator_spec=prometheus_operator_spec,
        clickhouse_operator_spec=clickhouse_operator_spec,
        chi=chi,
    )

    (prometheus_operator_spec, prometheus_spec, alertmanager_spec, clickhouse_operator_spec, chi,) = alerts.initialize(
        chi_file="manifests/chi/test-cluster-for-alerts.yaml",
        chi_template_file="manifests/chit/tpl-clickhouse-alerts.yaml",
        chi_name="test-cluster-for-alerts",
        keeper_type="zookeeper",
    )
    Scenario(test=test_zookeeper_alerts)(
        prometheus_operator_spec=prometheus_operator_spec,
        clickhouse_operator_spec=clickhouse_operator_spec,
        chi=chi,
    )

    (prometheus_operator_spec, prometheus_spec, alertmanager_spec, clickhouse_operator_spec, chi,) = alerts.initialize(
        chi_file="manifests/chi/test-cluster-for-alerts.yaml",
        chi_template_file="manifests/chit/tpl-clickhouse-alerts.yaml",
        chi_name="test-cluster-for-alerts",
        keeper_type=self.context.keeper_type,
    )

    test_cases = [
        test_prometheus_setup,
        test_read_only_replica,
        test_replicas_max_absolute_delay,
        test_metrics_exporter_down,
        test_clickhouse_dns_errors,
        test_distributed_connection_exceptions,
        test_insert_related_alerts,
        test_too_many_connections,
        test_too_much_running_queries,
        test_longest_running_query,
        test_system_settings_changed,
        test_version_changed,
        test_zookeeper_hardware_exceptions,
        test_distributed_sync_insertion_timeout,
        test_distributed_files_to_insert,
        test_detached_parts,
        test_clickhouse_server_reboot,
    ]
    for t in test_cases:
        Scenario(test=t)(
            prometheus_operator_spec=prometheus_operator_spec,
            clickhouse_operator_spec=clickhouse_operator_spec,
            chi=chi,
        )
