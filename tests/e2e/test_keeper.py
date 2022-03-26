import time

import e2e.clickhouse as clickhouse
import e2e.kubectl as kubectl
import e2e.settings as settings
import e2e.util as util

from testflows.core import *


def wait_keeper_ready(svc_name='zookeeper', keeper_type='zookeeper', pod_count=3, retries=10):
    expected_containers = "2/2" if keeper_type == "clickhouse-keeper" else "1/1"
    for i in range(retries):
        ready_pods = kubectl.launch(f"get pods | grep {keeper_type} | grep Running | grep '{expected_containers}' | wc -l")
        ready_endpoints = "0"
        if ready_pods == str(pod_count):
            ready_endpoints = kubectl.launch(f"get endpoints {svc_name} -o json | jq '.subsets[].addresses[].ip' | wc -l")
            if ready_endpoints == str(pod_count):
                break
        else:
            with Then(
                    f"Zookeeper Not ready yet ready_endpoints={ready_endpoints} ready_pods={ready_pods}, expected pod_count={pod_count}. "
                    f"Wait for {i * 3} seconds"
            ):
                time.sleep(i * 3)
        if i == retries - 1:
            Fail(f"Zookeeper failed, ready_endpoints={ready_endpoints} ready_pods={ready_pods}, expected pod_count={pod_count}")


def wait_clickhouse_no_readonly_replicas(chi, retries=10):
    expected_replicas = chi["spec"]["configuration"]["clusters"][0]["layout"]["replicasCount"]
    expected_replicas = "[" + ",".join(["0"] * expected_replicas) + "]"
    for i in range(retries):
        readonly_replicas = clickhouse.query(
            chi['metadata']['name'],
            "SELECT groupArray(value) FROM cluster('all-sharded',system.metrics) WHERE metric='ReadonlyReplica'"
        )
        if readonly_replicas == expected_replicas:
            break
        else:
            with Then(f"Check ReadonlyReplica actual={readonly_replicas}, expected={expected_replicas}, Wait for {i * 3} seconds"):
                time.sleep(i * 3)
        if i == retries - 1:
            Fail(f"Check ReadonlyReplica failed, actual={readonly_replicas}, expected={expected_replicas}")


@TestOutline
def test_keeper_outline(self,
                        keeper_type="zookeeper",
                        pod_for_insert_data="chi-test-cluster-for-zk-default-0-1-0",
                        keeper_manifest_1_node='zookeeper-1-node-1GB-for-tests-only.yaml',
                        keeper_manifest_3_node='zookeeper-3-nodes-1GB-for-tests-only.yaml',
                        ):
    """
    test scenario for Zoo/Clickhouse Keeper

    CH 1 -> 2 wait complete + Keeper 1 -> 3 nowait
    CH 2 -> 1 wait complete + Keeper 3 -> 1 nowait
    CH 1 -> 2 wait complete + Keeper 1 -> 3 nowait
    """

    def insert_replicated_data(chi, create_tables, insert_tables):
        with When(f'create if not exists replicated tables {create_tables}'):
            for table in create_tables:
                clickhouse.create_table_on_cluster(
                    chi, 'all-sharded', f'default.{table}',
                    f'(id UInt64) ENGINE=ReplicatedMergeTree(\'/clickhouse/tables/default.{table}/{{shard}}\',\'{{replica}}\') ORDER BY (id)',
                    if_not_exists=True,
                )
        with When(f'insert tables data {insert_tables}'):
            for table in insert_tables:
                clickhouse.query(
                    chi['metadata']['name'], f'INSERT INTO default.{table} SELECT rand()+number FROM numbers(1000)',
                    pod=pod_for_insert_data
                )

    def check_zk_root_znode(chi, pod_count, retry_count=5):
        for pod_num in range(pod_count):
            out = ""
            expected_out = ""
            for i in range(retry_count):
                if keeper_type == "zookeeper":
                    expected_out = "[clickhouse, zookeeper]"
                    keeper_cmd = './bin/zkCli.sh ls /'
                else:
                    expected_out = "clickhouse"
                    keeper_cmd = "if [[ ! $(command -v zookeepercli) ]]; then "
                    keeper_cmd += "wget -q -O /tmp/zookeepercli.deb https://github.com/outbrain/zookeepercli/releases/download/v1.0.12/zookeepercli_1.0.12_amd64.deb; "
                    keeper_cmd += "dpkg -i /tmp/zookeepercli.deb; "
                    keeper_cmd += "fi; "
                    keeper_cmd += "zookeepercli -servers 127.0.0.1:2181 -c ls /"

                out = kubectl.launch(f"exec {keeper_type}-{pod_num} -- bash -ce '{keeper_cmd}'", ns=settings.test_namespace, ok_to_fail=True)
                if expected_out in out:
                    break
                else:
                    with Then(f"{keeper_type} ROOT NODE not ready, wait {(i + 1) * 3} sec"):
                        time.sleep((i + 1) * 3)
            assert expected_out in out, f"Unexpected {keeper_type} `ls /` output"

        out = clickhouse.query(chi["metadata"]["name"], "SELECT count() FROM system.zookeeper WHERE path='/'")
        expected_out = "1" if keeper_type == "clickhouse-keeper" else "2"
        assert expected_out == out.strip(" \t\r\n"), f"Unexpected `SELECT count() FROM system.zookeeper WHERE path='/'` output {out}"

    def rescale_zk_and_clickhouse(ch_node_count, keeper_node_count, first_install=False):
        keeper_manifest = keeper_manifest_1_node if keeper_node_count == 1 else keeper_manifest_3_node
        _, chi = util.install_clickhouse_and_keeper(
            chi_file=f'manifests/chi/test-cluster-for-{keeper_type}-{ch_node_count}.yaml',
            chi_template_file='manifests/chit/tpl-clickhouse-latest.yaml',
            chi_name='test-cluster-for-zk',
            keeper_manifest=keeper_manifest,
            keeper_type=keeper_type,
            clean_ns=first_install,
            force_keeper_install=True,
            keeper_install_first=first_install,
            make_object_count=False,
        )
        return chi

    with When("Clean exists ClickHouse and Keeper"):
        kubectl.delete_all_keeper(self.context.keeper_type, settings.test_namespace)
        kubectl.delete_all_chi(settings.test_namespace)

    with When("Install CH 1 node ZK 1 node"):
        chi = rescale_zk_and_clickhouse(ch_node_count=1, keeper_node_count=1, first_install=True)
        util.wait_clickhouse_cluster_ready(chi)
        wait_keeper_ready(keeper_type=keeper_type, pod_count=1)
        check_zk_root_znode(chi, pod_count=1)

        util.wait_clickhouse_cluster_ready(chi)
        wait_clickhouse_no_readonly_replicas(chi)
        insert_replicated_data(chi, create_tables=['test_repl1'], insert_tables=['test_repl1'])

    total_iterations = 3
    for iteration in range(total_iterations):
        with When(f"ITERATION {iteration}"):
            with Then("CH 1 -> 2 wait complete + ZK 1 -> 3 nowait"):
                chi = rescale_zk_and_clickhouse(ch_node_count=2, keeper_node_count=3)
                wait_keeper_ready(keeper_type=keeper_type, pod_count=3)
                check_zk_root_znode(chi, pod_count=3)

                util.wait_clickhouse_cluster_ready(chi)
                wait_clickhouse_no_readonly_replicas(chi)
                insert_replicated_data(chi, create_tables=['test_repl2'], insert_tables=['test_repl1', 'test_repl2'])

            with Then("CH 2 -> 1 wait complete + ZK 3 -> 1 nowait"):
                chi = rescale_zk_and_clickhouse(ch_node_count=1, keeper_node_count=1)
                wait_keeper_ready(keeper_type=keeper_type, pod_count=1)
                check_zk_root_znode(chi, pod_count=1)

                util.wait_clickhouse_cluster_ready(chi)
                wait_clickhouse_no_readonly_replicas(chi)
                insert_replicated_data(chi, create_tables=['test_repl3'], insert_tables=['test_repl1', 'test_repl2', 'test_repl3'])

    with When("CH 1 -> 2 wait complete + ZK 1 -> 3 nowait"):
        chi = rescale_zk_and_clickhouse(ch_node_count=2, keeper_node_count=3)
        check_zk_root_znode(chi, pod_count=3)

    with Then('check data in tables'):
        for table, exptected_rows in {"test_repl1": str(1000 + 2000 * total_iterations), "test_repl2": str(2000 * total_iterations), "test_repl3": str(1000 * total_iterations)}.items():
            actual_rows = clickhouse.query(
                chi['metadata']['name'], f'SELECT count() FROM default.{table}', pod="chi-test-cluster-for-zk-default-0-1-0"
            )
            assert actual_rows == exptected_rows, f"Invalid rows counter after inserts {table} expected={exptected_rows} actual={actual_rows}"

    with Then('drop all created tables'):
        for i in range(3):
            clickhouse.drop_table_on_cluster(chi, 'all-sharded', f'default.test_repl{i + 1}')


@TestScenario
@Name("test_zookeeper_rescale. Check ZK scale-up / scale-down cases")
def test_zookeeper_rescale(self):
    test_keeper_outline(
        keeper_type="zookeeper",
        pod_for_insert_data="chi-test-cluster-for-zk-default-0-1-0",
        keeper_manifest_1_node='zookeeper-1-node-1GB-for-tests-only.yaml',
        keeper_manifest_3_node='zookeeper-3-nodes-1GB-for-tests-only.yaml',
    )


@TestScenario
@Name("test_clickhouse_keeper_rescale. Check KEEPER scale-up / scale-down cases")
def test_clickhouse_keeper_rescale(self):
    test_keeper_outline(
        keeper_type="clickhouse-keeper",
        pod_for_insert_data="chi-test-cluster-for-zk-default-0-1-0",
        keeper_manifest_1_node='clickhouse-keeper-1-node-256M-for-test-only.yaml',
        keeper_manifest_3_node='clickhouse-keeper-3-nodes-256M-for-test-only.yaml',
    )


@TestModule
@Name("e2e.test_keeper")
def test(self):
    all_tests = [
        test_clickhouse_keeper_rescale,
        test_zookeeper_rescale,
    ]

    util.install_operator_if_not_exist()
    for t in all_tests:
        Scenario(test=t)()
