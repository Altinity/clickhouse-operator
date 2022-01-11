import time

import e2e.clickhouse as clickhouse
import e2e.kubectl as kubectl
import e2e.settings as settings
import e2e.util as util

from testflows.core import *


def wait_zookeeper_ready(svc_name='zookeeper', pod_count=3, retries=10):
    for i in range(retries):
        ready_pods = kubectl.launch(f"get pods | grep {svc_name} | grep Running | grep '1/1' | wc -l")
        ready_endpoints = "0"
        if ready_pods == str(pod_count):
            ready_endpoints = kubectl.launch(f"get endpoints {svc_name} -o json | jq '.subsets[].addresses[].ip' | wc -l")
            if ready_endpoints == str(pod_count):
                break
        else:
            with Then(
                f"Zookeeper Not ready yet ready_endpoints={ready_endpoints} ready_pods={ready_pods}, expected pod_count={pod_count}. "
                f"Wait for {i*3} seconds"
            ):
                time.sleep(i*3)
        if i == retries - 1:
            Fail(f"Zookeeper failed, ready_endpoints={ready_endpoints} ready_pods={ready_pods}, expected pod_count={pod_count}")


def wait_clickhouse_no_readonly_replicas(chi, retries=10):
    expected_replicas = chi["spec"]["configuration"]["clusters"][0]["layout"]["replicasCount"]
    expected_replicas = "[" + ",".join(["0"] * expected_replicas) + "]"
    for i in range(retries):
        readonly_replicas=clickhouse.query(
            chi['metadata']['name'],
            "SELECT groupArray(value) FROM cluster('all-sharded',system.metrics) WHERE metric='ReadonlyReplica'"
        )
        if readonly_replicas == expected_replicas:
            break
        else:
            with Then(f"Clickhouse have readonly_replicas={readonly_replicas}, expected={expected_replicas}, Wait for {i*3} seconds"):
                time.sleep(i*3)
        if i == retries - 1:
            Fail(f"ClickHouse ZK failed, readonly_replicas={readonly_replicas}, expected={expected_replicas}")


@TestScenario
@Name("test_zookeeper_rescale. Check ZK scale-up / scale-down cases")
def test_zookeeper_rescale(self):
    """
    test scenario for ZK

    CH 1 -> 2 wait complete + ZK 1 -> 3 nowait
    CH 2 -> 1 wait complete + ZK 3 -> 1 nowait
    CH 1 -> 2 wait complete + ZK 1 -> 3 nowait
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
                    pod="chi-test-cluster-for-zk-default-0-1-0"
                )

    def check_zk_root_znode(chi, pod_count, zk_retry=5):
        for pod_num in range(pod_count):
            out = ""
            for i in range(zk_retry):
                out = kubectl.launch(f"exec zookeeper-{pod_num} -- bash -ce './bin/zkCli.sh ls /'", ns=settings.test_namespace, ok_to_fail=True)
                if "[clickhouse, zookeeper]" in out:
                    break
                else:
                    with Then(f"Zookeeper ROOT NODE not ready, wait { (i+1)*3} sec"):
                        time.sleep((i+1)*3)
            assert "[clickhouse, zookeeper]" in out, "Unexpected `zkCli.sh ls /` output"

        out = clickhouse.query(chi["metadata"]["name"], "SELECT count() FROM system.zookeeper WHERE path='/'")
        assert "2" == out.strip(" \t\r\n"), f"Unexpected `SELECT count() FROM system.zookeeper WHERE path='/'` output {out}"

    def rescale_zk_and_clickhouse(ch_node_count, zk_node_count, first_install=False):
        zk_manifest = 'zookeeper-1-node-1GB-for-tests-only.yaml' if zk_node_count == 1 else 'zookeeper-3-nodes-1GB-for-tests-only.yaml'
        _, chi = util.install_clickhouse_and_zookeeper(
            chi_file=f'manifests/chi/test-cluster-for-zookeeper-{ch_node_count}.yaml',
            chi_template_file='manifests/chit/tpl-clickhouse-latest.yaml',
            chi_name='test-cluster-for-zk',
            zk_manifest=zk_manifest,
            clean_ns=first_install,
            force_zk_install=True,
            zk_install_first=first_install,
            make_object_count=False,
        )
        return chi

    with When("Clean exists ClickHouse and Zookeeper"):
        kubectl.delete_all_zookeeper(settings.test_namespace)
        kubectl.delete_all_chi(settings.test_namespace)

    with When("Install CH 1 node ZK 1 node"):
        chi = rescale_zk_and_clickhouse(ch_node_count=1, zk_node_count=1, first_install=True)
        util.wait_clickhouse_cluster_ready(chi)
        wait_zookeeper_ready(pod_count=1)
        check_zk_root_znode(chi, pod_count=1)

        util.wait_clickhouse_cluster_ready(chi)
        wait_clickhouse_no_readonly_replicas(chi)
        insert_replicated_data(chi, create_tables=['test_repl1'], insert_tables=['test_repl1'])

    total_iterations = 5
    for iteration in range(total_iterations):
        with When(f"ITERATION {iteration}"):
            with Then("CH 1 -> 2 wait complete + ZK 1 -> 3 nowait"):
                chi = rescale_zk_and_clickhouse(ch_node_count=2, zk_node_count=3)
                wait_zookeeper_ready(pod_count=3)
                check_zk_root_znode(chi, pod_count=3)

                util.wait_clickhouse_cluster_ready(chi)
                insert_replicated_data(chi, create_tables=['test_repl2'], insert_tables=['test_repl1', 'test_repl2'])

            with Then("CH 2 -> 1 wait complete + ZK 3 -> 1 nowait"):
                chi = rescale_zk_and_clickhouse(ch_node_count=1, zk_node_count=1)
                wait_zookeeper_ready(pod_count=1)
                check_zk_root_znode(chi, pod_count=1)

                util.wait_clickhouse_cluster_ready(chi)
                insert_replicated_data(chi, create_tables=['test_repl3'], insert_tables=['test_repl1', 'test_repl2', 'test_repl3'])

    with When("CH 1 -> 2 wait complete + ZK 1 -> 3 nowait"):
        chi = rescale_zk_and_clickhouse(ch_node_count=2, zk_node_count=3)
        check_zk_root_znode(chi, pod_count=3)

    with Then('check data in tables'):
        for table, exptected_rows in {"test_repl1": str(1000 + 2000 * total_iterations) , "test_repl2": str(2000*total_iterations), "test_repl3": str(1000*total_iterations)}.items():
            actual_rows = clickhouse.query(
                chi['metadata']['name'], f'SELECT count() FROM default.{table}', pod="chi-test-cluster-for-zk-default-0-1-0"
            )
            assert actual_rows == exptected_rows, f"Invalid rows counter after inserts {table} expected={exptected_rows} actual={actual_rows}"

    with Then('drop all created tables'):
        for i in range(3):
            clickhouse.drop_table_on_cluster(chi, 'all-sharded', f'default.test_repl{i+1}')


@TestModule
@Name("e2e.test_zookeeper")
def test(self):
    all_tests = [
        test_zookeeper_rescale
    ]

    for t in all_tests:
        Scenario(test=t)()
