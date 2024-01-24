import time

import e2e.clickhouse as clickhouse
import e2e.kubectl as kubectl
import e2e.settings as settings
import e2e.util as util
import e2e.yaml_manifest as yaml_manifest
from testflows.core import *
from e2e.steps import *

from requirements.requirements import *

def wait_keeper_ready(keeper_type="zookeeper", pod_count=3, retries_number=10):
    svc_name = "zookeeper-client" if keeper_type == "zookeeper-operator" else "zookeeper"
    expected_containers = "1/1"
    expected_pod_prefix = "clickhouse-keeper" if "clickhouse-keeper" in keeper_type else "zookeeper"
    for i in range(retries_number):
        ready_pods = kubectl.launch(
            f"get pods | grep {expected_pod_prefix} | grep Running | grep '{expected_containers}' | wc -l", ok_to_fail=True
        )
        ready_endpoints = "0"
        if ready_pods == str(pod_count):
            ready_endpoints = kubectl.launch(
                f"get endpoints {svc_name} -o json | jq '.subsets[].addresses[].ip' | wc -l", ok_to_fail=True
            )
            if ready_endpoints == str(pod_count):
                break
        else:
            with Then(
                f"Zookeeper Not ready yet "
                f"ready_endpoints={ready_endpoints} ready_pods={ready_pods}, expected pod_count={pod_count}. "
                f"Wait for {i * 3} seconds"
            ):
                time.sleep(i * 3)
        if i == retries_number - 1:
            Fail(
                f"Zookeeper failed, "
                f"ready_endpoints={ready_endpoints} ready_pods={ready_pods}, expected pod_count={pod_count}"
            )


def insert_replicated_data(chi, pod_for_insert_data, create_tables, insert_tables):
    with When(f"create if not exists replicated tables {create_tables}"):
        for table in create_tables:
            clickhouse.create_table_on_cluster(
                chi,
                "all-sharded",
                f"default.{table}",
                f"(id UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/default.{table}/{{shard}}','{{replica}}') ORDER BY (id)",
                if_not_exists=True,
            )
    with When(f"insert tables data {insert_tables}"):
        for table in insert_tables:
            clickhouse.query(
                chi["metadata"]["name"],
                f"INSERT INTO default.{table} SELECT rand()+number FROM numbers(1000)",
                pod=pod_for_insert_data,
            )


def check_zk_root_znode(chi, keeper_type, pod_count, retry_count=15):
    for pod_num in range(pod_count):
        found = False
        for i in range(retry_count):
            if keeper_type == "zookeeper-operator":
                expected_outs = ("[clickhouse, zookeeper, zookeeper-operator]",)
                keeper_cmd = "./bin/zkCli.sh ls /"
                pod_prefix = "zookeeper"
            elif keeper_type == "zookeeper":
                expected_outs = ("[clickhouse, zookeeper]",)
                keeper_cmd = "./bin/zkCli.sh ls /"
                pod_prefix = "zookeeper"
            else:
                expected_outs = (
                    "keeper clickhouse",
                    "clickhouse keeper",
                    "keeper",
                )
                keeper_cmd = "clickhouse-keeper client -h 127.0.0.1 -p 2181 -q 'ls /'"
                pod_prefix = "clickhouse-keeper"

            out = kubectl.launch(
                f"exec {pod_prefix}-{pod_num} -- bash -ce '{keeper_cmd}'",
                ns=settings.test_namespace,
                ok_to_fail=True,
            )
            found = False
            for expected_out in expected_outs:
                if expected_out in out:
                    found = True
                    break
            if found:
                break
            else:
                with Then(f"{keeper_type} ROOT NODE not ready, wait {(i + 1) * 3} sec"):
                    time.sleep((i + 1) * 3)

        assert found, f"Unexpected {keeper_type} `ls /` output"
    # clickhouse could not reconnect to zookeeper. So we need to retry
    for i in range(retry_count):
        out = clickhouse.query(
            chi["metadata"]["name"], "SELECT count() FROM system.zookeeper WHERE path='/'", with_error=True
        )
        expected_out = {
            "zookeeper": "2",
            "zookeeper-operator": "3",
            "clickhouse-keeper": "2",
            "clickhouse-keeper_with_CHKI": "2",
        }
        if expected_out[keeper_type] != out.strip(" \t\r\n") and i + 1 < retry_count:
            with Then(f"{keeper_type} system.zookeeper not ready, wait {(i + 1) * 3} sec"):
                time.sleep((i + 1) * 3)

            continue

        assert expected_out[keeper_type] == out.strip(
            " \t\r\n"
        ), f"Unexpected `SELECT count() FROM system.zookeeper WHERE path='/'` output {out}"


def rescale_zk_and_clickhouse(
        ch_node_count,
        keeper_node_count,
        keeper_type,
        keeper_manifest_1_node,
        keeper_manifest_3_node,
        first_install=False,
        clean_ns=None,
):
    keeper_manifest = keeper_manifest_1_node if keeper_node_count == 1 else keeper_manifest_3_node
    _, chi = util.install_clickhouse_and_keeper(
        chi_file=f"manifests/chi/test-cluster-for-{keeper_type}-{ch_node_count}.yaml",
        chi_template_file="manifests/chit/tpl-clickhouse-latest.yaml",
        chi_name="test-cluster-for-zk",
        keeper_manifest=keeper_manifest,
        keeper_type=keeper_type,
        clean_ns=first_install if clean_ns is None else clean_ns,
        force_keeper_install=True,
        keeper_install_first=first_install,
        make_object_count=False,
    )
    return chi


def delete_keeper_pvc(keeper_type):
    pvc_list = kubectl.get(
        kind="pvc",
        name="",
        label=f"-l app={keeper_type}",
        ns=settings.test_namespace,
        ok_to_fail=False,
    )
    for pvc in pvc_list["items"]:
        if pvc["metadata"]["name"][-2:] != "-0":
            kubectl.launch(f"delete pvc {pvc['metadata']['name']}", ns=settings.test_namespace)


def start_stop_zk_and_clickhouse(chi_name, ch_stop, keeper_replica_count, keeper_type, keeper_manifest_1_node,
                                 keeper_manifest_3_node):
    kubectl.launch(f"patch chi --type=merge {chi_name} -p '{{\"spec\":{{ \"stop\": \"{ch_stop}\" }} }}'")
    keeper_manifest = keeper_manifest_1_node
    if keeper_replica_count > 1:
        keeper_manifest = keeper_manifest_3_node
    if keeper_type == "zookeeper":
        keeper_manifest = f"../../deploy/zookeeper/zookeeper-manually/quick-start-persistent-volume/{keeper_manifest}"
    if keeper_type == "clickhouse-keeper":
        keeper_manifest = f"../../deploy/clickhouse-keeper/clickhouse-keeper-manually/{keeper_manifest}"
    if keeper_type == "zookeeper-operator":
        keeper_manifest = f"../../deploy/zookeeper/zookeeper-with-zookeeper-operator/{keeper_manifest}"

    zk_manifest = yaml_manifest.get_multidoc_manifest_data(util.get_full_path(keeper_manifest, lookup_in_host=True))
    for doc in zk_manifest:
        if doc["kind"] == "StatefulSet":
            with When(f"Patch {keeper_type} replicas: {keeper_replica_count}"):
                keeper_name = doc["metadata"]["name"]
                kubectl.launch(
                    f"patch --type=merge sts {keeper_name} -p '{{\"spec\":{{\"replicas\":{keeper_replica_count}}}}}'"
                )
                retries_num = 20
                for i in range(retries_num):
                    pod_counts = kubectl.get_count("pod", f"-l app={keeper_type}")
                    if pod_counts == keeper_replica_count:
                        break
                    elif i >= retries_num - 1:
                        assert pod_counts == keeper_replica_count
                    with Then(f"Zookeeper not ready. "
                              f"Pods expected={keeper_replica_count} actual={pod_counts}, wait {3*(i+1)} seconds"):
                        time.sleep(3*(i+1))


@TestOutline
def test_keeper_rescale_outline(
        self,
        keeper_type="zookeeper",
        pod_for_insert_data="chi-test-cluster-for-zk-default-0-1-0",
        keeper_manifest_1_node="zookeeper-1-node-1GB-for-tests-only.yaml",
        keeper_manifest_3_node="zookeeper-3-nodes-1GB-for-tests-only.yaml",
):
    """
    test scenario for Zoo/Clickhouse Keeper

    CH 1 -> 2 wait complete + Keeper 1 -> 3 nowait
    CH 2 -> 1 wait complete + Keeper 3 -> 1 nowait
    CH 1 -> 2 wait complete + Keeper 1 -> 3 nowait

    Keeper replicas 3 > 0 > 1 > 3
    """

    with When("Clean exists ClickHouse Keeper and ZooKeeper"):
        kubectl.delete_all_keeper(settings.test_namespace)
        kubectl.delete_all_chi(settings.test_namespace)

    with When("Install CH 1 node ZK 1 node"):
        chi = rescale_zk_and_clickhouse(
            ch_node_count=1,
            keeper_node_count=1,
            keeper_type=keeper_type,
            keeper_manifest_1_node=keeper_manifest_1_node,
            keeper_manifest_3_node=keeper_manifest_3_node,
            first_install=True,
        )
        util.wait_clickhouse_cluster_ready(chi)
        wait_keeper_ready(keeper_type=keeper_type, pod_count=1)
        check_zk_root_znode(chi, keeper_type, pod_count=1)
        util.wait_clickhouse_no_readonly_replicas(chi)
        insert_replicated_data(
            chi,
            pod_for_insert_data,
            create_tables=["test_repl1"],
            insert_tables=["test_repl1"],
        )

    total_iterations = 3
    for iteration in range(total_iterations):
        with When(f"ITERATION {iteration}"):
            with Then("CH 1 -> 2 wait complete + ZK 1 -> 3 nowait"):
                chi = rescale_zk_and_clickhouse(
                    ch_node_count=2,
                    keeper_node_count=3,
                    keeper_type=keeper_type,
                    keeper_manifest_1_node=keeper_manifest_1_node,
                    keeper_manifest_3_node=keeper_manifest_3_node,
                )
                wait_keeper_ready(keeper_type=keeper_type, pod_count=3)
                check_zk_root_znode(chi, keeper_type, pod_count=3)

                util.wait_clickhouse_cluster_ready(chi)
                util.wait_clickhouse_no_readonly_replicas(chi)
                insert_replicated_data(
                    chi,
                    pod_for_insert_data,
                    create_tables=["test_repl2"],
                    insert_tables=["test_repl1", "test_repl2"],
                )

            with Then("CH 2 -> 1 wait complete + ZK 3 -> 1 nowait"):
                chi = rescale_zk_and_clickhouse(
                    ch_node_count=1,
                    keeper_node_count=1,
                    keeper_type=keeper_type,
                    keeper_manifest_1_node=keeper_manifest_1_node,
                    keeper_manifest_3_node=keeper_manifest_3_node,
                )
                wait_keeper_ready(keeper_type=keeper_type, pod_count=1)
                check_zk_root_znode(chi, keeper_type, pod_count=1)
                if keeper_type == "zookeeper" and "scaleout-pvc" in keeper_manifest_1_node:
                    delete_keeper_pvc(keeper_type=keeper_type)

                util.wait_clickhouse_cluster_ready(chi)
                util.wait_clickhouse_no_readonly_replicas(chi)
                insert_replicated_data(
                    chi,
                    pod_for_insert_data,
                    create_tables=["test_repl3"],
                    insert_tables=["test_repl1", "test_repl2", "test_repl3"],
                )

    with When("CH 1 -> 2 wait complete + ZK 1 -> 3 nowait"):
        chi = rescale_zk_and_clickhouse(
            ch_node_count=2,
            keeper_node_count=3,
            keeper_type=keeper_type,
            keeper_manifest_1_node=keeper_manifest_1_node,
            keeper_manifest_3_node=keeper_manifest_3_node,
        )
        check_zk_root_znode(chi, keeper_type, pod_count=3)

    for keeper_replica_count in [1, 3]:
        with When("Stop CH + ZK"):
            start_stop_zk_and_clickhouse(
                chi['metadata']['name'],
                ch_stop=True,
                keeper_replica_count=0,
                keeper_type=keeper_type,
                keeper_manifest_1_node=keeper_manifest_1_node,
                keeper_manifest_3_node=keeper_manifest_3_node
            )
        with Then(f"Start CH + ZK, expect keeper node count={keeper_replica_count}"):
            start_stop_zk_and_clickhouse(
                chi['metadata']['name'],
                ch_stop=False,
                keeper_replica_count=keeper_replica_count,
                keeper_type=keeper_type,
                keeper_manifest_1_node=keeper_manifest_1_node,
                keeper_manifest_3_node=keeper_manifest_3_node
            )

    with Then("check data in tables"):
        check_zk_root_znode(chi, keeper_type, pod_count=3)
        util.wait_clickhouse_cluster_ready(chi)
        util.wait_clickhouse_no_readonly_replicas(chi)
        for table_name, exptected_rows in {
            "test_repl1": str(1000 + 2000 * total_iterations),
            "test_repl2": str(2000 * total_iterations),
            "test_repl3": str(1000 * total_iterations),
        }.items():
            actual_rows = clickhouse.query(
                chi["metadata"]["name"],
                f"SELECT count() FROM default.{table_name}",
                pod="chi-test-cluster-for-zk-default-0-1-0",
            )
            assert (
                    actual_rows == exptected_rows
            ), f"Invalid rows counter after inserts {table_name} expected={exptected_rows} actual={actual_rows}"

    with Then("drop all created tables"):
        for i in range(3):
            clickhouse.drop_table_on_cluster(chi, "all-sharded", f"default.test_repl{i + 1}")


@TestScenario
@Name("test_zookeeper_rescale. Check ZK scale-up / scale-down cases")
def test_zookeeper_rescale(self):
    test_keeper_rescale_outline(
        keeper_type="zookeeper",
        pod_for_insert_data="chi-test-cluster-for-zk-default-0-1-0",
        keeper_manifest_1_node="zookeeper-1-node-1GB-for-tests-only.yaml",
        keeper_manifest_3_node="zookeeper-3-nodes-1GB-for-tests-only.yaml",
    )


@TestScenario
@Name("test_clickhouse_keeper_rescale. Check KEEPER scale-up / scale-down cases")
def test_clickhouse_keeper_rescale(self):
    test_keeper_rescale_outline(
        keeper_type="clickhouse-keeper",
        pod_for_insert_data="chi-test-cluster-for-zk-default-0-1-0",
        keeper_manifest_1_node="clickhouse-keeper-1-node-256M-for-test-only.yaml",
        keeper_manifest_3_node="clickhouse-keeper-3-nodes-256M-for-test-only.yaml",
    )


@TestScenario
@Name("test_clickhouse_keeper_rescale_CHKI using ClickHouseKeeperInstallation. Check KEEPER scale-up / scale-down cases")
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Kind_ClickHouseKeeperInstallation("1.0"))
def test_clickhouse_keeper_rescale_CHKI(self):
    test_keeper_rescale_outline(
        keeper_type="clickhouse-keeper_with_CHKI",
        pod_for_insert_data="chi-test-cluster-for-zk-default-0-1-0",
        keeper_manifest_1_node="clickhouse-keeper-1-node-for-test-only.yaml",
        keeper_manifest_3_node="clickhouse-keeper-3-node-for-test-only.yaml",
    )


@TestScenario
@Name("test_zookeeper_operator_rescale. Check Zookeeper OPERATOR scale-up / scale-down cases")
def test_zookeeper_operator_rescale(self):
    test_keeper_rescale_outline(
        keeper_type="zookeeper-operator",
        pod_for_insert_data="chi-test-cluster-for-zk-default-0-1-0",
        keeper_manifest_1_node="zookeeper-operator-1-node.yaml",
        keeper_manifest_3_node="zookeeper-operator-3-nodes.yaml",
    )


@TestScenario
@Name("test_zookeeper_pvc_scaleout_rescale. Check ZK+PVC scale-up / scale-down cases")
def test_zookeeper_pvc_scaleout_rescale(self):
    test_keeper_rescale_outline(
        keeper_type="zookeeper",
        pod_for_insert_data="chi-test-cluster-for-zk-default-0-1-0",
        keeper_manifest_1_node="zookeeper-1-node-1GB-for-tests-only-scaleout-pvc.yaml",
        keeper_manifest_3_node="zookeeper-3-nodes-1GB-for-tests-only-scaleout-pvc.yaml",
    )


@TestOutline
def test_keeper_probes_outline(
        self,
        keeper_type="zookeeper",
        keeper_manifest_1_node="zookeeper-1-node-1GB-for-tests-only.yaml",
        keeper_manifest_3_node="zookeeper-3-nodes-1GB-for-tests-only.yaml",
):
    with When("Clean exists ClickHouse Keeper and ZooKeeper"):
        kubectl.delete_all_chi(settings.test_namespace)
        kubectl.delete_all_keeper(settings.test_namespace)

    with Then("Install CH 2 node ZK 3 node"):
        chi = rescale_zk_and_clickhouse(
            ch_node_count=2,
            keeper_node_count=3,
            keeper_type=keeper_type,
            keeper_manifest_1_node=keeper_manifest_1_node,
            keeper_manifest_3_node=keeper_manifest_3_node,
            first_install=True,
            clean_ns=False,
        )
        util.wait_clickhouse_cluster_ready(chi)
        wait_keeper_ready(keeper_type=keeper_type, pod_count=3)
        check_zk_root_znode(chi, keeper_type, pod_count=3)
        util.wait_clickhouse_no_readonly_replicas(chi)

    with Then("Create keeper_bench table"):
        clickhouse.query(chi["metadata"]["name"], "DROP DATABASE IF EXISTS keeper_bench ON CLUSTER '{cluster}' SYNC")
        clickhouse.query(chi["metadata"]["name"], "CREATE DATABASE keeper_bench ON CLUSTER '{cluster}'")
        clickhouse.query(
            chi["metadata"]["name"],
            """
            CREATE TABLE keeper_bench.keeper_bench ON CLUSTER '{cluster}' (p UInt64, x UInt64)
            ENGINE=ReplicatedSummingMergeTree('/clickhouse/tables/{database}/{table}', '{replica}' )
            ORDER BY tuple()
            PARTITION BY p
            SETTINGS in_memory_parts_enable_wal=0,
                min_bytes_for_wide_part=104857600,
                min_bytes_for_compact_part=10485760,
                parts_to_delay_insert=1000000,
                parts_to_throw_insert=1000000,
                max_parts_in_total=1000000;        
        """,
        )
    with Then("Insert data to keeper_bench for make zookeeper workload"):
        pod_prefix = "chi-test-cluster-for-zk-default"
        rows = 100000
        for pod in ("0-0-0", "0-1-0"):
            clickhouse.query(
                chi["metadata"]["name"],
                f"""
                INSERT INTO keeper_bench.keeper_bench SELECT rand(1)%100, rand(2) FROM numbers({rows})
                SETTINGS max_block_size=1,
                  min_insert_block_size_bytes=1,
                  min_insert_block_size_rows=1,
                  insert_deduplicate=0,
                  max_threads=128,
                  max_insert_threads=128
                """,
                pod=f"{pod_prefix}-{pod}",
                timeout=rows,
            )

    with Then("Check liveness and readiness probes fail"):
        zk_pod_prefix = "clickhouse-keeper" if keeper_type == "clickhouse-keeper" else "zookeeper"
        for zk_pod in range(3):
            out = kubectl.launch(f"describe pod {zk_pod_prefix}-{zk_pod}")
            assert "probe failed" not in out, "all probes shall be successful"

    with Then("Check ReadOnlyReplica"):
        out = clickhouse.query(
            chi["metadata"]["name"],
            "SELECT count() FROM cluster('all-sharded',system.metric_log) WHERE CurrentMetric_ReadonlyReplica > 0",
        )
        assert out == "0", "ReadOnlyReplica shall be zero"


@TestScenario
@Name(
    "test_zookeeper_probes_workload. Liveness + Readiness probes shall works fine "
    "under workload in multi-datacenter installation"
)
def test_zookeeper_probes_workload(self):
    test_keeper_probes_outline(
        keeper_type="zookeeper",
        keeper_manifest_1_node="zookeeper-1-node-1GB-for-tests-only.yaml",
        keeper_manifest_3_node="zookeeper-3-nodes-1GB-for-tests-only.yaml",
    )


@TestScenario
@Name(
    "test_zookeeper_pvc_probes_workload. Liveness + Readiness probes shall works fine "
    "under workload in multi-datacenter installation"
)
def test_zookeeper_pvc_probes_workload(self):
    test_keeper_probes_outline(
        keeper_type="zookeeper",
        keeper_manifest_1_node="zookeeper-1-node-1GB-for-tests-only-scaleout-pvc.yaml",
        keeper_manifest_3_node="zookeeper-3-nodes-1GB-for-tests-only-scaleout-pvc.yaml",
    )


@TestScenario
@Name(
    "test_zookeeper_operator_probes_workload. Liveness + Readiness probes shall works fine "
    "under workload in multi-datacenter installation"
)
def test_zookeeper_operator_probes_workload(self):
    test_keeper_probes_outline(
        keeper_type="zookeeper-operator",
        keeper_manifest_1_node="zookeeper-operator-1-node.yaml",
        keeper_manifest_3_node="zookeeper-operator-3-nodes.yaml",

        # uncomment only if you know how to use it
        # keeper_manifest_1_node='zookeeper-operator-1-node-with-custom-probes.yaml',
        # keeper_manifest_3_node='zookeeper-operator-3-nodes-with-custom-probes.yaml',
    )


@TestScenario
@Name(
    "test_clickhouse_keeper_probes_workload. Liveness + Readiness probes shall works fine "
    "under workload in multi-datacenter installation"
)
def test_clickhouse_keeper_probes_workload(self):
    test_keeper_probes_outline(
        keeper_type="clickhouse-keeper",
        keeper_manifest_1_node="clickhouse-keeper-1-node-256M-for-test-only.yaml",
        keeper_manifest_3_node="clickhouse-keeper-3-nodes-256M-for-test-only.yaml",
    )


@TestScenario
@Name(
    "test_clickhouse_keeper_probes_workload_with_CHKI. Liveness + Readiness probes shall works fine "
    "under workload in multi-datacenter installation"
)
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Kind_ClickHouseKeeperInstallation("1.0"))
def test_clickhouse_keeper_probes_workload_with_CHKI(self):
    test_keeper_probes_outline(
        keeper_type="clickhouse-keeper_with_CHKI",
        keeper_manifest_1_node="clickhouse-keeper-1-node-for-test-only.yaml",
        keeper_manifest_3_node="clickhouse-keeper-3-node-for-test-only.yaml",
    )


@TestModule
@Name("e2e.test_keeper")
def test(self):
    with Given("set settings"):
        set_settings()
        self.context.test_namespace = "test"
        self.context.operator_namespace = "test"
    with Given("I create shell"):
        shell = get_shell()
        self.context.shell = shell

    all_tests = [
        test_zookeeper_operator_rescale,
        test_clickhouse_keeper_rescale,
        test_clickhouse_keeper_rescale_CHKI,
        test_zookeeper_pvc_scaleout_rescale,
        test_zookeeper_rescale,

        test_zookeeper_probes_workload,
        test_zookeeper_pvc_probes_workload,
        test_zookeeper_operator_probes_workload,
        test_clickhouse_keeper_probes_workload,
        test_clickhouse_keeper_probes_workload_with_CHKI,
    ]

    util.clean_namespace(delete_chi=True, delete_keeper=True)
    util.install_operator_if_not_exist()
    for t in all_tests:
        Scenario(test=t)()
