import time

import e2e.clickhouse as clickhouse
import e2e.kubectl as kubectl
import e2e.settings as settings
import e2e.util as util

from testflows.core import *


def wait_zookeeper_ready(svc_name='zookeeper', pod_count=3, retries=10):
    for pod_num in range(pod_count):
        kubectl.wait_pod_status(f'{svc_name}-{pod_num}', 'Running', settings.test_namespace)
    for i in range(retries):
        ready_pods = kubectl.launch(f"get pods | grep {svc_name} | grep Running | grep 1/1 | wc -l")
        ready_endpoints = kubectl.launch(f"get endpoints {svc_name} -o json | jq '.subsets[].addresses[].ip' | wc -l")
        if ready_pods == str(pod_count) and ready_endpoints == str(pod_count):
            break
        else:
            with Then(
                f"Zookeeper Not ready yet ready_endpoints={ready_endpoints} ready_pods={ready_pods}, expected pod_count={pod_count}. "
                f"Wait for {i*3} seconds"
            ):
                time.sleep(i*3)
        if i == retries - 1:
            Fail(f"Zookeeper failed, ready_endpoints={ready_endpoints} ready_pods={ready_pods}, expected pod_count={pod_count}")


def wait_clickhouse_readonly_replicas(expected_replicas='[0,0]', retries=10):
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
def test_zookeeper_rescale(self, chi):
    with When('create replicated table #1'):
        clickhouse.create_table_on_cluster(
            chi, 'all-sharded', 'default.zk_repl',
            '(id UInt64) ENGINE=ReplicatedMergeTree(\'/clickhouse/tables/default.zk_repl/{shard}\',\'{replica}\') ORDER BY (id)'
        )

    with Then('insert data x1 to table #1'):
        clickhouse.query(
            chi['metadata']['name'], 'INSERT INTO default.zk_repl SELECT number FROM numbers(1000)',
            pod="chi-test-cluster-for-zk-default-0-0-0"
        )

    with Then('scale up zookeeper to 3 nodes'):
        util.require_zookeeper('zookeeper-3-nodes-1GB-for-tests-only.yaml', force_install=True)
        wait_zookeeper_ready(pod_count=3)
        wait_clickhouse_readonly_replicas()

    with When('create replicated table #2'):
        clickhouse.create_table_on_cluster(
            chi, 'all-sharded', 'default.zk_repl2',
            '(id UInt64) ENGINE=ReplicatedMergeTree(\'/clickhouse/tables/default.zk_repl2/{shard}\',\'{replica}\') ORDER BY (id)'
        )

    with Then('insert data x2 to table #1 and #2'):
        for table in ('zk_repl', 'zk_repl2'):
            clickhouse.query(
                chi['metadata']['name'], f'INSERT INTO default.{table} SELECT number*2 FROM numbers(1000)',
                pod="chi-test-cluster-for-zk-default-0-1-0"
            )

    with Then('scale down zookeeper to 1 nodes'):
        util.require_zookeeper('zookeeper-1-node-1GB-for-tests-only.yaml', force_install=True)
        wait_zookeeper_ready(pod_count=1)
        wait_clickhouse_readonly_replicas()

    with When('create replicated table #3'):
        clickhouse.create_table_on_cluster(
            chi, 'all-sharded', 'default.zk_repl3',
            '(id UInt64) ENGINE=ReplicatedMergeTree(\'/clickhouse/tables/default.zk_repl3/{shard}\',\'{replica}\') ORDER BY (id)'
        )

    with Then('insert data x3 to table #1, #2, #3'):
        for table in ('zk_repl', 'zk_repl2', 'zk_repl3'):
            clickhouse.query(
                chi['metadata']['name'], f'INSERT INTO default.{table} SELECT number*3 FROM numbers(1000)',
                pod="chi-test-cluster-for-zk-default-0-0-0"
            )

    with Then('check data in table #1, #2, #3'):
        for table, rows in {"zk_repl": "3000", "zk_repl2": "2000", "zk_repl3": "1000"}.items():
            assert clickhouse.query(
                chi['metadata']['name'], f'SELECT count() FROM default.{table}', pod="chi-test-cluster-for-zk-default-0-1-0"
            ) == rows, "Invalid rows counter after inserts"

    with Then('drop all created tables'):
        clickhouse.drop_table_on_cluster(chi, 'all-sharded', 'default.zk_repl')
        clickhouse.drop_table_on_cluster(chi, 'all-sharded', 'default.zk_repl2')
        clickhouse.drop_table_on_cluster(chi, 'all-sharded', 'default.zk_repl3')


@TestModule
@Name("e2e.test_zookeeper")
def test(self):
    """
    Perform test for zookeeper scale-up / scale-down scenarios
    """
    _, chi = util.install_clickhouse_and_zookeeper(
        chi_file='configs/test-cluster-for-zookeeper.yaml',
        chi_template_file='templates/tpl-clickhouse-latest.yaml',
        chi_name='test-cluster-for-zk',
    )
    util.wait_clickhouse_cluster_ready(chi)

    all_tests = [
        test_zookeeper_rescale
    ]

    for t in all_tests:
        if callable(t):
            Scenario(test=t)(chi=chi)
        else:
            Scenario(test=t[0])(args=t[1], chi=chi)
