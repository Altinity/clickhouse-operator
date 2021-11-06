import e2e.clickhouse as clickhouse
import e2e.kubectl as kubectl
import e2e.settings as settings
import e2e.util as util

from testflows.core import *

@TestScenario
@Name("test_zookeeper_rescale. Check ZK scale-up / scale-down cases")
def test_zookeeper_rescale(self, chi):
    with When('create replicated table'):
        clickhouse.create_table_on_cluster(
            chi, 'all-sharded', 'default.zk_repl',
            '(id UInt64) ENGINE=ReplicatedMergeTree(\'/clickhouse/tables/default.zk_repl/{shard}\',\'{replica}\') ORDER BY (id)'
        )
    with Then('insert data x1'):
        clickhouse.query(
            chi['metadata']['name'], 'INSERT INTO default.zk_repl SELECT number FROM numbers(1000)',
            pod="chi-test-cluster-for-zk-default-0-0-0"
        )
    with Then('scale up zookeeper to 3 nodes'):
        util.require_zookeeper('zookeeper-3-nodes-1GB-for-tests-only.yaml', force_install=True)
        kubectl.wait_pod_status('zookeeper-0', 'Running', settings.test_namespace)
        kubectl.wait_pod_status('zookeeper-1', 'Running', settings.test_namespace)
        kubectl.wait_pod_status('zookeeper-2', 'Running', settings.test_namespace)

    with Then('insert data x2'):
        clickhouse.query(
            chi['metadata']['name'], 'INSERT INTO default.zk_repl SELECT number*2 FROM numbers(1000)',
            pod="chi-test-cluster-for-zk-default-0-1-0"
        )

    with Then('scale down zookeeper to 1 nodes'):
        util.require_zookeeper('zookeeper-1-node-1GB-for-tests-only.yaml', force_install=True)
        kubectl.wait_pod_status('zookeeper-0', 'Running', settings.test_namespace)

    with Then('insert data x2'):
        clickhouse.query(
            chi['metadata']['name'], 'INSERT INTO default.zk_repl SELECT number*3 FROM numbers(1000)',
            pod="chi-test-cluster-for-zk-default-0-0-0"
        )

    assert clickhouse.query(
        chi['metadata']['name'], 'SELECT count() FROM default.zk_repl', pod="chi-test-cluster-for-zk-default-0-1-0"
    ) == '3000', "Invalid rows after 3x1000 inserts"

    clickhouse.drop_table_on_cluster(chi, 'all-sharded', 'default.zk_repl')


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
