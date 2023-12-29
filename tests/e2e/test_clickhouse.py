import time

import e2e.clickhouse as clickhouse
import e2e.kubectl as kubectl
import e2e.yaml_manifest as yaml_manifest
import e2e.settings as settings
import e2e.util as util

from testflows.core import *
from testflows.asserts import error


@TestScenario
@Name("test_ch_001. Insert quorum")
def test_ch_001(self):
    util.require_keeper(keeper_type=self.context.keeper_type)
    quorum_template = "manifests/chit/tpl-clickhouse-stable.yaml"
    chit_data = yaml_manifest.get_manifest_data(util.get_full_path(quorum_template))

    kubectl.launch(
        f"delete chit {chit_data['metadata']['name']}",
        ns=settings.test_namespace,
        ok_to_fail=True,
    )
    kubectl.create_and_check(
        "manifests/chi/test-ch-001-insert-quorum.yaml",
        {
            "apply_templates": {quorum_template},
            "pod_count": 2,
            "do_not_delete": 1,
        },
    )

    chi = yaml_manifest.get_chi_name(util.get_full_path("manifests/chi/test-ch-001-insert-quorum.yaml"))
    chi_data = kubectl.get("chi", ns=settings.test_namespace, name=chi)
    util.wait_clickhouse_cluster_ready(chi_data)

    host0 = "chi-test-ch-001-insert-quorum-default-0-0"
    host1 = "chi-test-ch-001-insert-quorum-default-0-1"

    create_table = """
    create table t1 on cluster default (a Int8, d Date default today())
    Engine = ReplicatedMergeTree('/clickhouse/tables/{table}', '{replica}')
    partition by d order by a 
    TTL d + interval 5 second
    SETTINGS merge_with_ttl_timeout=5""".replace(
        "\r", ""
    ).replace(
        "\n", ""
    )

    create_mv_table2 = """
    create table t2 on cluster default (a Int8)
    Engine = ReplicatedMergeTree('/clickhouse/tables/{table}', '{replica}')
    partition by tuple() order by a""".replace(
        "\r", ""
    ).replace(
        "\n", ""
    )

    create_mv_table3 = """
    create table t3 on cluster default (a Int8)
    Engine = ReplicatedMergeTree('/clickhouse/tables/{table}', '{replica}')
    partition by tuple() order by a""".replace(
        "\r", ""
    ).replace(
        "\n", ""
    )

    create_mv2 = "create materialized view t_mv2 on cluster default to t2 as select a from t1"
    create_mv3 = "create materialized view t_mv3 on cluster default to t3 as select a from t1"

    with Given("Tables t1, t2, t3 and MVs t1->t2, t1-t3 are created"):
        clickhouse.query(chi, create_table)
        clickhouse.query(chi, create_mv_table2)
        clickhouse.query(chi, create_mv_table3)

        clickhouse.query(chi, create_mv2)
        clickhouse.query(chi, create_mv3)

        with When("Add a row to an old partition"):
            clickhouse.query(chi, "insert into t1(a,d) values(6, today()-1)", host=host0)

        with When("Stop fetches for t1 at replica1"):
            clickhouse.query(chi, "system stop fetches default.t1", host=host1)

            with Then("Wait 10 seconds and the data should be dropped by TTL"):
                time.sleep(10)
                out = clickhouse.query(chi, "select count() from t1 where a=6", host=host0)
                assert out == "0", error()

        with When("Resume fetches for t1 at replica1"):
            clickhouse.query(chi, "system start fetches default.t1", host=host1)
            time.sleep(5)

            with Then("Inserts should resume"):
                clickhouse.query(chi, "insert into t1(a) values(7)", host=host0)

        clickhouse.query(chi, "insert into t1(a) values(1)")

        with When("Stop fetches for t2 at replica1"):
            clickhouse.query(chi, "system stop fetches default.t2", host=host1)

            with Then("Insert should fail since it can not reach the quorum"):
                out = clickhouse.query_with_error(chi, "insert into t1(a) values(2)", host=host0)
                assert "Timeout while waiting for quorum" in out, error()

        # kubectl(f"exec {host0}-0 -n test -- cp /var/lib//clickhouse/data/default/t2/all_1_1_0/a.mrk2 /var/lib//clickhouse/data/default/t2/all_1_1_0/a.bin")
        # with Then("Corrupt data part in t2"):
        #    kubectl(f"exec {host0}-0 -n test -- sed -i \"s/b/c/\" /var/lib/clickhouse/data/default/t2/all_1_1_0/a.bin")

        with When("Resume fetches for t2 at replica1"):
            clickhouse.query(chi, "system start fetches default.t2", host=host1)
            i = 0
            while (
                "2"
                != clickhouse.query(
                    chi,
                    "select active_replicas from system.replicas where database='default' and table='t1'",
                    pod=host0,
                )
                and i < 10
            ):
                with Then("Not ready, wait 5 seconds"):
                    time.sleep(5)
                    i += 1

            with Then("Inserts should fail with an error regarding not satisfied quorum"):
                out = clickhouse.query_with_error(chi, "insert into t1(a) values(3)", host=host0)
                assert "Quorum for previous write has not been satisfied yet" in out, error()

            with And("Second insert of the same block should pass"):
                clickhouse.query(chi, "insert into t1(a) values(3)", host=host0)

            with And("Insert of the new block should fail"):
                out = clickhouse.query_with_error(chi, "insert into t1(a) values(4)", host=host0)
                assert "Quorum for previous write has not been satisfied yet" in out, error()

            with And(
                "Second insert of the same block with 'deduplicate_blocks_in_dependent_materialized_views' setting should fail"
            ):
                out = clickhouse.query_with_error(
                    chi,
                    "set deduplicate_blocks_in_dependent_materialized_views=1; insert into t1(a) values(5)",
                    host=host0,
                )
                assert "Quorum for previous write has not been satisfied yet" in out, error()

        out = clickhouse.query_with_error(
            chi,
            "select t1.a t1_a, t2.a t2_a from t1 left outer join t2 using (a) order by t1_a settings join_use_nulls=1",
        )
        note(out)

        # cat /var/log/clickhouse-server/clickhouse-server.log | grep t2 | grep -E "all_1_1_0|START|STOP"


@TestScenario
@Name("test_ch_002. Row-level security")
def test_ch_002(self):
    kubectl.create_and_check(
        "manifests/chi/test-ch-002-row-level.yaml",
        {
            "apply_templates": {"manifests/chit/tpl-clickhouse-stable.yaml"},
            "do_not_delete": 1,
        },
    )

    chi = "test-ch-002-row-level"
    create_table = """create table test (d Date default today(), team LowCardinality(String), user String) Engine = MergeTree() PARTITION BY d ORDER BY d;"""

    with When("Create test table"):
        clickhouse.query(chi, create_table)

    with And("Insert some data"):
        clickhouse.query(
            chi,
            "INSERT INTO test(team, user) values('team1', 'user1'),('team2', 'user2'),('team3', 'user3'),('team4', 'user4')",
        )

    with Then(
        "Make another query for different users. It should be restricted to corresponding team by row-level security"
    ):
        for user in ["user1", "user2", "user3", "user4"]:
            out = clickhouse.query(chi, "select user from test", user=user, pwd=user)
            assert out == user, error()

    with Then(
        "Make a count() query for different users. It should be restricted to corresponding team by row-level security"
    ):
        for user in ["user1", "user2", "user3", "user4"]:
            out = clickhouse.query(chi, "select count() from test", user=user, pwd=user)
            assert out == "1", error()

    kubectl.delete_chi(chi)


@TestFeature
@Name("e2e.test_clickhouse")
def test(self):
    util.clean_namespace(delete_chi=False)
    all_tests = [
        test_ch_001,
        test_ch_002,
    ]

    run_test = all_tests

    # placeholder for selective test running
    # run_test = [test_ch_002]

    for t in run_test:
        Scenario(test=t)()
