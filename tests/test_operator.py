import time

import clickhouse
import kubectl
import settings
import util
import manifest

from testflows.core import TestScenario, Name, When, Then, Given, And, main, run, Module, TE
from testflows.asserts import error


@TestScenario
@Name("test_001. 1 node")
def test_001():
    kubectl.create_and_check(
        config="configs/test-001.yaml",
        check={
            "object_counts": {
                "statefulset": 1,
                "pod": 1,
                "service": 2,
            },
            "configmaps": 1,
        }
    )


@TestScenario
@Name("test_002. useTemplates for pod, volume templates, and distribution")
def test_002():
    kubectl.create_and_check(
        config="configs/test-002-tpl.yaml",
        check={
            "pod_count": 1,
            "apply_templates": {
                settings.clickhouse_template,
                "templates/tpl-log-volume.yaml",
                "templates/tpl-one-per-host.yaml",
            },
            "pod_image": settings.clickhouse_version,
            "pod_volumes": {
                "/var/log/clickhouse-server",
            },
            "pod_podAntiAffinity": 1
        }
    )


@TestScenario
@Name("test_003. 4 nodes with custom layout definition")
def test_003():
    kubectl.create_and_check(
        config="configs/test-003-complex-layout.yaml",
        check={
            "object_counts": {
                "statefulset": 4,
                "pod": 4,
                "service": 5,
            },
        },
    )


@TestScenario
@Name("test_004. Compatibility test if old syntax with volumeClaimTemplate is still supported")
def test_004():
    kubectl.create_and_check(
        config="configs/test-004-tpl.yaml",
        check={
            "pod_count": 1,
            "pod_volumes": {
                "/var/lib/clickhouse",
            },
        }
    )


@TestScenario
@Name("test_005. Test manifest created by ACM")
def test_005():
    kubectl.create_and_check(
        config="configs/test-005-acm.yaml",
        check={
            "pod_count": 1,
            "pod_volumes": {
                "/var/lib/clickhouse",
            },
        },
    )


@TestScenario
@Name("test_006. Test clickhouse version upgrade from one version to another using podTemplate change")
def test_006():
    with Then("Create initial position"):
        kubectl.create_and_check(
            config="configs/test-006-ch-upgrade-1.yaml",
            check={
                "pod_count": 2,
                "pod_image": "yandex/clickhouse-server:19.11",
                "do_not_delete": 1,
            }
        )
    with Then("Use different podTemplate and confirm that pod image is updated"):
        kubectl.create_and_check(
            config="configs/test-006-ch-upgrade-2.yaml",
            check={
                "pod_count": 2,
                "pod_image": "yandex/clickhouse-server:19.16",
                "do_not_delete": 1,
            }
        )
    with Then("Change image in podTemplate itself and confirm that pod image is updated"):
        kubectl.create_and_check(
            config="configs/test-006-ch-upgrade-3.yaml",
            check={
                "pod_count": 2,
                "pod_image": "yandex/clickhouse-server:19.11",
            }
        )


@TestScenario
@Name("test_007. Test template with custom clickhouse ports")
def test_007():
    kubectl.create_and_check(
        config="configs/test-007-custom-ports.yaml",
        check={
            "pod_count": 1,
            "pod_ports": [8124, 9001, 9010],
        }
    )


def test_operator_upgrade(config, version_from, version_to=settings.operator_version):
    version_to = settings.operator_version
    with Given(f"clickhouse-operator {version_from}"):
        set_operator_version(version_from)
        config = util.get_full_path(config)
        chi = manifest.get_chi_name(config)

        kubectl.create_and_check(
            config=config,
            check={
                "object_counts": {
                    "statefulset": 1,
                    "pod": 1,
                    "service": 2,
                },
                "do_not_delete": 1,
            }
        )
        start_time = kubectl.get_field("pod", f"chi-{chi}-{chi}-0-0-0", ".status.startTime")

        with When(f"upgrade operator to {version_to}"):
            set_operator_version(version_to, timeout=120)
            time.sleep(5)
            kubectl.wait_chi_status(chi, "Completed", retries=6)
            kubectl.wait_objects(chi, {"statefulset": 1, "pod": 1, "service": 2})
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-{chi}-0-0-0", ".status.startTime")
            # TODO: assert
            if start_time != new_start_time:
                print("!!!Pods have been restarted!!!")

        kubectl.delete_chi(chi)


def test_operator_restart(config, version=settings.operator_version):
    with Given(f"clickhouse-operator {version}"):
        set_operator_version(version)
        config = util.get_full_path(config)
        chi = manifest.get_chi_name(config)

        kubectl.create_and_check(
            config=config,
            check={
                "object_counts": {
                    "statefulset": 1,
                    "pod": 1,
                    "service": 2,
                },
                "do_not_delete": 1,
            })
        start_time = kubectl.get_field("pod", f"chi-{chi}-{chi}-0-0-0", ".status.startTime")

        with When("Restart operator"):
            restart_operator()
            time.sleep(5)
            kubectl.wait_chi_status(chi, "Completed")
            kubectl.wait_objects(chi, {"statefulset": 1, "pod": 1, "service": 2})
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-{chi}-0-0-0", ".status.startTime")
            # TODO: assert
            if start_time != new_start_time:
                print("!!!Pods have been restarted!!!")

        kubectl.delete_chi(chi)


@TestScenario
@Name("test_008. Test operator restart")
def test_008():
    with Then("Test simple chi for operator restart"):
        test_operator_restart("configs/test-008-operator-restart-1.yaml")
    with Then("Test advanced chi for operator restart"):
        test_operator_restart("configs/test-008-operator-restart-2.yaml")


@TestScenario
@Name("test_009. Test operator upgrade")
def test_009(version_from="0.8.0", version_to=settings.operator_version):
    with Then("Test simple chi for operator upgrade"):
        test_operator_upgrade("configs/test-009-operator-upgrade-1.yaml", version_from, version_to)
    with Then("Test advanced chi for operator upgrade"):
        test_operator_upgrade("configs/test-009-operator-upgrade-2.yaml", version_from, version_to)


def set_operator_version(version, ns=settings.operator_namespace, timeout=60):
    kubectl.launch(
        f"set image deployment.v1.apps/clickhouse-operator clickhouse-operator=altinity/clickhouse-operator:{version}",
        ns=ns)
    kubectl.launch(
        f"set image deployment.v1.apps/clickhouse-operator metrics-exporter=altinity/metrics-exporter:{version}",
        ns=ns)
    kubectl.launch("rollout status deployment.v1.apps/clickhouse-operator", ns=ns, timeout=timeout)
    assert kubectl.get_count("pod", ns=ns, label="-l app=clickhouse-operator") > 0, error()


def restart_operator(ns=settings.operator_namespace, timeout=60):
    pod_name = kubectl.get("pod", name="", ns=ns, label="-l app=clickhouse-operator")["items"][0]["metadata"]["name"]
    kubectl.launch(f"delete pod {pod_name}", ns=ns, timeout=timeout)
    kubectl.wait_object("pod", name="", ns=ns, label="-l app=clickhouse-operator")
    pod_name = kubectl.get("pod", name="", ns=ns, label="-l app=clickhouse-operator")["items"][0]["metadata"]["name"]
    kubectl.wait_pod_status(pod_name, "Running", ns=ns)


def require_zookeeper():
    with Given("Install Zookeeper if missing"):
        if kubectl.get_count("service", name="zookeeper") == 0:
            config = util.get_full_path(
                "../deploy/zookeeper/quick-start-persistent-volume/zookeeper-1-node-1GB-for-tests-only.yaml")
            kubectl.apply(config)
            kubectl.wait_object("pod", "zookeeper-0")
            kubectl.wait_pod_status("zookeeper-0", "Running")


@TestScenario
@Name("test_010. Test zookeeper initialization")
def test_010():
    set_operator_version(settings.operator_version)
    require_zookeeper()

    kubectl.create_and_check(
        config="configs/test-010-zkroot.yaml",
        check={
            "apply_templates": {
                settings.clickhouse_template,
            },
            "pod_count": 1,
            "do_not_delete": 1,
        }
    )
    with And("ClickHouse should complain regarding zookeeper path"):
        out = clickhouse.query_with_error("test-010-zkroot", "select * from system.zookeeper where path = '/'")
        assert "You should create root node /clickhouse/test-010-zkroot before start" in out, error()

    kubectl.delete_chi("test-010-zkroot")


@TestScenario
@Name("test_011. Test user security and network isolation")
def test_011():
    with Given("test-011-secured-cluster.yaml and test-011-insecured-cluster.yaml"):
        kubectl.create_and_check(
            config="configs/test-011-secured-cluster.yaml",
            check={
                "pod_count": 2,
                "service": [
                    "chi-test-011-secured-cluster-default-1-0",
                    "ClusterIP",
                ],
                "apply_templates": {
                    settings.clickhouse_template,
                    "templates/tpl-log-volume.yaml",
                },
                "do_not_delete": 1,
            }
        )

        kubectl.create_and_check(
            config="configs/test-011-insecured-cluster.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            }
        )

        with Then("Connection to localhost should succeed with default user"):
            out = clickhouse.query_with_error("test-011-secured-cluster", "select 'OK'")
            assert out == 'OK', f"out={out} should be 'OK'"

        with And("Connection from secured to secured host should succeed"):
            out = clickhouse.query_with_error(
                "test-011-secured-cluster",
                "select 'OK'",
                host="chi-test-011-secured-cluster-default-1-0"
            )
            assert out == 'OK'

        with And("Connection from insecured to secured host should fail for default"):
            out = clickhouse.query_with_error(
                "test-011-insecured-cluster",
                "select 'OK'",
                host="chi-test-011-secured-cluster-default-1-0"
            )
            assert out != 'OK'

        with And("Connection from insecured to secured host should fail for user with no password"):
            time.sleep(10)  # FIXME
            out = clickhouse.query_with_error(
                "test-011-insecured-cluster",
                "select 'OK'",
                host="chi-test-011-secured-cluster-default-1-0",
                user="user1"
            )
            assert "Password" in out or "password" in out

        with And("Connection from insecured to secured host should work for user with password"):
            out = clickhouse.query_with_error(
                "test-011-insecured-cluster",
                "select 'OK'",
                host="chi-test-011-secured-cluster-default-1-0",
                user="user1",
                pwd="topsecret"
            )
            assert out == 'OK'

        with And("Password should be encrypted"):
            cfm = kubectl.get("configmap", "chi-test-011-secured-cluster-common-usersd")
            users_xml = cfm["data"]["chop-generated-users.xml"]
            assert "<password>" not in users_xml
            assert "<password_sha256_hex>" in users_xml

        with And("User with no password should get default automatically"):
            out = clickhouse.query_with_error(
                "test-011-secured-cluster",
                "select 'OK'",
                user="user2",
                pwd="default"
            )
            assert out == 'OK'

        with And("User with both plain and sha256 password should get the latter one"):
            out = clickhouse.query_with_error(
                "test-011-secured-cluster",
                "select 'OK'",
                user="user3",
                pwd="clickhouse_operator_password"
            )
            assert out == 'OK'

        with And("User with row-level security should have it applied"):
            out = clickhouse.query_with_error(
                "test-011-secured-cluster",
                "select * from system.numbers limit 1",
                user="restricted",
                pwd="secret"
            )
            assert out == '1000'

        kubectl.delete_chi("test-011-secured-cluster")
        kubectl.delete_chi("test-011-insecured-cluster")


@TestScenario
@Name("test_011_1. Test default user security")
def test_011_1():
    with Given("test-011-secured-default.yaml with password_sha256_hex for default user"):
        kubectl.create_and_check(
            config="configs/test-011-secured-default.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            }
        )

        with Then("Default user password should be '_removed_'"):
            chi = kubectl.get("chi", "test-011-secured-default")
            assert "default/password" in chi["status"]["normalized"]["configuration"]["users"]
            assert chi["status"]["normalized"]["configuration"]["users"]["default/password"] == "_removed_"

        with And("Connection to localhost should succeed with default user"):
            out = clickhouse.query_with_error(
                "test-011-secured-default",
                "select 'OK'",
                pwd="clickhouse_operator_password"
            )
            assert out == 'OK'

        with When("Trigger installation update"):
            kubectl.create_and_check(
                config="configs/test-011-secured-default-2.yaml",
                check={
                    "do_not_delete": 1,
                }
            )
            with Then("Default user password should be '_removed_'"):
                chi = kubectl.get("chi", "test-011-secured-default")
                assert "default/password" in chi["status"]["normalized"]["configuration"]["users"]
                assert chi["status"]["normalized"]["configuration"]["users"]["default/password"] == "_removed_"

        with When("Default user is assigned the different profile"):
            kubectl.create_and_check(
                config="configs/test-011-secured-default-3.yaml",
                check={
                    "do_not_delete": 1,
                }
            )
            with Then("Connection to localhost should succeed with default user"):
                out = clickhouse.query_with_error(
                    "test-011-secured-default",
                    "select 'OK'"
                )
                assert out == 'OK'

        kubectl.delete_chi("test-011-secured-default")


@TestScenario
@Name("test_012. Test service templates")
def test_012():
    kubectl.create_and_check(
        config="configs/test-012-service-template.yaml",
        check={
            "object_counts": {
                "statefulset": 2,
                "pod": 2,
                "service": 4,
            },
            "service": [
                "service-test-012",
                "ClusterIP",
            ],
            "do_not_delete": 1,
        }
    )
    with Then("There should be a service for shard 0"):
        kubectl.check_service("service-test-012-0-0", "ClusterIP")
    with And("There should be a service for shard 1"):
        kubectl.check_service("service-test-012-1-0", "ClusterIP")
    with And("There should be a service for default cluster"):
        kubectl.check_service("service-default", "ClusterIP")

    kubectl.delete_chi("test-012")


@TestScenario
@Name("test_013. Test adding shards and creating local and distributed tables automatically")
def test_013():
    config = "configs/test-013-add-shards-1.yaml"
    chi = manifest.get_chi_name(config)
    cluster = "default"

    kubectl.create_and_check(
        config=config,
        check={
            "apply_templates": {
                settings.clickhouse_template,
            },
            "object_counts": {
                "statefulset": 1,
                "pod": 1,
                "service": 2,
            },
            "do_not_delete": 1,
        }
    )
    start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")

    schema_objects = [
        'test_local',
        'test_distr',
        'events-distr',
    ]
    with Then("Create local and distributed tables"):
        clickhouse.query(
            chi,
            "CREATE TABLE test_local Engine = Log as SELECT * FROM system.one")
        clickhouse.query(
            chi,
            "CREATE TABLE test_distr as test_local Engine = Distributed('default', default, test_local)")
        clickhouse.query(
            chi,
            "CREATE DATABASE \\\"test-db\\\"")
        clickhouse.query(
            chi,
            "CREATE TABLE \\\"test-db\\\".\\\"events-distr\\\" as system.events "
            "ENGINE = Distributed('all-sharded', system, events)")
    with Then("Add shards"):
        kubectl.create_and_check(
            config="configs/test-013-add-shards-2.yaml",
            check={
                "object_counts": {
                    "statefulset": 3,
                    "pod": 3,
                    "service": 4,
                },
                "do_not_delete": 1,
            }
        )

    # Give some time for replication to catch up
    time.sleep(10)

    new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
    # TODO: assert
    if start_time != new_start_time:
        print("!!!Pods have been restarted!!!")

    with And("Schema objects should be migrated to new shards"):
        for obj in schema_objects:
            out = clickhouse.query(
                chi,
                f"SELECT count() FROM system.tables WHERE name = '{obj}'",
                host=f"chi-{chi}-{cluster}-1-0"
            )
            assert out == "1"
            out = clickhouse.query(
                chi,
                f"SELECT count() FROM system.tables WHERE name = '{obj}'",
                host=f"chi-{chi}-{cluster}-2-0"
            )
            assert out == "1"

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_014. Test that replication works")
def test_014():
    require_zookeeper()

    create_table = """
    create table test_local(a Int8) 
    Engine = ReplicatedMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}', '{replica}')
    partition by tuple() order by a""".replace('\r', '').replace('\n', '')

    kubectl.create_and_check(
        config="configs/test-014-replication.yaml",
        check={
            "apply_templates": {
                settings.clickhouse_template,
                "templates/tpl-persistent-volume-100Mi.yaml",
            },
            "object_counts": {
                "statefulset": 2,
                "pod": 2,
                "service": 3,
            },
            "do_not_delete": 1,
        }
    )

    schema_objects = ['test_local', 'test_view', 'test_mv', 'a_view']
    with Given("Create schema objects"):
        clickhouse.query(
            "test-014-replication",
            create_table,
            host="chi-test-014-replication-default-0-0")
        clickhouse.query(
            "test-014-replication",
            "CREATE VIEW test_view as SELECT * from test_local",
            host="chi-test-014-replication-default-0-0")
        clickhouse.query(
            "test-014-replication",
            "CREATE VIEW a_view as SELECT * from test_view",
            host="chi-test-014-replication-default-0-0")
        clickhouse.query(
            "test-014-replication",
            "CREATE MATERIALIZED VIEW test_mv Engine = Log as SELECT * from test_local",
            host="chi-test-014-replication-default-0-0")

    with Given("Replicated table is created on a first replica and data is inserted"):
        clickhouse.query(
            "test-014-replication",
            "insert into test_local values(1)",
            host="chi-test-014-replication-default-0-0")
        with When("Table is created on the second replica"):
            clickhouse.query("test-014-replication", create_table, host="chi-test-014-replication-default-0-1")
            with Then("Data should be replicated"):
                out = clickhouse.query(
                    "test-014-replication",
                    "select a from test_local",
                    host="chi-test-014-replication-default-0-1")
                assert out == "1"

    with When("Add one more replica"):
        kubectl.create_and_check(
            config="configs/test-014-replication-2.yaml",
            check={
                "pod_count": 3,
                "do_not_delete": 1,
            }
        )
        # that also works:
        # kubectl patch chi test-014-replication -n test --type=json -p '[{"op":"add", "path": "/spec/configuration/clusters/0/layout/shards/0/replicasCount", "value": 3}]'
        with Then("Schema objects should be migrated to the new replica"):
            for obj in schema_objects:
                out = clickhouse.query(
                    "test-014-replication",
                    f"SELECT count() FROM system.tables WHERE name = '{obj}'",
                    host="chi-test-014-replication-default-0-2")
                assert out == "1"

        with And("Replicated table should have the data"):
            out = clickhouse.query(
                "test-014-replication",
                "SELECT a FROM test_local",
                host="chi-test-014-replication-default-0-2")
            assert out == "1"

    with When("Remove replica"):
        kubectl.create_and_check(
            config="configs/test-014-replication.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            })
        with Then("Replica needs to be removed from the Zookeeper as well"):
            out = clickhouse.query(
                "test-014-replication",
                "select count() from system.replicas where table='test_local'")
            assert out == "1"

    with When("Restart Zookeeper pod"):
        with Then("Delete Zookeeper pod"):
            kubectl.launch("delete pod zookeeper-0")
            time.sleep(1)

        with Then("Insert into the table while there is no Zookeeper -- table should be in readonly mode"):
            out = clickhouse.query_with_error("test-014-replication", "insert into test_local values(2)")
            assert "Table is in readonly mode" in out

        with Then("Wait for Zookeeper pod to come back"):
            kubectl.wait_object("pod", "zookeeper-0")
            kubectl.wait_pod_status("zookeeper-0", "Running")

        with Then("Wait for ClickHouse to reconnect to Zookeeper and switch to read-write mode"):
            time.sleep(30)
        # with Then("Restart clickhouse pods"):
        #    kubectl("delete pod chi-test-014-replication-default-0-0-0")
        #    kubectl("delete pod chi-test-014-replication-default-0-1-0")

        with Then("Table should be back to normal"):
            clickhouse.query("test-014-replication", "insert into test_local values(3)")

    kubectl.delete_chi("test-014-replication")


@TestScenario
@Name("test_015. Test circular replication with hostNetwork")
def test_015():
    kubectl.create_and_check(
        config="configs/test-015-host-network.yaml",
        check={
            "pod_count": 2,
            "do_not_delete": 1,
        })

    time.sleep(30)

    with Then("Query from one server to another one should work"):
        out = clickhouse.query(
            "test-015-host-network",
            host="chi-test-015-host-network-default-0-0",
            port="10000",
            sql="SELECT * FROM remote('chi-test-015-host-network-default-0-1', system.one)")
        print("remote out=")
        print(out)

    with Then("Distributed query should work"):
        out = clickhouse.query(
            "test-015-host-network",
            host="chi-test-015-host-network-default-0-0",
            port="10000",
            sql="SELECT count() FROM cluster('all-sharded', system.one) settings receive_timeout=10")
        print("cluster out=")
        print(out)
        assert out == "2"

    kubectl.delete_chi("test-015-host-network")


@TestScenario
@Name("test_016. Test advanced settings options")
def test_016():
    chi = "test-016-settings"
    kubectl.create_and_check(
        config="configs/test-016-settings.yaml",
        check={
            "apply_templates": {
                settings.clickhouse_template,
            },
            "pod_count": 1,
            "do_not_delete": 1,
        })

    with Then("Custom macro 'layer' should be available"):
        out = clickhouse.query(chi, sql="select substitution from system.macros where macro='layer'")
        assert out == "01"

    with And("Custom macro 'test' should be available"):
        out = clickhouse.query(chi, sql="select substitution from system.macros where macro='test'")
        assert out == "test"

    with And("dictGet() should work"):
        out = clickhouse.query(chi, sql="select dictGet('one', 'one', toUInt64(0))")
        assert out == "0"

    with And("query_log should be disabled"):
        clickhouse.query(chi, sql="system flush logs")
        out = clickhouse.query_with_error(chi, sql="select count() from system.query_log")
        assert "doesn't exist" in out

    with And("max_memory_usage should be 7000000000"):
        out = clickhouse.query(chi, sql="select value from system.settings where name='max_memory_usage'")
        assert out == "7000000000"

    with And("test_usersd user should be available"):
        clickhouse.query(chi, sql="select version()", user="test_usersd")

    with And("system.clusters should be empty due to remote_servers override"):
        out = clickhouse.query(chi, sql="select count() from system.clusters")
        assert out == "0"

    with When("Update usersd settings"):
        start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
        kubectl.create_and_check(
            config="configs/test-016-settings-2.yaml",
            check={
                "do_not_delete": 1,
            })
        with Then("Wait for configmap changes to apply"):
            config_map_applied_num = "0"
            i = 1
            while config_map_applied_num == "0" and i < 10:
                config_map_applied_num = kubectl.launch(
                    f"exec chi-{chi}-default-0-0-0 -- bash -c \"grep test_norestart /etc/clickhouse-server/users.d/my_users.xml | wc -l\""
                )
                if config_map_applied_num != "0":
                    break
                with And(f"not applied, wait {15 * i}s"):
                    time.sleep(15 * i)
                    i += 1

            assert config_map_applied_num != "0", "ConfigMap should be applied"

        with Then("test_norestart user should be available"):
            clickhouse.query(chi, sql="select version()", user="test_norestart")
        with And("ClickHouse should not be restarted"):
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
            assert start_time == new_start_time

    kubectl.delete_chi("test-016-settings")


@TestScenario
@Name("test-017-multi-version. Test certain functions across multiple versions")
def test_017():
    kubectl.create_and_check(
        config="configs/test-017-multi-version.yaml",
        check={
            "pod_count": 4,
            "do_not_delete": 1,
        })
    chi = "test-017-multi-version"
    queries = [
        "CREATE TABLE test_max (epoch Int32, offset SimpleAggregateFunction(max, Int64)) ENGINE = AggregatingMergeTree() ORDER BY epoch",
        "insert into test_max select 0, 3650487030+number from numbers(5) settings max_block_size=1",
        "insert into test_max select 0, 5898217176+number from numbers(5)",
        "insert into test_max select 0, 5898217176+number from numbers(10) settings max_block_size=1",
        "OPTIMIZE TABLE test_max FINAL"]

    for q in queries:
        print(f"{q}")
    test_query = "select min(offset), max(offset) from test_max"
    print(f"{test_query}")

    for shard in range(4):
        host = f"chi-{chi}-default-{shard}-0"
        for q in queries:
            clickhouse.query(chi, host=host, sql=q)
        out = clickhouse.query(chi, host=host, sql=test_query)
        ver = clickhouse.query(chi, host=host, sql="select version()")

        print(f"version: {ver}, result: {out}")

    kubectl.delete_chi(chi)


@TestScenario
@Name("test-018-configmap. Test that configuration is properly updated")
def test_018():
    kubectl.create_and_check(
        config="configs/test-018-configmap.yaml",
        check={
            "pod_count": 1,
            "do_not_delete": 1,
        })
    chi_name = "test-018-configmap"

    with Then("user1/networks/ip should be in config"):
        chi = kubectl.get("chi", chi_name)
        assert "user1/networks/ip" in chi["spec"]["configuration"]["users"]

    start_time = kubectl.get_field("pod", f"chi-{chi_name}-default-0-0-0", ".status.startTime")

    kubectl.create_and_check(
        config="configs/test-018-configmap-2.yaml",
        check={
            "pod_count": 1,
            "do_not_delete": 1,
        })
    with Then("user2/networks should be in config"):
        chi = kubectl.get("chi", chi_name)
        assert "user2/networks/ip" in chi["spec"]["configuration"]["users"]
        with And("user1/networks/ip should NOT be in config"):
            assert "user1/networks/ip" not in chi["spec"]["configuration"]["users"]
        with And("Pod should not be restarted"):
            new_start_time = kubectl.get_field("pod", f"chi-{chi_name}-default-0-0-0", ".status.startTime")
            assert start_time == new_start_time

    kubectl.delete_chi(chi_name)


@TestScenario
@Name("test-019-retain-volume. Test that volume is correctly retained and can be re-attached")
def test_019(config="configs/test-019-retain-volume.yaml"):
    require_zookeeper()

    chi = manifest.get_chi_name(util.get_full_path(config))
    kubectl.create_and_check(
        config=config,
        check={
            "pod_count": 1,
            "do_not_delete": 1,
        })

    create_non_replicated_table = "create table t1 Engine = Log as select 1 as a"
    create_replicated_table = """
    create table t2 
    Engine = ReplicatedMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}', '{replica}')
    partition by tuple() order by a
    as select 1 as a""".replace('\r', '').replace('\n', '')

    with Given("ClickHouse has some data in place"):
        clickhouse.query(chi, sql=create_non_replicated_table)
        clickhouse.query(chi, sql=create_replicated_table)

    with When("CHI with retained volume is deleted"):
        pvc_count = kubectl.get_count("pvc")
        pv_count = kubectl.get_count("pv")

        kubectl.delete_chi(chi)

        with Then("PVC should be retained"):
            assert kubectl.get_count("pvc") == pvc_count
            assert kubectl.get_count("pv") == pv_count

    with When("Re-create CHI"):
        kubectl.create_and_check(
            config=config,
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            })

    with Then("PVC should be re-mounted"):
        with And("Non-replicated table should have data"):
            out = clickhouse.query(chi, sql="select a from t1")
            assert out == "1"
        with And("Replicated table should have data"):
            out = clickhouse.query(chi, sql="select a from t2")
            assert out == "1"

    kubectl.delete_chi(chi)


@TestScenario
@Name("test-020-multi-volume. Test multi-volume configuration")
def test_020(config="configs/test-020-multi-volume.yaml"):
    chi = manifest.get_chi_name(util.get_full_path(config))
    kubectl.create_and_check(
        config=config,
        check={
            "pod_count": 1,
            "pod_volumes": {
                "/var/lib/clickhouse",
                "/var/lib/clickhouse2",
            },
            "do_not_delete": 1,
        })

    with When("Create a table and insert 1 row"):
        clickhouse.query(chi, "create table test_disks(a Int8) Engine = MergeTree() order by a")
        clickhouse.query(chi, "insert into test_disks values (1)")

        with Then("Data should be placed on default disk"):
            out = clickhouse.query(chi, "select disk_name from system.parts where table='test_disks'")
            assert out == 'default'

    with When("alter table test_disks move partition tuple() to disk 'disk2'"):
        clickhouse.query(chi, "alter table test_disks move partition tuple() to disk 'disk2'")

        with Then("Data should be placed on disk2"):
            out = clickhouse.query(chi, "select disk_name from system.parts where table='test_disks'")
            assert out == 'disk2'

    kubectl.delete_chi(chi)


@TestScenario
@Name("test-021-rescale-volume. Test rescaling storage")
def test_021(config="configs/test-021-rescale-volume.yaml"):
    with Given("Default storage class is expandable"):
        default_storage_class = kubectl.get_default_storage_class()
        assert default_storage_class is not None
        assert len(default_storage_class) > 0
        allow_volume_expansion = kubectl.get_field("storageclass", default_storage_class, ".allowVolumeExpansion")
        if allow_volume_expansion != "true":
            kubectl.launch(f"patch storageclass {default_storage_class} -p '{{\"allowVolumeExpansion\":true}}'")

    chi = manifest.get_chi_name(util.get_full_path(config))
    kubectl.create_and_check(
        config=config,
        check={
            "pod_count": 1,
            "do_not_delete": 1,
        })

    with Then("Storage size should be 100Mi"):
        size = kubectl.get_pvc_size("disk1-chi-test-021-rescale-volume-simple-0-0-0")
        assert size == "100Mi"

    with When("Re-scale volume configuration to 200Mb"):
        kubectl.create_and_check(
            config="configs/test-021-rescale-volume-add-storage.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            })

        with Then("Storage size should be 200Mi"):
            size = kubectl.get_pvc_size("disk1-chi-test-021-rescale-volume-simple-0-0-0")
            assert size == "200Mi"

    with When("Add second disk 50Mi"):
        kubectl.create_and_check(
            config="configs/test-021-rescale-volume-add-disk.yaml",
            check={
                "pod_count": 1,
                "pod_volumes": {
                    "/var/lib/clickhouse",
                    "/var/lib/clickhouse2",
                },
                "do_not_delete": 1,
            })

        with Then("There should be two PVC"):
            size = kubectl.get_pvc_size("disk1-chi-test-021-rescale-volume-simple-0-0-0")
            assert size == "200Mi"
            size = kubectl.get_pvc_size("disk2-chi-test-021-rescale-volume-simple-0-0-0")
            assert size == "50Mi"

        with And("There should be two disks recognized by ClickHouse"):
            out = clickhouse.query(chi, "select count() from system.disks")
            assert out == "2"

    kubectl.delete_chi(chi)


@TestScenario
@Name("test-022-broken-image. Test broken image")
def test_022(config="configs/test-022-broken-image.yaml"):
    chi = manifest.get_chi_name(util.get_full_path(config))
    kubectl.create_and_check(
        config=config,
        check={
            "pod_count": 1,
            "do_not_delete": 1,
            "chi_status": "InProgress",
        })
    with When("ClickHouse image can not be retrieved"):
        kubectl.wait_field(
            "pod",
            "chi-test-022-broken-image-default-0-0-0",
            ".status.containerStatuses[0].state.waiting.reason",
            "ErrImagePull"
        )
        kubectl.delete_chi(chi)
