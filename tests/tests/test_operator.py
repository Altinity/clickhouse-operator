import time

import tests.clickhouse as clickhouse
import tests.kubectl as kubectl
import tests.manifest as manifest
import tests.settings as settings
import tests.util as util

from testflows.core import TestScenario, Name, When, Then, Given, And, main, Scenario, Module, TE
from testflows.asserts import error


@TestScenario
@Name("test_001. 1 node")
def test_001(self):
    kubectl.create_and_check(
        self.context.runner,
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
def test_002(self):
    kubectl.create_and_check(
        node=self.context.runner,
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
def test_003(self):
    kubectl.create_and_check(
        node=self.context.runner,
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
def test_004(self):
    kubectl.create_and_check(
        node=self.context.runner,
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
def test_005(self):
    kubectl.create_and_check(
        node=self.context.runner,
        config="configs/test-005-acm.yaml",
        check={
            "pod_count": 1,
            "pod_volumes": {
                "/var/lib/clickhouse",
            },
        },
        timeout=1200,
    )


@TestScenario
@Name("test_006. Test clickhouse version upgrade from one version to another using podTemplate change")
def test_006(self):
    old_version = "yandex/clickhouse-server:21.3"
    new_version = "altinity/clickhouse-server:21.8.altinity_prestable"
    with Then("Create initial position"):
        kubectl.create_and_check(
            node=self.context.runner,
            config="configs/test-006-ch-upgrade-1.yaml",
            check={
                "pod_count": 2,
                "pod_image": old_version,
                "do_not_delete": 1,
            }
        )
    with Then("Use different podTemplate and confirm that pod image is updated"):
        kubectl.create_and_check(
            node=self.context.runner,
            config="configs/test-006-ch-upgrade-2.yaml",
            check={
                "pod_count": 2,
                "pod_image": new_version,
                "do_not_delete": 1,
            }
        )
    with Then("Change image in podTemplate itself and confirm that pod image is updated"):
        kubectl.create_and_check(
            node=self.context.runner,
            config="configs/test-006-ch-upgrade-3.yaml",
            check={
                "pod_count": 2,
                "pod_image": old_version,
            }
        )


@TestScenario
@Name("test_007. Test template with custom clickhouse ports")
def test_007(self):
    kubectl.create_and_check(
        self.context.runner,
        config="configs/test-007-custom-ports.yaml",
        check={
            "pod_count": 1,
            "pod_ports": [8124, 9001, 9010],
        }
    )


def test_operator_upgrade(node, config, version_from, version_to=settings.operator_version):
    with Given(f"clickhouse-operator FROM {version_from}"):
        util.set_operator_version(node, version_from)
        chi = manifest.get_chi_name(util.get_full_path(config, True))

        kubectl.create_and_check(
            node,
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
        kubectl.wait_chi_status(node, chi, "Completed", retries=20)
        start_time = kubectl.get_field(node, "pod", f"chi-{chi}-{chi}-0-0-0", ".status.startTime")

        with Then("Create a table"):
            clickhouse.query(node, chi, "CREATE TABLE test_local Engine = Log as SELECT 1")

        with When(f"upgrade operator TO {version_to}"):
            util.set_operator_version(node, version_to, timeout=120)
            kubectl.wait_chi_status(node, chi, "Completed", retries=20)

            kubectl.wait_objects(node, chi, {"statefulset": 1, "pod": 1, "service": 2})

            with Then("Check that table is here"):
                tables = clickhouse.query(node, chi, "SHOW TABLES")
                assert "test_local" in tables
                out = clickhouse.query(node, chi, "SELECT * FROM test_local")
                assert out == "1"

            with Then("ClickHouse pods should not be restarted"):
                new_start_time = kubectl.get_field(node, "pod", f"chi-{chi}-{chi}-0-0-0", ".status.startTime")
                if start_time != new_start_time:
                    kubectl.launch(node, f"describe chi -n {settings.test_namespace} {chi}")
                    kubectl.launch(node,
                        f"logs -n {settings.test_namespace} pod/$(kubectl get pods -o name | grep clickhouse-operator) -c clickhouse-operator"
                    )
                assert start_time == new_start_time, error(f"{start_time} != {new_start_time}, pod restarted after operator upgrade")
        kubectl.delete_chi(node, chi)


def test_operator_restart(node  , config, version=settings.operator_version):
    with Given(f"clickhouse-operator {version}"):
        util.set_operator_version(node, version)
        chi = manifest.get_chi_name(util.get_full_path(config))
        cluster = chi

        kubectl.create_and_check(
            node,
            config=config,
            check={
                "object_counts": {
                    "statefulset": 1,
                    "pod": 1,
                    "service": 2,
                },
                "do_not_delete": 1,
            })
        time.sleep(10)
        start_time = kubectl.get_field(node, "pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")

        with When("Restart operator"):
            util.restart_operator(node)
            time.sleep(10)
            kubectl.wait_chi_status(node, chi, "Completed")
            kubectl.wait_objects(node, chi,
                {
                    "statefulset": 1,
                    "pod": 1,
                    "service": 2,
                })
            time.sleep(10)

            with Then("ClickHouse pods should not be restarted"):
                new_start_time = kubectl.get_field(node, "pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
                assert start_time == new_start_time

        kubectl.delete_chi(node, chi)


@TestScenario
@Name("test_008. Test operator restart")
def test_008(self):
    with Then("Test simple chi for operator restart"):
        test_operator_restart(self.context.runner, "configs/test-008-operator-restart-1.yaml")
    with Then("Test advanced chi for operator restart"):
        test_operator_restart(self.context.runner, "configs/test-008-operator-restart-2.yaml")


@TestScenario
@Name("test_009. Test operator upgrade")
def test_009(self, version_from="0.14.1", version_to=settings.operator_version):
    with Then("Test simple chi for operator upgrade"):
        test_operator_upgrade(self.context.runner, "configs/test-009-operator-upgrade-1.yaml", version_from, version_to)
    with Then("Test advanced chi for operator upgrade"):
        test_operator_upgrade(self.context.runner, "configs/test-009-operator-upgrade-2.yaml", version_from, version_to)


@TestScenario
@Name("test_010. Test zookeeper initialization")
def test_010(self):
    util.set_operator_version(self.context.runner, settings.operator_version)
    util.require_zookeeper(self.context.runner)

    kubectl.create_and_check(
        node=self.context.runner,
        config="configs/test-010-zkroot.yaml",
        check={
            "apply_templates": {
                settings.clickhouse_template,
            },
            "pod_count": 1,
            "do_not_delete": 1,
        }
    )
    time.sleep(10)
    with And("ClickHouse should complain regarding zookeeper path"):
        out = clickhouse.query_with_error(self.context.runner, "test-010-zkroot", "select * from system.zookeeper where path = '/'")
        assert "DB::Exception" in out, error()

    kubectl.delete_chi(self.context.runner, "test-010-zkroot")


@TestScenario
@Name("test_011. Test user security and network isolation")
def test_011(self):
    with Given("test-011-secured-cluster.yaml and test-011-insecured-cluster.yaml"):
        kubectl.create_and_check(
            node=self.context.runner,
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
            node=self.context.runner,
            config="configs/test-011-insecured-cluster.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            }
        )

        time.sleep(60)

        with Then("Connection to localhost should succeed with default user"):
            out = clickhouse.query_with_error(self.context.runner, "test-011-secured-cluster", "select 'OK'")
            assert out == 'OK', f"out={out} should be 'OK'"

        with And("Connection from secured to secured host should succeed"):
            out = clickhouse.query_with_error(
                self.context.runner,
                "test-011-secured-cluster",
                "select 'OK'",
                host="chi-test-011-secured-cluster-default-1-0"
            )
            assert out == 'OK'

        with And("Connection from insecured to secured host should fail for default"):
            out = clickhouse.query_with_error(
                self.context.runner,
                "test-011-insecured-cluster",
                "select 'OK'",
                host="chi-test-011-secured-cluster-default-1-0"
            )
            assert out != 'OK'

        with And("Connection from insecured to secured host should fail for user with no password"):
            time.sleep(10)  # FIXME
            out = clickhouse.query_with_error(
                self.context.runner,
                "test-011-insecured-cluster",
                "select 'OK'",
                host="chi-test-011-secured-cluster-default-1-0",
                user="user1"
            )
            assert "Password" in out or "password" in out

        with And("Connection from insecured to secured host should work for user with password"):
            out = clickhouse.query_with_error(
                self.context.runner,
                "test-011-insecured-cluster",
                "select 'OK'",
                host="chi-test-011-secured-cluster-default-1-0",
                user="user1",
                pwd="topsecret"
            )
            assert out == 'OK'

        with And("Password should be encrypted"):
            cfm = kubectl.get(self.context.runner, "configmap", "chi-test-011-secured-cluster-common-usersd")
            users_xml = cfm["data"]["chop-generated-users.xml"]
            assert "<password>" not in users_xml
            assert "<password_sha256_hex>" in users_xml

        with And("User with no password should get default automatically"):
            out = clickhouse.query_with_error(
                self.context.runner,
                "test-011-secured-cluster",
                "select 'OK'",
                user="user2",
                pwd="default"
            )
            assert out == 'OK'

        with And("User with both plain and sha256 password should get the latter one"):
            out = clickhouse.query_with_error(
                self.context.runner,
                "test-011-secured-cluster",
                "select 'OK'",
                user="user3",
                pwd="clickhouse_operator_password"
            )
            assert out == 'OK'

        with And("User with row-level security should have it applied"):
            out = clickhouse.query_with_error(
                self.context.runner,
                "test-011-secured-cluster",
                "select * from system.numbers limit 1",
                user="restricted",
                pwd="secret"
            )
            assert out == '1000'

        kubectl.delete_chi(self.context.runner, "test-011-secured-cluster")
        kubectl.delete_chi(self.context.runner, "test-011-insecured-cluster")


@TestScenario
@Name("test_011_1. Test default user security")
def test_011_1(self):
    with Given("test-011-secured-default.yaml with password_sha256_hex for default user"):
        kubectl.create_and_check(
            node=self.context.runner,
            config="configs/test-011-secured-default.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            }
        )

        with Then("Default user password should be '_removed_'"):
            chi = kubectl.get(self.context.runner, "chi", "test-011-secured-default")
            assert "default/password" in chi["status"]["normalized"]["configuration"]["users"]
            assert chi["status"]["normalized"]["configuration"]["users"]["default/password"] == "_removed_"

        with And("Connection to localhost should succeed with default user"):
            out = clickhouse.query_with_error(
                self.context.runner,
                "test-011-secured-default",
                "select 'OK'",
                pwd="clickhouse_operator_password"
            )
            assert out == 'OK'

        with When("Trigger installation update"):
            kubectl.create_and_check(
                node=self.context.runner,
                config="configs/test-011-secured-default-2.yaml",
                check={
                    "do_not_delete": 1,
                }
            )
            with Then("Default user password should be '_removed_'"):
                chi = kubectl.get(self.context.runner, "chi", "test-011-secured-default")
                assert "default/password" in chi["status"]["normalized"]["configuration"]["users"]
                assert chi["status"]["normalized"]["configuration"]["users"]["default/password"] == "_removed_"

        with When("Default user is assigned the different profile"):
            kubectl.create_and_check(
                node=self.context.runner,
                config="configs/test-011-secured-default-3.yaml",
                check={
                    "do_not_delete": 1,
                }
            )
            with Then("Wait until configmap is reloaded"):
                # Need to wait to make sure configuration is reloaded. For some reason it takes long here
                # Maybe we can restart the pod to speed it up
                time.sleep(120)
            with Then("Connection to localhost should succeed with default user"):
                out = clickhouse.query_with_error(
                    self.context.runner,
                    "test-011-secured-default",
                    "select 'OK'"
                )
                assert out == 'OK'

        kubectl.delete_chi(self.context.runner, "test-011-secured-default")


@TestScenario
@Name("test_012. Test service templates")
def test_012(self):
    kubectl.create_and_check(
        node=self.context.runner,
        config="configs/test-012-service-template.yaml",
        check={
            "object_counts": {
                "statefulset": 2,
                "pod": 2,
                "service": 4,
            },
            "do_not_delete": 1,
        }
    )
    with Then("There should be a service for chi"):
        kubectl.check_service(self.context.runner, "service-test-012", "LoadBalancer")
    with And("There should be a service for shard 0"):
        kubectl.check_service(self.context.runner, "service-test-012-0-0", "ClusterIP")
    with And("There should be a service for shard 1"):
        kubectl.check_service(self.context.runner, "service-test-012-1-0", "ClusterIP")
    with And("There should be a service for default cluster"):
        kubectl.check_service(self.context.runner, "service-default", "ClusterIP")

    node_port = kubectl.get(self.context.runner, "service", "service-test-012")["spec"]["ports"][0]["nodePort"]

    with Then("Update chi"):
        kubectl.create_and_check(
            self.context.runner,
            config="configs/test-012-service-template-2.yaml",
            check={
                "object_counts": {
                    "statefulset": 1,
                    "pod": 1,
                    "service": 3,
                },
                "do_not_delete": 1,
            }
        )

        with And("NodePort should not change"):
            new_node_port = kubectl.get(self.context.runner, "service", "service-test-012")["spec"]["ports"][0]["nodePort"]
            assert new_node_port == node_port, \
                f"LoadBalancer.spec.ports[0].nodePort changed from {node_port} to {new_node_port}"

    kubectl.delete_chi(self.context.runner, "test-012")


@TestScenario
@Name("test_013. Test adding shards and creating local and distributed tables automatically")
def test_013(self):
    config = "configs/test-013-add-shards-1.yaml"
    chi = manifest.get_chi_name(util.get_full_path(config))
    cluster = "default"

    kubectl.create_and_check(
        self.context.runner,
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
    start_time = kubectl.get_field(self.context.runner, "pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
    out = ""
    # wait for cluster to start
    for _ in range(20):
        time.sleep(10)
        out = clickhouse.query_with_error(self.context.runner, chi,
            "SELECT count() FROM cluster('all-sharded', system.one) settings receive_timeout=10")
        print("cluster out=")
        print(out)
        if out == "1":
            break
    else:
        assert out == "1"

    schema_objects = [
        'test_local',
        'test_distr',
        'events-distr',
    ]
    with Then("Create local and distributed tables"):
        clickhouse.query(self.context.runner, chi,
            "CREATE TABLE test_local Engine = Log as SELECT * FROM system.one")
        clickhouse.query(self.context.runner, chi,
            "CREATE TABLE test_distr as test_local Engine = Distributed('default', default, test_local)")
        clickhouse.query(self.context.runner, chi,
            "CREATE DATABASE \\\"test-db\\\"")
        clickhouse.query(self.context.runner, chi,
            "CREATE TABLE \\\"test-db\\\".\\\"events-distr\\\" as system.events "
            "ENGINE = Distributed('all-sharded', system, events)")

    with Then("Add shards"):
        kubectl.create_and_check(
            self.context.runner,
            config="configs/test-013-add-shards-2.yaml",
            check={
                "object_counts": {
                    "statefulset": 3,
                    "pod": 3,
                    "service": 4,
                },
                "do_not_delete": 1,
            },
            timeout=1500,
        )

    # wait for cluster to start
    out = ""
    for _ in range(20):
        time.sleep(10)
        out = clickhouse.query_with_error(self.context.runner, chi,
            "SELECT count() FROM cluster('all-sharded', system.one) settings receive_timeout=10")
        print("cluster out=")
        print(out)
        if out == "3":
            break
    else:
        assert out == "3"

    with Then("Unaffected pod should not be restarted"):
        new_start_time = kubectl.get_field(self.context.runner, "pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
        assert start_time == new_start_time

    with And("Schema objects should be migrated to new shards"):
        for obj in schema_objects:
            out = clickhouse.query(self.context.runner, chi,
                f"SELECT count() FROM system.tables WHERE name = '{obj}'",
                host=f"chi-{chi}-{cluster}-1-0"
            )
            assert out == "1"
            out = clickhouse.query(self.context.runner, chi,
                f"SELECT count() FROM system.tables WHERE name = '{obj}'",
                host=f"chi-{chi}-{cluster}-2-0"
            )
            assert out == "1"

    with When("Remove shards"):
        kubectl.create_and_check(
            self.context.runner,
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

        # wait for cluster to start
        for _ in range(20):
            time.sleep(10)
            out = clickhouse.query_with_error(self.context.runner, chi,
                "SELECT count() FROM cluster('all-sharded', system.one) settings receive_timeout=10")
            print("cluster out=")
            print(out)
            if out == "1":
                break
        else:
            assert out == "1"

        with Then("Unaffected pod should not be restarted"):
            new_start_time = kubectl.get_field(self.context.runner, "pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
            assert start_time == new_start_time

    kubectl.delete_chi(self.context.runner, chi)


@TestScenario
@Name("test_014. Test that replication works")
def test_014(self):
    util.require_zookeeper(self.context.runner)

    create_table = """
    CREATE TABLE test_local(a Int8)
    Engine = ReplicatedMergeTree('/clickhouse/{installation}/tables/{shard}/{database}/{table}', '{replica}')
    PARTITION BY tuple()
    ORDER BY a
    """.replace('\r', '').replace('\n', '')

    config = "configs/test-014-replication-1.yaml"
    chi = manifest.get_chi_name(util.get_full_path(config))
    cluster = "default"

    kubectl.create_and_check(
        self.context.runner,
        config=config,
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
        },
        timeout=600,
    )

    start_time = kubectl.get_field(self.context.runner, "pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")

    schema_objects = [
        'test_local',
        'test_view',
        'test_mv',
        'test_buffer',
        'a_view'
    ]
    with Given("Create schema objects"):
        clickhouse.query(self.context.runner, chi, create_table,
            host=f"chi-{chi}-{cluster}-0-0")
        clickhouse.query(self.context.runner, chi,
            "CREATE VIEW test_view as SELECT * from test_local",
            host=f"chi-{chi}-{cluster}-0-0")
        clickhouse.query(self.context.runner, chi,
            "CREATE VIEW a_view as SELECT * from test_view",
            host=f"chi-{chi}-{cluster}-0-0")
        clickhouse.query(self.context.runner, chi,
            "CREATE MATERIALIZED VIEW test_mv Engine = Log as SELECT * from test_local",
            host=f"chi-{chi}-{cluster}-0-0")
        clickhouse.query(self.context.runner, chi,
            "CREATE DICTIONARY test_dict (a Int8, b Int8) PRIMARY KEY a SOURCE(CLICKHOUSE(host 'localhost' port 9000 table 'test_local' user 'default')) LAYOUT(FLAT()) LIFETIME(0)",
            host=f"chi-{chi}-{cluster}-0-0")
        clickhouse.query(self.context.runner, chi,
            "CREATE TABLE test_buffer(a Int8) Engine = Buffer(default, test_local, 16, 10, 100, 10000, 1000000, 10000000, 100000000)",
            host=f"chi-{chi}-{cluster}-0-0")

    with Given("Replicated table is created on a first replica and data is inserted"):
        clickhouse.query(self.context.runner, chi,
            "INSERT INTO test_local values(1)",
            host=f"chi-{chi}-{cluster}-0-0")
        with When("Table is created on the second replica"):
            clickhouse.query(self.context.runner, chi, create_table,
                host=f"chi-{chi}-{cluster}-0-1")
            # Give some time for replication to catch up
            time.sleep(10)
            with Then("Data should be replicated"):
                out = clickhouse.query(self.context.runner, chi,
                    "SELECT a FROM test_local",
                    host=f"chi-{chi}-{cluster}-0-1")
                assert out == "1"

    with When("Add one more replica"):
        kubectl.create_and_check(
            self.context.runner,
            config="configs/test-014-replication-2.yaml",
            check={
                "pod_count": 3,
                "do_not_delete": 1,
            },
            timeout=600,
        )
        # Give some time for replication to catch up
        time.sleep(10)

        new_start_time = kubectl.get_field(self.context.runner, "pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
        assert start_time == new_start_time

        with Then("Schema objects should be migrated to the new replica"):
            for obj in schema_objects:
                out = clickhouse.query(self.context.runner, chi,
                    f"SELECT count() FROM system.tables WHERE name = '{obj}'",
                    host=f"chi-{chi}-{cluster}-0-2")
                assert out == "1"
            # Check dictionary
            out = clickhouse.query(self.context.runner, chi,
                    f"SELECT count() FROM system.dictionaries WHERE name = 'test_dict'",
                    host=f"chi-{chi}-{cluster}-0-2")
            assert out == "1"

        with And("Replicated table should have the data"):
            out = clickhouse.query(self.context.runner, chi,
                "SELECT a FROM test_local",
                host=f"chi-{chi}-{cluster}-0-2")
            assert out == "1"

    with When("Remove replica"):
        kubectl.create_and_check(
            self.context.runner,
            config=config,
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            })

        new_start_time = kubectl.get_field(self.context.runner, "pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
        assert start_time == new_start_time

        with Then("Replica needs to be removed from the ZooKeeper as well"):
            out = clickhouse.query(self.context.runner, chi,
                "SELECT total_replicas FROM system.replicas WHERE table='test_local'")
            print(f"Found {out} total replicas")
            assert out == "2"

    with When("Restart Zookeeper pod"):
        with Then("Delete Zookeeper pod"):
            kubectl.launch(self.context.runner, "delete pod zookeeper-0")
            time.sleep(1)

        with Then("Insert into the table while there is no Zookeeper -- table should be in readonly mode"):
            out = clickhouse.query_with_error(self.context.runner, chi, "INSERT INTO test_local values(2)")
            assert "Table is in readonly mode" in out

        with Then("Wait for Zookeeper pod to come back"):
            kubectl.wait_object(self.context.runner, "pod", "zookeeper-0")
            kubectl.wait_pod_status(self.context.runner, "zookeeper-0", "Running")

        with Then("Wait for ClickHouse to reconnect to Zookeeper and switch to read-write mode"):
            time.sleep(30)

        with Then("Table should be back to normal"):
            clickhouse.query(self.context.runner, chi, "INSERT INTO test_local values(3)")

    with When("Delete chi"):
        kubectl.delete_chi(self.context.runner, "test-014-replication")
        with Then("Tables should be deleted. We can test it re-creating the chi and checking ZooKeeper contents"):
            kubectl.create_and_check(
                self.context.runner,
                config=config,
                check={
                    "pod_count": 1,
                    "do_not_delete": 1,
                    })
            out = clickhouse.query(self.context.runner, chi,
                f"select count() from system.zookeeper where path ='/clickhouse/{chi}/tables/0/default'")
            print(f"Found {out} replicated tables in ZooKeeper")
            assert out == "0"

    kubectl.delete_chi(self.context.runner, "test-014-replication")

@TestScenario
@Name("test_015. Test circular replication with hostNetwork")
def test_015(self):
    kubectl.create_and_check(
        self.context.runner,
        config="configs/test-015-host-network.yaml",
        check={
            "pod_count": 2,
            "do_not_delete": 1,
        },
        timeout=600,
    )

    time.sleep(30)
    with Then("Query from one server to another one should work"):
        out = clickhouse.query(
            self.context.runner,
            "test-015-host-network",
            host="chi-test-015-host-network-default-0-0",
            port="10000",
            sql="SELECT * FROM remote('chi-test-015-host-network-default-0-1:11000', system.one)")
        print("remote out=")
        print(out)

    with Then("Distributed query should work"):
        for _ in range(20):
            time.sleep(10)
            out = clickhouse.query_with_error(
                self.context.runner,
                "test-015-host-network",
                host="chi-test-015-host-network-default-0-0",
                port="10000",
                sql="SELECT count() FROM cluster('all-sharded', system.one) settings receive_timeout=10")
            print("cluster out=")
            print(out)
            if out == "2":
                break
        else:
            assert out == "2"

    kubectl.delete_chi(self.context.runner, "test-015-host-network")


@TestScenario
@Name("test_016. Test advanced settings options")
def test_016(self):
    chi = "test-016-settings"
    kubectl.create_and_check(
        self.context.runner,
        config="configs/test-016-settings-01.yaml",
        check={
            "apply_templates": {
                settings.clickhouse_template,
            },
            "pod_count": 1,
            "do_not_delete": 1,
        },
    )

    with Then("Custom macro 'layer' should be available"):
        out = clickhouse.query(self.context.runner, chi, sql="select substitution from system.macros where macro='layer'")
        assert out == "01"

    with And("Custom macro 'test' should be available"):
        out = clickhouse.query(self.context.runner, chi,
                               sql="select substitution from system.macros where macro='test'")
        assert out == "test"

    with And("dictGet() should work"):
        out = clickhouse.query(self.context.runner, chi,
                               sql="select dictGet('one', 'one', toUInt64(0))")
        assert out == "0"

    with And("query_log should be disabled"):
        clickhouse.query(self.context.runner, chi, sql="system flush logs")
        out = clickhouse.query_with_error(self.context.runner, chi, sql="select count() from system.query_log")
        assert "doesn't exist" in out

    with And("max_memory_usage should be 7000000000"):
        out = clickhouse.query(self.context.runner, chi, sql="select value from system.settings where name='max_memory_usage'")
        assert out == "7000000000"

    with And("test_usersd user should be available"):
        clickhouse.query(self.context.runner, chi, sql="select version()", user="test_usersd")

    with And("user1 user should be available"):
        clickhouse.query(self.context.runner, chi, sql="select version()", user="user1", pwd="qwerty")

    with And("system.clusters should have a custom cluster"):
        out = clickhouse.query(self.context.runner, chi, sql="select count() from system.clusters where cluster='custom'")
        assert out == "1"

    # test-016-settings-02.yaml
    with When("Update usersd settings"):
        start_time = kubectl.get_field(self.context.runner, "pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
        kubectl.create_and_check(
            self.context.runner,
            config="configs/test-016-settings-02.yaml",
            check={
                "do_not_delete": 1,
            },
        )
        with Then("Wait for configmap changes to apply"):
            kubectl.wait_command(self.context.runner,
                f"exec chi-{chi}-default-0-0-0 -- bash -c \"grep test_norestart /etc/clickhouse-server/users.d/my_users.xml | wc -l\"",
                "1")

        with Then("Force reload config"):
            clickhouse.query(self.context.runner, chi, sql="SYSTEM RELOAD CONFIG")
        with Then("test_norestart user should be available"):
            version = clickhouse.query(self.context.runner, chi, sql="select version()", user="test_norestart")
        with And("user1 user should not be available"):
            version_user1 = clickhouse.query_with_error(self.context.runner, chi, sql="select version()", user="user1", pwd="qwerty")
            assert version != version_user1
        with And("user2 user should be available"):
            version_user2 = clickhouse.query(self.context.runner, chi, sql="select version()", user="user2", pwd="qwerty")
            assert version == version_user2
        with And("ClickHouse SHOULD NOT be restarted"):
            new_start_time = kubectl.get_field(self.context.runner, "pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
            assert start_time == new_start_time

    # test-016-settings-03.yaml
    with When("Update custom.xml settings"):
        start_time = kubectl.get_field(self.context.runner, "pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
        kubectl.create_and_check(
            self.context.runner,
            config="configs/test-016-settings-03.yaml",
            check={
                "do_not_delete": 1,
            })
        with Then("Wait for configmap changes to apply"):
            kubectl.wait_command(self.context.runner,
                f"exec chi-{chi}-default-0-0-0 -- bash -c \"grep test-03 /etc/clickhouse-server/config.d/custom.xml | wc -l\"",
                "1")

        with And("Custom macro 'test' should change the value"):
            out = clickhouse.query(self.context.runner, chi, sql="select substitution from system.macros where macro='test'")
            assert out == "test-03"

        with And("ClickHouse SHOULD BE restarted"):
            new_start_time = kubectl.get_field(self.context.runner, "pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
            assert start_time < new_start_time

    # test-016-settings-04.yaml
    with When("Add new custom2.xml config file"):
        start_time = kubectl.get_field(self.context.runner, "pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
        kubectl.create_and_check(
            self.context.runner,
            config="configs/test-016-settings-04.yaml",
            check={
                "do_not_delete": 1,
            })
        with Then("Wait for configmap changes to apply"):
            kubectl.wait_command(
                self.context.runner,
                f"exec chi-{chi}-default-0-0-0 -- bash -c \"grep test-custom2 /etc/clickhouse-server/config.d/custom2.xml | wc -l\"",
                "1")

        with And("Custom macro 'test-custom2' should be found"):
            out = clickhouse.query(self.context.runner, chi, sql="select substitution from system.macros where macro='test-custom2'")
            assert out == "test-custom2"

        with And("ClickHouse SHOULD BE restarted"):
            new_start_time = kubectl.get_field(self.context.runner, "pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
            assert start_time < new_start_time

    kubectl.delete_chi(self.context.runner, "test-016-settings")


@TestScenario
@Name("test_017. Test deployment of multiple versions in a cluster")
def test_017(self):
    pod_count = 2
    kubectl.create_and_check(
        self.context.runner,
        config="configs/test-017-multi-version.yaml",
        check={
            "pod_count": pod_count,
            "do_not_delete": 1,
        },
        timeout=600,
    )
    chi = "test-017-multi-version"
    queries = [
        "CREATE TABLE test_max (epoch Int32, offset SimpleAggregateFunction(max, Int64)) ENGINE = AggregatingMergeTree() ORDER BY epoch",
        "insert into test_max select 0, 3650487030+number from numbers(5) settings max_block_size=1",
        "insert into test_max select 0, 5898217176+number from numbers(5)",
        "insert into test_max select 0, 5898217176+number from numbers(10) settings max_block_size=1",
        "OPTIMIZE TABLE test_max FINAL",
    ]

    for q in queries:
        print(f"{q}")
    test_query = "select min(offset), max(offset) from test_max"
    print(f"{test_query}")

    for shard in range(pod_count):
        host = f"chi-{chi}-default-{shard}-0-0"
        for q in queries:
            clickhouse.query(self.context.runner, chi, host=host, sql=q)
        out = clickhouse.query(self.context.runner, chi, host=host, sql=test_query)
        ver = clickhouse.query(self.context.runner, chi, host=host, sql="select version()")

        print(f"version: {ver}, result: {out}")

    kubectl.delete_chi(self.context.runner, chi)


@TestScenario
@Name("test_018. Test that server settings are applied before statefulset is started")
# Obsolete, covered by test_016
def test_018(self):
    chi = "test-018-configmap"
    kubectl.create_and_check(
        self.context.runner,
        config="configs/test-018-configmap-1.yaml",
        check={
            "pod_count": 1,
            "do_not_delete": 1,
        },
    )

    with When("Update settings"):
        kubectl.create_and_check(
            self.context.runner,
            config="configs/test-018-configmap-2.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
                },
            )

        with Then("Configmap on the pod should be updated"):
            display_name = kubectl.launch(self.context.runner,
                f"exec chi-{chi}-default-0-0-0 -- bash -c \"grep display_name /etc/clickhouse-server/config.d/chop-generated-settings.xml\"")
            print(display_name)
            assert "new_display_name" in display_name
            with Then("And ClickHouse should pick them up"):
                macros = clickhouse.query(self.context.runner, chi, "SELECT substitution from system.macros where macro = 'test'")
                print(macros)
                assert "new_test" == macros

    kubectl.delete_chi(self.context.runner, chi)


@TestScenario
@Name("test_019. Test that volume is correctly retained and can be re-attached")
def test_019(self):
    util.require_zookeeper(self.context.runner)

    config="configs/test-019-retain-volume-1.yaml"
    chi = manifest.get_chi_name(util.get_full_path(config))
    kubectl.create_and_check(
        self.context.runner,
        config=config,
        check={
            "pod_count": 1,
            "do_not_delete": 1,
        },
    )

    create_non_replicated_table = "drop table if exists t1; create table t1 Engine = Log as select 1 as a"
    create_replicated_table = """
    drop table if exists t2;
    create table t2
    Engine = ReplicatedMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}', '{replica}')
    partition by tuple() order by a
    as select 1 as a""".replace('\r', '').replace('\n', '')

    with Given("ClickHouse has some data in place"):
        clickhouse.query(self.context.runner, chi, sql=create_non_replicated_table)
        clickhouse.query(self.context.runner, chi, sql=create_replicated_table)

    with When("CHI with retained volume is deleted"):
        pvc_count = kubectl.get_count(self.context.runner, "pvc")
        pv_count = kubectl.get_count(self.context.runner, "pv")

        kubectl.delete_chi(self.context.runner, chi)

        with Then("PVC should be retained"):
            assert kubectl.get_count(self.context.runner, "pvc") == pvc_count
            assert kubectl.get_count(self.context.runner, "pv") == pv_count

    with When("Re-create CHI"):
        kubectl.create_and_check(
            self.context.runner,
            config=config,
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

    with Then("PVC should be re-mounted"):
        with Then("Non-replicated table should have data"):
            out = clickhouse.query(self.context.runner, chi, sql="select a from t1")
            assert out == "1"
        with And("Replicated table should have data"):
            out = clickhouse.query(self.context.runner, chi, sql="select a from t2")
            assert out == "1"

    with Then("Stop the Installation"):
        kubectl.create_and_check(
            self.context.runner,
            config="configs/test-019-retain-volume-2.yaml",
            check={
                "object_counts": {
                    "statefulset": 1,  # When stopping, pod is removed but statefulset and all volumes are in place
                    "pod": 0,
                    "service": 1,  # load balancer service should be removed
                },
                "do_not_delete": 1,
                },
            )

    with Then("Re-start the Installation"):
        kubectl.create_and_check(
            self.context.runner,
            config="configs/test-019-retain-volume-1.yaml",
            check={
                "object_counts": {
                    "statefulset": 1,
                    "pod": 1,
                    "service": 2,  # load balancer service should be back
                },
                "do_not_delete": 1,
                },
            )

    with Then("Data should be in place"):
        with Then("Non-replicated table should have data"):
            out = clickhouse.query(self.context.runner, chi, sql="select a from t1")
            assert out == "1"
        with And("Replicated table should have data"):
            out = clickhouse.query(self.context.runner, chi, sql="select a from t2")
            assert out == "1"

    with When("Add a second replica"):
        kubectl.create_and_check(
            self.context.runner,
            config="configs/test-019-retain-volume-3.yaml",
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )
        with Then("Replicated table should have two replicas now"):
            out = clickhouse.query(self.context.runner, chi, sql="select total_replicas from system.replicas where table='t2'")
            assert out == "2"

        with When("Remove a replica"):
            pvc_count = kubectl.get_count(self.context.runner, "pvc")
            pv_count = kubectl.get_count(self.context.runner, "pv")

            kubectl.create_and_check(
                self.context.runner,
                config="configs/test-019-retain-volume-1.yaml",
                check={
                    "pod_count": 1,
                    "do_not_delete": 1,
                },
            )
            with Then("Replica PVC should be retained"):
                assert kubectl.get_count(self.context.runner, "pvc") == pvc_count
                assert kubectl.get_count(self.context.runner, "pv") == pv_count

            with And("Replica should NOT be removed from ZooKeeper"):
                out = clickhouse.query(self.context.runner, chi, sql="select total_replicas from system.replicas where table='t2'")
                assert out == "2"

    with When("Add a second replica one more time"):
        kubectl.create_and_check(
            self.context.runner,
            config="configs/test-019-retain-volume-3.yaml",
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )

        with Then("Table should have data"):
            out = clickhouse.query(self.context.runner, chi, sql="select a from t2", host=f"chi-{chi}-simple-0-1")
            assert out == "1"

        with When("Set reclaim policy to Delete but do not wait for completion"):
            kubectl.create_and_check(
                self.context.runner,
                config="configs/test-019-retain-volume-4.yaml",
                check={
                    "pod_count": 2,
                    "do_not_delete": 1,
                    # "chi_status": "InProgress",
                },
            )

            with And("Remove a replica"):
                pvc_count = kubectl.get_count(self.context.runner, "pvc")
                pv_count = kubectl.get_count(self.context.runner, "pv")

                kubectl.create_and_check(
                    self.context.runner,
                    config="configs/test-019-retain-volume-1.yaml",
                    check={
                        "pod_count": 1,
                        "do_not_delete": 1,
                    },
                )
                with Then("Replica PVC sbould be deleted"):
                    assert kubectl.get_count(self.context.runner, "pvc") < pvc_count
                    assert kubectl.get_count(self.context.runner, "pv") < pv_count

                with And("Replica should be removed from ZooKeeper"):
                    out = clickhouse.query(self.context.runner, chi, sql="select total_replicas from system.replicas where table='t2'")
                    assert out == "1"

    kubectl.delete_chi(self.context.runner, chi)


@TestScenario
@Name("test_020. Test multi-volume configuration")
def test_020(self):
    config="configs/test-020-multi-volume.yaml"
    chi = manifest.get_chi_name(util.get_full_path(config))
    kubectl.create_and_check(
        self.context.runner,
        config=config,
        check={
            "pod_count": 1,
            "pod_volumes": {
                "/var/lib/clickhouse",
                "/var/lib/clickhouse2",
            },
            "do_not_delete": 1,
        },
    )

    with When("Create a table and insert 1 row"):
        clickhouse.query(self.context.runner, chi, "create table test_disks(a Int8) Engine = MergeTree() order by a")
        clickhouse.query(self.context.runner, chi, "insert into test_disks values (1)")

        with Then("Data should be placed on default disk"):
            out = clickhouse.query(self.context.runner, chi, "select disk_name from system.parts where table='test_disks'")
            assert out == 'default'

    with When("alter table test_disks move partition tuple() to disk 'disk2'"):
        clickhouse.query(self.context.runner, chi, "alter table test_disks move partition tuple() to disk 'disk2'")

        with Then("Data should be placed on disk2"):
            out = clickhouse.query(self.context.runner, chi, "select disk_name from system.parts where table='test_disks'")
            assert out == 'disk2'

    kubectl.delete_chi(self.context.runner, chi)


@TestScenario
@Name("test_021. Test rescaling storage")
def test_021(self):
    config = "configs/test-021-rescale-volume-01.yaml"

    with Given("Default storage class is expandable"):
        default_storage_class = kubectl.get_default_storage_class(self.context.runner)
        assert default_storage_class is not None
        assert len(default_storage_class) > 0
        allow_volume_expansion = kubectl.get_field(self.context.runner, "storageclass", default_storage_class, ".allowVolumeExpansion")
        if allow_volume_expansion != "true":
            kubectl.launch(self.context.runner, f"patch storageclass {default_storage_class} -p '{{\"allowVolumeExpansion\":true}}'")

    chi = manifest.get_chi_name(util.get_full_path(config))
    kubectl.create_and_check(
        self.context.runner,
        config=config,
        check={
            "pod_count": 1,
            "do_not_delete": 1,
        },
    )

    with Then("Storage size should be 1Gi"):
        size = kubectl.get_pvc_size(self.context.runner, "disk1-chi-test-021-rescale-volume-simple-0-0-0")
        assert size == "1Gi"

    with Then("Create a table with a single row"):
        clickhouse.query(self.context.runner, chi, "CREATE TABLE test_local Engine = Log as SELECT 1 AS wtf")

    with When("Re-scale volume configuration to 2Gi"):
        kubectl.create_and_check(
            self.context.runner,
            config="configs/test-021-rescale-volume-02-enlarge-disk.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("Storage size should be 2Gi"):
            kubectl.wait_field(self.context.runner, "pvc", "disk1-chi-test-021-rescale-volume-simple-0-0-0", ".spec.resources.requests.storage", "2Gi")
            size = kubectl.get_pvc_size(self.context.runner, "disk1-chi-test-021-rescale-volume-simple-0-0-0")
            assert size == "2Gi"

        with And("Table should exist"):
            out = clickhouse.query(self.context.runner, chi, "select * from test_local")
            assert out == "1"

    with When("Add a second disk"):
        kubectl.create_and_check(
            self.context.runner,
            config="configs/test-021-rescale-volume-03-add-disk.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )
        # Adding new volume takes time, so pod_volumes check does not work

        with Then("There should be two PVC"):
            size = kubectl.get_pvc_size(self.context.runner, "disk1-chi-test-021-rescale-volume-simple-0-0-0")
            assert size == "2Gi"
            kubectl.wait_object(self.context.runner, "pvc", "disk2-chi-test-021-rescale-volume-simple-0-0-0")
            kubectl.wait_field(self.context.runner, "pvc", "disk2-chi-test-021-rescale-volume-simple-0-0-0", ".status.phase", "Bound")
            size = kubectl.get_pvc_size(self.context.runner, "disk2-chi-test-021-rescale-volume-simple-0-0-0")
            assert size == "1Gi"

        with And("There should be two disks recognized by ClickHouse"):
            kubectl.wait_pod_status(self.context.runner, "chi-test-021-rescale-volume-simple-0-0-0", "Running")
            # ClickHouse requires some time to mount volume. Race conditions.
            # TODO: wait for proper pod state and check the liveness probe probably. This is better than waiting
            out = ""
            for i in range(8):
                out = clickhouse.query(self.context.runner, chi, "SELECT count() FROM system.disks")
                if out == "2":
                    break
                with Then(f"Not ready yet. Wait for {1<<i} seconds"):
                    time.sleep(1 << i)
            assert out == "2"

        with And("Table should exist"):
            out = clickhouse.query(self.context.runner, chi, "select * from test_local")
            assert out == "1"

    with When("Try reducing the disk size and also change a version to recreate the stateful set"):
        kubectl.create_and_check(
            self.context.runner,
            config="configs/test-021-rescale-volume-04-decrease-disk.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("Storage size should be unchanged 2Gi"):
            size = kubectl.get_pvc_size(self.context.runner, "disk1-chi-test-021-rescale-volume-simple-0-0-0")
            assert size == "2Gi"

        with And("Table should exist"):
            out = clickhouse.query(self.context.runner, chi, "select * from test_local")
            assert out == "1"

        with And("PVC status should not be Terminating"):
            status = kubectl.get_field(self.context.runner, "pvc", "disk2-chi-test-021-rescale-volume-simple-0-0-0", ".status.phase")
            assert status != "Terminating"

    with When("Revert disk size back to 2Gi"):
        kubectl.create_and_check(
            self.context.runner,
            config="configs/test-021-rescale-volume-03-add-disk.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("Storage size should be 2Gi"):
            kubectl.wait_field(self.context.runner, "pvc", "disk1-chi-test-021-rescale-volume-simple-0-0-0", ".spec.resources.requests.storage", "2Gi")
            size = kubectl.get_pvc_size(self.context.runner, "disk1-chi-test-021-rescale-volume-simple-0-0-0")
            assert size == "2Gi"

        with And("Table should exist"):
            out = clickhouse.query(self.context.runner, chi, "select * from test_local")
            assert out == "1"

    kubectl.delete_chi(self.context.runner, chi)


@TestScenario
@Name("test_022. Test that chi with broken image can be deleted")
def test_022(self):
    config="configs/test-022-broken-image.yaml"
    chi = manifest.get_chi_name(util.get_full_path(config))
    kubectl.create_and_check(
        self.context.runner,
        config=config,
        check={
            "pod_count": 1,
            "do_not_delete": 1,
            "chi_status": "InProgress",
        },
    )
    with When("ClickHouse image can not be retrieved"):
        kubectl.wait_field(
            self.context.runner,
            "pod",
            "chi-test-022-broken-image-default-0-0-0",
            ".status.containerStatuses[0].state.waiting.reason",
            "ErrImagePull"
        )
        with Then("CHI should be able to delete"):
            kubectl.launch(self.context.runner, f"delete chi {chi}", ok_to_fail=True, timeout=600)
            assert kubectl.get_count(self.context.runner, "chi", f"{chi}") == 0


@TestScenario
@Name("test_023. Test auto templates")
def test_023(self):
    chit_data = manifest.get_chit_data(util.get_full_path("templates/tpl-clickhouse-auto.yaml"))
    expected_image=chit_data['spec']['templates']['podTemplates'][0]['spec']['containers'][0]['image']
    kubectl.create_and_check(
        self.context.runner,
        config="configs/test-001.yaml",
        check={
            "pod_count": 1,
            "apply_templates": {
                settings.clickhouse_template,
                "templates/tpl-clickhouse-auto.yaml",
            },
            # test-001.yaml does not have a template reference but should get correct ClickHouse version
            "pod_image": expected_image,
        }
    )

    kubectl.launch(self.context.runner, f"delete chit {chit_data['metadata']['name']}")


@TestScenario
@Name("test_024. Test annotations for various template types")
def test_024(self):
    config="configs/test-024-template-annotations.yaml"
    chi = manifest.get_chi_name(util.get_full_path(config))
    kubectl.create_and_check(
        self.context.runner,
        config=config,
        check={
            "pod_count": 1,
            "do_not_delete": 1,
        },
    )

    with Then("Pod annotations should be populated"):
        assert kubectl.get_field(self.context.runner, "pod", "chi-test-024-default-0-0-0", ".metadata.annotations.test") == "test"
    with And("Service annotations should be populated"):
        assert kubectl.get_field(self.context.runner, "service", "clickhouse-test-024", ".metadata.annotations.test") == "test"
    with And("PVC annotations should be populated"):
        assert kubectl.get_field(self.context.runner, "pvc", "-l clickhouse.altinity.com/chi=test-024", ".metadata.annotations.test") == "test"

    kubectl.delete_chi(self.context.runner, chi)


@TestScenario
@Name("test_025. Test that service is available during re-scalaling, upgades etc.")
def test_025(self):
    util.require_zookeeper(self.context.runner)

    create_table = """
    CREATE TABLE test_local(a UInt32)
    Engine = ReplicatedMergeTree('/clickhouse/{installation}/tables/{shard}/{database}/{table}', '{replica}')
    PARTITION BY tuple()
    ORDER BY a
    """.replace('\r', '').replace('\n', '')

    config = "configs/test-025-rescaling.yaml"
    chi = manifest.get_chi_name(util.get_full_path(config))

    kubectl.create_and_check(
        self.context.runner,
        config=config,
        check={
            "apply_templates": {
                settings.clickhouse_template,
                "templates/tpl-persistent-volume-100Mi.yaml",
            },
            "object_counts": {
                "statefulset": 1,
                "pod": 1,
                "service": 2,
            },
            "do_not_delete": 1,
        },
        timeout=600,
    )

    kubectl.wait_jsonpath(self.context.runner, "pod", "chi-test-025-rescaling-default-0-0-0", "{.status.containerStatuses[0].ready}", "true",
                          ns=kubectl.namespace)

    numbers = "100000000"

    with Given("Create replicated table and populate it"):
        clickhouse.query(self.context.runner, chi, create_table)
        clickhouse.query(self.context.runner, chi, "CREATE TABLE test_distr as test_local Engine = Distributed('default', default, test_local)")
        clickhouse.query(self.context.runner, chi, f"INSERT INTO test_local select * from numbers({numbers})", timeout=120)

    with When("Add one more replica, but do not wait for completion"):
        kubectl.create_and_check(
            self.context.runner,
            config="configs/test-025-rescaling-2.yaml",
            check={
                "do_not_delete": 1,
                "pod_count": 2,
                "chi_status": "InProgress",  # do not wait
            },
            timeout=600,
        )

    with Then("Query second pod using service as soon as pod is in ready state"):
        kubectl.wait_field(
            self.context.runner,
            "pod", "chi-test-025-rescaling-default-0-1-0",
            ".metadata.labels.\"clickhouse\\.altinity\\.com/ready\"", "yes",
            backoff=1
        )
        start_time = time.time()
        lb_error_time = start_time
        distr_lb_error_time = start_time
        latent_replica_time = start_time
        for i in range(1, 100):
            cnt_local    = clickhouse.query_with_error(self.context.runner, chi, "select count() from test_local", "chi-test-025-rescaling-default-0-1.test.svc.cluster.local")
            cnt_lb       = clickhouse.query_with_error(self.context.runner, chi, "select count() from test_local")
            cnt_distr_lb = clickhouse.query_with_error(self.context.runner, chi, "select count() from test_distr")
            if "Exception" in cnt_lb or cnt_lb == 0:
                lb_error_time = time.time()
            if "Exception" in cnt_distr_lb or cnt_distr_lb == 0:
                distr_lb_error_time = time.time()
            print(f"local via loadbalancer: {cnt_lb}, distributed via loadbalancer: {cnt_distr_lb}")
            if "Exception" not in cnt_local:
                print(f"local: {cnt_local}, distr: {cnt_distr_lb}")
                if cnt_local == numbers:
                    break
                latent_replica_time = time.time()
                print("Replicated table did not catch up")
            print("Waiting 1 second.")
            time.sleep(1)
        print(f"Tables not ready: {round(distr_lb_error_time - start_time)}s, data not ready: {round(latent_replica_time - distr_lb_error_time)}s")

        with Then("Query to the distributed table via load balancer should never fail"):
            assert round(distr_lb_error_time - start_time) == 0
        with And("Query to the local table via load balancer should never fail"):
            assert round(lb_error_time - start_time) == 0

    kubectl.delete_chi(self.context.runner, chi)

@TestScenario
@Name("test_026. Test mixed single and multi-volume configuration in one cluster")
def test_026(self):
    util.require_zookeeper(self.context.runner)

    config="configs/test-026-mixed-replicas.yaml"
    chi = manifest.get_chi_name(util.get_full_path(config))
    kubectl.create_and_check(
        self.context.runner,
        config=config,
        check={
            "pod_count": 2,
            "do_not_delete": 1,
        },
    )

    with When("Cluster is ready"):
        with Then("Check that first replica has one disk"):
            out = clickhouse.query(self.context.runner, chi, host="chi-test-026-mixed-replicas-default-0-0", sql="select count() from system.disks")
            assert out == "1"

        with And("Check that second replica has two disks"):
            out = clickhouse.query(self.context.runner, chi, host="chi-test-026-mixed-replicas-default-0-1", sql="select count() from system.disks")
            assert out == "2"

    with When("Create a table and generate several inserts"):
        clickhouse.query(self.context.runner, chi,
            sql="create table test_disks ON CLUSTER '{cluster}' (a Int64) Engine = ReplicatedMergeTree('/clickhouse/{installation}/tables/{shard}/{database}/{table}', '{replica}') partition by (a%10) order by a"
        )
        clickhouse.query(
            self.context.runner, chi, host="chi-test-026-mixed-replicas-default-0-0",
            sql="insert into test_disks select * from numbers(100) settings max_block_size=1"
        )
        clickhouse.query(
            self.context.runner, chi, host="chi-test-026-mixed-replicas-default-0-0",
            sql="insert into test_disks select * from numbers(100) settings max_block_size=1"
        )
        time.sleep(5)

        with Then("Data should be placed on a single disk on a first replica"):
            out = clickhouse.query(
                self.context.runner, chi, host="chi-test-026-mixed-replicas-default-0-0",
                sql="select arraySort(groupUniqArray(disk_name)) from system.parts where table='test_disks'"
            )
            assert out == "['default']"

        with And("Data should be placed on a second disk on a second replica"):
            out = clickhouse.query(
                self.context.runner, chi, host="chi-test-026-mixed-replicas-default-0-1",
                sql="select arraySort(groupUniqArray(disk_name)) from system.parts where table='test_disks'"
            )
            assert out == "['disk2']"

    kubectl.delete_chi(self.context.runner, chi)

@TestScenario
@Name("test_027. Test troubleshooting mode")
def test_027(self):
    config = "configs/test-027-troubleshooting-1-bad-config.yaml"
    chi = manifest.get_chi_name(util.get_full_path(config))
    kubectl.create_and_check(
        self.context.runner,
        config=config,
        check={
            "pod_count": 1,
            "do_not_delete": 1,
            "chi_status": "InProgress",
        },
    )
    with When("ClickHouse can not start"):
        kubectl.wait_field(
            self.context.runner,
            "pod",
            "chi-test-027-trouble-default-0-0-0",
            ".status.containerStatuses[0].state.waiting.reason",
            "CrashLoopBackOff"
        )
        with Then("We can start in troubleshooting mode"):
            kubectl.create_and_check(
                self.context.runner,
                config="configs/test-027-troubleshooting-2-troubleshoot.yaml",
                check={
                    "pod_count": 1,
                    "do_not_delete": 1,
                },
            )
            with And("We can exec to the pod"):
                out = kubectl.launch(self.context.runner, f"exec chi-{chi}-default-0-0-0 -- bash -c \"echo Success\"")
                assert "Success" == out

        with Then("We can start in normal mode after correcting the problem"):
            kubectl.create_and_check(
                self.context.runner,
                config="configs/test-027-troubleshooting-3-fixed-config.yaml",
                check={
                    "pod_count": 1,
                },
            )
