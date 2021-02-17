import time

import clickhouse
import kubectl
import settings
import util
import manifest

from testflows.core import TestScenario, Name, When, Then, Given, And, main, Scenario, Module, TE
from testflows.asserts import error


@TestScenario
@Name("test_001. 1 node")
def test_001(self):
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
def test_002(self):
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
def test_003(self):
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
def test_004(self):
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
def test_005(self):
    kubectl.create_and_check(
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
    old_version = "yandex/clickhouse-server:20.8.6.6"
    new_version = "yandex/clickhouse-server:20.8.7.15"
    with Then("Create initial position"):
        kubectl.create_and_check(
            config="configs/test-006-ch-upgrade-1.yaml",
            check={
                "pod_count": 2,
                "pod_image": old_version,
                "do_not_delete": 1,
            }
        )
    time.sleep(60)
    with Then("Use different podTemplate and confirm that pod image is updated"):
        kubectl.create_and_check(
            config="configs/test-006-ch-upgrade-2.yaml",
            check={
                "pod_count": 2,
                "pod_image": new_version,
                "do_not_delete": 1,
            }
        )
    time.sleep(60)
    with Then("Change image in podTemplate itself and confirm that pod image is updated"):
        kubectl.create_and_check(
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
            time.sleep(10)
            kubectl.wait_chi_status(chi, "Completed", retries=6)
            kubectl.wait_objects(chi, {"statefulset": 1, "pod": 1, "service": 2})
            with Then("ClickHouse pods should not be restarted"):
                new_start_time = kubectl.get_field("pod", f"chi-{chi}-{chi}-0-0-0", ".status.startTime")
                assert start_time == new_start_time

        kubectl.delete_chi(chi)


def test_operator_restart(config, version=settings.operator_version):
    with Given(f"clickhouse-operator {version}"):
        set_operator_version(version)
        config = util.get_full_path(config)
        chi = manifest.get_chi_name(config)
        cluster = chi

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
        time.sleep(10)
        start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")

        with When("Restart operator"):
            restart_operator()
            time.sleep(10)
            kubectl.wait_chi_status(chi, "Completed")
            kubectl.wait_objects(
                chi,
                {
                    "statefulset": 1,
                    "pod": 1,
                    "service": 2,
                })
            time.sleep(10)
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
            assert start_time == new_start_time

        kubectl.delete_chi(chi)


@TestScenario
@Name("test_008. Test operator restart")
def test_008(self):
    with Then("Test simple chi for operator restart"):
        test_operator_restart("configs/test-008-operator-restart-1.yaml")
    with Then("Test advanced chi for operator restart"):
        test_operator_restart("configs/test-008-operator-restart-2.yaml")


@TestScenario
@Name("test_009. Test operator upgrade")
def test_009(self, version_from="0.11.0", version_to=settings.operator_version):
    with Then("Test simple chi for operator upgrade"):
        test_operator_upgrade("configs/test-009-operator-upgrade-1.yaml", version_from, version_to)
    with Then("Test advanced chi for operator upgrade"):
        test_operator_upgrade("configs/test-009-operator-upgrade-2.yaml", version_from, version_to)


def set_operator_version(version, ns=settings.operator_namespace, timeout=60):
    operator_image = f"{settings.operator_docker_repo}:{version}"
    metrics_exporter_image = f"{settings.metrics_exporter_docker_repo}:{version}"
    kubectl.launch(f"set image deployment.v1.apps/clickhouse-operator clickhouse-operator={operator_image}", ns=ns)
    kubectl.launch(f"set image deployment.v1.apps/clickhouse-operator metrics-exporter={metrics_exporter_image}", ns=ns)
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
def test_010(self):
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
    time.sleep(10)
    with And("ClickHouse should complain regarding zookeeper path"):
        out = clickhouse.query_with_error("test-010-zkroot", "select * from system.zookeeper where path = '/'")
        assert "Received exception from server" in out, error()

    kubectl.delete_chi("test-010-zkroot")


@TestScenario
@Name("test_011. Test user security and network isolation")
def test_011(self):
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

        time.sleep(60)

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
def test_011_1(self):
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
            with Then("Wait until configmap is reloaded"):
                # Need to wait to make sure configuration is reloaded. For some reason it takes long here
                # Maybe we can restart the pod to speed it up
                time.sleep(120)
            with Then("Connection to localhost should succeed with default user"):
                out = clickhouse.query_with_error(
                    "test-011-secured-default",
                    "select 'OK'"
                )
                assert out == 'OK'

        kubectl.delete_chi("test-011-secured-default")


@TestScenario
@Name("test_012. Test service templates")
def test_012(self):
    kubectl.create_and_check(
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
        kubectl.check_service("service-test-012", "LoadBalancer")
    with And("There should be a service for shard 0"):
        kubectl.check_service("service-test-012-0-0", "ClusterIP")
    with And("There should be a service for shard 1"):
        kubectl.check_service("service-test-012-1-0", "ClusterIP")
    with And("There should be a service for default cluster"):
        kubectl.check_service("service-default", "ClusterIP")

    node_port = kubectl.get("service", "service-test-012")["spec"]["ports"][0]["nodePort"]

    with Then("Update chi"):
        kubectl.create_and_check(
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
            new_node_port = kubectl.get("service", "service-test-012")["spec"]["ports"][0]["nodePort"]
            assert new_node_port == node_port, \
                f"LoadBalancer.spec.ports[0].nodePort changed from {node_port} to {new_node_port}"

    kubectl.delete_chi("test-012")


@TestScenario
@Name("test_013. Test adding shards and creating local and distributed tables automatically")
def test_013(self):
    config = "configs/test-013-add-shards-1.yaml"
    chi = manifest.get_chi_name(util.get_full_path(config))
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

    # wait for cluster to start
    for i in range(20):
        time.sleep(10)
        out = clickhouse.query_with_error(
            chi,
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
            },
            timeout=1500,
        )

    # wait for cluster to start
    for i in range(20):
        time.sleep(10)
        out = clickhouse.query_with_error(
            chi,
            "SELECT count() FROM cluster('all-sharded', system.one) settings receive_timeout=10")
        print("cluster out=")
        print(out)
        if out == "3":
            break
    else:
        assert out == "3"

    with Then("Unaffected pod should not be restarted"):
        new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
        assert start_time == new_start_time

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

    with When("Remove shards"):
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

        # wait for cluster to start
        for i in range(20):
            time.sleep(10)
            out = clickhouse.query_with_error(
                chi,
                "SELECT count() FROM cluster('all-sharded', system.one) settings receive_timeout=10")
            print("cluster out=")
            print(out)
            if out == "1":
                break
        else:
            assert out == "1"

        with Then("Unaffected pod should not be restarted"):
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
            assert start_time == new_start_time

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_014. Test that replication works")
def test_014(self):
    require_zookeeper()

    create_table = """
    CREATE TABLE test_local(a Int8) 
    Engine = ReplicatedMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}', '{replica}')
    PARTITION BY tuple() 
    ORDER BY a
    """.replace('\r', '').replace('\n', '')

    config = "configs/test-014-replication-1.yaml"
    chi = manifest.get_chi_name(util.get_full_path(config))
    cluster = "default"

    kubectl.create_and_check(
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

    start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")

    schema_objects = [
        'test_local',
        'test_view',
        'test_mv',
        'a_view'
    ]
    with Given("Create schema objects"):
        clickhouse.query(
            chi,
            create_table,
            host=f"chi-{chi}-{cluster}-0-0")
        clickhouse.query(
            chi,
            "CREATE VIEW test_view as SELECT * from test_local",
            host=f"chi-{chi}-{cluster}-0-0")
        clickhouse.query(
            chi,
            "CREATE VIEW a_view as SELECT * from test_view",
            host=f"chi-{chi}-{cluster}-0-0")
        clickhouse.query(
            chi,
            "CREATE MATERIALIZED VIEW test_mv Engine = Log as SELECT * from test_local",
            host=f"chi-{chi}-{cluster}-0-0")
        clickhouse.query(
            chi,
            "CREATE DICTIONARY test_dict (a Int8, b Int8) PRIMARY KEY a SOURCE(CLICKHOUSE(host 'localhost' port 9000 table 'test_local' user 'default')) LAYOUT(FLAT()) LIFETIME(0)",
            host=f"chi-{chi}-{cluster}-0-0")

    with Given("Replicated table is created on a first replica and data is inserted"):
        clickhouse.query(
            chi,
            "INSERT INTO test_local values(1)",
            host=f"chi-{chi}-{cluster}-0-0")
        with When("Table is created on the second replica"):
            clickhouse.query(
                chi,
                create_table,
                host=f"chi-{chi}-{cluster}-0-1")
            # Give some time for replication to catch up
            time.sleep(10)
            with Then("Data should be replicated"):
                out = clickhouse.query(
                    chi,
                    "SELECT a FROM test_local",
                    host=f"chi-{chi}-{cluster}-0-1")
                assert out == "1"

    with When("Add one more replica"):
        kubectl.create_and_check(
            config="configs/test-014-replication-2.yaml",
            check={
                "pod_count": 3,
                "do_not_delete": 1,
            },
            timeout=600,
        )
        # Give some time for replication to catch up
        time.sleep(10)

        new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
        assert start_time == new_start_time

        with Then("Schema objects should be migrated to the new replica"):
            for obj in schema_objects:
                out = clickhouse.query(
                    chi,
                    f"SELECT count() FROM system.tables WHERE name = '{obj}'",
                    host=f"chi-{chi}-{cluster}-0-2")
                assert out == "1"
            # Check dictionary
            out = clickhouse.query(
                    chi,
                    f"SELECT count() FROM system.dictionaries WHERE name = 'test_dict'",
                    host=f"chi-{chi}-{cluster}-0-2")
            assert out == "1"

        with And("Replicated table should have the data"):
            out = clickhouse.query(
                chi,
                "SELECT a FROM test_local",
                host=f"chi-{chi}-{cluster}-0-2")
            assert out == "1"

    with When("Remove replica"):
        kubectl.create_and_check(
            config=config,
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            })

        new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
        assert start_time == new_start_time

        with Then("Replica needs to be removed from the Zookeeper as well"):
            out = clickhouse.query(
                chi,
                "SELECT count() FROM system.replicas WHERE table='test_local'")
            assert out == "1"

    with When("Restart Zookeeper pod"):
        with Then("Delete Zookeeper pod"):
            kubectl.launch("delete pod zookeeper-0")
            time.sleep(1)

        with Then("Insert into the table while there is no Zookeeper -- table should be in readonly mode"):
            out = clickhouse.query_with_error(chi, "INSERT INTO test_local values(2)")
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
            clickhouse.query(chi, "INSERT INTO test_local values(3)")

    kubectl.delete_chi("test-014-replication")


@TestScenario
@Name("test_015. Test circular replication with hostNetwork")
def test_015(self):
    kubectl.create_and_check(
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
            "test-015-host-network",
            host="chi-test-015-host-network-default-0-0",
            port="10000",
            sql="SELECT * FROM remote('chi-test-015-host-network-default-0-1:11000', system.one)")
        print("remote out=")
        print(out)

    with Then("Distributed query should work"):
        for i in range(20):
            time.sleep(10)
            out = clickhouse.query_with_error(
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

    kubectl.delete_chi("test-015-host-network")


@TestScenario
@Name("test_016. Test advanced settings options")
def test_016(self):
    chi = "test-016-settings"
    kubectl.create_and_check(
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

    with And("user1 user should be available"):
        clickhouse.query(chi, sql="select version()", user="user1", pwd="qwerty")

    with And("system.clusters should have a custom cluster"):
        out = clickhouse.query(chi, sql="select count() from system.clusters where cluster='custom'")
        assert out == "1"

    # test-016-settings-02.yaml
    with When("Update usersd settings"):
        start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
        kubectl.create_and_check(
            config="configs/test-016-settings-02.yaml",
            check={
                "do_not_delete": 1,
            },
        )
        with Then("Wait for configmap changes to apply"):
            kubectl.wait_command(
                f"exec chi-{chi}-default-0-0-0 -- bash -c \"grep test_norestart /etc/clickhouse-server/users.d/my_users.xml | wc -l\"",
                "1")

        with Then("Force reload config"):
            clickhouse.query(chi, sql="SYSTEM RELOAD CONFIG")
        with Then("test_norestart user should be available"):
            version = clickhouse.query(chi, sql="select version()", user="test_norestart")
        with And("user1 user should not be available"):
            version_user1 = clickhouse.query_with_error(chi, sql="select version()", user="user1", pwd="qwerty")
            assert version != version_user1
        with And("user2 user should be available"):
            version_user2 = clickhouse.query(chi, sql="select version()", user="user2", pwd="qwerty")
            assert version == version_user2
        with And("ClickHouse SHOULD NOT be restarted"):
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
            assert start_time == new_start_time

    # test-016-settings-03.yaml
    with When("Update custom.xml settings"):
        start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
        kubectl.create_and_check(
            config="configs/test-016-settings-03.yaml",
            check={
                "do_not_delete": 1,
            })
        with Then("Wait for configmap changes to apply"):
            kubectl.wait_command(
                f"exec chi-{chi}-default-0-0-0 -- bash -c \"grep test-03 /etc/clickhouse-server/config.d/custom.xml | wc -l\"",
                "1")
        
        with And("Custom macro 'test' should change the value"):
            out = clickhouse.query(chi, sql="select substitution from system.macros where macro='test'")
            assert out == "test-03"
        
        with And("ClickHouse SHOULD BE restarted"):
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
            assert start_time < new_start_time

    # test-016-settings-04.yaml
    with When("Add new custom2.xml config file"):
        start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
        kubectl.create_and_check(
            config="configs/test-016-settings-04.yaml",
            check={
                "do_not_delete": 1,
            })
        with Then("Wait for configmap changes to apply"):
            kubectl.wait_command(
                f"exec chi-{chi}-default-0-0-0 -- bash -c \"grep test-custom2 /etc/clickhouse-server/config.d/custom2.xml | wc -l\"",
                "1")
        
        with And("Custom macro 'test-custom2' should be found"):
            out = clickhouse.query(chi, sql="select substitution from system.macros where macro='test-custom2'")
            assert out == "test-custom2"
        
        with And("ClickHouse SHOULD BE restarted"):
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
            assert start_time < new_start_time

    kubectl.delete_chi("test-016-settings")


@TestScenario
@Name("test_017. Test deployment of multiple versions in a cluster")
def test_017(self):
    pod_count = 2
    kubectl.create_and_check(
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
        host = f"chi-{chi}-default-{shard}-0"
        for q in queries:
            clickhouse.query(chi, host=host, sql=q)
        out = clickhouse.query(chi, host=host, sql=test_query)
        ver = clickhouse.query(chi, host=host, sql="select version()")

        print(f"version: {ver}, result: {out}")

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_018. Test that configuration is properly updated")
def test_018(self): # Obsolete, covered by test_016
    kubectl.create_and_check(
        config="configs/test-018-configmap.yaml",
        check={
            "pod_count": 1,
            "do_not_delete": 1,
        },
    )
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
        },
    )
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
@Name("test_019. Test that volume is correctly retained and can be re-attached")
def test_019(self):
    require_zookeeper()

    config="configs/test-019-retain-volume.yaml"
    chi = manifest.get_chi_name(util.get_full_path(config))
    kubectl.create_and_check(
        config=config,
        check={
            "pod_count": 1,
            "do_not_delete": 1,
        },
    )

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
            },
        )

    with Then("PVC should be re-mounted"):
        with Then("Non-replicated table should have data"):
            out = clickhouse.query(chi, sql="select a from t1")
            assert out == "1"
        with And("Replicated table should have data"):
            out = clickhouse.query(chi, sql="select a from t2")
            assert out == "1"
            
    with Then("Stop the Installation"):
        kubectl.create_and_check(
            config="configs/test-019-retain-volume-2.yaml",
            check={
                "object_counts": { 
                    "statefulset": 1, # When stopping, pod is removed but statefulset and all volumes are in place
                    "pod": 0,
                    "service": 1, # load balancer service should be removed
                },
                "do_not_delete": 1,
                },
            )

    with Then("Re-start the Installation"):
        kubectl.create_and_check(
            config="configs/test-019-retain-volume.yaml",
            check={
                "object_counts": {
                    "statefulset": 1,
                    "pod": 1,
                    "service": 2, # load balancer service should be back
                },
                "do_not_delete": 1,
                },
            )

    with Then("Data should be in place"):
        with Then("Non-replicated table should have data"):
            out = clickhouse.query(chi, sql="select a from t1")
            assert out == "1"
        with And("Replicated table should have data"):
            out = clickhouse.query(chi, sql="select a from t2")
            assert out == "1"


    kubectl.delete_chi(chi)


@TestScenario
@Name("test_020. Test multi-volume configuration")
def test_020(self):
    config="configs/test-020-multi-volume.yaml"
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
        },
    )

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
@Name("test_021. Test rescaling storage")
def test_021(self):
    config = "configs/test-021-rescale-volume-01.yaml"
    
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
        },
    )

    with Then("Storage size should be 100Mi"):
        size = kubectl.get_pvc_size("disk1-chi-test-021-rescale-volume-simple-0-0-0")
        assert size == "100Mi"

    with When("Re-scale volume configuration to 200Mb"):
        kubectl.create_and_check(
            config="configs/test-021-rescale-volume-02-enlarge-disk.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("Storage size should be 200Mi"):
            kubectl.wait_field("pvc", "disk1-chi-test-021-rescale-volume-simple-0-0-0", ".spec.resources.requests.storage", "200Mi")
            size = kubectl.get_pvc_size("disk1-chi-test-021-rescale-volume-simple-0-0-0")
            assert size == "200Mi"

    with When("Add second disk 50Mi"):
        kubectl.create_and_check(
            config="configs/test-021-rescale-volume-03-add-disk.yaml",
            check={
                "pod_count": 1,
                # "pod_volumes": {
                #   "/var/lib/clickhouse",
                #    "/var/lib/clickhouse2",
                # },
                "do_not_delete": 1,
            },
        )
        # Adding new volume takes time, so pod_volumes check does not work

        with Then("There should be two PVC"):
            size = kubectl.get_pvc_size("disk1-chi-test-021-rescale-volume-simple-0-0-0")
            assert size == "200Mi"
            kubectl.wait_object("pvc", "disk2-chi-test-021-rescale-volume-simple-0-0-0")
            kubectl.wait_field("pvc", "disk2-chi-test-021-rescale-volume-simple-0-0-0", ".status.phase", "Bound")
            size = kubectl.get_pvc_size("disk2-chi-test-021-rescale-volume-simple-0-0-0")
            assert size == "50Mi"

        with And("There should be two disks recognized by ClickHouse"):
            kubectl.wait_pod_status("chi-test-021-rescale-volume-simple-0-0-0", "Running")
            # ClickHouse requires some time to mount volume. Race conditions.
            # TODO: wait for proper pod state and check the liveness probe probably. This is better than waiting
            for i in range(8):
                out = clickhouse.query(chi, "SELECT count() FROM system.disks")                    
                if out == "2":
                    break
                with Then(f"Not ready yet. Wait for {1<<i} seconds"):
                    time.sleep(1<<i)
            print("SELECT count() FROM system.disks RETURNED:")
            print(out)
            assert out == "2"

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_022. Test that chi with broken image can be deleted")
def test_022(self):
    config="configs/test-022-broken-image.yaml"
    chi = manifest.get_chi_name(util.get_full_path(config))
    kubectl.create_and_check(
        config=config,
        check={
            "pod_count": 1,
            "do_not_delete": 1,
            "chi_status": "InProgress",
        },
    )
    with When("ClickHouse image can not be retrieved"):
        kubectl.wait_field(
            "pod",
            "chi-test-022-broken-image-default-0-0-0",
            ".status.containerStatuses[0].state.waiting.reason",
            "ErrImagePull"
        )
        with Then("CHI should be able to delete"):
            kubectl.launch(f"delete chi {chi}", ok_to_fail=True, timeout=60)
            assert kubectl.get_count("chi", f"{chi}") == 0

@TestScenario
@Name("test_023. Test auto templates")
def test_023(self):
    kubectl.create_and_check(
        config="configs/test-001.yaml",
        check={
            "pod_count": 1,
            "apply_templates": {
                "templates/tpl-clickhouse-auto.yaml",
            },
            # test-001.yaml does not have a template reference but should get correct ClickHouse version
            "pod_image": settings.clickhouse_version,
        }
    )
    
    kubectl.launch("delete chit clickhouse-stable")

@TestScenario
@Name("test_024. Test annotations for various template types")
def test_024(self):
    config="configs/test-024-template-annotations.yaml"
    chi = manifest.get_chi_name(util.get_full_path(config))
    kubectl.create_and_check(
        config=config,
        check={
            "pod_count": 1,
            "do_not_delete": 1,
        },
    )
    
    with Then("Pod annotations should be populated"):
        assert kubectl.get_field("pod", "chi-test-024-default-0-0-0", ".metadata.annotations.test") == "test"
    with And("Service annotations should be populated"):
        assert kubectl.get_field("service", "clickhouse-test-024", ".metadata.annotations.test") == "test"
    with And("PV annotations should be populated"):
        assert kubectl.get_field("pv", "-l clickhouse.altinity.com/chi=test-024", ".metadata.annotations.test") == "test"
        
    kubectl.delete_chi(chi)
    
@TestScenario
@Name("test_025. Test that service is available during re-scalaling, upgades etc.")
def test_025(self):
    require_zookeeper()

    create_table = """
    CREATE TABLE test_local(a UInt32) 
    Engine = ReplicatedMergeTree('/clickhouse/{installation}/tables/{shard}/{database}/{table}', '{replica}')
    PARTITION BY tuple() 
    ORDER BY a
    """.replace('\r', '').replace('\n', '')

    config = "configs/test-025-rescaling.yaml"
    chi = manifest.get_chi_name(util.get_full_path(config))
    cluster = "default"

    kubectl.create_and_check(
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

    numbers = "100000000"

    with Given("Create replicated table and populate it"):
        clickhouse.query(chi, create_table)
        clickhouse.query(chi, "CREATE TABLE test_distr as test_local Engine = Distributed('default', default, test_local)")
        clickhouse.query(chi, f"INSERT INTO test_local select * from numbers({numbers})", timeout=120)

    with When("Add one more replica, but do not wait for completion"):
        kubectl.create_and_check(
            config="configs/test-025-rescaling-2.yaml",
            check={
                "do_not_delete": 1,
                "pod_count": 2,
                "chi_status": "InProgress", # do not wait
            },
            timeout=600,
        )
    
    with Then("Query second pod using service as soon as pod is in ready state"):
        kubectl.wait_field(
            "pod", "chi-test-025-rescaling-default-0-1-0",
            ".status.containerStatuses[0].ready", "true",
            backoff = 1
        )
        start_time = time.time()
        lb_error_time = start_time
        distr_lb_error_time = start_time
        latent_replica_time = start_time
        for i in range(1, 100):
            cnt_local    = clickhouse.query_with_error(chi, "select count() from test_local", "chi-test-025-rescaling-default-0-1.test.svc.cluster.local")
            cnt_lb       = clickhouse.query_with_error(chi, "select count() from test_local")
            cnt_distr_lb = clickhouse.query_with_error(chi, "select count() from test_distr")
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

    kubectl.delete_chi(chi)
   