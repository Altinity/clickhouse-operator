import time

import e2e.clickhouse as clickhouse
import e2e.kubectl as kubectl
import e2e.yaml_manifest as yaml_manifest
import e2e.settings as settings
import e2e.util as util

from testflows.core import *
from testflows.asserts import error


@TestScenario
@Name("test_001. 1 node")
def test_001(self):
    kubectl.create_and_check(
        manifest="manifests/chi/test-001.yaml",
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
        manifest="manifests/chi/test-002-tpl.yaml",
        check={
            "pod_count": 1,
            "apply_templates": {
                settings.clickhouse_template,
                "manifests/chit/tpl-log-volume.yaml",
                "manifests/chit/tpl-one-per-host.yaml",
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
        manifest="manifests/chi/test-003-complex-layout.yaml",
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
        manifest="manifests/chi/test-004-tpl.yaml",
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
        manifest="manifests/chi/test-005-acm.yaml",
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
    new_version = "yandex/clickhouse-server:21.8"
    with Then("Create initial position"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-006-ch-upgrade-1.yaml",
            check={
                "pod_count": 2,
                "pod_image": old_version,
                "do_not_delete": 1,
            }
        )
    with Then("Use different podTemplate and confirm that pod image is updated"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-006-ch-upgrade-2.yaml",
            check={
                "pod_count": 2,
                "pod_image": new_version,
                "do_not_delete": 1,
            }
        )
    with Then("Change image in podTemplate itself and confirm that pod image is updated"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-006-ch-upgrade-3.yaml",
            check={
                "pod_count": 2,
                "pod_image": old_version,
            }
        )


@TestScenario
@Name("test_007. Test template with custom clickhouse ports")
def test_007(self):
    kubectl.create_and_check(
        manifest="manifests/chi/test-007-custom-ports.yaml",
        check={
            "pod_count": 1,
            "pod_ports": [8124, 9001, 9010],
        }
    )


def test_operator_upgrade(manifest, version_from, version_to=settings.operator_version):
    with Given(f"clickhouse-operator FROM {version_from}"):
        util.set_operator_version(version_from)
        chi = yaml_manifest.get_chi_name(util.get_full_path(manifest, True))
        cluster = "test-009"

        kubectl.create_and_check(
            manifest=manifest,
            check={
                "object_counts": {
                    "statefulset": 1,
                    "pod": 1,
                    "service": 2,
                },
                "do_not_delete": 1,
            }
        )
        start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")

        with Then("Create a table"):
            clickhouse.query(chi, "CREATE TABLE test_local Engine = Log as SELECT 1")
            clickhouse.query(chi, "CREATE TABLE test_dist as system.one Engine = Distributed('test-009', system, one)")


        with When(f"upgrade operator TO {version_to}"):
            util.set_operator_version(version_to, timeout=120)
            kubectl.wait_chi_status(chi, "Completed", retries=20)

            kubectl.wait_objects(chi, {"statefulset": 1, "pod": 1, "service": 2})

            with Then("Check that table is here"):
                tables = clickhouse.query(chi, "SHOW TABLES")
                assert "test_local" in tables
                out = clickhouse.query(chi, "SELECT * FROM test_local")
                assert "1" == out

            with Then("ClickHouse pods should not be restarted"):
                new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
                if start_time != new_start_time:
                    kubectl.launch(f"describe chi -n {settings.test_namespace} {chi}")
                    kubectl.launch(
                        f"logs -n {settings.test_namespace} pod/$(kubectl get pods -o name | grep clickhouse-operator) -c clickhouse-operator"
                    )
                assert start_time == new_start_time, error(f"{start_time} != {new_start_time}, pod restarted after operator upgrade")
        kubectl.delete_chi(chi)


def test_operator_restart(manifest, version=settings.operator_version):
    with Given(f"clickhouse-operator {version}"):
        util.set_operator_version(version)
        chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
        cluster = chi

        kubectl.create_and_check(
            manifest=manifest,
            check={
                "object_counts": {
                    "statefulset": 1,
                    "pod": 1,
                    "service": 2,
                },
                "do_not_delete": 1,
            })
        time.sleep(10)
        kubectl.wait_chi_status(chi, "Completed")

        check_operator_restart(chi, {"statefulset": 1, "pod": 1, "service": 2}, f"chi-{chi}-{cluster}-0-0-0")

        kubectl.delete_chi(chi)


def check_operator_restart(chi, wait_objects, pod):
    start_time = kubectl.get_field("pod", pod, ".status.startTime")
    with When("Restart operator"):
        util.restart_operator()
        time.sleep(10)
        kubectl.wait_objects(chi, wait_objects)
        time.sleep(10)
        kubectl.wait_chi_status(chi, "Completed")
        new_start_time = kubectl.get_field("pod", pod, ".status.startTime")

        with Then("ClickHouse pods should not be restarted"):
            assert start_time == new_start_time

@TestScenario
@Name("test_008. Test operator restart")
def test_008(self):
    with Then("Test simple chi for operator restart"):
        test_operator_restart("manifests/chi/test-008-operator-restart-1.yaml")
    with Then("Test advanced chi for operator restart"):
        test_operator_restart("manifests/chi/test-008-operator-restart-2.yaml")


@TestScenario
@Name("test_009. Test operator upgrade")
def test_009(self, version_from="0.16.0", version_to=settings.operator_version):
    with Then("Test simple chi for operator upgrade"):
        test_operator_upgrade("manifests/chi/test-009-operator-upgrade-1.yaml", version_from, version_to)
    with Then("Test advanced chi for operator upgrade"):
        test_operator_upgrade("manifests/chi/test-009-operator-upgrade-2.yaml", version_from, version_to)


@TestScenario
@Name("test_010. Test zookeeper initialization")
def test_010(self):
    util.set_operator_version(settings.operator_version)
    util.require_zookeeper()

    kubectl.create_and_check(
        manifest="manifests/chi/test-010-zkroot.yaml",
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
        assert "DB::Exception" in out, error()

    kubectl.delete_chi("test-010-zkroot")


@TestScenario
@Name("test_011. Test user security and network isolation")
def test_011(self):
    with Given("test-011-secured-cluster.yaml and test-011-insecured-cluster.yaml"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-011-secured-cluster.yaml",
            check={
                "pod_count": 2,
                "service": [
                    "chi-test-011-secured-cluster-default-1-0",
                    "ClusterIP",
                ],
                "apply_templates": {
                    settings.clickhouse_template,
                    "manifests/chit/tpl-log-volume.yaml",
                },
                "do_not_delete": 1,
            }
        )

        kubectl.create_and_check(
            manifest="manifests/chi/test-011-insecured-cluster.yaml",
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
                user="user1", pwd="topsecret"
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
                user="user2", pwd="default"
            )
            assert out == 'OK'

        with And("User with both plain and sha256 password should get the latter one"):
            out = clickhouse.query_with_error(
                "test-011-secured-cluster",
                "select 'OK'",
                user="user3", pwd="clickhouse_operator_password"
            )
            assert out == 'OK'

        with And("User with row-level security should have it applied"):
            out = clickhouse.query_with_error(
                "test-011-secured-cluster",
                "select * from system.numbers limit 1",
                user="restricted", pwd="secret"
            )
            assert out == '1000'

        with And("User with NO access management enabled CAN NOT run SHOW USERS"):
            out = clickhouse.query_with_error(
                "test-011-secured-cluster",
                "SHOW USERS",
            )
            assert 'ACCESS_DENIED' in out

        with And("User with access management enabled CAN run SHOW USERS"):
            out = clickhouse.query(
                "test-011-secured-cluster",
                "SHOW USERS",
                user="user4", pwd="secret"
            )
            assert 'ACCESS_DENIED' not in out

        kubectl.delete_chi("test-011-secured-cluster")
        kubectl.delete_chi("test-011-insecured-cluster")


@TestScenario
@Name("test_011_1. Test default user security")
def test_011_1(self):
    with Given("test-011-secured-default-1.yaml with password_sha256_hex for default user"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-011-secured-default-1.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            }
        )

        with Then("Default user plain password should be removed"):
            chi = kubectl.get("chi", "test-011-secured-default")
            assert "default/password" in chi["status"]["normalized"]["spec"]["configuration"]["users"]
            assert chi["status"]["normalized"]["spec"]["configuration"]["users"]["default/password"] == ""

            cfm = kubectl.get("configmap", "chi-test-011-secured-default-common-usersd")
            assert "<password remove=\"1\"></password>" in cfm["data"]["chop-generated-users.xml"]


        with And("Connection to localhost should succeed with default user"):
            out = clickhouse.query_with_error(
                "test-011-secured-default",
                "select 'OK'",
                pwd="clickhouse_operator_password"
            )
            assert out == 'OK'

        with When("Trigger installation update"):
            kubectl.create_and_check(
                manifest="manifests/chi/test-011-secured-default-2.yaml",
                check={
                    "do_not_delete": 1,
                }
            )
            with Then("Default user plain password should be removed"):
                chi = kubectl.get("chi", "test-011-secured-default")
                assert "default/password" in chi["status"]["normalized"]["spec"]["configuration"]["users"]
                assert chi["status"]["normalized"]["spec"]["configuration"]["users"]["default/password"] == ""

                cfm = kubectl.get("configmap", "chi-test-011-secured-default-common-usersd")
                assert "<password remove=\"1\"></password>" in cfm["data"]["chop-generated-users.xml"]

        with When("Default user password is removed"):
            kubectl.create_and_check(
                manifest="manifests/chi/test-011-secured-default-3.yaml",
                check={
                    "do_not_delete": 1,
                }
            )
            with Then("Connection to localhost should succeed with default user and no password"):
                out = clickhouse.query_with_error(
                    "test-011-secured-default",
                    "select 'OK'"
                )
                assert out == 'OK'

        kubectl.delete_chi("test-011-secured-default")


@TestScenario
@Name("test_011_2. Test k8s secrets usage")
def test_011_2(self):
    with Given("test-011-secrets.yaml with secret storage"):
        kubectl.apply(util.get_full_path("manifests/chi/test-011-secret.yaml", False),
                      ns=settings.test_namespace, timeout=300)

        kubectl.create_and_check(
            manifest="manifests/chi/test-011-secrets.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            }
        )

        with Then("Connection to localhost should succeed with user1"):
            out = clickhouse.query_with_error(
                "test-011-secrets",
                "select 'OK'",
                user="user1",
                pwd="pwduser1"
            )
            assert out == 'OK'

        with And("Connection to localhost should succeed with user2"):
            out = clickhouse.query_with_error(
                "test-011-secrets",
                "select 'OK'",
                user="user2",
                pwd="pwduser2"
            )
            assert out == 'OK'

        with And("Connection to localhost should succeed with user3"):
            out = clickhouse.query_with_error(
                "test-011-secrets",
                "select 'OK'",
                user="user3",
                pwd="pwduser3"
            )
            assert out == 'OK'

        kubectl.delete_chi("test-011-secrets")
        kubectl.launch("delete secret test-011-secret", ns=settings.test_namespace, timeout=600, ok_to_fail=True)


@TestScenario
@Name("test_012. Test service templates")
def test_012(self):
    kubectl.create_and_check(
        manifest="manifests/chi/test-012-service-template.yaml",
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
            manifest="manifests/chi/test-012-service-template-2.yaml",
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
    manifest = "manifests/chi/test-013-add-shards-1.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    cluster = "default"

    kubectl.create_and_check(
        manifest=manifest,
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
    out = ""
    # wait for cluster to start
    for _ in range(20):
        time.sleep(10)
        out = clickhouse.query_with_error(
            chi,
            "SELECT count() FROM cluster('all-sharded', system.one) settings receive_timeout=10")
        note(f"cluster out:\n{out}")
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

    nShards = 2
    with Then(f"Add {nShards-1} shards"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-013-add-shards-2.yaml",
            check={
                "object_counts": {
                    "statefulset": nShards,
                    "pod": nShards,
                    "service": nShards+1,
                },
                "do_not_delete": 1,
            },
            timeout=1500,
        )

    # wait for cluster to start
    out = ""
    for _ in range(10):
        out = clickhouse.query_with_error(
            chi,
            "SELECT count() FROM cluster('all-sharded', system.one) settings receive_timeout=10")
        note(f"cluster out:\n{out}")
        if out == str(nShards):
            break
        time.sleep(10)
    else:
        assert out == str(nShards)

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

    with When("Remove shards"):
        kubectl.create_and_check(
            manifest=manifest,
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
        for _ in range(10):
            out = clickhouse.query_with_error(
                chi,
                "SELECT count() FROM cluster('all-sharded', system.one) settings receive_timeout=10")
            note(f"cluster out:\n{out}")
            if out == "1":
                break
            time.sleep(10)
        else:
            assert out == "1"

        with Then("Unaffected pod should not be restarted"):
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
            assert start_time == new_start_time

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_014. Test that replication works")
def test_014(self):
    util.require_zookeeper()

    create_table = """
    CREATE TABLE test_local(a Int8)
    Engine = ReplicatedMergeTree('/clickhouse/{installation}/tables/{shard}/{database}/{table}', '{replica}')
    PARTITION BY tuple()
    ORDER BY a
    """.replace('\r', '').replace('\n', '')

    manifest = "manifests/chi/test-014-replication-1.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    cluster = "default"

    kubectl.create_and_check(
        manifest=manifest,
        check={
            "apply_templates": {
                settings.clickhouse_template,
                "manifests/chit/tpl-persistent-volume-100Mi.yaml",
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
        'test_buffer',
        'a_view',
        'test_local2'
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
        clickhouse.query(
            chi,
            "CREATE TABLE test_buffer(a Int8) Engine = Buffer(default, test_local, 16, 10, 100, 10000, 1000000, 10000000, 100000000)",
            host=f"chi-{chi}-{cluster}-0-0")
        clickhouse.query(
            chi,
            "CREATE DATABASE test_db Engine = Atomic",
            host=f"chi-{chi}-{cluster}-0-0")
        clickhouse.query(
            chi,
            "CREATE TABLE test_db.test_local2 (a Int8) Engine = ReplicatedMergeTree('/clickhouse/{installation}/tables/{shard}/{database}/{table}', '{replica}') ORDER BY tuple()",
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

    with When("Add more replicas"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-014-replication-2.yaml",
            check={
                "pod_count": 4,
                "do_not_delete": 1,
            },
            timeout=600,
        )
        # Give some time for replication to catch up
        time.sleep(10)

        new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
        assert start_time == new_start_time

        with Then("Schema objects should be migrated to the new replicas"):
            for replica in [2, 3]:
                host = f"chi-{chi}-{cluster}-0-{replica}"
                print(f"Checking replica {host}")
                print("Checking tables and views")
                for obj in schema_objects:
                    out = clickhouse.query(
                        chi,
                        f"SELECT count() FROM system.tables WHERE name = '{obj}'",
                        host=host)
                    assert out == "1"

                print("Checking dictionaries")
                out = clickhouse.query(
                    chi,
                    f"SELECT count() FROM system.dictionaries WHERE name = 'test_dict'",
                    host=host)
                assert out == "1"

                print("Checking database engine")
                out =  clickhouse.query(
                    chi,
                    f"SELECT engine FROM system.databases WHERE name = 'test_db'",
                    host=host)
                assert out == "Atomic"

        with And("Replicated table should have the data"):
            out = clickhouse.query(
                chi,
                "SELECT a FROM test_local",
                host=f"chi-{chi}-{cluster}-0-2")
            assert out == "1"

    with When("Remove replicas"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            })

        new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
        assert start_time == new_start_time

        with Then("Replica needs to be removed from the ZooKeeper as well"):
            out = clickhouse.query(
                chi,
                "SELECT total_replicas FROM system.replicas WHERE table='test_local'")
            note(f"Found {out} total replicas")
            assert out == "2"

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

        with Then("Table should be back to normal"):
            clickhouse.query(chi, "INSERT INTO test_local values(3)")

    with When("Delete chi"):
        kubectl.delete_chi("test-014-replication")

        with Then("Tables should be deleted. We can test it re-creating the chi and checking ZooKeeper contents"):
            kubectl.create_and_check(
                manifest=manifest,
                check={
                    "pod_count": 1,
                    "do_not_delete": 1,
                })
            out = clickhouse.query(
                chi,
                f"select count() from system.zookeeper where path ='/clickhouse/{chi}/tables/0/default'")
            note(f"Found {out} replicated tables in ZooKeeper")
            assert out == "0"

    kubectl.delete_chi("test-014-replication")


@TestScenario
@Name("test_015. Test circular replication with hostNetwork")
def test_015(self):
    kubectl.create_and_check(
        manifest="manifests/chi/test-015-host-network.yaml",
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
        note(f"remote out:\n{out}")

    with Then("Distributed query should work"):
        for _ in range(20):
            time.sleep(10)
            out = clickhouse.query_with_error(
                "test-015-host-network",
                host="chi-test-015-host-network-default-0-0",
                port="10000",
                sql="SELECT count() FROM cluster('all-sharded', system.one) settings receive_timeout=10")
            note(f"cluster out:\n{out}")
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
        manifest="manifests/chi/test-016-settings-01.yaml",
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
            manifest="manifests/chi/test-016-settings-02.yaml",
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
            manifest="manifests/chi/test-016-settings-03.yaml",
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
            manifest="manifests/chi/test-016-settings-04.yaml",
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
        manifest="manifests/chi/test-017-multi-version.yaml",
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
        note(q)
    test_query = "select min(offset), max(offset) from test_max"
    note(test_query)

    for shard in range(pod_count):
        host = f"chi-{chi}-default-{shard}-0"
        for q in queries:
            clickhouse.query(chi, host=host, sql=q)
        out = clickhouse.query(chi, host=host, sql=test_query)
        ver = clickhouse.query(chi, host=host, sql="select version()")
        note(f"version: {ver}, result: {out}")

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_018. Test that server settings are applied before statefulset is started")
# Obsolete, covered by test_016
def test_018(self):
    chi = "test-018-configmap"
    kubectl.create_and_check(
        manifest="manifests/chi/test-018-configmap-1.yaml",
        check={
            "pod_count": 1,
            "do_not_delete": 1,
        },
    )

    with When("Update settings"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-018-configmap-2.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("Configmap on the pod should be updated"):
            display_name = kubectl.launch(
                f"exec chi-{chi}-default-0-0-0 -- bash -c \"grep display_name /etc/clickhouse-server/config.d/chop-generated-settings.xml\"")
            note(display_name)
            assert "new_display_name" in display_name
            with Then("And ClickHouse should pick them up"):
                macros = clickhouse.query(chi, "SELECT substitution from system.macros where macro = 'test'")
                note(macros)
                assert "new_test" == macros

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_019. Test that volume is correctly retained and can be re-attached")
def test_019(self):
    util.require_zookeeper()
    manifest = "manifests/chi/test-019-retain-volume-1.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    kubectl.create_and_check(
        manifest=manifest,
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
            manifest=manifest,
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

    with When("Stop the Installation"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-019-retain-volume-2.yaml",
            check={
                "object_counts": {
                    "statefulset": 1,  # When stopping, pod is removed but StatefulSet and all volumes are in place
                    "pod": 0,
                    "service": 1,  # load balancer service should be removed
                },
                "do_not_delete": 1,
            },
        )

        with And("Re-start the Installation"):
            kubectl.create_and_check(
                manifest="manifests/chi/test-019-retain-volume-1.yaml",
                check={
                    "pod_count": 1,
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

    with When("Add a second replica"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-019-retain-volume-3.yaml",
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )
        with Then("Replicated table should have two replicas now"):
            out = clickhouse.query(chi, sql="select total_replicas from system.replicas where table='t2'")
            assert out == "2"

        with When("Remove a replica"):
            pvc_count = kubectl.get_count("pvc")
            pv_count = kubectl.get_count("pv")

            kubectl.create_and_check(
                manifest="manifests/chi/test-019-retain-volume-1.yaml",
                check={
                    "pod_count": 1,
                    "do_not_delete": 1,
                },
            )
            with Then("Replica PVC should be retained"):
                assert kubectl.get_count("pvc") == pvc_count
                assert kubectl.get_count("pv") == pv_count

            with And("Replica should NOT be removed from ZooKeeper"):
                out = clickhouse.query(chi, sql="select total_replicas from system.replicas where table='t2'")
                assert out == "2"

    with When("Add a second replica one more time"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-019-retain-volume-3.yaml",
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )

        with Then("Table should have data"):
            out = clickhouse.query(chi, sql="select a from t2", host=f"chi-{chi}-simple-0-1")
            assert out == "1"

        with When("Set reclaim policy to Delete but do not wait for completion"):
            kubectl.create_and_check(
                manifest="manifests/chi/test-019-retain-volume-4.yaml",
                check={
                    "pod_count": 2,
                    "do_not_delete": 1,
                    # "chi_status": "InProgress",
                },
            )

            with And("Remove a replica"):
                pvc_count = kubectl.get_count("pvc")
                pv_count = kubectl.get_count("pv")

                kubectl.create_and_check(
                    manifest="manifests/chi/test-019-retain-volume-1.yaml",
                    check={
                        "pod_count": 1,
                        "do_not_delete": 1,
                    },
                )
                # Not implemented yet
                with Then("Replica PVC should be deleted"):
                    assert kubectl.get_count("pvc") < pvc_count
                    assert kubectl.get_count("pv") < pv_count

                with And("Replica should be removed from ZooKeeper"):
                    out = clickhouse.query(chi, sql="select total_replicas from system.replicas where table='t2'")
                    assert out == "1"

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_020. Test multi-volume configuration")
def test_020(self):
    manifest = "manifests/chi/test-020-multi-volume.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    kubectl.create_and_check(
        manifest=manifest,
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
    manifest = "manifests/chi/test-021-rescale-volume-01.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))

    with Given("Default storage class is expandable"):
        default_storage_class = kubectl.get_default_storage_class()
        assert default_storage_class is not None
        assert len(default_storage_class) > 0
        allow_volume_expansion = kubectl.get_field("storageclass", default_storage_class, ".allowVolumeExpansion")
        if allow_volume_expansion != "true":
            kubectl.launch(f"patch storageclass {default_storage_class} -p '{{\"allowVolumeExpansion\":true}}'")

    kubectl.create_and_check(
        manifest=manifest,
        check={
            "pod_count": 1,
            "do_not_delete": 1,
        },
    )

    with Then("Storage size should be 1Gi"):
        size = kubectl.get_pvc_size("disk1-chi-test-021-rescale-volume-simple-0-0-0")
        assert size == "1Gi"

    with Then("Create a table with a single row"):
        clickhouse.query(chi, "CREATE TABLE test_local Engine = Log as SELECT 1 AS wtf")

    with When("Re-scale volume configuration to 2Gi"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-021-rescale-volume-02-enlarge-disk.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("Storage size should be 2Gi"):
            kubectl.wait_field("pvc", "disk1-chi-test-021-rescale-volume-simple-0-0-0", ".spec.resources.requests.storage", "2Gi")
            size = kubectl.get_pvc_size("disk1-chi-test-021-rescale-volume-simple-0-0-0")
            assert size == "2Gi"

        with And("Table should exist"):
            out = clickhouse.query(chi, "select * from test_local")
            assert out == "1"

    with When("Add a second disk"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-021-rescale-volume-03-add-disk.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )
        # Adding new volume takes time, so pod_volumes check does not work

        with Then("There should be two PVC"):
            size = kubectl.get_pvc_size("disk1-chi-test-021-rescale-volume-simple-0-0-0")
            assert size == "2Gi"
            kubectl.wait_object("pvc", "disk2-chi-test-021-rescale-volume-simple-0-0-0")
            kubectl.wait_field("pvc", "disk2-chi-test-021-rescale-volume-simple-0-0-0", ".status.phase", "Bound")
            size = kubectl.get_pvc_size("disk2-chi-test-021-rescale-volume-simple-0-0-0")
            assert size == "1Gi"

        with And("There should be two disks recognized by ClickHouse"):
            kubectl.wait_pod_status("chi-test-021-rescale-volume-simple-0-0-0", "Running")
            # ClickHouse requires some time to mount volume. Race conditions.
            # TODO: wait for proper pod state and check the liveness probe probably. This is better than waiting
            out = ""
            for i in range(8):
                out = clickhouse.query(chi, "SELECT count() FROM system.disks")
                if out == "2":
                    break
                with Then(f"Not ready yet. Wait for {1 << i} seconds"):
                    time.sleep(1 << i)
            assert out == "2"

        with And("Table should exist"):
            out = clickhouse.query(chi, "select * from test_local")
            assert out == "1"

    with When("Try reducing the disk size and also change a version to recreate the stateful set"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-021-rescale-volume-04-decrease-disk.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("Storage size should be unchanged 2Gi"):
            size = kubectl.get_pvc_size("disk1-chi-test-021-rescale-volume-simple-0-0-0")
            assert size == "2Gi"

        with And("Table should exist"):
            out = clickhouse.query(chi, "select * from test_local")
            assert out == "1"

        with And("PVC status should not be Terminating"):
            status = kubectl.get_field("pvc", "disk2-chi-test-021-rescale-volume-simple-0-0-0", ".status.phase")
            assert status != "Terminating"

    with When("Revert disk size back to 2Gi"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-021-rescale-volume-03-add-disk.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("Storage size should be 2Gi"):
            kubectl.wait_field("pvc", "disk1-chi-test-021-rescale-volume-simple-0-0-0", ".spec.resources.requests.storage", "2Gi")
            size = kubectl.get_pvc_size("disk1-chi-test-021-rescale-volume-simple-0-0-0")
            assert size == "2Gi"

        with And("Table should exist"):
            out = clickhouse.query(chi, "select * from test_local")
            assert out == "1"

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_022. Test that chi with broken image can be deleted")
def test_022(self):
    manifest = "manifests/chi/test-022-broken-image.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    kubectl.create_and_check(
        manifest=manifest,
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
            kubectl.launch(f"delete chi {chi}", ok_to_fail=True, timeout=600)
            assert kubectl.get_count("chi", f"{chi}") == 0


@TestScenario
@Name("test_023. Test auto templates")
def test_023(self):
    manifest = "manifests/chi/test-001.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))

    chit_data = yaml_manifest.get_manifest_data(util.get_full_path("manifests/chit/tpl-clickhouse-auto-1.yaml"))
    expected_image = chit_data['spec']['templates']['podTemplates'][0]['spec']['containers'][0]['image']

    kubectl.create_and_check(
        manifest="manifests/chi/test-001.yaml",
        check={
            "pod_count": 1,
            "apply_templates": {
                settings.clickhouse_template,
                "manifests/chit/tpl-clickhouse-auto-1.yaml",
                "manifests/chit/tpl-clickhouse-auto-2.yaml"
            },
            # test-001.yaml does not have a template reference but should get correct ClickHouse version
            "pod_image": expected_image,
            "do_not_delete": 1
        }
    )
    with Then("Annotation from a template should be populated"):
        assert kubectl.get_field("chi", chi, ".status.normalized.metadata.annotations.test") == "test"
    with Then("Pod annotation should populated from template"):
        assert kubectl.get_field("pod", "chi-test-001-single-0-0-0", ".metadata.annotations.test") == "test"

    kubectl.delete_chi(chi)
    kubectl.delete(util.get_full_path("manifests/chit/tpl-clickhouse-auto-1.yaml"))
    kubectl.delete(util.get_full_path("manifests/chit/tpl-clickhouse-auto-2.yaml"))


@TestScenario
@Name("test_024. Test annotations for various template types")
def test_024(self):
    manifest = "manifests/chi/test-024-template-annotations.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    kubectl.create_and_check(
        manifest=manifest,
        check={
            "pod_count": 1,
            "do_not_delete": 1,
        },
    )

    with Then("Pod annotation should populated from a podTemplate"):
        assert kubectl.get_field("pod", "chi-test-024-default-0-0-0", ".metadata.annotations.podtemplate/test") == "test"
    with And("Service annotation should be populated from a serviceTemplate"):
        assert kubectl.get_field("service", "clickhouse-test-024", ".metadata.annotations.servicetemplate/test") == "test"
    with And("PVC annotation should be populated from a volumeTemplate"):
        assert kubectl.get_field("pvc", "-l clickhouse.altinity.com/chi=test-024", ".metadata.annotations.pvc/test") == "test"

    with And("Pod annotation should populated from a CHI"):
        assert kubectl.get_field("pod", "chi-test-024-default-0-0-0", ".metadata.annotations.chi/test") == "test"
    with And("Service annotation should be populated from a CHI"):
        assert kubectl.get_field("service", "clickhouse-test-024", ".metadata.annotations.chi/test") == "test"
    with And("PVC annotation should be populated from a CHI"):
        assert kubectl.get_field("pvc", "-l clickhouse.altinity.com/chi=test-024", ".metadata.annotations.chi/test") == "test"

    with And("Service annotation macros should be resolved"):
        assert kubectl.get_field("service", "clickhouse-test-024", ".metadata.annotations.servicetemplate/macro-test") == "test-024.example.com"
        assert kubectl.get_field("service", "service-test-024-0-0", ".metadata.annotations.servicetemplate/macro-test") == "test-024-0-0.example.com"


    kubectl.delete_chi(chi)


@TestScenario
@Name("test_025. Test that service is available during re-scalaling, upgades etc.")
def test_025(self):
    util.require_zookeeper()

    create_table = """
    CREATE TABLE test_local(a UInt32)
    Engine = ReplicatedMergeTree('/clickhouse/{installation}/tables/{shard}/{database}/{table}', '{replica}')
    PARTITION BY tuple()
    ORDER BY a
    """.replace('\r', '').replace('\n', '')

    manifest = "manifests/chi/test-025-rescaling.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))

    kubectl.create_and_check(
        manifest=manifest,
        check={
            "apply_templates": {
                settings.clickhouse_template,
                "manifests/chit/tpl-persistent-volume-100Mi.yaml",
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

    kubectl.wait_jsonpath("pod", "chi-test-025-rescaling-default-0-0-0", "{.status.containerStatuses[0].ready}", "true",
                          ns=kubectl.namespace)

    numbers = "100000000"

    with Given("Create replicated table and populate it"):
        clickhouse.query(chi, create_table)
        clickhouse.query(chi, "CREATE TABLE test_distr as test_local Engine = Distributed('default', default, test_local)")
        clickhouse.query(chi, f"INSERT INTO test_local select * from numbers({numbers})", timeout=120)

    with When("Add one more replica, but do not wait for completion"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-025-rescaling-2.yaml",
            check={
                "do_not_delete": 1,
                "pod_count": 2,
                "chi_status": "InProgress",  # do not wait
            },
            timeout=600,
        )

    with Then("Query second pod using service as soon as pod is in ready state"):
        kubectl.wait_field(
            "pod", "chi-test-025-rescaling-default-0-1-0",
            ".metadata.labels.\"clickhouse\\.altinity\\.com/ready\"", "yes",
            backoff=1
        )
        start_time = time.time()
        lb_error_time = start_time
        distr_lb_error_time = start_time
        latent_replica_time = start_time
        for i in range(1, 100):
            cnt_local = clickhouse.query_with_error(chi, "select count() from test_local", "chi-test-025-rescaling-default-0-1.test.svc.cluster.local")
            cnt_lb = clickhouse.query_with_error(chi, "select count() from test_local")
            cnt_distr_lb = clickhouse.query_with_error(chi, "select count() from test_distr")
            if "Exception" in cnt_lb or cnt_lb == 0:
                lb_error_time = time.time()
            if "Exception" in cnt_distr_lb or cnt_distr_lb == 0:
                distr_lb_error_time = time.time()
            note(f"local via loadbalancer: {cnt_lb}, distributed via loadbalancer: {cnt_distr_lb}")
            if "Exception" not in cnt_local:
                note(f"local: {cnt_local}, distr: {cnt_distr_lb}")
                if cnt_local == numbers:
                    break
                latent_replica_time = time.time()
                note("Replicated table did not catch up")
            note("Waiting 1 second.")
            time.sleep(1)
        note(f"Tables not ready: {round(distr_lb_error_time - start_time)}s, data not ready: {round(latent_replica_time - distr_lb_error_time)}s")

        with Then("Query to the distributed table via load balancer should never fail"):
            assert round(distr_lb_error_time - start_time) == 0
        with And("Query to the local table via load balancer should never fail"):
            assert round(lb_error_time - start_time) == 0

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_026. Test mixed single and multi-volume configuration in one cluster")
def test_026(self):
    util.require_zookeeper()

    manifest = "manifests/chi/test-026-mixed-replicas.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    kubectl.create_and_check(
        manifest=manifest,
        check={
            "pod_count": 2,
            "do_not_delete": 1,
        },
    )

    with When("Cluster is ready"):
        with Then("Check that first replica has one disk"):
            out = clickhouse.query(chi, host="chi-test-026-mixed-replicas-default-0-0", sql="select count() from system.disks")
            assert out == "1"

        with And("Check that second replica has two disks"):
            out = clickhouse.query(chi, host="chi-test-026-mixed-replicas-default-0-1", sql="select count() from system.disks")
            assert out == "2"

    with When("Create a table and generate several inserts"):
        clickhouse.query(
            chi,
            sql="create table test_disks ON CLUSTER '{cluster}' (a Int64) Engine = ReplicatedMergeTree('/clickhouse/{installation}/tables/{shard}/{database}/{table}', '{replica}') partition by (a%10) order by a"
        )
        clickhouse.query(
            chi, host="chi-test-026-mixed-replicas-default-0-0",
            sql="insert into test_disks select * from numbers(100) settings max_block_size=1"
        )
        clickhouse.query(
            chi, host="chi-test-026-mixed-replicas-default-0-0",
            sql="insert into test_disks select * from numbers(100) settings max_block_size=1"
        )
        time.sleep(5)

        with Then("Data should be placed on a single disk on a first replica"):
            out = clickhouse.query(
                chi, host="chi-test-026-mixed-replicas-default-0-0",
                sql="select arraySort(groupUniqArray(disk_name)) from system.parts where table='test_disks'"
            )
            assert out == "['default']"

        with And("Data should be placed on a second disk on a second replica"):
            out = clickhouse.query(
                chi, host="chi-test-026-mixed-replicas-default-0-1",
                sql="select arraySort(groupUniqArray(disk_name)) from system.parts where table='test_disks'"
            )
            assert out == "['disk2']"

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_027. Test troubleshooting mode")
def test_027(self):
    # TODO: Add a case for a custom endpoint
    manifest = "manifests/chi/test-027-troubleshooting-1-bad-config.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    kubectl.create_and_check(
        manifest=manifest,
        check={
            "pod_count": 1,
            "do_not_delete": 1,
            "chi_status": "InProgress",
        },
    )
    with When("ClickHouse can not start"):
        kubectl.wait_field(
            "pod",
            "chi-test-027-trouble-default-0-0-0",
            ".status.containerStatuses[0].state.waiting.reason",
            "CrashLoopBackOff"
        )
        with Then("We can start in troubleshooting mode"):
            kubectl.create_and_check(
                manifest="manifests/chi/test-027-troubleshooting-2-troubleshoot.yaml",
                check={
                    "object_counts": {
                        "statefulset": 1,
                        "pod": 1,
                        "service": 2,
                    },
                    "do_not_delete": 1,
                },
            )
            with And("We can exec to the pod"):
                out = kubectl.launch(f"exec chi-{chi}-default-0-0-0 -- bash -c \"echo Success\"")
                assert "Success" == out

        with Then("We can start in normal mode after correcting the problem"):
            kubectl.create_and_check(
                manifest="manifests/chi/test-027-troubleshooting-3-fixed-config.yaml",
                check={
                    "pod_count": 1,
                },
            )


@TestScenario
@Name("test_028. Test restart scenarios")
def test_028(self):
    util.require_zookeeper()

    manifest = "manifests/chi/test-014-replication-1.yaml"

    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    kubectl.create_and_check(
        manifest=manifest,
        check={
            "pod_count": 2,
            "do_not_delete": 1,
        },
    )

    clickhouse.query(chi, "CREATE TABLE test_dist as system.one Engine = Distributed('default', system, one)")

    sql = """select getMacro('replica') as replica, uptime() as uptime,
     (select count() from system.clusters where cluster='all-sharded') as total_hosts,
     (select count() online_hosts from cluster('all-sharded', system.one) settings skip_unavailable_shards=1) as online_hosts
     FORMAT JSONEachRow"""
    note("Before restart")
    out = clickhouse.query_with_error(chi, sql)
    note(out)
    with When("CHI is patched with a restart attribute"):
        cmd = f"patch chi {chi} --type='json' --patch='[{{\"op\":\"add\",\"path\":\"/spec/restart\",\"value\":\"RollingUpdate\"}}]'"
        kubectl.launch(cmd)
        with Then("Operator should let the query to finish"):
            out = clickhouse.query_with_error(chi, "select count(sleepEachRow(1)) from numbers(30)")
            assert out == "30"

        with Then("Operator should start processing a change"):
            # TODO: Test needs to be improved
            kubectl.wait_chi_status(chi, "InProgress")
            start_time = time.time()
            ch1_downtime = 0
            ch2_downtime = 0
            chi_downtime = 0
            with And("Queries keep running"):
                while kubectl.get_field("chi", chi, ".status.status") == "InProgress":
                    ch1 = clickhouse.query_with_error(chi, sql, host = "chi-test-014-replication-default-0-0-0")
                    ch2 = clickhouse.query_with_error(chi, sql, host = "chi-test-014-replication-default-0-1-0")
                    print(ch1 + "   " + ch2)
                    if "error" in ch1 or "Exception" in ch1 or ch2.endswith("1"):
                        ch1_downtime = ch1_downtime + 5
                    if "error" in ch2 or "Exception" in ch2 or ch1.endswith("1"):
                        ch2_downtime = ch2_downtime + 5
                    if ("error" in ch1 or "Exception" in ch1) and ("error" in ch2 or "Exception" in ch2):
                        chi_downtime = chi_downtime + 5
                    # print("Waiting 5 seconds")
                    time.sleep(5)
            end_time = time.time()
            print(f"Total restart time: {str(round(end_time - start_time))}")
            print(f"First replica downtime: {ch1_downtime}")
            print(f"Second replica downtime: {ch2_downtime}")
            print(f"CHI downtime: {chi_downtime}")
            assert chi_downtime == 0

        with Then("Check restart attribute"):
            restart = kubectl.get_field("chi", chi, ".spec.restart")
            if restart == "":
                note("Restart is cleaned automatically")
            else:
                note("Restart needs to be cleaned")
                start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")

        with Then("Restart operator. CHI should not be restarted"):
            check_operator_restart(chi, {"statefulset": 2, "pod": 2, "service": 3}, f"chi-{chi}-default-0-0-0")

        with Then("Re-apply the original config. CHI should not be restarted"):
            kubectl.create_and_check(manifest=manifest, check={"do_not_delete": 1} )
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
            assert start_time == new_start_time


    with When("Stop installation"):
        cmd = f"patch chi {chi} --type='json' --patch='[{{\"op\":\"add\",\"path\":\"/spec/stop\",\"value\":\"yes\"}}]'"
        kubectl.launch(cmd)
        kubectl.wait_chi_status(chi, "Completed")
        with Then("Stateful sets should be there but no running pods"):
            kubectl.wait_objects(chi, {"statefulset": 2, "pod": 0, "service": 2})

    kubectl.delete_chi(chi)

@TestScenario
@Name("test_029. Test different distribution settings")
def test_029(self):
    # TODO: this test needs to be extended in order to handle more distribution types
    manifest = "manifests/chi/test-029-distribution.yaml"

    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest, lookup_in_host=True))
    kubectl.create_and_check(
        manifest=manifest,
        check={
            "pod_count": 2,
            "do_not_delete": 1,
            "chi_status": "InProgress",  # do not wait
        },
    )

    kubectl.check_pod_antiaffinity(chi, "chi-test-029-distribution-t1-0-0-0", topologyKey = "kubernetes.io/hostname")
    kubectl.check_pod_antiaffinity(chi, "chi-test-029-distribution-t1-0-1-0",
                    match_labels = {
                        "clickhouse.altinity.com/chi": f"{chi}",
                        "clickhouse.altinity.com/namespace": f"{kubectl.namespace}",
                        "clickhouse.altinity.com/replica": "1",
                    },
                    topologyKey = "kubernetes.io/os")

    kubectl.delete_chi(chi)

@TestScenario
@Name("test_030. Test CRD deletion")
def test_030(self):
    manifest = "manifests/chi/test-001.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    object_counts = {"statefulset": 1, "pod": 1, "service": 2}

    kubectl.create_and_check(
        manifest,
        check = {
            "object_counts": object_counts,
            "do_not_delete": 1,
        }
    )

    with When("Delete CRD"):
        kubectl.launch("delete crd clickhouseinstallations.clickhouse.altinity.com")
        with Then("CHI should be deleted"):
            kubectl.wait_object("chi", "test-001", count=0)
            with And("CHI objects SHOULD NOT be deleted"):
                assert kubectl.count_objects(label=f"-l clickhouse.altinity.com/chi={chi}") == object_counts

    pod = kubectl.get_pod_names(chi)[0]
    start_time = kubectl.get_field("pod", pod, ".status.startTime")

    with When("Reinstall the operator"):
        util.install_operator_if_not_exist(reinstall = True)
        with Then("Re-create CHI"):
            kubectl.create_and_check(
                manifest,
                check = {
                    "object_counts": object_counts,
                    "do_not_delete": 1,
                    }
                )
        with Then("Pods should not be restarted"):
            new_start_time = kubectl.get_field("pod", pod, ".status.startTime")
            assert start_time == new_start_time
    kubectl.delete_chi(chi)

@TestModule
@Name("e2e.test_operator")
def test(self):
    util.clean_namespace(delete_chi=True)
    util.install_operator_if_not_exist()
    with Given(f"Install ClickHouse template {settings.clickhouse_template}"):
        kubectl.apply(util.get_full_path(settings.clickhouse_template, lookup_in_host=False), settings.test_namespace)

    with Given(f"ClickHouse version {settings.clickhouse_version}"):
        pass

    all_tests = [
        test_001,
        test_002,
        test_003,
        test_004,
        test_005,
        test_006,
        test_007,
        test_008,
        (test_009, {"version_from": "0.16.1"}),
        test_010,
        test_011,
        test_011_1,
        test_011_2,
        test_012,
        test_013,
        test_014,
        test_015,
        test_016,
        test_017,
        test_018,
        test_019,
        test_020,
        test_021,
        test_022,
        test_023,
        test_024,
        test_025,
        test_026,
        test_027,
        test_028,
        test_029,
        test_030,
    ]
    run_tests = all_tests

    # placeholder for selective test running
    # run_tests = [test_008, (test_009, {"version_from": "0.9.10"})]
    # run_tests = [test_002]

    for t in run_tests:
        if callable(t):
            Scenario(test=t)()
        else:
            Scenario(test=t[0], args=t[1])()
