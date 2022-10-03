import time
import yaml
import threading

import e2e.clickhouse as clickhouse
import e2e.kubectl as kubectl
import e2e.yaml_manifest as yaml_manifest
import e2e.settings as settings
import e2e.util as util
import xml.etree.ElementTree as etree


from testflows.core import *
from testflows.asserts import error
from requirements.requirements import *
from testflows.connect import Shell


@TestScenario
@Name("test_001. 1 node")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_Create("1.0")
)
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
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_UseTemplates("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_UseTemplates_Name("1.0")
)
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
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters_Cluster_Layout("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters_Cluster_Layout_Shards_Name("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters_Cluster_Layout_Replicas_Name("1.0"),
)
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
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_VolumeClaimTemplates("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_VolumeClaimTemplates_Name("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_VolumeClaimTemplates_Spec("1.0")
)
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
@Requirements(
    RQ_SRS_026_ClickHouseOperator_ACM("1.0")
)
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
@Requirements(
    RQ_SRS_026_ClickHouseOperator_Managing_VersionUpgrades("1.0")
)
def test_006(self):
    old_version = "clickhouse/clickhouse-server:21.8"
    new_version = "clickhouse/clickhouse-server:22.3"
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
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_HostTemplates_Spec_InterServerHttpPort("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_HostTemplates_Spec_TcpPort("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_HostTemplates_Spec_HttpPort("1.0")
)
def test_007(self):
    kubectl.create_and_check(
        manifest="manifests/chi/test-007-custom-ports.yaml",
        check={
            "pod_count": 1,
            "pod_ports": [8124, 9001, 9010],
        }
    )


@TestStep
def test_operator_upgrade(self, manifest, version_from, version_to=settings.operator_version):
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
                assert start_time == new_start_time, error(
                    f"{start_time} != {new_start_time}, pod restarted after operator upgrade")
        kubectl.delete_chi(chi)


@TestStep
def check_operator_restart(self, chi, wait_objects, pod):
    start_time = kubectl.get_field("pod", pod, ".status.startTime")
    with When("Restart operator"):
        util.restart_operator()
        kubectl.wait_objects(chi, wait_objects)
        kubectl.wait_chi_status(chi, "Completed")
        new_start_time = kubectl.get_field("pod", pod, ".status.startTime")

        with Then("ClickHouse pods should not be restarted"):
            assert start_time == new_start_time


@TestStep
def test_operator_restart(self, manifest, version=settings.operator_version):
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

        check_operator_restart(chi=chi, wait_objects={"statefulset": 1, "pod": 1, "service": 2},
                               pod=f"chi-{chi}-{cluster}-0-0-0")

        kubectl.delete_chi(chi)


@TestScenario
@Name("test_008. Test operator restart")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_Managing_RestartingOperator("1.0")
)
def test_008(self):
    with Then("Test simple chi for operator restart"):
        test_operator_restart(manifest="manifests/chi/test-008-operator-restart-1.yaml")
    with Then("Test advanced chi for operator restart"):
        test_operator_restart(manifest="manifests/chi/test-008-operator-restart-2.yaml")


@TestScenario
@Name("test_008_2. Test operator restart in the middle of reconcile")
def test_008_2(self):
    manifest = "manifests/chi/test-003-complex-layout.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))

    full_cluster = {"statefulset": 4, "pod": 4, "service": 5}
    # half_cluster = {"statefulset": 2, "pod": 2, "service": 3}

    with Given("4-node CHI creation started"):
        with Then("Wait for a half of the cluster to start"):
            kubectl.create_and_check(
                manifest,
                check={
                    "pod_count": 2,
                    "do_not_delete": 1,
                    "chi_status": "InProgress"
                })
        with When("Restart operator"):
            util.restart_operator()
            with Then("Cluster creation should continue after a restart"):
                # Fail faster
                kubectl.wait_object("pod", "", label=f"-l clickhouse.altinity.com/chi={chi}", count=3, retries=10)
                kubectl.wait_objects(chi, full_cluster)
                kubectl.wait_chi_status(chi, "Completed")

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_009. Test operator upgrade")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_Managing_UpgradingOperator("1.0")
)
def test_009(self):
    version_from = self.context.test_009_version_from
    version_to = self.context.test_009_version_to

    with Then("Test simple chi for operator upgrade"):
        test_operator_upgrade(manifest="manifests/chi/test-009-operator-upgrade-1.yaml", version_from=version_from,
                              version_to=version_to)
    with Then("Test advanced chi for operator upgrade"):
        test_operator_upgrade(manifest="manifests/chi/test-009-operator-upgrade-2.yaml", version_from=version_from,
                              version_to=version_to)


@TestScenario
@Name("test_010. Test zookeeper initialization")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_ZooKeeper("1.0")
)
def test_010(self):
    util.set_operator_version(settings.operator_version)
    util.require_keeper(keeper_type=self.context.keeper_type)

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


def get_user_xml_from_configmap(chi, user):
    users_xml = kubectl.get("configmap", f"chi-{chi}-common-usersd")["data"]["chop-generated-users.xml"]
    root_node = etree.fromstring(users_xml)
    return root_node.find(f"users/{user}")

@TestScenario
@Name("test_011. Test user security and network isolation")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_DefaultUsers("1.0")
)
def test_011(self):
    with Given("test-011-secured-cluster.yaml and test-011-insecured-cluster.yaml"):

        # Create clusters in parallel to speed it up
        kubectl.create_and_check(
            manifest="manifests/chi/test-011-secured-cluster.yaml",
            check={
                "apply_templates": {
                    settings.clickhouse_template,
                },
                "chi_status": "InProgress",
                "do_not_delete": 1,
            }
        )

        kubectl.create_and_check(
            manifest="manifests/chi/test-011-insecured-cluster.yaml",
            check={
                "chi_status": "InProgress",
                "do_not_delete": 1,
            }
        )

        kubectl.wait_chi_status("test-011-secured-cluster", "Completed")
        kubectl.wait_chi_status("test-011-insecured-cluster", "Completed")

        # Tests default user security
        def test_default_user():
            with Then("Default user should have 5 allowed ips"):
                # Check normalized CHI
                chi = kubectl.get("chi", "test-011-secured-cluster")
                ips = chi["status"]["normalized"]["spec"]["configuration"]["users"]["default/networks/ip"]
                print(f"normalized: {ips}") # should be ['::1', '127.0.0.1', '127.0.0.2', ip1, ip2]
                assert len(ips) == 5

            with And("Configmap should be updated"):
                ips = get_user_xml_from_configmap("test-011-secured-cluster", "default").findall('networks/ip')
                ips_l = []
                for ip in ips:
                    ips_l.append(ip.text)
                print(f"users.xml: {ips_l}") # should be ['::1', '127.0.0.1', '127.0.0.2', ip1, ip2]
                assert len(ips) == 5

            clickhouse.query("test-011-secured-cluster", "SYSTEM RELOAD CONFIG")
            with And("Connection to localhost should succeed with default user"):
                out = clickhouse.query_with_error("test-011-secured-cluster", "select 'OK'")
                assert out == 'OK'

            with And("Connection from secured to secured host should succeed"):
                out = clickhouse.query_with_error("test-011-secured-cluster", "select 'OK'",
                    host="chi-test-011-secured-cluster-default-1-0"
                )
                assert out == 'OK'

            with And("Connection from insecured to secured host should fail for default user"):
                out = clickhouse.query_with_error("test-011-insecured-cluster", "select 'OK'",
                    host="chi-test-011-secured-cluster-default-1-0"
                )
                assert out != 'OK'


        test_default_user()

        with When("Remove host_regexp for default user"):
            kubectl.create_and_check(
                manifest="manifests/chi/test-011-secured-cluster-2.yaml",
                    check={ "do_not_delete": 1 }
            )
            # Double check
            kubectl.wait_chi_status("test-011-secured-cluster", "Completed")

            with Then("Make sure host_regexp is disabled"):
                regexp = get_user_xml_from_configmap("test-011-secured-cluster", "default").find('networks/host_regexp').text
                print(f"users.xml: {regexp}")
                assert regexp == "disabled"

            with Then("Wait until configmap changes are propagated to the pod"):
                time.sleep(60)

            test_default_user()

        with And("Connection from insecured to secured host should fail for user 'user1' with no password"):
            out = clickhouse.query_with_error(
                "test-011-insecured-cluster",
                "select 'OK'",
                host="chi-test-011-secured-cluster-default-1-0",
                user="user1"
            )
            assert "Password" in out or "password" in out

        with And("Connection from insecured to secured host should work for user 'user1' with password"):
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

        with And("User 'user2' with no password should get default automatically"):
            out = clickhouse.query_with_error(
                "test-011-secured-cluster",
                "select 'OK'",
                user="user2", pwd="default"
            )
            assert out == 'OK'

        with And("User 'user3' with both plain and sha256 password should get the latter one"):
            out = clickhouse.query_with_error(
                "test-011-secured-cluster",
                "select 'OK'",
                user="user3", pwd="clickhouse_operator_password"
            )
            assert out == 'OK'

        with And("User 'restricted' with row-level security should have it applied"):
            out = clickhouse.query_with_error(
                "test-011-secured-cluster",
                "select * from system.numbers limit 1",
                user="restricted", pwd="secret"
            )
            assert out == '1000'

        with And("User 'default' with NO access management enabled CAN NOT run SHOW USERS"):
            out = clickhouse.query_with_error(
                "test-011-secured-cluster",
                "SHOW USERS",
            )
            assert 'ACCESS_DENIED' in out

        with And("User 'user4' with access management enabled CAN run SHOW USERS"):
            out = clickhouse.query(
                "test-011-secured-cluster",
                "SHOW USERS",
                user="user4", pwd="secret"
            )
            assert 'ACCESS_DENIED' not in out

        with And("User 'user5' with google.com as a host filter can not login"):
            out = clickhouse.query_with_error(
                "test-011-insecured-cluster",
                "select 'OK'",
                user="user5", pwd="secret"
            )
            assert out != 'OK'

        with And("User 'clickhouse_operator' with can login with custom password"):
            out = clickhouse.query_with_error(
                "test-011-insecured-cluster",
                "select 'OK'",
                user="clickhouse_operator", pwd="operator_secret"
            )
            assert out != 'OK'

        kubectl.delete_chi("test-011-secured-cluster")
        kubectl.delete_chi("test-011-insecured-cluster")


@TestScenario
@Name("test_011_1. Test default user security")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_DefaultUsers("1.0")
)
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
@Requirements(
    RQ_SRS_026_ClickHouseOperator_Secrets("1.0")
)
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
@Requirements(
    RQ_SRS_026_ClickHouseOperator_ServiceTemplates("1.0"),
    RQ_SRS_026_ClickHouseOperator_ServiceTemplates_NameGeneration("1.0"),
    RQ_SRS_026_ClickHouseOperator_ServiceTemplates_LoadBalancer("1.0"),
    RQ_SRS_026_ClickHouseOperator_ServiceTemplates_Annotations("1.0")
)
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
@Requirements(
    RQ_SRS_026_ClickHouseOperator_Managing_ClusterScaling_AddingShards("1.0"),
    RQ_SRS_026_ClickHouseOperator_Managing_ClusterScaling_SchemaPropagation("1.0")
)
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

    schema_objects = [
        'test_local_013',
        'test_distr_013',
        'events-distr_013',
    ]
    with Then("Create local and distributed tables"):
        clickhouse.query(chi, "CREATE DATABASE \\\"test-db-013\\\"")
        clickhouse.query(
            chi,
            "CREATE TABLE \\\"test-db-013\\\".test_local_013 Engine = Log AS SELECT * FROM system.one"
        )
        clickhouse.query(
            chi,
            "CREATE TABLE \\\"test-db-013\\\".test_distr_013 Engine = Distributed('all-sharded', \\\"test-db-013\\\", test_local_013)"
        )
        clickhouse.query(
            chi,
            "CREATE TABLE \\\"test-db-013\\\".\\\"events-distr_013\\\" as system.events "
            "ENGINE = Distributed('all-sharded', system, events)"
        )

    nShards = 2
    with Then(f"Add {nShards - 1} shards"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-013-add-shards-2.yaml",
            check={
                "object_counts": {
                    "statefulset": nShards,
                    "pod": nShards,
                    "service": nShards + 1,
                },
                "do_not_delete": 1,
            },
            timeout=1500,
        )

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

        with Then("Unaffected pod should not be restarted"):
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
            assert start_time == new_start_time

    kubectl.delete_chi(chi)


def get_shards_from_remote_servers(chi, cluster):
    if cluster == '':
        cluster = chi
    remote_servers = kubectl.get("configmap", f"chi-{chi}-common-configd")["data"]["chop-generated-remote_servers.xml"]

    chi_start = remote_servers.find(f"<{cluster}>")
    chi_end = remote_servers.find(f"</{cluster}>")
    if chi_start < 0:
        print(f"unable to find '<{cluster}>' in:")
        print(remote_servers)
        with Then(f"Remote servers should contain {cluster} cluster"):
            assert chi_start >= 0

    chi_cluster = remote_servers[chi_start:chi_end]
    # print(chi_cluster)
    chi_shards = chi_cluster.count("<shard>")

    return chi_shards


@TestScenario
@Name("test_014. Test that schema is correctly propagated on replicas")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters_Cluster_ZooKeeper("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters("1.0")
)
def test_014(self):
    util.require_keeper(keeper_type=self.context.keeper_type)

    manifest = "manifests/chi/test-014-replication-1.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    cluster = "default"
    shards = [0,1]
    n_shards = len(shards)

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
    kubectl.wait_chi_status(chi, "Completed", retries=20)

    start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")

    schema_objects = [
        'test_local_014',
        'test_view_014',
        'test_mv_014',
        'test_buffer_014',
        'a_view_014',
        'test_local2_014',
        'test_local_uuid_014',
        'test_uuid_014',
        'test_mv2_014'
    ]
    replicated_tables = [
        'default.test_local_014',
        'test_atomic_014.test_local2_014',
        'test_atomic_014.test_local_uuid_014',
        'test_atomic_014.test_mv2_014'
    ]
    create_ddls = [
        "CREATE TABLE test_local_014 ON CLUSTER '{cluster}' (a Int8) Engine = ReplicatedMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/{table}', '{replica}') ORDER BY tuple()",
        "CREATE VIEW test_view_014 as SELECT * FROM test_local_014",
        "CREATE VIEW a_view_014 as SELECT * FROM test_view_014",
        "CREATE MATERIALIZED VIEW test_mv_014 Engine = Log as SELECT * from test_local_014",
        "CREATE DICTIONARY test_dict_014 (a Int8, b Int8) PRIMARY KEY a SOURCE(CLICKHOUSE(host 'localhost' port 9000 table 'test_local_014' user 'default')) LAYOUT(FLAT()) LIFETIME(0)",
        "CREATE TABLE test_buffer_014(a Int8) Engine = Buffer(default, test_local_014, 16, 10, 100, 10000, 1000000, 10000000, 100000000)",
        "CREATE DATABASE test_atomic_014 ON CLUSTER '{cluster}' Engine = Atomic",
        "CREATE TABLE test_atomic_014.test_local2_014 ON CLUSTER '{cluster}' (a Int8) Engine = ReplicatedMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/{table}', '{replica}') ORDER BY tuple()",
        "CREATE TABLE test_atomic_014.test_local_uuid_014 ON CLUSTER '{cluster}' (a Int8) Engine = ReplicatedMergeTree ORDER BY tuple()",
        "CREATE TABLE test_atomic_014.test_uuid_014 ON CLUSTER '{cluster}' (a Int8) Engine = Distributed('{cluster}', test_atomic_014, test_local_uuid_014, rand())",
        "CREATE MATERIALIZED VIEW test_atomic_014.test_mv2_014 ON CLUSTER '{cluster}' Engine = ReplicatedMergeTree ORDER BY tuple() PARTITION BY tuple() as SELECT * from test_atomic_014.test_local2_014"
    ]
    with Given(f"Cluster {cluster} is properly configured"):
        with By(f"remote_servers have {n_shards} shards"):
            assert n_shards == get_shards_from_remote_servers(chi, cluster)
        with By(f"ClickHouse recognizes {n_shards} shards in the cluster"):
            cnt = ""
            for i in range(1, 10):
                cnt = clickhouse.query(chi, f"select count() from system.clusters where cluster ='{cluster}'", host=f"chi-{chi}-{cluster}-0-0")
                if cnt == str(n_shards):
                    break
                with Then("Not ready. Wait for " + str(i * 5) + " seconds"):
                    time.sleep(i * 5)
            assert str(n_shards) == clickhouse.query(chi, f"select count() from system.clusters where cluster ='{cluster}'", host=f"chi-{chi}-{cluster}-0-0")

        with Then("Create schema objects"):
            for q in create_ddls:
                clickhouse.query(chi, q, host=f"chi-{chi}-{cluster}-0-0")

    with Given("Replicated table is created on a first replica and data is inserted"):
        for table in replicated_tables:
            if table != 'test_atomic_014.test_mv2_014':
                clickhouse.query(chi, f"INSERT INTO {table} values(0)", host=f"chi-{chi}-{cluster}-0-0")
                clickhouse.query(chi, f"INSERT INTO {table} values(1)", host=f"chi-{chi}-{cluster}-1-0")

    def check_schema_propagation(replicas):
        with Then("Schema objects should be migrated to the new replicas"):
            for replica in replicas:
                host = f"chi-{chi}-{cluster}-0-{replica}"
                print(f"Checking replica {host}")
                print("Checking tables and views")
                for obj in schema_objects:
                    print(f"Checking {obj}")
                    out = clickhouse.query(
                        chi,
                        f"SELECT count() FROM system.tables WHERE name = '{obj}'",
                        host=host)
                    assert out == "1"

                print("Checking dictionaries")
                out = clickhouse.query(
                    chi,
                    f"SELECT count() FROM system.dictionaries WHERE name = 'test_dict_014'",
                    host=host)
                assert out == "1"

                print("Checking database engine")
                out = clickhouse.query(
                    chi,
                    f"SELECT engine FROM system.databases WHERE name = 'test_atomic_014'",
                    host=host)
                assert out == "Atomic"

        with And("Replicated table should have the data"):
            for replica in replicas:
                for shard in shards:
                    for table in replicated_tables:
                        print(f"Checking {table}")
                        out = clickhouse.query(chi, f"SELECT a FROM {table} where a = {shard}", host=f"chi-{chi}-{cluster}-{shard}-{replica}")
                        assert out == f"{shard}"

    # replicas = [1]
    replicas = [1,2]
    with When(f"Add {len(replicas)} more replicas"):
        manifest = f"manifests/chi/test-014-replication-{1+len(replicas)}.yaml"
        chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 2 + 2*len(replicas),
                "do_not_delete": 1,
            },
            timeout=600,
        )
        kubectl.wait_chi_status(chi, "Completed", retries=20)
        # Give some time for replication to catch up
        time.sleep(10)

        new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
        assert start_time == new_start_time

        check_schema_propagation(replicas)

    with When("Remove replicas"):
        manifest = "manifests/chi/test-014-replication-1.yaml"
        chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            })
        kubectl.wait_chi_status(chi, "Completed", retries=20)

        new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
        assert start_time == new_start_time

        with Then("Replica needs to be removed from the Keeper as well"):
            for shard in shards:
                out = clickhouse.query(chi, f"SELECT max(total_replicas) FROM system.replicas",
                                            host=f"chi-{chi}-{cluster}-{shard}-0")
                assert out == "1"

    with When("Restart keeper pod"):
        with Then("Delete Zookeeper pod"):
            kubectl.launch(f"delete pod {self.context.keeper_type}-0")
            time.sleep(1)

        with Then(
                f"try insert into the table while {self.context.keeper_type} offline table should be in readonly mode"):
            out = clickhouse.query_with_error(chi, "INSERT INTO test_local_014 VALUES(2)")
            assert "Table is in readonly mode" in out

        with Then(f"Wait for {self.context.keeper_type} pod to come back"):
            kubectl.wait_object("pod", f"{self.context.keeper_type}-0")
            kubectl.wait_pod_status(f"{self.context.keeper_type}-0", "Running")

        with Then(f"Wait for ClickHouse to reconnect to {self.context.keeper_type} and switch to read-write mode"):
            time.sleep(30)

        with Then("Table should be back to normal"):
            clickhouse.query(chi, "INSERT INTO test_local_014 VALUES(3)")

    with When("Add replica one more time"):
        manifest = "manifests/chi/test-014-replication-2.yaml"
        chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 4,
                "do_not_delete": 1,
            },
            timeout=600,
        )
        kubectl.wait_chi_status(chi, "Completed", retries=20)
        # Give some time for replication to catch up
        time.sleep(10)
        check_schema_propagation([1])

    with When("Delete chi"):
        kubectl.delete_chi("test-014-replication")

        with Then(f"Tables should be deleted in {self.context.keeper_type}. We can test it re-creating the chi and checking {self.context.keeper_type} contents"):
            manifest = "manifests/chi/test-014-replication-1.yaml"
            kubectl.create_and_check(
                manifest=manifest,
                check={
                    "pod_count": 2,
                    "do_not_delete": 1,
                })
            kubectl.wait_chi_status(chi, "Completed", retries=20)
            with Then("Tables are deleted in ZooKeeper"):
                out = clickhouse.query_with_error(
                    chi,
                    f"SELECT count() FROM system.zookeeper WHERE path ='/clickhouse/{chi}/tables/0/default'")
                note(f"Found {out} replicated tables in {self.context.keeper_type}")
                assert "DB::Exception: No node" in out or out == "0"

    kubectl.delete_chi("test-014-replication")


@TestScenario
@Name("test_014_1. Test replication under different configuration scenarios")
def test_014_1(self):
    util.require_keeper(keeper_type=self.context.keeper_type)

    manifest = "manifests/chi/test-014-1-replication-1.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    cluster = "default"

    kubectl.create_and_check(
        manifest=manifest,
        check={
            "apply_templates": {
                settings.clickhouse_template,
                "manifests/chit/tpl-persistent-volume-100Mi.yaml",
            },
            "pod_count": 2,
            "do_not_delete": 1,
        },
        timeout=600,
    )

    create_table = "CREATE TABLE test_local_014_1 ON CLUSTER '{cluster}' (a Int8, r UInt64) Engine = ReplicatedMergeTree('/clickhouse/{cluster}/tables/{database}/{table}', '{replica}') ORDER BY tuple()"
    table = "test_local_014_1"
    replicas = [0, 1]

    with Given("Create schema objects"):
        clickhouse.query(chi, create_table)

    def checkDataIsReplicated(replicas, v):
        with When("Data is inserted on two replicas"):
            for replica in replicas:
                clickhouse.query(chi, f"INSERT INTO {table} values({v}, rand())", host=f"chi-{chi}-{cluster}-0-{replica}")

            # Give some time for replication to catch up
            time.sleep(10)

            with Then("Data is replicated"):
                for replica in replicas:
                    out = clickhouse.query(chi, f"SELECT count() FROM {table} where a = {v}", host=f"chi-{chi}-{cluster}-0-{replica}")
                    assert int(out) == len(replicas)
                    print(f"{table} is ok")

    with When("replicasUseFQDN is disabled"):
        with Then("Replica service should be used as interserver_http_host"):
            for replica in replicas:
                cfm = kubectl.get("configmap", f"chi-{chi}-deploy-confd-{cluster}-0-{replica}")
                assert f"<interserver_http_host>chi-{chi}-{cluster}-0-{replica}</interserver_http_host>" in cfm["data"]["chop-generated-hostname-ports.xml"]


        checkDataIsReplicated(replicas, 1)

    with When("replicasUseFQDN is enabled"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-014-1-replication-2.yaml",
            check={
                "do_not_delete": 1,
            },
        )

        with Then("FQDN should be used as interserver_http_host"):
            for replica in replicas:
                cfm = kubectl.get("configmap", f"chi-{chi}-deploy-confd-{cluster}-0-{replica}")
                print(cfm["data"]["chop-generated-hostname-ports.xml"])
                assert f"<interserver_http_host>chi-{chi}-{cluster}-0-{replica}." in cfm["data"]["chop-generated-hostname-ports.xml"]

        checkDataIsReplicated(replicas, 2)

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_015. Test circular replication with hostNetwork")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_Deployments_CircularReplication("1.0")
)
def test_015(self):
    kubectl.create_and_check(
        manifest="manifests/chi/test-015-host-network.yaml",
        check={
            "object_counts": {
                    "statefulset": 2,
                    "pod": 2,
                    "service": 3,
                },
            "do_not_delete": 1,
        },
        timeout=600,
    )

    with Then("Query from one server to another one should work"):
        for i in range(1, 10):
            out = clickhouse.query_with_error(
                "test-015-host-network",
                host="chi-test-015-host-network-default-0-0",
                port="10000",
                sql="SELECT count() FROM remote('chi-test-015-host-network-default-0-1:11000', system.one)")
            if "DNS_ERROR" not in out:
                break
            print(f"DNS_ERROR. Wait for {i * 5} seconds")
            time.sleep(i*5)
        assert out == "1"

    with And("Distributed query should work"):
        out = clickhouse.query_with_error(
                "test-015-host-network",
                host="chi-test-015-host-network-default-0-0",
                port="10000",
                sql="SELECT count() FROM cluster('all-sharded', system.one) settings receive_timeout=10")
        note(f"cluster out:\n{out}")
        assert out == "2"

    kubectl.delete_chi("test-015-host-network")


@TestScenario
@Name("test_016. Test advanced settings options")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_ConfigurationFileControl_EmbeddedXML("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters("1.0")
)
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
@Requirements(
    RQ_SRS_026_ClickHouseOperator_Deployments_DifferentClickHouseVersionsOnReplicasAndShards("1.0")
)
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
@Requirements(
    RQ_SRS_026_ClickHouseOperator_RetainingVolumeClaimTemplates("1.0")
)
def test_019(self):
    util.require_keeper(keeper_type=self.context.keeper_type)
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
@Requirements(
    RQ_SRS_026_ClickHouseOperator_Deployments_MultipleStorageVolumes("1.0")
)
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
    kubectl.wait_chi_status(chi, "Completed", retries=20)

    with When("Create a table and insert 1 row"):
        clickhouse.query(chi, "create table test_disks(a Int8) Engine = MergeTree() order by a")
        clickhouse.query(chi, "insert into test_disks values (1)")

        with Then("Data should be placed on default disk"):
            out = clickhouse.query(chi, "select disk_name from system.parts where table='test_disks'")
            print(f"out : {out}")
            print(f"want: default")
            assert out == 'default'

    with When("alter table test_disks move partition tuple() to disk 'disk2'"):
        clickhouse.query(chi, "alter table test_disks move partition tuple() to disk 'disk2'")

        with Then("Data should be placed on disk2"):
            out = clickhouse.query(chi, "select disk_name from system.parts where table='test_disks'")
            print(f"out : {out}")
            print(f"want: disk2")
            assert out == 'disk2'

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_021. Test rescaling storage")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_StorageProvisioning("1.0")
)
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
        clickhouse.query(chi, "CREATE TABLE test_local_021 Engine = Log as SELECT 1 AS wtf")

    with When("Re-scale volume configuration to 2Gi"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-021-rescale-volume-02-enlarge-disk.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("Storage size should be 2Gi"):
            kubectl.wait_field("pvc", "disk1-chi-test-021-rescale-volume-simple-0-0-0",
                               ".spec.resources.requests.storage", "2Gi")
            size = kubectl.get_pvc_size("disk1-chi-test-021-rescale-volume-simple-0-0-0")
            assert size == "2Gi"

        with And("Table should exist"):
            out = clickhouse.query(chi, "select * from test_local_021")
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
            out = clickhouse.query(chi, "select * from test_local_021")
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
            out = clickhouse.query(chi, "select * from test_local_021")
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
            kubectl.wait_field("pvc", "disk1-chi-test-021-rescale-volume-simple-0-0-0",
                               ".spec.resources.requests.storage", "2Gi")
            size = kubectl.get_pvc_size("disk1-chi-test-021-rescale-volume-simple-0-0-0")
            assert size == "2Gi"

        with And("Table should exist"):
            out = clickhouse.query(chi, "select * from test_local_021")
            assert out == "1"

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_022. Test that chi with broken image can be deleted")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_DeleteBroken("1.0")
)
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
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templating("1.0")
)
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
@Requirements(
    RQ_SRS_026_ClickHouseOperator_AnnotationsInTemplates("1.0")
)
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
        assert kubectl.get_field("pod", "chi-test-024-default-0-0-0",
                                 ".metadata.annotations.podtemplate/test") == "test"
    with And("Service annotation should be populated from a serviceTemplate"):
        assert kubectl.get_field("service", "clickhouse-test-024",
                                 ".metadata.annotations.servicetemplate/test") == "test"
    with And("PVC annotation should be populated from a volumeTemplate"):
        assert kubectl.get_field("pvc", "-l clickhouse.altinity.com/chi=test-024",
                                 ".metadata.annotations.pvc/test") == "test"

    with And("Pod annotation should populated from a CHI"):
        assert kubectl.get_field("pod", "chi-test-024-default-0-0-0", ".metadata.annotations.chi/test") == "test"
    with And("Service annotation should be populated from a CHI"):
        assert kubectl.get_field("service", "clickhouse-test-024", ".metadata.annotations.chi/test") == "test"
    with And("PVC annotation should be populated from a CHI"):
        assert kubectl.get_field("pvc", "-l clickhouse.altinity.com/chi=test-024",
                                 ".metadata.annotations.chi/test") == "test"

    with And("Service annotation macros should be resolved"):
        assert kubectl.get_field("service", "clickhouse-test-024",
                                 ".metadata.annotations.servicetemplate/macro-test") == "test-024.example.com"
        assert kubectl.get_field("service", "service-test-024-0-0",
                                 ".metadata.annotations.servicetemplate/macro-test") == "test-024-0-0.example.com"

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_025. Test that service is available during re-scaling, upgrades etc.")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_Managing_ClusterScaling_AddingReplicas("1.0")
)
def test_025(self):
    util.require_keeper(keeper_type=self.context.keeper_type)

    create_table = """
    CREATE TABLE test_local_025(a UInt32)
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
        clickhouse.query(chi,
                         "CREATE TABLE test_distr_025 AS test_local_025 Engine = Distributed('default', default, test_local_025)")
        clickhouse.query(chi, f"INSERT INTO test_local_025 SELECT * FROM numbers({numbers})", timeout=120)

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
            cnt_local = clickhouse.query_with_error(chi, "SELECT count() FROM test_local_025",
                                                    "chi-test-025-rescaling-default-0-1.test.svc.cluster.local")
            cnt_lb = clickhouse.query_with_error(chi, "SELECT count() FROM test_local_025")
            cnt_distr_lb = clickhouse.query_with_error(chi, "SELECT count() FROM test_distr_025")
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
        note(
            f"Tables not ready: {round(distr_lb_error_time - start_time)}s, data not ready: {round(latent_replica_time - distr_lb_error_time)}s")

        with Then("Query to the distributed table via load balancer should never fail"):
            assert round(distr_lb_error_time - start_time) == 0
        with And("Query to the local table via load balancer should never fail"):
            assert round(lb_error_time - start_time) == 0

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_026. Test mixed single and multi-volume configuration in one cluster")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters_Cluster_Layout("1.0")
)
def test_026(self):
    util.require_keeper(keeper_type=self.context.keeper_type)

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
            out = clickhouse.query(chi, host="chi-test-026-mixed-replicas-default-0-0",
                                   sql="select count() from system.disks")
            assert out == "1"

        with And("Check that second replica has two disks"):
            out = clickhouse.query(chi, host="chi-test-026-mixed-replicas-default-0-1",
                                   sql="select count() from system.disks")
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
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Troubleshoot("1.0")
)
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
@Requirements(
    RQ_SRS_026_ClickHouseOperator_Managing_RestartingOperator("1.0")
)
def test_028(self):
    util.require_keeper(keeper_type=self.context.keeper_type)

    manifest = "manifests/chi/test-014-replication-1.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))

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
                    ch1 = clickhouse.query_with_error(chi, sql, pod="chi-test-014-replication-default-0-0-0", host="chi-test-014-replication-default-0-0", advanced_params="--connect_timeout=1 --send_timeout=10 --receive_timeout=10")
                    ch2 = clickhouse.query_with_error(chi, sql, pod="chi-test-014-replication-default-1-0-0", host="chi-test-014-replication-default-1-0", advanced_params="--connect_timeout=1 --send_timeout=10 --receive_timeout=10")

                    if "error" in ch1 or "Exception" in ch1 or ch2.endswith("1"):
                        ch1_downtime = ch1_downtime + 5
                    if "error" in ch2 or "Exception" in ch2 or ch1.endswith("1"):
                        ch2_downtime = ch2_downtime + 5
                    if ("error" in ch1 or "Exception" in ch1) and ("error" in ch2 or "Exception" in ch2):
                        chi_downtime = chi_downtime + 5

                    print(ch1 + "\t" + ch2)

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

        with Then("Clear RollingUpdate restart policy"):
            cmd = f"patch chi {chi} --type='json' --patch='[{{\"op\":\"remove\",\"path\":\"/spec/restart\"}}]'"
            kubectl.launch(cmd)
            time.sleep(10)
            kubectl.wait_chi_status(chi, "Completed")

        with Then("Restart operator. CHI should not be restarted"):
            check_operator_restart(chi=chi, wait_objects={"statefulset": 2, "pod": 2, "service": 3},
                                  pod=f"chi-{chi}-default-0-0-0")

        with Then("Re-apply the original config. CHI should not be restarted"):
            kubectl.create_and_check(manifest=manifest, check={"do_not_delete": 1})
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
            print(f"old_start_time: {start_time}")
            print(f"new_start_time: {new_start_time}")
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
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_PodTemplates_podDistribution("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_PodTemplates_podDistribution_Type("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_PodTemplates_podDistribution_Scope("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_PodTemplates_podDistribution_TopologyKey("1.0")
)
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

    kubectl.check_pod_antiaffinity(chi, "chi-test-029-distribution-t1-0-0-0", topologyKey="kubernetes.io/hostname")
    kubectl.check_pod_antiaffinity(chi, "chi-test-029-distribution-t1-0-1-0",
                                   match_labels={
                                       "clickhouse.altinity.com/chi": f"{chi}",
                                       "clickhouse.altinity.com/namespace": f"{kubectl.namespace}",
                                       "clickhouse.altinity.com/replica": "1",
                                   },
                                   topologyKey="kubernetes.io/os")

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_030. Test CRD deletion")
def test_030(self):
    manifest = "manifests/chi/test-001.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    object_counts = {"statefulset": 1, "pod": 1, "service": 2}

    kubectl.create_and_check(
        manifest,
        check={
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
        util.install_operator_if_not_exist(reinstall=True)
        with Then("Re-create CHI"):
            kubectl.create_and_check(
                manifest,
                check={
                    "object_counts": object_counts,
                    "do_not_delete": 1,
                }
            )
        with Then("Pods should not be restarted"):
            new_start_time = kubectl.get_field("pod", pod, ".status.startTime")
            assert start_time == new_start_time
    kubectl.delete_chi(chi)


@TestScenario
@Name("test_031. Test excludeFromPropagationAnnotations work")
def test_031(self):
    chi_manifest = "manifests/chi/test-031-wo-tpl.yaml"
    chi = "test-031-wo-tpl"

    with Given("I generate CHO deploy manifest"):
        with open(util.get_full_path(settings.clickhouse_operator_install_manifest)) as base_template, \
                open(util.get_full_path("../../config/config.yaml")) as config_file:
            manifest_yaml = list(yaml.safe_load_all(base_template.read()))

            config_yaml = yaml.safe_load(config_file.read())
            config_yaml["annotation"]["exclude"] = ["excl", ]
            config_contents = yaml.dump(config_yaml, default_flow_style=False)

            for doc in manifest_yaml:
                if doc["metadata"]["name"] == "etc-clickhouse-operator-files":
                    doc["data"]["config.yaml"] = config_contents
                    debug(config_contents)
                    break

            import tempfile
            with tempfile.NamedTemporaryFile(suffix=".yaml") as f:
                f.write(yaml.dump_all(manifest_yaml).encode())
                util.install_operator_if_not_exist(reinstall=True, manifest=f.name)

    with And("Restart operator"):
        util.restart_operator(ns=settings.operator_namespace)

    with When("I apply chi"):
        kubectl.create_and_check(chi_manifest, check={"do_not_delete": 1})

    with Then("I check only allowed annotations are propagated"):
        obj_types = {"statefulset", "configmap", "persistentvolumeclaim", "service"}
        for obj_type in obj_types:
            with By(f"Check that {obj_type}s annotations are correct"):
                objs = kubectl.get_obj_names(chi_name=chi, obj_type=obj_type + "s")
                for o in objs:
                    annotations = kubectl.launch(command=f"get {obj_type} {o} -o jsonpath='{{.metadata.annotations}}'")
                    assert "incl" in annotations, error()
                    assert "excl" not in annotations, error()

    kubectl.delete_chi(chi)

    with Finally("I restore original operator state"):
        util.install_operator_if_not_exist(reinstall=True,
                                           manifest=util.get_full_path(settings.clickhouse_operator_install_manifest,
                                                                       False))
        util.restart_operator(ns=settings.operator_namespace)


@TestCheck
def run_select_query(self, client_pod, user_name, password, trigger_event):
    """Run a select query in parallel until the stop signal is received."""
    i = 0
    try:
        self.context.shell = Shell()
        while not trigger_event.is_set():
            cnt_test_local = kubectl.launch(f"exec -n {kubectl.namespace} {client_pod} -- clickhouse-client --user={user_name} --password={password} -h clickhouse-test-032-rescaling  -q 'select count() from test_local' ")
            assert cnt_test_local == '100000000', error()
            i += 1
        with By(f"{i} queries have been executed during upgrade with no errors"):
            True
    finally:
        if hasattr(self.context, "shell"):
            self.context.shell.close()


@TestScenario
@Name("test_032. Test rolling update logic")
def test_032(self):
    """Test rolling update logic."""

    util.require_keeper(keeper_type=self.context.keeper_type)
    create_table = """
    CREATE TABLE test_local ON CLUSTER 'default' (a UInt32)
    Engine = ReplicatedMergeTree('/clickhouse/{installation}/tables/{shard}/{database}/{table}', '{replica}')
    PARTITION BY tuple()
    ORDER BY a
    """.replace('\r', '').replace('\n', '')

    manifest = "manifests/chi/test-032-rescaling.yaml"

    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))

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
            "do_not_delete": 2,
        },
        timeout=600,
    )

    kubectl.wait_jsonpath("pod", "chi-test-032-rescaling-default-0-0-0", "{.status.containerStatuses[0].ready}", "true",
                          ns=kubectl.namespace)
    kubectl.launch(f'run clickhouse-test-032-client --image=clickhouse/clickhouse-server:21.8 -- /bin/sh -c "sleep 3600"')
    for attempt in retries(timeout=300, delay=20, count=10):
        with attempt:
            kubectl.wait_jsonpath("pod", "clickhouse-test-032-client", "{.status.containerStatuses[0].ready}", "true",
                                ns=kubectl.namespace)

    numbers = "100000000"

    with Given("Create replicated table and populate it"):
        clickhouse.query(chi, create_table)
        clickhouse.query(chi, "CREATE TABLE test_distr as test_local Engine = Distributed('default', default, test_local)")
        clickhouse.query(chi, f"INSERT INTO test_local select * from numbers({numbers})", timeout=120)

    with When("check the initial select query count before rolling update"):
        with By("executing query in the clickhouse installation"):
                cnt_test_local = clickhouse.query(chi_name=chi, sql="select count() from test_local", with_error=True)
        with Then("checking expected result"):
            assert cnt_test_local == '100000000', error()

    trigger_event = threading.Event()

    Check("run select query until receive stop event",
        test=run_select_query,
        parallel=True)(
            client_pod = "clickhouse-test-032-client",
            user_name = "test_032",
            password = "test_032",
            trigger_event = trigger_event,
            )

    with When("Change the image in the podTemplate by updating the chi version to test the rolling update logic"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-032-rescaling-2.yaml",
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
            "do_not_delete": 2,
        },
        timeout=int(1000),
        )

    note("Setting the thread event to true...")
    trigger_event.set()
    note("Joining the parallel thread with main...")
    join()

    kubectl.delete_chi(chi)
    kubectl.launch(f"delete pod clickhouse-test-032-client", ns=kubectl.namespace, timeout=600)

@TestModule
@Name("e2e.test_operator")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_APIVersion("1.0")
)
def test(self):
    util.clean_namespace(delete_chi=True)
    util.install_operator_if_not_exist()
    with Given(f"Install ClickHouse template {settings.clickhouse_template}"):
        kubectl.apply(util.get_full_path(settings.clickhouse_template, lookup_in_host=False), settings.test_namespace)

    with Given(f"ClickHouse version {settings.clickhouse_version}"):
        pass

    # placeholder for selective test running
    # run_tests = [test_008, test_009]
    # for t in run_tests:
    #     if callable(t):
    #         Scenario(test=t)()
    #     else:
    #         Scenario(test=t[0], args=t[1])()

    # define values for Operator upgrade test (test_009)
    self.context.test_009_version_from = "0.19.2"
    self.context.test_009_version_to = settings.operator_version

    for scenario in loads(current_module(), Scenario, Suite):
        Scenario(run=scenario)
