import time
import yaml
import threading
import re

import e2e.yaml_manifest as yaml_manifest
import xml.etree.ElementTree as etree
import e2e.clickhouse as clickhouse
import e2e.settings as settings
import e2e.kubectl as kubectl
import e2e.util as util

from requirements.requirements import *
from testflows.connect import Shell
from testflows.asserts import error
from testflows.core import *
from e2e.steps import *


@TestScenario
@Name("test_001. 1 node")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
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
            "pdb": ["single"],
        },
    )


@TestScenario
@Name("test_002. useTemplates for pod, volume templates, and distribution")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_UseTemplates("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_UseTemplates_Name("1.0"),
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
            "pod_podAntiAffinity": 1,
        },
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
                "statefulset": 5,
                "pod": 5,
                "service": 6,
            },
            "pdb": ["cluster1", "cluster2"],
        },
    )


@TestScenario
@Name("test_004. Compatibility test if old syntax with volumeClaimTemplate is still supported")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_VolumeClaimTemplates("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_VolumeClaimTemplates_Name("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_VolumeClaimTemplates_Spec("1.0"),
)
def test_004(self):
    kubectl.create_and_check(
        manifest="manifests/chi/test-004-tpl.yaml",
        check={
            "pod_count": 1,
            "pod_volumes": {
                "/var/lib/clickhouse",
            },
        },
    )


@TestScenario
@Name("test_005. Test manifest created by ACM")
@Requirements(RQ_SRS_026_ClickHouseOperator_ACM("1.0"))
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
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_VersionUpgrades("1.0"))
def test_006(self):
    old_version = "clickhouse/clickhouse-server:22.3"
    new_version = "clickhouse/clickhouse-server:22.8"
    with Then("Create initial position"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-006-ch-upgrade-1.yaml",
            check={
                "pod_count": 2,
                "pod_image": old_version,
                "do_not_delete": 1,
            },
        )
    with Then("Use different podTemplate and confirm that pod image is updated"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-006-ch-upgrade-2.yaml",
            check={
                "pod_count": 2,
                "pod_image": new_version,
                "do_not_delete": 1,
            },
        )
    with Then("Change image in podTemplate itself and confirm that pod image is updated"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-006-ch-upgrade-3.yaml",
            check={
                "pod_count": 2,
                "pod_image": old_version,
            },
        )


@TestScenario
@Name("test_007. Test template with custom clickhouse ports")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_HostTemplates_Spec_InterServerHttpPort("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_HostTemplates_Spec_TcpPort("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_HostTemplates_Spec_HttpPort("1.0"),
)
def test_007(self):
    kubectl.create_and_check(
        manifest="manifests/chi/test-007-custom-ports.yaml",
        check={
            "pod_count": 1,
            "pod_ports": [8124, 9001, 9010],
        },
    )


@TestCheck
def test_operator_upgrade(self, manifest, service, version_from, version_to=settings.operator_version):
    with Given(f"clickhouse-operator from {version_from}"):
        util.install_operator_version(version_from)
        chi = yaml_manifest.get_chi_name(util.get_full_path(manifest, True))
        cluster = chi

        kubectl.create_and_check(
            manifest=manifest,
            check={
                "object_counts": {
                    "statefulset": 2,
                    "pod": 2,
                    "service": 3,
                },
                "do_not_delete": 1,
            },
        )
        start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")

        with Then("Create tables"):
            for h in [f"chi-{chi}-{cluster}-0-0-0", f"chi-{chi}-{cluster}-1-0-0"]:
                clickhouse.query(
                    chi,
                    "CREATE TABLE IF NOT EXISTS test_local (a UInt32) Engine = Log",
                    host=h,
                )
                clickhouse.query(chi, "INSERT INTO test_local SELECT 1", host=h)

    trigger_event = threading.Event()

    Check("run query until receive stop event", test=run_select_query, parallel=True)(
        host=service,
        user="test_009",
        password="test_009",
        query="select count() from cluster('{cluster}', system.one)",
        res1="2",
        res2="1",
        trigger_event=trigger_event,
    )

    Check("Check that cluster definition does not change during restart", test=check_remote_servers, parallel=True,)(
        chi=chi,
        shards=2,
        trigger_event=trigger_event,
    )

    try:
        with When(f"upgrade operator to {version_to}"):
            util.install_operator_version(version_to)
            time.sleep(15)

            kubectl.wait_chi_status(chi, "Completed", retries=20)
            kubectl.wait_objects(chi, {"statefulset": 2, "pod": 2, "service": 3})

    finally:
        trigger_event.set()
        join()

    with Then("Check that table is here"):
        tables = clickhouse.query(chi, "SHOW TABLES")
        assert "test_local" in tables
        out = clickhouse.query(chi, "SELECT count() FROM test_local")
        assert out == "1"

    with Then("ClickHouse pods should not be restarted during upgrade"):
        new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
        if start_time != new_start_time:
            kubectl.launch(f"describe chi -n {settings.test_namespace} {chi}")
            kubectl.launch(
                # In my env "pod/: prefix is already returned by $(kubectl get pods -o name -n {settings.operator_namespace} | grep clickhouse-operator)
                # f"logs -n {settings.operator_namespace} pod/$(kubectl get pods -o name -n {settings.operator_namespace} | grep clickhouse-operator) -c clickhouse-operator"
                f"logs -n {settings.operator_namespace} $(kubectl get pods -o name -n {settings.operator_namespace} | grep clickhouse-operator) -c clickhouse-operator"
            )
            assert start_time == new_start_time, error(
                f"{start_time} != {new_start_time}, pod restarted after operator upgrade"
            )

    kubectl.delete_chi(chi)


def wait_operator_restart(chi, wait_objects):
    with When("Restart operator"):
        util.restart_operator()
        time.sleep(15)
        kubectl.wait_objects(chi, wait_objects)
        kubectl.wait_chi_status(chi, "Completed")


def check_operator_restart(chi, wait_objects, pod):
    start_time = kubectl.get_field("pod", pod, ".status.startTime")
    with When("Restart operator"):
        util.restart_operator()
        time.sleep(15)
        kubectl.wait_objects(chi, wait_objects)
        kubectl.wait_chi_status(chi, "Completed")
        new_start_time = kubectl.get_field("pod", pod, ".status.startTime")

        with Then("ClickHouse pods should not be restarted during operator's restart"):
            print(f"pod start_time old: {start_time}'")
            print(f"pod start_time new: {new_start_time}")
            assert start_time == new_start_time


@TestCheck
def test_operator_restart(self, manifest, service, version=settings.operator_version):
    with Given(f"clickhouse-operator {version}"):
        util.set_operator_version(version)
        chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
        cluster = chi

        kubectl.create_and_check(
            manifest=manifest,
            check={
                "object_counts": {
                    "statefulset": 2,
                    "pod": 2,
                    "service": 3,
                },
                "do_not_delete": 1,
            },
        )

    with Then("Create tables"):
        for h in [f"chi-{chi}-{cluster}-0-0-0", f"chi-{chi}-{cluster}-1-0-0"]:
            clickhouse.query(
                chi,
                "CREATE TABLE IF NOT EXISTS test_local (a UInt32) Engine = Log",
                host=h,
            )
            clickhouse.query(
                chi,
                "CREATE TABLE IF NOT EXISTS test_dist as test_local Engine = Distributed('{cluster}', default, test_local, a)",
                host=h,
            )

    trigger_event = threading.Event()

    Check("run query until receive stop event", test=run_select_query, parallel=True)(
        host=service,
        user="test_008",
        password="test_008",
        query="select count() from cluster('{cluster}', system.one)",
        res1="2",
        res2="1",
        trigger_event=trigger_event,
    )

    Check("insert into distributed table until receive stop event", test=run_insert_query, parallel=True,)(
        host=service,
        user="test_008",
        password="test_008",
        query="insert into test_dist select number from numbers(2)",
        trigger_event=trigger_event,
    )

    Check("Check that cluster definition does not change during restart", test=check_remote_servers, parallel=True,)(
        chi=chi,
        shards=2,
        trigger_event=trigger_event,
    )

    check_operator_restart(
        chi=chi,
        wait_objects={"statefulset": 2, "pod": 2, "service": 3},
        pod=f"chi-{chi}-{cluster}-0-0-0",
    )
    trigger_event.set()
    join()

    # This section keeps failing, comment out for now
    #    with Then("Local tables should have exactly the same number of rows"):
    #        cnt0 = clickhouse.query(chi, "select count() from test_local", host=f'chi-{chi}-{cluster}-0-0-0')
    #        cnt1 = clickhouse.query(chi, "select count() from test_local", host=f'chi-{chi}-{cluster}-1-0-0')
    #        print(f"{cnt0} {cnt1}")
    #        assert cnt0 == cnt1 and cnt0 != "0"

    kubectl.delete_chi(chi)


def get_shards_from_remote_servers(chi, cluster):
    if cluster == "":
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


def get_replicas_from_remote_servers(chi, cluster):
    if cluster == "":
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
    chi_replicas = chi_cluster.count("<replica>")

    return chi_replicas / chi_shards


@TestCheck
def check_remote_servers(self, chi, shards, trigger_event, cluster=""):
    """Check cluster definition in configmap until signal is received"""
    if cluster == "":
        cluster = chi

    with Given("I have a shell"):
        self.context.shell = get_shell()
    ok = 0

    while not trigger_event.is_set():
        chi_shards = get_shards_from_remote_servers(chi, cluster)

        if chi_shards != shards:
            with Then(f"Number of shards in {cluster} cluster should be {shards}"):
                assert chi_shards == shards
        ok += 1
        time.sleep(1)

    with By(f"remote_servers were always correct {ok} times"):
        assert ok > 0


@TestScenario
@Name("test_008_1. Test operator restart")
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_RestartingOperator("1.0"))
def test_008_1(self):
    with Check("Test simple chi for operator restart"):
        test_operator_restart(
            manifest="manifests/chi/test-008-operator-restart-1.yaml",
            service="clickhouse-test-008-1",
        )


@TestScenario
@Name("test_008_2. Test operator restart")
def test_008_2(self):
    with Check("Test advanced chi for operator restart"):
        test_operator_restart(
            manifest="manifests/chi/test-008-operator-restart-2.yaml",
            service="service-test-008-2",
        )


@TestScenario
@Name("test_008_3. Test operator restart in the middle of reconcile")
def test_008_3(self):
    manifest = "manifests/chi/test-008-operator-restart-3-1.yaml"
    manifest_2 = "manifests/chi/test-008-operator-restart-3-2.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    cluster = chi

    util.require_keeper(keeper_type=self.context.keeper_type)

    full_cluster = {"statefulset": 4, "pod": 4, "service": 5}

    with Given("4-node CHI creation started"):
        with Then("Wait for a half of the cluster to start"):
            kubectl.create_and_check(
                manifest,
                check={
                    "apply_templates": {
                        "manifests/chit/tpl-persistent-volume-100Mi.yaml",
                    },
                    "pod_count": 2,
                    "do_not_delete": 1,
                    "chi_status": "InProgress",
                },
            )
        with When("Restart operator"):
            util.restart_operator()
            with Then("Cluster creation should continue after a restart"):
                # Fail faster
                kubectl.wait_object(
                    "pod",
                    "",
                    label=f"-l clickhouse.altinity.com/chi={chi}",
                    count=3,
                    retries=10,
                )
                kubectl.wait_objects(chi, full_cluster)
                kubectl.wait_chi_status(chi, "Completed")

    with Then("Upgrade ClickHouse version to run a reconcile"):
        kubectl.create_and_check(manifest_2, check={"do_not_delete": 1, "chi_status": "InProgress"})

    trigger_event = threading.Event()

    Check("Check that cluster definition does not change during restart", test=check_remote_servers, parallel=True,)(
        chi=chi,
        shards=2,
        trigger_event=trigger_event,
    )
    # Just wait for restart operator. After restart it will update cluster with new ClickHouse version
    wait_operator_restart(
        chi=chi,
        wait_objects={"statefulset": 4, "pod": 4, "service": 5},
    )
    trigger_event.set()
    join()

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_009_1. Test operator upgrade")
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_UpgradingOperator("1.0"))
def test_009_1(self):
    with Check("Test simple chi for operator upgrade"):
        test_operator_upgrade(
            manifest="manifests/chi/test-009-operator-upgrade-1.yaml",
            service="clickhouse-test-009-1",
            version_from=self.context.test_009_version_from,
            version_to=self.context.test_009_version_to,
        )


@TestScenario
@Name("test_009_2. Test operator upgrade")
def test_009_2(self):
    with Check("Test advanced chi for operator upgrade"):
        test_operator_upgrade(
            manifest="manifests/chi/test-009-operator-upgrade-2.yaml",
            service="service-test-009-2",
            version_from=self.context.test_009_version_from,
            version_to=self.context.test_009_version_to,
        )


@TestScenario
@Name("test_010. Test zookeeper initialization")
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_ZooKeeper("1.0"))
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
        },
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
@Requirements(RQ_SRS_026_ClickHouseOperator_DefaultUsers("1.0"))
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
            },
        )

        kubectl.create_and_check(
            manifest="manifests/chi/test-011-insecured-cluster.yaml",
            check={
                "chi_status": "InProgress",
                "do_not_delete": 1,
            },
        )

        kubectl.wait_chi_status("test-011-secured-cluster", "Completed")
        kubectl.wait_chi_status("test-011-insecured-cluster", "Completed")

        # Tests default user security
        def test_default_user():
            with Then("Default user should have 5 allowed ips"):
                ips = get_user_xml_from_configmap("test-011-secured-cluster", "default").findall("networks/ip")
                ips_l = []
                for ip in ips:
                    ips_l.append(ip.text)
                print(f"users.xml: {ips_l}")  # should be ['::1', '127.0.0.1', '127.0.0.2', ip1, ip2]
                assert len(ips) == 5

            clickhouse.query("test-011-secured-cluster", "SYSTEM RELOAD CONFIG")
            with And("Connection to localhost should succeed with default user"):
                out = clickhouse.query_with_error("test-011-secured-cluster", "select 'OK'")
                assert out == "OK"

            with And("Connection from secured to secured host should succeed"):
                out = clickhouse.query_with_error(
                    "test-011-secured-cluster",
                    "select 'OK'",
                    host="chi-test-011-secured-cluster-default-1-0",
                )
                assert out == "OK"

            with And("Connection from insecured to secured host should fail for default user"):
                out = clickhouse.query_with_error(
                    "test-011-insecured-cluster",
                    "select 'OK'",
                    host="chi-test-011-secured-cluster-default-1-0",
                )
                assert out != "OK"

        test_default_user()

        with When("Remove host_regexp for default user"):
            kubectl.create_and_check(
                manifest="manifests/chi/test-011-secured-cluster-2.yaml",
                check={"do_not_delete": 1},
            )
            # Double check
            kubectl.wait_chi_status("test-011-secured-cluster", "Completed")

            with Then("Make sure host_regexp is disabled"):
                regexp = (
                    get_user_xml_from_configmap("test-011-secured-cluster", "default").find("networks/host_regexp").text
                )
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
                user="user1",
            )
            assert "Password" in out or "password" in out

        with And("Connection from insecured to secured host should work for user 'user1' with password"):
            out = clickhouse.query_with_error(
                "test-011-insecured-cluster",
                "select 'OK'",
                host="chi-test-011-secured-cluster-default-1-0",
                user="user1",
                pwd="topsecret",
            )
            assert out == "OK"

        with And("Password should be encrypted"):
            cfm = kubectl.get("configmap", "chi-test-011-secured-cluster-common-usersd")
            users_xml = cfm["data"]["chop-generated-users.xml"]
            assert "<password>" not in users_xml
            assert "<password_sha256_hex>" in users_xml

        with And("User 'user2' with no password should get default automatically"):
            out = clickhouse.query_with_error("test-011-secured-cluster", "select 'OK'", user="user2", pwd="default")
            assert out == "OK"

        with And("User 'user3' with both plain and sha256 password should get the latter one"):
            out = clickhouse.query_with_error(
                "test-011-secured-cluster",
                "select 'OK'",
                user="user3",
                pwd="clickhouse_operator_password",
            )
            assert out == "OK"

        with And("User 'restricted' with row-level security should have it applied"):
            out = clickhouse.query_with_error(
                "test-011-secured-cluster",
                "select * from system.numbers limit 1",
                user="restricted",
                pwd="secret",
            )
            assert out == "1000"

        with And("User 'default' with NO access management enabled CAN NOT run SHOW USERS"):
            out = clickhouse.query_with_error(
                "test-011-secured-cluster",
                "SHOW USERS",
            )
            assert "ACCESS_DENIED" in out

        with And("User 'user4' with access management enabled CAN run SHOW USERS"):
            out = clickhouse.query("test-011-secured-cluster", "SHOW USERS", user="user4", pwd="secret")
            assert "ACCESS_DENIED" not in out

        with And("User 'user5' with google.com as a host filter can not login"):
            out = clickhouse.query_with_error("test-011-insecured-cluster", "select 'OK'", user="user5", pwd="secret")
            assert out != "OK"

        with And("User 'clickhouse_operator' with can login with custom password"):
            out = clickhouse.query_with_error(
                "test-011-insecured-cluster",
                "select 'OK'",
                user="clickhouse_operator",
                pwd="operator_secret",
            )
            assert out != "OK"

        kubectl.delete_chi("test-011-secured-cluster")
        kubectl.delete_chi("test-011-insecured-cluster")


@TestScenario
@Name("test_011_1. Test default user security")
@Requirements(RQ_SRS_026_ClickHouseOperator_DefaultUsers("1.0"))
def test_011_1(self):
    with Given("test-011-secured-default-1.yaml with password_sha256_hex for default user"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-011-secured-default-1.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("Default user plain password should be removed"):
            chi = kubectl.get("chi", "test-011-secured-default")
            assert "default/password" in chi["status"]["normalizedCompleted"]["spec"]["configuration"]["users"]
            assert chi["status"]["normalizedCompleted"]["spec"]["configuration"]["users"]["default/password"] == ""

            cfm = kubectl.get("configmap", "chi-test-011-secured-default-common-usersd")
            assert '<password remove="1"></password>' in cfm["data"]["chop-generated-users.xml"]

        with And("Connection to localhost should succeed with default user"):
            out = clickhouse.query_with_error(
                "test-011-secured-default",
                "select 'OK'",
                pwd="clickhouse_operator_password",
            )
            assert out == "OK"

        with When("Trigger installation update"):
            kubectl.create_and_check(
                manifest="manifests/chi/test-011-secured-default-2.yaml",
                check={
                    "do_not_delete": 1,
                },
            )
            with Then("Default user plain password should be removed"):
                chi = kubectl.get("chi", "test-011-secured-default")
                assert "default/password" in chi["status"]["normalizedCompleted"]["spec"]["configuration"]["users"]
                assert chi["status"]["normalizedCompleted"]["spec"]["configuration"]["users"]["default/password"] == ""

                cfm = kubectl.get("configmap", "chi-test-011-secured-default-common-usersd")
                assert '<password remove="1"></password>' in cfm["data"]["chop-generated-users.xml"]

        with When("Default user password is removed"):
            kubectl.create_and_check(
                manifest="manifests/chi/test-011-secured-default-3.yaml",
                check={
                    "do_not_delete": 1,
                },
            )
            with Then("Connection to localhost should succeed with default user and no password"):
                out = clickhouse.query_with_error("test-011-secured-default", "select 'OK'")
                assert out == "OK"

        kubectl.delete_chi("test-011-secured-default")


@TestScenario
@Name("test_011_2. Test k8s secrets usage")
@Requirements(RQ_SRS_026_ClickHouseOperator_Secrets("1.0"))
def test_011_2(self):
    with Given("test-011-secrets.yaml with secret storage"):
        kubectl.apply(
            util.get_full_path("manifests/secret/test-011-secret.yaml", False),
            ns=settings.test_namespace,
            timeout=300,
        )

        kubectl.create_and_check(
            manifest="manifests/chi/test-011-secrets.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("Connection to localhost should succeed with user1"):
            out = clickhouse.query_with_error("test-011-secrets", "select 'OK'", user="user1", pwd="pwduser1")
            assert out == "OK"

        with And("Connection to localhost should succeed with user2"):
            out = clickhouse.query_with_error("test-011-secrets", "select 'OK'", user="user2", pwd="pwduser2")
            assert out == "OK"

        with And("Connection to localhost should succeed with user3"):
            out = clickhouse.query_with_error("test-011-secrets", "select 'OK'", user="user3", pwd="pwduser3")
            assert out == "OK"

        kubectl.delete_chi("test-011-secrets")
        kubectl.launch(
            "delete secret test-011-secret",
            ns=settings.test_namespace,
            timeout=600,
            ok_to_fail=True,
        )


@TestScenario
@Name("test_012. Test service templates")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_ServiceTemplates("1.0"),
    RQ_SRS_026_ClickHouseOperator_ServiceTemplates_NameGeneration("1.0"),
    RQ_SRS_026_ClickHouseOperator_ServiceTemplates_LoadBalancer("1.0"),
    RQ_SRS_026_ClickHouseOperator_ServiceTemplates_Annotations("1.0"),
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
        },
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
            },
        )

        with And("NodePort should not change"):
            new_node_port = kubectl.get("service", "service-test-012")["spec"]["ports"][0]["nodePort"]
            assert (
                new_node_port == node_port
            ), f"LoadBalancer.spec.ports[0].nodePort changed from {node_port} to {new_node_port}"

    kubectl.delete_chi("test-012")


@TestScenario
@Requirements(
    RQ_SRS_026_ClickHouseOperator_Managing_ClusterScaling_AddingShards("1.0"),
    RQ_SRS_026_ClickHouseOperator_Managing_ClusterScaling_SchemaPropagation("1.0"),
)
@Name("test_013_1. Automatic schema propagation for shards")
def test_013_1(self):
    """Check clickhouse operator supports automatic schema propagation for shards."""
    cluster = "simple"
    manifest = f"manifests/chi/test-013-1-1-schema-propagation.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    n_shards = 2

    util.require_keeper(keeper_type=self.context.keeper_type)

    with When("chi with 1 shard exists"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "apply_templates": {
                    settings.clickhouse_template,
                    "manifests/chit/tpl-persistent-volume-100Mi.yaml",
                },
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

    create_table_queries = [
        "CREATE TABLE mergetree_table (d DATE, a String, b UInt8, x String, y Int8) ENGINE = "
        "MergeTree() PARTITION BY y ORDER BY d",
        "CREATE TABLE replacing_mergetree_table (d DATE, a String, b UInt8, x String, y Int8) ENGINE = "
        "ReplacingMergeTree() PARTITION BY y ORDER BY d",
        "CREATE TABLE summing_mergetree_table (d DATE, a String, b UInt8, x String, y Int8) ENGINE = "
        "SummingMergeTree() PARTITION BY y ORDER BY d",
        "CREATE TABLE aggregating_mergetree_table (d DATE, a String, b UInt8, x String, y Int8) ENGINE = "
        "AggregatingMergeTree() PARTITION BY y ORDER BY d",
        "CREATE TABLE collapsing_mergetree_table (d DATE, a String, b UInt8, x String, y Int8, Sign Int8) "
        "ENGINE = CollapsingMergeTree(Sign) PARTITION BY y ORDER BY d",
        "CREATE TABLE versionedcollapsing_mergetree_table (d Date, a String, b UInt8, x String, y Int8, version UInt64,"
        "sign Int8 DEFAULT 1) ENGINE = VersionedCollapsingMergeTree(sign, version) PARTITION BY y ORDER BY d",
        "CREATE TABLE replicated_table (d DATE, a String, b UInt8, x String, y Int8) ENGINE = "
        "ReplicatedMergeTree('/clickhouse/{cluster}/tables/{database}/replicated_table', "
        "'{replica}') PARTITION BY y ORDER BY d",
        "CREATE TABLE replicated_replacing_table (d DATE, a String, b UInt8, x String, y Int8) ENGINE = "
        "ReplicatedReplacingMergeTree ('/clickhouse/{cluster}/tables/{database}/replicated_replacing_table', "
        "'{replica}') PARTITION BY y ORDER BY d",
        "CREATE TABLE replicated_summing_table (d DATE, a String, b UInt8, x String, y Int8) ENGINE = "
        "ReplicatedSummingMergeTree('/clickhouse/{cluster}/tables/{database}/replicated_summing_table', "
        "'{replica}') PARTITION BY y ORDER BY d",
        "CREATE TABLE replicated_aggregating_table (d DATE, a String, b UInt8, x String, y Int8) ENGINE ="
        "ReplicatedAggregatingMergeTree('/clickhouse/{cluster}/tables/{database}/replicated_aggregating_table',"
        "'{replica}') PARTITION BY y ORDER BY d",
        "CREATE TABLE replicated_collapsing_table ON CLUSTER 'simple' (d DATE, a String, b UInt8, x String, y Int8, Sign Int8) "
        "ENGINE = ReplicatedCollapsingMergeTree(Sign) PARTITION BY y ORDER BY d",
        "CREATE TABLE replicated_versionedcollapsing_table ON CLUSTER 'simple' (d Date, a String, b UInt8, x String, y Int8, version UInt64,"
        " sign Int8 DEFAULT 1) ENGINE = ReplicatedVersionedCollapsingMergeTree(sign, version) PARTITION "
        "BY y ORDER BY d",
        "CREATE TABLE table_for_dict ( key_column UInt64, third_column String ) "
        "ENGINE = MergeTree() ORDER BY key_column",
        "CREATE DICTIONARY ndict ON CLUSTER 'simple' ( key_column UInt64 DEFAULT 0, "
        "third_column String DEFAULT 'qqq' ) PRIMARY KEY key_column "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict' "
        "PASSWORD '' DB 'default')) LIFETIME(MIN 1 MAX 10) LAYOUT(HASHED())",
        "CREATE TABLE table_for_distributed (d Date, a String, b UInt8 DEFAULT 1, x String, "
        "y Int8 ) ENGINE = SummingMergeTree PARTITION BY y ORDER BY d SETTINGS index_granularity = 8192",
        "CREATE TABLE IF NOT EXISTS distr_test ON CLUSTER 'simple' (d Date, a String, b UInt8) "
        "ENGINE = Distributed('simple', default, table_for_distributed, rand())",
        "CREATE TABLE table_for_kafka (readings_id Int32 Codec(DoubleDelta, LZ4), "
        "time DateTime Codec(DoubleDelta, LZ4), date ALIAS toDate(time), temperature Decimal(5,2) "
        "Codec(T64, LZ4) ) Engine = MergeTree PARTITION BY toYYYYMM(time) ORDER BY (readings_id, time)",
        "CREATE TABLE kafka_readings_queue (readings_id Int32, time DateTime, "
        "temperature Decimal(5,2) ) ENGINE = Kafka SETTINGS "
        "kafka_broker_list = 'kafka-headless.kafka:9092', kafka_topic_list = 'table_for_kafka', "
        "kafka_group_name = 'readings_consumer_group1', kafka_format = 'CSV', "
        "kafka_max_block_size = 1048576",
        "CREATE TABLE table_for_view (date Date, id Int8, name String, value Int64) "
        "ENGINE = MergeTree() Order by date",
        "CREATE VIEW test_view AS SELECT * FROM table_for_view",
        "CREATE TABLE table_for_materialized_view (when DateTime, userid UInt32, bytes Float32) "
        "ENGINE = MergeTree PARTITION BY toYYYYMM(when) ORDER BY (userid, when)",
        "CREATE MATERIALIZED VIEW materialized_view ENGINE = SummingMergeTree "
        "PARTITION BY toYYYYMM(day) ORDER BY (userid, day) "
        "POPULATE AS SELECT toStartOfDay(when) AS day, userid, count() as downloads, "
        "sum(bytes) AS bytes FROM table_for_materialized_view GROUP BY userid, day",
        "CREATE TABLE table_for_live_vew (d DATE, a String, b UInt8, x String, y Int8) ENGINE = "
        "ReplicatedMergeTree('/clickhouse/{cluster}/tables/{shard}/default/table_for_live_vew', "
        "'{replica}') PARTITION BY y ORDER BY d",
        "CREATE LIVE VIEW test_live_view AS SELECT * FROM table_for_live_vew",
        "CREATE TABLE table_for_window_view on cluster 'simple' (id UInt64, timestamp DateTime) ENGINE = ReplicatedMergeTree() order by id",
        "CREATE WINDOW VIEW wv ENGINE = Log() as select count(id), tumbleStart(w_id) as window_start from table_for_window_view "
        "group by tumble(timestamp, INTERVAL '10' SECOND) as w_id",
        "CREATE TABLE tinylog_table (id UInt64, value1 UInt8, value2 UInt16, value3 UInt32, value4 UInt64) ENGINE=TinyLog",
        "CREATE TABLE log_table (id UInt64, value1 Nullable(UInt64), value2 Nullable(UInt64), value3 Nullable(UInt64)) ENGINE=Log",
        "CREATE TABLE stripelog_table (timestamp DateTime, message_type String, message String ) ENGINE = StripeLog",
        "CREATE TABLE null_table (a String, b Int8, x UInt8) ENGINE = Null",
        "CREATE TABLE merge_table (id Int32) Engine = Merge(default, '_*')",
        "CREATE TABLE set_table (userid UInt64) ENGINE = Set",
        "CREATE TABLE left_join_table (x UInt32, s String) engine = Join(ALL, LEFT, x)",
        "CREATE TABLE url_table (word String, value UInt64) ENGINE=URL('http://127.0.0.1:12345/', CSV)",
        "CREATE TABLE memory_table (a Int64, b Nullable(Int64), c String) engine = Memory",
        "CREATE TABLE table_for_buffer (EventDate Date, UTCEventTime DateTime, MoscowEventDate Date "
        "DEFAULT toDate(UTCEventTime)) ENGINE = MergeTree() Order by EventDate",
        "CREATE TABLE buffer_table AS table_for_buffer ENGINE = Buffer('default', "
        "'table_for_buffer', 16, 10, 100, 10000, 1000000, 10000000, 100000000)",
        "CREATE TABLE generate_random_table (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)",
        "CREATE TABLE file_engine_table (name String, value UInt32) ENGINE = File(TabSeparated)",
        "CREATE TABLE odbc (BannerID UInt64, CompaignID UInt64) ENGINE = "
        "ODBC('DSN=pgconn;Database=postgres', somedb, bannerdict)",
        "CREATE TABLE jdbc_table (Str String) ENGINE = JDBC('{}', 'default', 'ExternalTable')",
        "CREATE TABLE mysql_table (float_nullable Nullable(Float32), int_id Int32 ) "
        "ENGINE = MySQL('localhost:3306', 'vs_db', 'vs_table', 'vs_user', 'vs_pass')",
        "CREATE TABLE mongodb_table ( key UInt64, data String ) ENGINE = "
        "MongoDB('mongo1:27017', 'vs_db', 'vs_collection', 'testuser', 'clickhouse_password')",
        "CREATE TABLE hdfs_table (name String, value UInt32) ENGINE = " "HDFS('hdfs://hdfs1:9000/some_file', 'TSV')",
        "CREATE TABLE s3_engine_table (name String, value UInt32)ENGINE = S3("
        "'https://storage.test.net/my-test1/test-data.csv.gz', 'CSV', 'gzip')",
        "CREATE TABLE embeddedrocksdb_table (key UInt64, value String) Engine = EmbeddedRocksDB " "PRIMARY KEY(key)",
        "CREATE TABLE postgresql_table (float_nullable Nullable(Float32), str String,"
        " int_id Int32 ) ENGINE = PostgreSQL('localhost:5432', 'public_db', 'test_table', "
        "'postges_user', 'postgres_password')",
        "CREATE TABLE externaldistributed_table (id UInt32, name String, age UInt32, money UInt32) ENGINE = "
        "ExternalDistributed('PostgreSQL', 'localhost:5432', 'clickhouse', "
        "'test_replicas', 'postgres', 'mysecretpassword')",
        # "CREATE TABLE materialized_postgresql_table (key UInt64, value UInt64) ENGINE = "
        # "MaterializedPostgreSQL('localhost:5433', 'postgres_database', 'postgresql_replica', "
        # "'postgres_user', 'postgres_password')PRIMARY KEY key",
        # "CREATE TABLE rabbitmq_table (key UInt64, value UInt64 ) ENGINE = RabbitMQ SETTINGS "
        # "rabbitmq_host_port = 'localhost:5672', rabbitmq_exchange_name = 'exchange1', "
        # "rabbitmq_exchange_type = 'headers', rabbitmq_routing_key_list = 'format=logs,type=report,"
        # "year=2020', rabbitmq_format = 'JSONEachRow', rabbitmq_num_consumers = 5",
    ]

    with And("I create tables with every engine"):
        for query in create_table_queries:
            clickhouse.query(chi, query)

    start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
    with When("I add 1 more shard"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-013-1-2-schema-propagation.yaml",
            check={
                "apply_templates": {
                    settings.clickhouse_template,
                },
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )

    with Then("Unaffected pod should not be restarted"):
        new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
        assert start_time == new_start_time

    with Then("remote_servers.xml should contain 2 shards"):
        assert get_shards_from_remote_servers(chi, cluster) == 2

    table_names = clickhouse.query(chi, "SHOW TABLES", pod="chi-test-013-1-schema-propagation-simple-0-0-0").split()

    with Then("I check tables are propagated correctly 1"):
        for attempt in retries(timeout=500, delay=1):
            with attempt:
                for table_name in table_names:
                    if table_name[0] != ".":
                        expected_describe = clickhouse.query(
                            chi,
                            f"DESCRIBE {table_name}",
                            pod="chi-test-013-1-schema-propagation-simple-0-0-0",
                        )
                        actual_describe = clickhouse.query(
                            chi,
                            f"DESCRIBE {table_name}",
                            pod="chi-test-013-1-schema-propagation-simple-1-0-0",
                        )
                        assert expected_describe == actual_describe, error()

    with When("I delete second shard"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

    with Then("Unaffected pod should not be restarted"):
        new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
        assert start_time == new_start_time

    with Then("remote_servers.xml should contain 1 shard"):
        assert get_shards_from_remote_servers(chi, cluster) == 1

    with When("I add 1 more shard with DistributedTablesOnly schema policy"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-013-1-3-schema-propagation.yaml",
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )

    tables_on_second_shard = clickhouse.query(
        chi, f"show tables", pod="chi-test-013-1-schema-propagation-simple-1-0-0"
    ).split()

    with Then("I check tables are propagated correctly 2"):
        for attempt in retries(timeout=500, delay=1):
            with attempt:
                assert len(tables_on_second_shard) == 2, error()
                assert ("distr_test" in tables_on_second_shard) and (
                    "table_for_distributed" in tables_on_second_shard
                ), error()
                for table_name in tables_on_second_shard:
                    expected_describe = clickhouse.query(
                        chi,
                        f"DESCRIBE {table_name}",
                        pod="chi-test-013-1-schema-propagation-simple-0-0-0",
                    )
                    actual_describe = clickhouse.query(
                        chi,
                        f"DESCRIBE {table_name}",
                        pod="chi-test-013-1-schema-propagation-simple-1-0-0",
                    )
                    assert expected_describe == actual_describe, error()

    with When("I delete second shard"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

    with When("I add 1 more shard with None schema policy"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-013-1-4-schema-propagation.yaml",
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )

    with Then("I check tables are not propagated"):
        tables_on_second_shard = clickhouse.query(
            chi, f"show tables", pod="chi-test-013-1-schema-propagation-simple-1-0-0"
        ).split()
        assert len(tables_on_second_shard) == 0, error()

    kubectl.delete_chi(chi)


def get_shards_from_remote_servers(chi, cluster):
    if cluster == "":
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
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters("1.0"),
)
def test_014(self):
    util.require_keeper(keeper_type=self.context.keeper_type)

    manifest = "manifests/chi/test-014-replication-1.yaml"
    chi_name = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    cluster = "default"
    shards = [0, 1]
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
    kubectl.wait_chi_status(chi_name, "Completed", retries=20)

    start_time = kubectl.get_field("pod", f"chi-{chi_name}-{cluster}-0-0-0", ".status.startTime")

    schema_objects = [
        "test_local_014",
        "test_view_014",
        "test_mv_014",
        "test_lv_014",
        "test_buffer_014",
        "a_view_014",
        "test_local2_014",
        "test_local_uuid_014",
        "test_uuid_014",
        "test_mv2_014",
    ]
    replicated_tables = [
        "default.test_local_014",
        "test_atomic_014.test_local2_014",
        "test_atomic_014.test_local_uuid_014",
        "test_atomic_014.test_mv2_014",
    ]
    create_ddls = [
        "CREATE TABLE test_local_014 ON CLUSTER '{cluster}' (a Int8) Engine = ReplicatedMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/{table}', '{replica}') ORDER BY tuple()",
        "CREATE VIEW test_view_014 as SELECT * FROM test_local_014",
        "CREATE VIEW a_view_014 as SELECT * FROM test_view_014",
        "CREATE MATERIALIZED VIEW test_mv_014 Engine = Log as SELECT * from test_local_014",
        "CREATE LIVE VIEW test_lv_014 as SELECT * from test_local_014",
        "CREATE DICTIONARY test_dict_014 (a Int8, b Int8) PRIMARY KEY a SOURCE(CLICKHOUSE(host 'localhost' port 9000 table 'test_local_014' user 'default')) LAYOUT(FLAT()) LIFETIME(0)",
        "CREATE TABLE test_buffer_014(a Int8) Engine = Buffer(default, test_local_014, 16, 10, 100, 10000, 1000000, 10000000, 100000000)",
        "CREATE DATABASE test_atomic_014 ON CLUSTER '{cluster}' Engine = Atomic",
        "CREATE TABLE test_atomic_014.test_local2_014 ON CLUSTER '{cluster}' (a Int8) Engine = ReplicatedMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/{table}', '{replica}') ORDER BY tuple()",
        "CREATE TABLE test_atomic_014.test_local_uuid_014 ON CLUSTER '{cluster}' (a Int8) Engine = ReplicatedMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/{table}/{uuid}', '{replica}') ORDER BY tuple()",
        "CREATE TABLE test_atomic_014.test_uuid_014 ON CLUSTER '{cluster}' (a Int8) Engine = Distributed('{cluster}', test_atomic_014, test_local_uuid_014, rand())",
        "CREATE MATERIALIZED VIEW test_atomic_014.test_mv2_014 ON CLUSTER '{cluster}' Engine = ReplicatedMergeTree ORDER BY tuple() PARTITION BY tuple() as SELECT * from test_atomic_014.test_local2_014",
    ]
    with Given(f"Cluster {cluster} is properly configured"):
        with By(f"remote_servers have {n_shards} shards"):
            assert n_shards == get_shards_from_remote_servers(chi_name, cluster)
        with By(f"ClickHouse recognizes {n_shards} shards in the cluster"):
            cnt = ""
            for i in range(1, 10):
                cnt = clickhouse.query(
                    chi_name,
                    f"select count() from system.clusters where cluster ='{cluster}'",
                    host=f"chi-{chi_name}-{cluster}-0-0",
                )
                if cnt == str(n_shards):
                    break
                with Then("Not ready. Wait for " + str(i * 5) + " seconds"):
                    time.sleep(i * 5)
            assert str(n_shards) == clickhouse.query(
                chi_name,
                f"select count() from system.clusters where cluster ='{cluster}'",
                host=f"chi-{chi_name}-{cluster}-0-0",
            )

        with Then("Create schema objects"):
            for q in create_ddls:
                clickhouse.query(chi_name, q, host=f"chi-{chi_name}-{cluster}-0-0")

    with Given("Replicated table is created on a first replica and data is inserted"):
        for table in replicated_tables:
            if table != "test_atomic_014.test_mv2_014":
                clickhouse.query(
                    chi_name,
                    f"INSERT INTO {table} values(0)",
                    host=f"chi-{chi_name}-{cluster}-0-0",
                )
                clickhouse.query(
                    chi_name,
                    f"INSERT INTO {table} values(1)",
                    host=f"chi-{chi_name}-{cluster}-1-0",
                )

    def check_schema_propagation(replicas):
        with Then("Schema objects should be migrated to the new replicas"):
            for replica in replicas:
                host = f"chi-{chi_name}-{cluster}-0-{replica}"
                print(f"Checking replica {host}")
                print("Checking tables and views")
                for obj in schema_objects:
                    print(f"Checking {obj}")
                    out = clickhouse.query(
                        chi_name,
                        f"SELECT count() FROM system.tables WHERE name = '{obj}'",
                        host=host,
                    )
                    assert out == "1"

                print("Checking dictionaries")
                out = clickhouse.query(
                    chi_name,
                    f"SELECT count() FROM system.dictionaries WHERE name = 'test_dict_014'",
                    host=host,
                )
                assert out == "1"

                print("Checking database engine")
                out = clickhouse.query(
                    chi_name,
                    f"SELECT engine FROM system.databases WHERE name = 'test_atomic_014'",
                    host=host,
                )
                assert out == "Atomic"

        with And("Replicated table should have the data"):
            for replica in replicas:
                for shard in shards:
                    for table in replicated_tables:
                        print(f"Checking {table}")
                        out = clickhouse.query(
                            chi_name,
                            f"SELECT a FROM {table} where a = {shard}",
                            host=f"chi-{chi_name}-{cluster}-{shard}-{replica}",
                        )
                        assert out == f"{shard}"

    # replicas = [1]
    replicas = [1, 2]
    with When(f"Add {len(replicas)} more replicas"):
        manifest = f"manifests/chi/test-014-replication-{1+len(replicas)}.yaml"
        chi_name = yaml_manifest.get_chi_name(util.get_full_path(manifest))
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 2 + 2 * len(replicas),
                "do_not_delete": 1,
            },
            timeout=600,
        )
        kubectl.wait_chi_status(chi_name, "Completed", retries=20)
        # Give some time for replication to catch up
        time.sleep(10)

        new_start_time = kubectl.get_field("pod", f"chi-{chi_name}-{cluster}-0-0-0", ".status.startTime")
        assert start_time == new_start_time

        check_schema_propagation(replicas)

    with When("Remove replicas"):
        manifest = "manifests/chi/test-014-replication-1.yaml"
        chi_name = yaml_manifest.get_chi_name(util.get_full_path(manifest))
        chi = yaml_manifest.get_manifest_data(util.get_full_path(manifest))
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )
        kubectl.wait_chi_status(chi_name, "Completed", retries=20)
        with Then("Replica is removed from remote_servers.xml as well"):
            assert get_replicas_from_remote_servers(chi_name, cluster) == 1

        new_start_time = kubectl.get_field("pod", f"chi-{chi_name}-{cluster}-0-0-0", ".status.startTime")
        assert start_time == new_start_time

        with Then("Replica needs to be removed from the Keeper as well"):
            for shard in shards:
                out = clickhouse.query(
                    chi_name,
                    f"SELECT max(total_replicas) FROM system.replicas",
                    host=f"chi-{chi_name}-{cluster}-{shard}-0",
                )
                assert out == "1"

    with When("Restart keeper pod"):
        with Then("Delete Zookeeper pod"):
            kubectl.launch(f"delete pod {self.context.keeper_type}-0")
            time.sleep(1)

        with Then(
            f"try insert into the table while {self.context.keeper_type} offline table should be in readonly mode"
        ):
            out = clickhouse.query_with_error(chi_name, "INSERT INTO test_local_014 VALUES(2)")
            assert "Table is in readonly mode" in out

        with Then(f"Wait for {self.context.keeper_type} pod to come back"):
            kubectl.wait_object("pod", f"{self.context.keeper_type}-0")
            kubectl.wait_pod_status(f"{self.context.keeper_type}-0", "Running")

        with Then(f"Wait for ClickHouse to reconnect to {self.context.keeper_type} and switch from read-write mode"):
            util.wait_clickhouse_no_readonly_replicas(chi)

        with Then("Table should be back to normal"):
            clickhouse.query(chi_name, "INSERT INTO test_local_014 VALUES(3)")

    with When("Add replica one more time"):
        manifest = "manifests/chi/test-014-replication-2.yaml"
        chi_name = yaml_manifest.get_chi_name(util.get_full_path(manifest))
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 4,
                "do_not_delete": 1,
            },
            timeout=600,
        )
        kubectl.wait_chi_status(chi_name, "Completed", retries=20)
        # Give some time for replication to catch up
        time.sleep(10)
        check_schema_propagation([1])

    with When("Delete chi"):
        kubectl.delete_chi("test-014-replication")

        with Then(
            f"Tables should be deleted in {self.context.keeper_type}. We can test it re-creating the chi and checking {self.context.keeper_type} contents"
        ):
            manifest = "manifests/chi/test-014-replication-1.yaml"
            kubectl.create_and_check(
                manifest=manifest,
                check={
                    "pod_count": 2,
                    "do_not_delete": 1,
                },
            )
            kubectl.wait_chi_status(chi_name, "Completed", retries=20)
            with Then("Tables are deleted in ZooKeeper"):
                out = clickhouse.query_with_error(
                    chi_name,
                    f"SELECT count() FROM system.zookeeper WHERE path ='/clickhouse/{chi_name}/tables/0/default'",
                )
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

    def check_data_is_replicated(replicas, v):
        with When("Data is inserted on two replicas"):
            for replica in replicas:
                clickhouse.query(
                    chi,
                    f"INSERT INTO {table} values({v}, rand())",
                    host=f"chi-{chi}-{cluster}-0-{replica}",
                )

            # Give some time for replication to catch up
            time.sleep(10)

            with Then("Data is replicated"):
                for replica in replicas:
                    out = clickhouse.query(
                        chi,
                        f"SELECT count() FROM {table} where a = {v}",
                        host=f"chi-{chi}-{cluster}-0-{replica}",
                    )
                    assert int(out) == len(replicas)
                    print(f"{table} is ok")

    with When("replicasUseFQDN is disabled"):
        with Then("Replica service should be used as interserver_http_host"):
            for replica in replicas:
                cfm = kubectl.get("configmap", f"chi-{chi}-deploy-confd-{cluster}-0-{replica}")
                assert (
                    f"<interserver_http_host>chi-{chi}-{cluster}-0-{replica}</interserver_http_host>"
                    in cfm["data"]["chop-generated-hostname-ports.xml"]
                )

        check_data_is_replicated(replicas, 1)

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
                print("looking for:")
                print(f"<interserver_http_host>chi-{chi}-{cluster}-0-{replica}.")
                print("in")
                print(cfm["data"]["chop-generated-hostname-ports.xml"])
                assert (
                    f"<interserver_http_host>chi-{chi}-{cluster}-0-{replica}."
                    in cfm["data"]["chop-generated-hostname-ports.xml"]
                )

        check_data_is_replicated(replicas, 2)

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_015. Test circular replication with hostNetwork")
@Requirements(RQ_SRS_026_ClickHouseOperator_Deployments_CircularReplication("1.0"))
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
                sql="SELECT count() FROM remote('chi-test-015-host-network-default-0-1:11000', system.one)",
            )
            if "DNS_ERROR" not in out:
                break
            print(f"DNS_ERROR. Wait for {i * 5} seconds")
            time.sleep(i * 5)
        assert out == "1"

    with And("Distributed query should work"):
        out = clickhouse.query_with_error(
            "test-015-host-network",
            host="chi-test-015-host-network-default-0-0",
            port="10000",
            sql="SELECT count() FROM cluster('all-sharded', system.one) settings receive_timeout=10",
        )
        note(f"cluster out:\n{out}")
        assert out == "2"

    kubectl.delete_chi("test-015-host-network")


@TestScenario
@Name("test_016. Test advanced settings options")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_ConfigurationFileControl_EmbeddedXML("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters("1.0"),
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
                f'exec chi-{chi}-default-0-0-0 -- bash -c "grep test_norestart /etc/clickhouse-server/users.d/my_users.xml | wc -l"',
                "1",
            )

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
            },
        )
        with Then("Wait for configmap changes to apply"):
            kubectl.wait_command(
                f'exec chi-{chi}-default-0-0-0 -- bash -c "grep test-03 /etc/clickhouse-server/config.d/custom.xml | wc -l"',
                "1",
            )

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
            },
        )
        with Then("Wait for configmap changes to apply"):
            kubectl.wait_command(
                f'exec chi-{chi}-default-0-0-0 -- bash -c "grep test-custom2 /etc/clickhouse-server/config.d/custom2.xml | wc -l"',
                "1",
            )

        with And("Custom macro 'test-custom2' should be found"):
            out = clickhouse.query(
                chi,
                sql="select substitution from system.macros where macro='test-custom2'",
            )
            assert out == "test-custom2"

        with And("ClickHouse SHOULD BE restarted"):
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
            assert start_time < new_start_time

    kubectl.delete_chi("test-016-settings")


@TestScenario
@Name("test_017. Test deployment of multiple versions in a cluster")
@Requirements(RQ_SRS_026_ClickHouseOperator_Deployments_DifferentClickHouseVersionsOnReplicasAndShards("1.0"))
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
                f'exec chi-{chi}-default-0-0-0 -- bash -c "grep display_name /etc/clickhouse-server/config.d/chop-generated-settings.xml"'
            )
            note(display_name)
            assert "new_display_name" in display_name
            with Then("And ClickHouse should pick them up"):
                macros = clickhouse.query(chi, "SELECT substitution from system.macros where macro = 'test'")
                note(macros)
                assert "new_test" == macros

    kubectl.delete_chi(chi)


@TestCheck
def test_019(self, step=1):
    util.require_keeper(keeper_type=self.context.keeper_type)
    manifest = f"manifests/chi/test-019-{step}-retain-volume-1.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    kubectl.create_and_check(
        manifest=manifest,
        check={
            "pod_count": 1,
            "do_not_delete": 1,
        },
    )

    create_non_replicated_table = "create or replace table t1 Engine = Log as select 1 as a"
    create_replicated_table = """
    create or replace table t2
    Engine = ReplicatedMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}', '{replica}')
    partition by tuple() order by a
    as select 1 as a""".replace(
        "\r", ""
    ).replace(
        "\n", ""
    )

    with Given("ClickHouse has some data in place"):
        clickhouse.query(chi, sql=create_non_replicated_table)
        clickhouse.query(chi, sql=create_replicated_table)

    with When("CHI with retained volume is deleted"):
        pvc_count = kubectl.get_count("pvc", chi=chi)
        pv_count = kubectl.get_count("pv")
        kubectl.delete_chi(chi)

        with Then("PVC should be retained"):
            assert kubectl.get_count("pvc", chi=chi) == pvc_count
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
            manifest=f"manifests/chi/test-019-{step}-retain-volume-2.yaml",
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
                manifest=f"manifests/chi/test-019-{step}-retain-volume-1.yaml",
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
            manifest=f"manifests/chi/test-019-{step}-retain-volume-3.yaml",
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )
        with Then("Replicated table should have two replicas now"):
            out = clickhouse.query(chi, sql="select total_replicas from system.replicas where table='t2'")
            assert out == "2"

        with When("Remove a replica"):
            pvc_count = kubectl.get_count("pvc", chi=chi)
            pv_count = kubectl.get_count("pv")

            kubectl.create_and_check(
                manifest=f"manifests/chi/test-019-{step}-retain-volume-1.yaml",
                check={
                    "pod_count": 1,
                    "do_not_delete": 1,
                },
            )
            with Then("Replica PVC should be retained"):
                assert kubectl.get_count("pvc", chi=chi) == pvc_count
                assert kubectl.get_count("pv") == pv_count

            with And("Replica should NOT be removed from ZooKeeper"):
                out = clickhouse.query(
                    chi,
                    sql="select total_replicas from system.replicas where table='t2'",
                )
                assert out == "2"

    with When("Add a second replica one more time"):
        kubectl.create_and_check(
            manifest=f"manifests/chi/test-019-{step}-retain-volume-3.yaml",
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
                manifest=f"manifests/chi/test-019-{step}-retain-volume-4.yaml",
                check={
                    "pod_count": 2,
                    "do_not_delete": 1,
                    # "chi_status": "InProgress", # !!!!!
                },
            )

            with And("Remove a replica"):
                pvc_count = kubectl.get_count("pvc", chi=chi)
                pv_count = kubectl.get_count("pv")

                kubectl.create_and_check(
                    manifest=f"manifests/chi/test-019-{step}-retain-volume-1.yaml",
                    check={
                        "pod_count": 1,
                        "do_not_delete": 1,
                    },
                )
                # Not implemented yet
                with Then("Replica PVC should be deleted"):
                    assert kubectl.get_count("pvc", chi=chi) < pvc_count
                    assert kubectl.get_count("pv") < pv_count

                with And("Replica should be removed from ZooKeeper"):
                    out = clickhouse.query(
                        chi,
                        sql="select total_replicas from system.replicas where table='t2'",
                    )
                    assert out == "1"

    with When("Delete chi"):
        kubectl.delete_chi(chi)
        with Then("One PVC should be left because relcaim policy is not set anymore"):
            assert kubectl.get_count("pvc", chi=chi) == 1
        with Then("Cleanup PVCs"):
            for pvc in kubectl.get_obj_names(chi, "pvc"):
                kubectl.launch(f"delete pvc {pvc}")


@TestScenario
@Name("test_019_1. Test that volume is correctly retained and can be re-attached. Provisioner: StatefulSet")
@Requirements(RQ_SRS_026_ClickHouseOperator_RetainingVolumeClaimTemplates("1.0"))
def test_019_1(self):
    test_019(step=1)


@TestScenario
@Name("test_019_2. Test that volume is correctly retained and can be re-attached. Provisioner: Operator")
@Requirements(RQ_SRS_026_ClickHouseOperator_RetainingVolumeClaimTemplates("1.0"))
def test_019_2(self):
    test_019(step=2)


@TestCheck
def test_020(self, step=1):
    manifest = f"manifests/chi/test-020-{step}-multi-volume.yaml"
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
            assert out == "default"

    with When("alter table test_disks move partition tuple() to disk 'disk2'"):
        clickhouse.query(chi, "alter table test_disks move partition tuple() to disk 'disk2'")

        with Then("Data should be placed on disk2"):
            out = clickhouse.query(chi, "select disk_name from system.parts where table='test_disks'")
            assert out == "disk2"

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_020_1. Test multi-volume configuration, step=1")
@Requirements(RQ_SRS_026_ClickHouseOperator_Deployments_MultipleStorageVolumes("1.0"))
def test_020_1(self):
    test_020(step=1)


@TestScenario
@Name("test_020_2. Test multi-volume configuration, step=2")
@Requirements(RQ_SRS_026_ClickHouseOperator_Deployments_MultipleStorageVolumes("1.0"))
def test_020_2(self):
    test_020(step=2)


@TestCheck
def test_021(self, step=1):
    manifest = f"manifests/chi/test-021-{step}-rescale-volume-01.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    cluster = "simple"

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
            "apply_templates": {settings.clickhouse_template},
            "pod_count": 1,
            "do_not_delete": 1,
        },
    )

    with Then("Storage size should be 1Gi"):
        size = kubectl.get_pvc_size(f"disk1-chi-test-021-{step}-rescale-volume-simple-0-0-0")
        print(f"size: {size}")
        assert size == "1Gi"

    with Then("Create a table with a single row"):
        clickhouse.query(chi, "drop table if exists test_local_021;")
        clickhouse.query(chi, "create table test_local_021(a Int8) Engine = MergeTree() order by a")
        clickhouse.query(chi, "insert into test_local_021 values (1)")

    start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")

    with When("Re-scale volume configuration to 2Gi"):
        kubectl.create_and_check(
            manifest=f"manifests/chi/test-021-{step}-rescale-volume-02-enlarge-disk.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("Storage size should be 2Gi"):
            kubectl.wait_field(
                "pvc",
                f"disk1-chi-test-021-{step}-rescale-volume-simple-0-0-0",
                ".spec.resources.requests.storage",
                "2Gi",
            )
            size = kubectl.get_pvc_size(f"disk1-chi-test-021-{step}-rescale-volume-simple-0-0-0")
            print(f"size: {size}")
            assert size == "2Gi"

        with And("Table should exist"):
            out = clickhouse.query(chi, "select * from test_local_021")
            assert out == "1"

        with And("Check if pod has been restarted"):
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
            if step == 1:
                with Then("Storage provisioner is StatefulSet. Pod should be restarted"):
                    assert start_time != new_start_time
            if step == 2:
                with Then("Storage provisioner is Operator. Pod should not be restarted"):
                    assert start_time == new_start_time

    with When("Add a second disk"):
        kubectl.create_and_check(
            manifest=f"manifests/chi/test-021-{step}-rescale-volume-03-add-disk.yaml",
            check={
                "pod_count": 1,
                "pod_volumes": {
                    "/var/lib/clickhouse",
                    "/var/lib/clickhouse2",
                },
                "do_not_delete": 1,
            },
        )
        # Adding new volume takes time, so pod_volumes check does not work

        with Then("There should be two PVC"):
            size = kubectl.get_pvc_size(f"disk1-chi-test-021-{step}-rescale-volume-simple-0-0-0")
            assert size == "2Gi"
            kubectl.wait_object("pvc", f"disk2-chi-test-021-{step}-rescale-volume-simple-0-0-0")
            kubectl.wait_field(
                "pvc",
                f"disk2-chi-test-021-{step}-rescale-volume-simple-0-0-0",
                ".status.phase",
                "Bound",
            )
            size = kubectl.get_pvc_size(f"disk2-chi-test-021-{step}-rescale-volume-simple-0-0-0")
            print(f"size: {size}")
            assert size == "1Gi"

        with And("There should be two disks recognized by ClickHouse"):
            kubectl.wait_pod_status(f"chi-test-021-{step}-rescale-volume-simple-0-0-0", "Running")
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

    with When("There are two disks test the move"):
        with Then("Data should be initially on a default disk"):
            out = clickhouse.query(chi, "select disk_name from system.parts where table='test_local_021'")
            print(f"out : {out}")
            print(f"want: default")
            assert out == "default"

        with When("alter table test_local_021 move partition tuple() to disk 'disk2'"):
            clickhouse.query(chi, "alter table test_local_021 move partition tuple() to disk 'disk2'")

            with Then("Data should be moved to disk2"):
                out = clickhouse.query(
                    chi,
                    "select disk_name from system.parts where table='test_local_021'",
                )
                print(f"out : {out}")
                print(f"want: disk2")
                assert out == "disk2"

    with When("Try reducing the disk size and also change a version to recreate the stateful set"):
        kubectl.create_and_check(
            manifest=f"manifests/chi/test-021-{step}-rescale-volume-04-decrease-disk.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("Storage size should be unchanged 2Gi"):
            size = kubectl.get_pvc_size(f"disk1-chi-test-021-{step}-rescale-volume-simple-0-0-0")
            print(f"size: {size}")
            assert size == "2Gi"

        with And("Table should exist"):
            out = clickhouse.query(chi, "select * from test_local_021")
            assert out == "1"

        with And("PVC status should not be Terminating"):
            status = kubectl.get_field(
                "pvc",
                f"disk2-chi-test-021-{step}-rescale-volume-simple-0-0-0",
                ".status.phase",
            )
            assert status != "Terminating"

    with When("Revert disk size back to 2Gi"):
        kubectl.create_and_check(
            manifest=f"manifests/chi/test-021-{step}-rescale-volume-03-add-disk.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("Storage size should be 2Gi"):
            kubectl.wait_field(
                "pvc",
                f"disk1-chi-test-021-{step}-rescale-volume-simple-0-0-0",
                ".spec.resources.requests.storage",
                "2Gi",
            )
            size = kubectl.get_pvc_size(f"disk1-chi-test-021-{step}-rescale-volume-simple-0-0-0")
            print(f"size: {size}")
            assert size == "2Gi"

        with And("Table should exist"):
            out = clickhouse.query(chi, "select * from test_local_021")
            assert out == "1"

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_021_1. Test rescaling storage. Provisioner: StatefulSet")
@Requirements(RQ_SRS_026_ClickHouseOperator_StorageProvisioning("1.0"))
def test_021_1(self):
    test_021(step=1)


@TestScenario
@Name("test_021_2. Test rescaling storage. Provisioner: Operator")
@Requirements(RQ_SRS_026_ClickHouseOperator_StorageProvisioning("1.0"))
def test_021_2(self):
    test_021(step=2)


@TestScenario
@Name("test_022. Test that chi with broken image can be deleted")
@Requirements(RQ_SRS_026_ClickHouseOperator_DeleteBroken("1.0"))
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
            "ErrImagePull",
        )
        with Then("CHI should be able to delete"):
            kubectl.launch(f"delete chi {chi}", ok_to_fail=True, timeout=600)
            assert kubectl.get_count("chi", f"{chi}") == 0


@TestScenario
@Name("test_023. Test auto templates")
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templating("1.0"))
def test_023(self):
    manifest = "manifests/chi/test-023-auto-templates.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))

    with Given("Auto templates are deployed"):
        kubectl.apply(util.get_full_path("manifests/chit/tpl-clickhouse-auto-1.yaml"))
        kubectl.apply(util.get_full_path("manifests/chit/tpl-clickhouse-auto-2.yaml"))

    chit_data = yaml_manifest.get_manifest_data(util.get_full_path("manifests/chit/tpl-clickhouse-auto-1.yaml"))
    expected_image = chit_data["spec"]["templates"]["podTemplates"][0]["spec"]["containers"][0]["image"]

    kubectl.create_and_check(
        manifest=manifest,
        check={
            "pod_count": 1,
            "pod_image": expected_image,
            "do_not_delete": 1,
        },
    )
    with Then("Annotation from a template should be populated"):
        assert kubectl.get_field("chi", chi, ".status.normalizedCompleted.metadata.annotations.test") == "test"
    with Then("Pod annotation should populated from template"):
        assert kubectl.get_field("pod", f"chi-{chi}-single-0-0-0", ".metadata.annotations.test") == "test"
    with Then("Environment variable from a template should be populated"):
        pod = kubectl.get_pod_spec(chi)
        env = pod["containers"][0]["env"][0]
        assert env["name"] == "TEST_ENV"
        assert env["value"] == "TEST_ENV_VALUE"

    kubectl.delete_chi(chi)
    kubectl.delete(util.get_full_path("manifests/chit/tpl-clickhouse-auto-1.yaml"))
    kubectl.delete(util.get_full_path("manifests/chit/tpl-clickhouse-auto-2.yaml"))


@TestScenario
@Name("test_024. Test annotations for various template types")
@Requirements(RQ_SRS_026_ClickHouseOperator_AnnotationsInTemplates("1.0"))
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
        assert (
            kubectl.get_field(
                "pod",
                "chi-test-024-default-0-0-0",
                ".metadata.annotations.podtemplate/test",
            )
            == "test"
        )
    with And("Service annotation should be populated from a serviceTemplate"):
        assert (
            kubectl.get_field(
                "service",
                "clickhouse-test-024",
                ".metadata.annotations.servicetemplate/test",
            )
            == "test"
        )
    with And("PVC annotation should be populated from a volumeTemplate"):
        assert (
            kubectl.get_field(
                "pvc",
                "-l clickhouse.altinity.com/chi=test-024",
                ".metadata.annotations.pvc/test",
            )
            == "test"
        )

    with And("Pod annotation should populated from a CHI"):
        assert kubectl.get_field("pod", "chi-test-024-default-0-0-0", ".metadata.annotations.chi/test") == "test"
    with And("Service annotation should be populated from a CHI"):
        assert kubectl.get_field("service", "clickhouse-test-024", ".metadata.annotations.chi/test") == "test"
    with And("PVC annotation should be populated from a CHI"):
        assert (
            kubectl.get_field(
                "pvc",
                "-l clickhouse.altinity.com/chi=test-024",
                ".metadata.annotations.chi/test",
            )
            == "test"
        )

    with And("Service annotation macros should be resolved"):
        assert (
            kubectl.get_field(
                "service",
                "clickhouse-test-024",
                ".metadata.annotations.servicetemplate/macro-test",
            )
            == "test-024.example.com"
        )
        assert (
            kubectl.get_field(
                "service",
                "service-test-024-0-0",
                ".metadata.annotations.servicetemplate/macro-test",
            )
            == "test-024-0-0.example.com"
        )

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_025. Test that service is available during re-scaling, upgrades etc.")
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_ClusterScaling_AddingReplicas("1.0"))
def test_025(self):
    util.require_keeper(keeper_type=self.context.keeper_type)

    create_table = """
    CREATE TABLE test_local_025(a UInt32)
    Engine = ReplicatedMergeTree('/clickhouse/{installation}/tables/{shard}/{database}/{table}', '{replica}')
    PARTITION BY tuple()
    ORDER BY a
    """.replace(
        "\r", ""
    ).replace(
        "\n", ""
    )

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

    kubectl.wait_jsonpath(
        "pod",
        "chi-test-025-rescaling-default-0-0-0",
        "{.status.containerStatuses[0].ready}",
        "true",
        ns=kubectl.namespace,
    )

    numbers = "100000000"

    with Given("Create replicated table and populate it"):
        clickhouse.query(chi, create_table)
        clickhouse.query(
            chi,
            "CREATE TABLE test_distr_025 AS test_local_025 Engine = Distributed('default', default, test_local_025)",
        )
        clickhouse.query(
            chi,
            f"INSERT INTO test_local_025 SELECT * FROM numbers({numbers})",
            timeout=120,
        )

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
            "pod",
            "chi-test-025-rescaling-default-0-1-0",
            '.metadata.labels."clickhouse\\.altinity\\.com/ready"',
            "yes",
            backoff=1,
        )
        start_time = time.time()
        lb_error_time = start_time
        distr_lb_error_time = start_time
        latent_replica_time = start_time
        for i in range(1, 100):
            cnt_local = clickhouse.query_with_error(
                chi,
                "SELECT count() FROM test_local_025",
                "chi-test-025-rescaling-default-0-1.test.svc.cluster.local",
            )
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
            f"Tables not ready: {round(distr_lb_error_time - start_time)}s, data not ready: {round(latent_replica_time - distr_lb_error_time)}s"
        )

        with Then("Query to the distributed table via load balancer should never fail"):
            assert round(distr_lb_error_time - start_time) == 0
        with And("Query to the local table via load balancer should never fail"):
            assert round(lb_error_time - start_time) == 0

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_026. Test mixed single and multi-volume configuration in one cluster")
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters_Cluster_Layout("1.0"))
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
            out = clickhouse.query(
                chi,
                host="chi-test-026-mixed-replicas-default-0-0",
                sql="select count() from system.disks",
            )
            assert out == "1"

        with And("Check that second replica has two disks"):
            out = clickhouse.query(
                chi,
                host="chi-test-026-mixed-replicas-default-0-1",
                sql="select count() from system.disks",
            )
            assert out == "2"

    with When("Create a table and generate several inserts"):
        clickhouse.query(
            chi,
            sql="DROP TABLE IF EXISTS test_disks ON CLUSTER '{cluster}' SYNC; CREATE TABLE test_disks ON CLUSTER '{cluster}' (a Int64) Engine = ReplicatedMergeTree('/clickhouse/{installation}/tables/{shard}/{database}/{table}', '{replica}') PARTITION BY (a%10) ORDER BY a",
        )
        clickhouse.query(
            chi,
            host="chi-test-026-mixed-replicas-default-0-0",
            sql="INSERT INTO test_disks SELECT * FROM numbers(100) SETTINGS max_block_size=1",
        )
        clickhouse.query(
            chi,
            host="chi-test-026-mixed-replicas-default-0-0",
            sql="INSERT INTO test_disks SELECT * FROM numbers(100) SETTINGS max_block_size=1",
        )
        time.sleep(5)

        with Then("Data should be placed on a single disk on a first replica"):
            out = clickhouse.query(
                chi,
                host="chi-test-026-mixed-replicas-default-0-0",
                sql="SELECT arraySort(groupUniqArray(disk_name)) FROM system.parts WHERE table='test_disks'",
            )
            assert out == "['default']"

        with And("Data should be placed on a second disk on a second replica"):
            out = clickhouse.query(
                chi,
                host="chi-test-026-mixed-replicas-default-0-1",
                sql="SELECT arraySort(groupUniqArray(disk_name)) FROM system.parts WHERE table='test_disks'",
            )
            assert out == "['disk2']"

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_027. Test troubleshooting mode")
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Troubleshoot("1.0"))
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
            "CrashLoopBackOff",
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
                out = kubectl.launch(f'exec chi-{chi}-default-0-0-0 -- bash -c "echo Success"')
                assert out == "Success"

        with Then("We can start in normal mode after correcting the problem"):
            kubectl.create_and_check(
                manifest="manifests/chi/test-027-troubleshooting-3-fixed-config.yaml",
                check={
                    "pod_count": 1,
                },
            )


@TestScenario
@Name("test_028. Test restart scenarios")
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_RestartingOperator("1.0"))
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

    sql = """SET skip_unavailable_shards=1; SYSTEM DROP DNS CACHE; SELECT getMacro('replica') AS replica, uptime() AS uptime,
     (SELECT count() FROM system.clusters WHERE cluster='all-sharded') AS total_hosts,
     (SELECT count() online_hosts FROM cluster('all-sharded', system.one) ) AS online_hosts
     FORMAT JSONEachRow"""
    note("Before restart")
    out = clickhouse.query_with_error(chi, sql)
    note(out)
    with When("CHI is patched with a restart attribute"):
        cmd = f'patch chi {chi} --type=\'json\' --patch=\'[{{"op":"add","path":"/spec/restart","value":"RollingUpdate"}}]\''
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
                    ch1 = clickhouse.query_with_error(
                        chi,
                        sql,
                        pod="chi-test-014-replication-default-0-0-0",
                        host="chi-test-014-replication-default-0-0",
                        advanced_params="--connect_timeout=1 --send_timeout=10 --receive_timeout=10",
                    )
                    ch2 = clickhouse.query_with_error(
                        chi,
                        sql,
                        pod="chi-test-014-replication-default-1-0-0",
                        host="chi-test-014-replication-default-1-0",
                        advanced_params="--connect_timeout=1 --send_timeout=10 --receive_timeout=10",
                    )

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

        # with Then("Clear RollingUpdate restart policy"):
        #    cmd = f"patch chi {chi} --type='json' --patch='[{{\"op\":\"remove\",\"path\":\"/spec/restart\"}}]'"
        #    kubectl.launch(cmd)
        #    time.sleep(10)
        #    kubectl.wait_chi_status(chi, "Completed")

        with Then("Restart operator. CHI should not be restarted"):
            check_operator_restart(
                chi=chi,
                wait_objects={"statefulset": 2, "pod": 2, "service": 3},
                pod=f"chi-{chi}-default-0-0-0",
            )

        with Then("Re-apply the original config. CHI should not be restarted"):
            kubectl.create_and_check(manifest=manifest, check={"do_not_delete": 1})
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
            print(f"old_start_time: {start_time}")
            print(f"new_start_time: {new_start_time}")
            assert start_time == new_start_time

    with When("Stop installation"):
        cmd = f'patch chi {chi} --type=\'json\' --patch=\'[{{"op":"add","path":"/spec/stop","value":"yes"}}]\''
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
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_PodTemplates_podDistribution_TopologyKey("1.0"),
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

    kubectl.check_pod_antiaffinity(
        chi,
        "chi-test-029-distribution-t1-0-0-0",
        topologyKey="kubernetes.io/hostname",
    )
    kubectl.check_pod_antiaffinity(
        chi,
        "chi-test-029-distribution-t1-0-1-0",
        match_labels={
            "clickhouse.altinity.com/chi": f"{chi}",
            "clickhouse.altinity.com/namespace": f"{kubectl.namespace}",
            "clickhouse.altinity.com/replica": "1",
        },
        topologyKey="kubernetes.io/os",
    )

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_030. Test CRD deletion")
def test_030(self):
    manifest = "manifests/chi/test-030.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    object_counts = {"statefulset": 2, "pod": 2, "service": 3}

    kubectl.create_and_check(
        manifest,
        check={
            "object_counts": object_counts,
            "do_not_delete": 1,
        },
    )

    trigger_event = threading.Event()
    Check("Check that cluster definition does not change during restart", test=check_remote_servers, parallel=True,)(
        chi=chi,
        cluster="default",
        shards=2,
        trigger_event=trigger_event,
    )

    with When("Delete CRD"):
        kubectl.launch("delete crd clickhouseinstallations.clickhouse.altinity.com")
        with Then("CHI should be deleted"):
            kubectl.wait_object("chi", chi, count=0)
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
                },
            )
        with Then("Pods should not be restarted"):
            new_start_time = kubectl.get_field("pod", pod, ".status.startTime")
            assert start_time == new_start_time

    trigger_event.set()
    join()

    kubectl.delete_chi(chi)


@TestScenario
@Name("test_031. Test excludeFromPropagationAnnotations work")
def test_031(self):
    chi_manifest = "manifests/chi/test-031-wo-tpl.yaml"
    chi = "test-031-wo-tpl"

    with Given("I generate CHO deploy manifest"):
        with open(util.get_full_path(settings.clickhouse_operator_install_manifest)) as base_template, open(
            util.get_full_path("../../config/config.yaml")
        ) as config_file:
            manifest_yaml = list(yaml.safe_load_all(base_template.read()))

            config_yaml = yaml.safe_load(config_file.read())
            config_yaml["annotation"]["exclude"] = [
                "excl",
            ]
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
        util.install_operator_if_not_exist(
            reinstall=True,
            manifest=util.get_full_path(settings.clickhouse_operator_install_manifest, False),
        )
        util.restart_operator(ns=settings.operator_namespace)


@TestCheck
def run_select_query(self, host, user, password, query, res1, res2, trigger_event):
    """Run a select query in parallel until the stop signal is received."""

    client_pod = "clickhouse-client"
    with Given("I have a shell"):
        self.context.shell = get_shell()
    try:

        kubectl.launch(f'run {client_pod} --image={settings.clickhouse_version} -- /bin/sh -c "sleep 3600"')
        kubectl.wait_pod_status(client_pod, "Running")

        ok = 0
        partial = 0
        errors = 0

        cmd = f'exec -n {kubectl.namespace} {client_pod} -- clickhouse-client --user={user} --password={password} -h {host} -q "{query}"'
        while not trigger_event.is_set():
            cnt_test = kubectl.launch(cmd, ok_to_fail=True)
            if cnt_test == res1:
                ok += 1
            if cnt_test == res2:
                partial += 1
            if cnt_test != res1 and cnt_test != res2:
                errors += 1
                print("*** RUN_QUERY ERROR ***")
                print(cnt_test)
            time.sleep(0.5)
        with By(
            f"{ok} queries have been executed with no errors, {partial} queries returned incomplete results. {errors} queries have failed"
        ):
            assert errors == 0
            if partial > 0:
                print(
                    f"*** WARNING ***: cluster was partially unavailable, {partial} queries returned incomplete results"
                )
    finally:
        kubectl.launch(f"delete pod {client_pod}")


@TestCheck
def run_insert_query(self, host, user, password, query, trigger_event):
    """Run an insert query in parallel until the stop signal is received."""

    client_pod = "clickhouse-insert"
    with Given("I have a shell"):
        self.context.shell = get_shell()
    try:

        kubectl.launch(f'run {client_pod} --image={settings.clickhouse_version} -- /bin/sh -c "sleep 3600"')
        kubectl.wait_pod_status(client_pod, "Running")

        ok = 0
        errors = 0

        cmd = f'exec -n {kubectl.namespace} {client_pod} -- clickhouse-client --user={user} --password={password} -h {host} -q "{query}"'
        while not trigger_event.is_set():
            res = kubectl.launch(cmd, ok_to_fail=True)
            if res == "":
                ok += 1
            else:
                errors += 1
        with By(f"{ok} inserts have been executed with no errors, {errors} inserts have failed"):
            assert errors == 0
    finally:
        kubectl.launch(f"delete pod {client_pod}")


@TestScenario
@Name("test_032. Test rolling update logic")
def test_032(self):
    """Test rolling update logic."""

    util.require_keeper(keeper_type=self.context.keeper_type)
    create_table = """
    CREATE TABLE test_local_032 ON CLUSTER 'default' (a UInt32)
    Engine = ReplicatedMergeTree('/clickhouse/{installation}/tables/{shard}/{database}/{table}', '{replica}')
    PARTITION BY tuple()
    ORDER BY a
    """.replace(
        "\r", ""
    ).replace(
        "\n", ""
    )

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
                "statefulset": 4,
                "pod": 4,
                "service": 5,
            },
            "do_not_delete": 1,
        },
        timeout=600,
    )

    numbers = 100

    with Given("Create replicated and distributed tables"):
        clickhouse.query(chi, create_table)
        clickhouse.query(
            chi,
            "CREATE TABLE test_distr_032 ON CLUSTER 'default' AS test_local_032 Engine = Distributed('default', default, test_local_032, a%2)",
        )
        clickhouse.query(chi, f"INSERT INTO test_distr_032 select * from numbers({numbers})")

    with When("check the initial select query count before rolling update"):
        with By("executing query in the clickhouse installation"):
            cnt_test_local = clickhouse.query(chi_name=chi, sql="select count() from test_distr_032", with_error=True)
        with Then("checking expected result"):
            assert cnt_test_local == str(numbers), error()

    trigger_event = threading.Event()

    Check("run query until receive stop event", test=run_select_query, parallel=True)(
        host="clickhouse-test-032-rescaling",
        user="test_032",
        password="test_032",
        query="SELECT count() FROM test_distr_032",
        res1=str(numbers),
        res2=str(numbers // 2),
        trigger_event=trigger_event,
    )

    Check("Check that cluster definition does not change during restart", test=check_remote_servers, parallel=True,)(
        chi=chi,
        cluster="default",
        shards=2,
        trigger_event=trigger_event,
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
                    "statefulset": 4,
                    "pod": 4,
                    "service": 5,
                },
                "do_not_delete": 1,
            },
            timeout=int(1000),
        )

    trigger_event.set()
    join()

    kubectl.delete_chi(chi)


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_EnableHttps("1.0"))
@Name("test_034. Check HTTPS support for health check")
def test_034(self):
    """Check ClickHouse-Operator HTTPS support by switching configuration to HTTPS using the chopconf file and
    creating a ClickHouse-Installation with HTTPS enabled and confirming the secure connectivity between them by
    monitoring the metrics endpoint on port 8888.
    """
    chopconf_file = "manifests/chopconf/test-034-chopconf.yaml"
    operator_namespace = settings.operator_namespace

    def check_metrics_monitoring(operator_namespace, operator_pod, expect_pattern, max_retries=10):
        with Then(f"metrics-exporter /metrics endpoint result should contain {expect_pattern}"):
            for i in range(1, max_retries):
                url_cmd = util.make_http_get_request("127.0.0.1", "8888", "/metrics")
                out = kubectl.launch(
                    f"exec {operator_pod} -c metrics-exporter -- {url_cmd}",
                    ns=operator_namespace,
                )
                rx = re.compile(expect_pattern, re.MULTILINE)
                matches = rx.findall(out)
                expected_pattern_found = False
                if matches:
                    expected_pattern_found = True

                if expected_pattern_found:
                    break
                with Then("Not ready. Wait for " + str(i * 5) + " seconds"):
                    time.sleep(i * 5)
            assert expected_pattern_found, error()

    with When("create the chi without secure connection"):
        manifest = "manifests/chi/test-034-http.yaml"
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
            },
            timeout=600,
        )

    with Then("check for `chi_clickhouse_metric_fetch_errors` string with zero value at the end 1"):
        out = kubectl.launch("get pods -l app=clickhouse-operator", ns=operator_namespace).splitlines()[1]
        operator_pod = re.split(r"[\t\r\n\s]+", out)[0]
        check_metrics_monitoring(
            operator_namespace,
            operator_pod,
            expect_pattern="^chi_clickhouse_metric_fetch_errors{(.*?)} 0$",
        )

    with And(f"apply ClickHouseOperatorConfiguration {chopconf_file} with https connection"):
        kubectl.apply(util.get_full_path(chopconf_file, lookup_in_host=False), operator_namespace)

    with And("reboot metrics exporter to update the configuration 1"):
        util.restart_operator()
        out = kubectl.launch("get pods -l app=clickhouse-operator", ns=settings.operator_namespace).splitlines()[1]
        operator_pod = re.split(r"[\t\r\n\s]+", out)[0]

    with Then("check for `chi_clickhouse_metric_fetch_errors` string with non zero value `1` at the end"):
        check_metrics_monitoring(
            operator_namespace,
            operator_pod,
            expect_pattern="^chi_clickhouse_metric_fetch_errors{(.*?)} 1$",
        )

    with When("remove the ClickHouseOperatorConfiguration"):
        kubectl.delete(util.get_full_path(chopconf_file, lookup_in_host=False), operator_namespace)

    with And("reboot metrics exporter to update the configuration 2"):
        util.restart_operator()
        out = kubectl.launch("get pods -l app=clickhouse-operator", ns=settings.operator_namespace).splitlines()[1]
        operator_pod = re.split(r"[\t\r\n\s]+", out)[0]

    with Then("check for `chi_clickhouse_metric_fetch_errors` string with zero value at the end"):
        check_metrics_monitoring(
            operator_namespace,
            operator_pod,
            expect_pattern="^chi_clickhouse_metric_fetch_errors{(.*?)} 0$",
        )

    kubectl.delete_chi(chi)

    with When("create the chi with secure connection"):
        manifest = "manifests/chi/test-034-https.yaml"
        chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))

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
            },
            timeout=600,
        )

    client_pod = "test-034-client"
    with And(f"Start {client_pod} pod"):
        kubectl.apply(util.get_full_path("manifests/chi/test-034-client.yaml"))
        kubectl.wait_pod_status(client_pod, "Running")

    with And("Confirm it can securely connect to clickhouse"):
        cmd = f"""exec {client_pod} -- clickhouse-client -h chi-test-034-https-default-0-0 --secure --port 9440 \
               --user=test_034_client --password=test_034 \
               -q 'select 1000'"""
        out = kubectl.launch(cmd, ok_to_fail=True)
        assert out == "1000", error()

        kubectl.launch(f"delete pod {client_pod}")

    with And(f"apply ClickHouseOperatorConfiguration {chopconf_file} with https connection"):
        kubectl.apply(util.get_full_path(chopconf_file, lookup_in_host=False), operator_namespace)

    with And("reboot metrics exporter to update the configuration 3"):
        util.restart_operator()
        out = kubectl.launch("get pods -l app=clickhouse-operator", ns=settings.operator_namespace).splitlines()[1]
        operator_pod = re.split(r"[\t\r\n\s]+", out)[0]

    with Then("check for `chi_clickhouse_metric_fetch_errors` string with zero value at the end"):
        check_metrics_monitoring(
            operator_namespace,
            operator_pod,
            expect_pattern="^chi_clickhouse_metric_fetch_errors{(.*?)} 0$",
        )

    with When("remove the ClickHouseOperatorConfiguration"):
        kubectl.delete(util.get_full_path(chopconf_file, lookup_in_host=False), operator_namespace)

    with And("reboot metrics exporter to update the configuration 4"):
        util.restart_operator()
        out = kubectl.launch("get pods -l app=clickhouse-operator", ns=settings.operator_namespace).splitlines()[1]
        operator_pod = re.split(r"[\t\r\n\s]+", out)[0]

    with Then("check for `chi_clickhouse_metric_fetch_errors` string with zero value at the end"):
        check_metrics_monitoring(
            operator_namespace,
            operator_pod,
            expect_pattern="^chi_clickhouse_metric_fetch_errors{(.*?)} 0$",
        )

    kubectl.delete_chi(chi)


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_ReprovisioningVolume("1.0"))
@Name("test_036. Check operator volume re-provisioning")
def test_036(self):
    # TODO fix this when functionality will be available
    return

    """Check clickhouse operator recreates volumes and schema if volume is broken."""
    manifest = f"manifests/chi/test-036-volume-re-provisioning.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    util.require_keeper(keeper_type=self.context.keeper_type)

    with Given("chi exists"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "apply_templates": {settings.clickhouse_template},
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )

    with And("I create replicated table with some data"):
        create_table = """
            CREATE TABLE test_local_036 ON CLUSTER '{cluster}' (a UInt32)
            Engine = ReplicatedMergeTree('/clickhouse/{installation}/tables/{shard}/{database}/{table}', '{replica}')
            PARTITION BY tuple()
            ORDER BY a
            """.replace(
            "\r", ""
        ).replace(
            "\n", ""
        )
        clickhouse.query(chi, create_table)
        clickhouse.query(chi, f"INSERT INTO test_local_036 select * from numbers(10000)")

    with When("I delete PV", description="delete PV on replica 0"):
        pv_name = kubectl.get_pv_name("default-chi-test-036-volume-re-provisioning-simple-0-0-0")

        kubectl.launch(f"delete pv {pv_name} --force &")
        kubectl.launch(
            f"""patch pv {pv_name} --type='json' --patch='[{{"op":"remove","path":"/metadata/finalizers"}}]'"""
        )

    with And("Wait for PVC to detect PV is lost"):
        kubectl.wait_field(
            "pvc",
            "default-chi-test-036-volume-re-provisioning-simple-0-0-0",
            ".status.phase",
            "Lost",
        )

    with Then("I check PV is recreated"):
        assert not "NOT IMPPLEMENTED"
        kubectl.wait_field(
            "pvc",
            "default-chi-test-036-volume-re-provisioning-simple-0-0-0",
            ".status.phase",
            "Bound",
        )
        kubectl.wait_object(
            "pv",
            kubectl.get_pv_name("default-chi-test-036-volume-re-provisioning-simple-0-0-0"),
        )
        size = kubectl.get_pv_size("default-chi-test-036-volume-re-provisioning-simple-0-0-0")
        assert size == "1Gi", error()

    with And("I check data on each replica"):
        with By("checking data on the replica 0"):
            r = clickhouse.query(
                chi,
                pod="chi-test-036-volume-re-provisioning-simple-0-0-0",
                sql="SELECT count(*) FROM test_local_036",
            )
            assert r == "10000", error()
        with And("checking data on the replica 1"):
            r = clickhouse.query(
                chi,
                pod="chi-test-036-volume-re-provisioning-simple-0-1-0",
                sql="SELECT count(*) FROM test_local_036",
            )
            assert r == "10000", error()

    kubectl.delete_chi(chi)


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_StorageManagementSwitch("1.0"))
@Name("test_037. StorageManagement switch")
def test_037(self):
    """Check clickhouse-operator supports switching storageManagement
    config option from default (StatefulSet) to Operator"""
    cluster = "default"
    manifest = f"manifests/chi/test-037-1-storagemanagement-switch.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    util.require_keeper(keeper_type=self.context.keeper_type)

    with Given("chi exists"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "apply_templates": {
                    settings.clickhouse_template,
                },
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

    with And("I time up pod start time"):
        start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")

    with And("I create a table with some data"):
        create_table = """
            CREATE TABLE test_local_037 (a UInt32)
            Engine = MergeTree()
            ORDER BY a
            """.replace(
            "\r", ""
        ).replace(
            "\n", ""
        )
        clickhouse.query(chi, create_table)
        clickhouse.query(chi, f"INSERT INTO test_local_037 select * from numbers(10000)")

    with When("I switch storageManagement to Operator"):
        kubectl.create_and_check(
            manifest=f"manifests/chi/test-037-2-storagemanagement-switch.yaml",
            check={
                "apply_templates": {
                    settings.clickhouse_template,
                },
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

    with And("I check cluster is restarted and time up new pod start time"):
        start_time_new = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
        assert start_time != start_time_new, error()
        start_time = start_time_new

    with And("I rescale volume configuration to 2Gi to check that storage management is switched"):
        kubectl.create_and_check(
            manifest=f"manifests/chi/test-037-3-storagemanagement-switch.yaml",
            check={
                "apply_templates": {
                    settings.clickhouse_template,
                },
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

    with Then("storage size should be 2Gi"):
        kubectl.wait_field(
            "pvc",
            f"default-chi-test-037-storagemanagement-switch-{cluster}-0-0-0",
            ".spec.resources.requests.storage",
            "2Gi",
        )
        size = kubectl.get_pvc_size(f"default-chi-test-037-storagemanagement-switch-{cluster}-0-0-0")
        assert size == "2Gi", error()

    with And("check the pod's start time to see if it has been restarted"):
        start_time_new = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
        with Then("storage provisioner is operator, pod should not be restarted"):
            assert start_time == start_time_new, error()

    with And("check data in the table"):
        r = clickhouse.query(
            chi,
            "SELECT count(*) from test_local_037",
            pod=f"chi-test-037-storagemanagement-switch-{cluster}-0-0-0",
        )
        assert r == "10000"

    kubectl.delete_chi(chi)


@TestCheck
@Name("test_039. Inter-cluster communications with secret")
def test_039(self, step=0):
    """Check clickhouse-operator support inter-cluster communications with secrets."""
    cluster = "default"
    manifest = f"manifests/chi/test-039-{step}-communications-with-secret.yaml"
    chi = yaml_manifest.get_chi_name(util.get_full_path(manifest))
    util.require_keeper(keeper_type=self.context.keeper_type)

    with Given("chi exists"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "apply_templates": {
                    settings.clickhouse_template,
                    "manifests/secret/test-038-secret.yaml",
                },
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )

    with When("I create distributed table that use secure port and insert data into it"):
        clickhouse.query(
            chi,
            "CREATE OR REPLACE TABLE secure on cluster '{cluster}' (a UInt32) ENGINE = MergeTree() PARTITION BY tuple() ORDER BY a",
            pwd="qkrq",
        )
        clickhouse.query(
            chi,
            "CREATE OR REPLACE TABLE secure_dist on cluster '{cluster}' as secure ENGINE = Distributed('{cluster}', default, secure, a%2)",
            pwd="qkrq",
        )
        clickhouse.query(
            chi,
            "INSERT INTO secure_dist select number as a from numbers(10)",
            pwd="qkrq",
        )

    if step == 0:
        with Then("Select in cluster with no secret should fail"):
            r = clickhouse.query_with_error(chi, "SELECT count(a) FROM secure_dist", pwd="qkrq")
            assert "AUTHENTICATION_FAILED" in r
    if step > 0:
        with Then("Select in cluster with secret should pass"):
            r = clickhouse.query(chi, "SELECT count() FROM secure_dist", pwd="qkrq")
            assert r == "10"

    if step == 4:
        with Then("Create replicated table to test interserver_https_port"):
            clickhouse.query(
                chi,
                "CREATE OR REPLACE TABLE secure_repl on cluster 'all-replicated' (a UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/tables/{uuid}', '{replica}')  PARTITION BY tuple() ORDER BY a",
                pwd="qkrq",
            )
            clickhouse.query(
                chi,
                "INSERT INTO secure_repl select number as a from numbers(10)",
                pwd="qkrq",
            )
        kubectl.delete_chi(chi)


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_InterClusterCommunicationWithSecret("1.0"))
@Name("test_039_0. Inter-cluster communications with no secret defined")
def test_039_0(self):
    test_039(step=0)


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_InterClusterCommunicationWithSecret("1.0"))
@Name("test_039_1. Inter-cluster communications with 'auto' secret")
def test_039_1(self):
    """Check clickhouse-operator support inter-cluster communications with 'auto' secret."""
    test_039(step=1)


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_InterClusterCommunicationWithSecret("1.0"))
@Name("test_039_2. Inter-cluster communications with plan text secret")
def test_039_2(self):
    """Check clickhouse-operator support inter-cluster communications with plan text secret."""
    test_039(step=2)


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_InterClusterCommunicationWithSecret("1.0"))
@Name("test_039_3. Inter-cluster communications with k8s secret")
def test_039_3(self):
    """Check clickhouse-operator support inter-cluster communications with k8s secret."""
    test_039(step=3)


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_InterClusterCommunicationWithSecret("1.0"))
@Name("test_039_4. Inter-cluster communications over HTTPS")
def test_039_4(self):
    """Check clickhouse-operator support inter-cluster communications over HTTPS."""
    test_039(step=4)


@TestModule
@Name("e2e.test_operator")
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_APIVersion("1.0"))
def test(self):
    util.clean_namespace(delete_chi=True)
    util.install_operator_if_not_exist()
    with Given(f"Install ClickHouse template {settings.clickhouse_template}"):
        kubectl.apply(
            util.get_full_path(settings.clickhouse_template, lookup_in_host=False),
            settings.test_namespace,
        )

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
    self.context.test_009_version_from = "0.20.3"
    self.context.test_009_version_to = settings.operator_version

    for scenario in loads(current_module(), Scenario, Suite):
        Scenario(run=scenario)
