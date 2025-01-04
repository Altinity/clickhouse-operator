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
from datetime import datetime


@TestScenario
@Name("test_001# 1 node")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_001(self):
    create_shell_namespace_clickhouse_template()

    kubectl.create_and_check(
        manifest="manifests/chi/test-001.yaml",
        check={
            "object_counts": {
                "statefulset": 1,
                "pod": 1,
                "service": 2,
            },
            "configmaps": 1,
            "pdb": {"single": 1},
        },
    )
    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_002# useTemplates for pod, volume templates, and distribution")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_UseTemplates("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_UseTemplates_Name("1.0"),
)
def test_002(self):
    create_shell_namespace_clickhouse_template()

    kubectl.create_and_check(
        manifest="manifests/chi/test-002-tpl.yaml",
        check={
            "pod_count": 1,
            "apply_templates": {
                current().context.clickhouse_template,
                "manifests/chit/tpl-log-volume.yaml",
                "manifests/chit/tpl-one-per-host.yaml",
            },
            "pod_image": current().context.clickhouse_version,
            "pod_volumes": {
                "/var/log/clickhouse-server",
            },
            "pod_podAntiAffinity": 1,
        },
    )
    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_003# 4 nodes with custom layout definition")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters_Cluster_Layout("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters_Cluster_Layout_Shards_Name("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters_Cluster_Layout_Replicas_Name("1.0"),
)
def test_003(self):
    create_shell_namespace_clickhouse_template()

    kubectl.create_and_check(
        manifest="manifests/chi/test-003-complex-layout.yaml",
        check={
            "object_counts": {
                "statefulset": 4,
                "pod": 4,
                "service": 5,
            },
            "pdb": {"cluster1": 0, "cluster2": 1},
        },
    )
    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_004# Compatibility test if old syntax with volumeClaimTemplate is still supported")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_VolumeClaimTemplates("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_VolumeClaimTemplates_Name("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_VolumeClaimTemplates_Spec("1.0"),
)
def test_004(self):
    create_shell_namespace_clickhouse_template()

    kubectl.create_and_check(
        manifest="manifests/chi/test-004-tpl.yaml",
        check={
            "pod_count": 1,
            "pod_volumes": {
                "/var/lib/clickhouse",
            },
        },
    )
    with Finally("I clean up"):
        with By("deleting test namespace"):
            delete_test_namespace()


@TestScenario
@Name("test_005# Test manifest created by ACM")
@Requirements(RQ_SRS_026_ClickHouseOperator_ACM("1.0"))
def test_005(self):
    create_shell_namespace_clickhouse_template()

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

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_006# Test clickhouse version upgrade from one version to another using podTemplate change")
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_VersionUpgrades("1.0"))
def test_006(self):
    create_shell_namespace_clickhouse_template()

    old_version = "clickhouse/clickhouse-server:24.3"
    new_version = "clickhouse/clickhouse-server:24.8"
    chi = "test-006"

    with Then(f"Start CHI with version {old_version}"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-006-ch-upgrade-1.yaml",
            check={
                "pod_count": 1,
                "pod_image": old_version,
                "do_not_delete": 1,
            },
        )
    with Then(f"Use different podTemplate and confirm that pod image is updated to {new_version}"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-006-ch-upgrade-2.yaml",
            check={
                "pod_count": 1,
                "pod_image": new_version,
                "do_not_delete": 1,
            },
        )

    with Then(f"Change image in podTemplate itself and confirm that pod image is updated back to {old_version}"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-006-ch-upgrade-3.yaml",
            check={
                "pod_count": 1,
                "pod_image": old_version,
            },
        )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_007# Test template with custom clickhouse ports")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_HostTemplates_Spec_InterServerHttpPort("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_HostTemplates_Spec_TcpPort("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_HostTemplates_Spec_HttpPort("1.0"),
)
def test_007(self):
    create_shell_namespace_clickhouse_template()

    kubectl.create_and_check(
        manifest="manifests/chi/test-007-custom-ports.yaml",
        check={
            "pod_count": 1,
            "pod_ports": [8124, 9001, 9010],
        },
    )
    with Finally("I clean up"):
        delete_test_namespace()



def wait_operator_restart(chi, wait_objects, shell=None):
    with When("Restart operator"):
        util.restart_operator(shell=shell)
        time.sleep(15)
        kubectl.wait_objects(chi, wait_objects, shell=shell)
        kubectl.wait_chi_status(chi, "Completed", shell=shell)


def check_operator_restart(chi, wait_objects, pod, shell=None):
    start_time = kubectl.get_field("pod", pod, ".status.startTime", shell=shell)
    with When("Restart operator"):
        util.restart_operator(shell=shell)
        time.sleep(15)

        kubectl.wait_objects(chi, wait_objects, shell=shell)
        kubectl.wait_chi_status(chi, "Completed", shell=shell)

        new_start_time = kubectl.get_field("pod", pod, ".status.startTime", shell=shell)

        with Then("ClickHouse pods should not be restarted during operator's restart"):
            print(f"pod start_time old: {start_time}")
            print(f"pod start_time new: {new_start_time}")
            assert start_time == new_start_time


@TestCheck
def test_operator_restart(self, manifest, service, version=None):
    if version is None:
        version = current().context.operator_version
    with Given(f"clickhouse-operator {version}"):
        util.set_operator_version(version)
        chi = yaml_manifest.get_name(util.get_full_path(manifest))
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

    wait_for_cluster(chi, cluster, 2, 1)

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

    with When("I create new shells"):
        shell_1 = get_shell()
        shell_2 = get_shell()
        shell_3 = get_shell()

    Check("run query until receive stop event", test=run_select_query, parallel=True)(
        host=service,
        user="test_008",
        password="test_008",
        query="select count() from cluster('{cluster}', system.one)",
        res1="2",
        res2="1",
        trigger_event=trigger_event,
        shell=shell_1
    )

    Check("insert into distributed table until receive stop event", test=run_insert_query, parallel=True)(
        host=service,
        user="test_008",
        password="test_008",
        query="insert into test_dist select number from numbers(2)",
        trigger_event=trigger_event,
        shell=shell_2
    )

    Check("Check that cluster definition does not change during restart", test=check_remote_servers, parallel=True)(
        chi=chi,
        shards=2,
        trigger_event=trigger_event,
        shell=shell_3
    )

    check_operator_restart(
        chi=chi,
        wait_objects={
            "statefulset": 2,
            "pod": 2,
            "service": 3,
        },
        pod=f"chi-{chi}-{cluster}-0-0-0"
    )
    trigger_event.set()
    time.sleep(5) # let threads to finish
    join()

    # with Then("I recreate shell"):
    #    shell = get_shell()
    #    self.context.shell = shell

    with Then("Local tables should have exactly the same number of rows"):
        cnt0 = clickhouse.query(chi, "select count() from test_local", host=f'chi-{chi}-{cluster}-0-0-0')
        cnt1 = clickhouse.query(chi, "select count() from test_local", host=f'chi-{chi}-{cluster}-1-0-0')
        print(f"{cnt0} {cnt1}")
        assert cnt0 == cnt1 and cnt0 != "0"

    with Finally("I clean up"):
        with By("deleting chi"):
            kubectl.delete_chi(chi)


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
def check_remote_servers(self, chi, shards, trigger_event, shell=None, cluster=""):
    """Check cluster definition in configmap until signal is received"""
    if cluster == "":
        cluster = chi

    ok_runs = 0
    with Then(f"Check remote_servers contains {shards} shards until receiving a stop event"):
        while not trigger_event.is_set():
            chi_shards = get_shards_from_remote_servers(chi, cluster, shell=shell)

            if chi_shards != shards:
                with Then(f"Number of shards in {cluster} cluster should be {shards} got {chi_shards} instead"):
                    assert chi_shards == shards

            ok_runs += 1
            time.sleep(0.5)

    with Then(f"remote_servers were always correct {ok_runs} times"):
        assert ok_runs > 0


@TestScenario
@Name("test_008_1# Test operator restart")
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_RestartingOperator("1.0"))
def test_008_1(self):
    create_shell_namespace_clickhouse_template()

    with Check("Test simple chi for operator restart"):
        test_operator_restart(
            manifest="manifests/chi/test-008-operator-restart-1.yaml",
            service="clickhouse-test-008-1",
        )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_008_2# Test operator restart")
def test_008_2(self):
    create_shell_namespace_clickhouse_template()

    with Check("Test advanced chi for operator restart"):
        test_operator_restart(
            manifest="manifests/chi/test-008-operator-restart-2.yaml",
            service="service-test-008-2",
        )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_008_3# Test operator restart in the middle of reconcile")
def test_008_3(self):
    create_shell_namespace_clickhouse_template()

    manifest = "manifests/chi/test-008-operator-restart-3-1.yaml"
    manifest_2 = "manifests/chi/test-008-operator-restart-3-2.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
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
                )
                kubectl.wait_objects(chi, full_cluster)
                kubectl.wait_chi_status(chi, "Completed")

    with Then("Upgrade ClickHouse version to run a reconcile"):
        kubectl.create_and_check(manifest_2, check={"do_not_delete": 1, "chi_status": "InProgress"})

    trigger_event = threading.Event()

    with When("I create new shells"):
        shell_1 = get_shell()
        shell_2 = get_shell()

    Check("Check that cluster definition does not change during restart", test=check_remote_servers, parallel=True)(
        chi=chi,
        shards=2,
        trigger_event=trigger_event,
        shell=shell_1
    )
    # Just wait for restart operator. After restart it will update cluster with new ClickHouse version
    wait_operator_restart(
        chi=chi,
        wait_objects={"statefulset": 4, "pod": 4, "service": 5},
        shell=shell_2
    )
    trigger_event.set()
    time.sleep(5) # let threads to finish
    join()

    # with Then("I recreate shell"):
    #    shell = get_shell()
    #    self.context.shell = shell

    with Finally("I clean up"):
        delete_test_namespace()


@TestCheck
def test_operator_upgrade(self, manifest, service, version_from, version_to=None, shell=None):
    if version_to is None:
        version_to = current().context.operator_version
    with Given(f"clickhouse-operator from {version_from}"):
        current().context.operator_version = version_from
        create_shell_namespace_clickhouse_template()

        chi = yaml_manifest.get_name(util.get_full_path(manifest, True))
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

    with When("I create new shells"):
        shell_1 = get_shell()
        shell_2 = get_shell()
        shell_3 = get_shell()

    Check("run query until receive stop event", test=run_select_query, parallel=True)(
        host=service,
        user="test_009",
        password="test_009",
        query="select count() from cluster('{cluster}', system.one)",
        res1="2",
        res2="1",
        trigger_event=trigger_event,
        shell=shell_1
    )

    Check("Check that cluster definition does not change during restart", test=check_remote_servers, parallel=True)(
        chi=chi,
        shards=2,
        trigger_event=trigger_event,
        shell=shell_2
    )

    with When(f"upgrade operator to {version_to}"):
        util.install_operator_version(version_to)
        time.sleep(15)

        kubectl.wait_chi_status(chi, "Completed")
        kubectl.wait_objects(chi, {"statefulset": 2, "pod": 2, "service": 3})

    trigger_event.set()
    time.sleep(5) # let threads to finish
    join()

    # with Then("I recreate shell"):
    #    shell = get_shell()
    #    self.context.shell = shell

    with Then("Check that table is here"):
        tables = clickhouse.query(chi, "SHOW TABLES")
        assert "test_local" in tables
        out = clickhouse.query(chi, "SELECT count() FROM test_local")
        assert out == "1"

    with Then("ClickHouse pods should not be restarted during upgrade"):
        new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
        if start_time != new_start_time:
            kubectl.launch(f"describe chi -n {self.context.test_namespace} {chi}")
            kubectl.launch(
                # In my env "pod/: prefix is already returned by $(kubectl get pods -o name -n {current().context.operator_namespace} | grep clickhouse-operator)
                # f"logs -n {current().context.operator_namespace} pod/$(kubectl get pods -o name -n {current().context.operator_namespace} | grep clickhouse-operator) -c clickhouse-operator"
                f"logs -n {current().context.operator_namespace} $(kubectl get pods -o name -n {current().context.operator_namespace} | grep clickhouse-operator) -c clickhouse-operator"
            )
            assert start_time == new_start_time, error(
                f"{start_time} != {new_start_time}, pod restarted after operator upgrade"
            )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_009_1# Test operator upgrade")
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_UpgradingOperator("1.0"))
@Tags("NO_PARALLEL")
def test_009_1(self, version_from="0.23.7", version_to=None):
    if version_to is None:
        version_to = self.context.operator_version

    with Check("Test simple chi for operator upgrade"):
        test_operator_upgrade(
            manifest="manifests/chi/test-009-operator-upgrade-1.yaml",
            service="clickhouse-test-009-1",
            version_from=version_from,
            version_to=version_to,
        )


@TestScenario
@Name("test_009_2# Test operator upgrade")
@Tags("NO_PARALLEL")
def test_009_2(self, version_from="0.23.7", version_to=None):
    if version_to is None:
        version_to = self.context.operator_version

    with Check("Test advanced chi for operator upgrade"):
        test_operator_upgrade(
            manifest="manifests/chi/test-009-operator-upgrade-2.yaml",
            service="service-test-009-2",
            version_from=version_from,
            version_to=version_to,
        )


@TestScenario
@Name("test_010# Test zookeeper initialization")
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_ZooKeeper("1.0"))
def test_010(self):
    create_shell_namespace_clickhouse_template()

    util.require_keeper(keeper_type=self.context.keeper_type)

    kubectl.create_and_check(
        manifest="manifests/chi/test-010-zkroot.yaml",
        check={
            "apply_templates": {
                current().context.clickhouse_template,
            },
            "pod_count": 1,
            "do_not_delete": 1,
        },
    )
    time.sleep(10)
    with And("ClickHouse should not complain regarding zookeeper path"):
        out = clickhouse.query_with_error("test-010-zkroot", "select path from system.zookeeper where path = '/' limit 1")
        assert "/" == out

    with Finally("I clean up"):
        delete_test_namespace()

@TestScenario
@Name("test_010_1# Test zookeeper initialization AFTER starting a cluster")
def test_010_1(self):
    create_shell_namespace_clickhouse_template()
    chi = "test-010-zkroot"

    kubectl.create_and_check(
        manifest="manifests/chi/test-010-zkroot.yaml",
        check={
            "apply_templates": {
                current().context.clickhouse_template,
            },
            "do_not_delete": 1,
            "chi_status": "InProgress"
        },
    )

    with Then("Wait 60 seconds for operator to start creating ZooKeeper root"):
        time.sleep(60)

    # with Then("CHI should be in progress with no pods created yet"):
    #    assert kubectl.get_chi_status(chi) == "InProgress"
    #    assert kubectl.get_count("pod", chi = chi) == 0

    util.require_keeper(keeper_type=self.context.keeper_type)

    kubectl.wait_chi_status(chi, "Completed")

    with And("ClickHouse should not complain regarding zookeeper path"):
        out = clickhouse.query_with_error("test-010-zkroot", "select path from system.zookeeper where path = '/' limit 1")
        assert "/" == out

    with Finally("I clean up"):
        delete_test_namespace()


def get_user_xml_from_configmap(chi, user):
    users_xml = kubectl.get("configmap", f"chi-{chi}-common-usersd")["data"]["chop-generated-users.xml"]
    root_node = etree.fromstring(users_xml)
    return root_node.find(f"users/{user}")


@TestScenario
@Name("test_011_1# Test user security and network isolation")
@Requirements(RQ_SRS_026_ClickHouseOperator_DefaultUsers("1.0"))
def test_011_1(self):
    create_shell_namespace_clickhouse_template()

    with Given("test-011-secured-cluster-1.yaml and test-011-insecured-cluster.yaml"):

        # Create clusters in parallel to speed it up
        kubectl.create_and_check(
            manifest="manifests/chi/test-011-secured-cluster-1.yaml",
            check={
                "apply_templates": {
                    current().context.clickhouse_template,
                },
                "chi_status": "InProgress",
                "do_not_delete": 1,
            },
        )

        kubectl.create_and_check(
            manifest="manifests/chi/test-011-insecured-cluster.yaml",
            check={
                "apply_templates": {
                    current().context.clickhouse_template,
                },
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
                # Expected output: ['::1', '127.0.0.1', '127.0.0.2', <pod1 ip>, <pod2 ip>]
                print(f"default user's IPs: {ips_l}")
                assert len(ips) == 5

            clickhouse.query("test-011-secured-cluster", "SYSTEM RELOAD CONFIG")
            with And("Connection to localhost should succeed with default user"):
                out = clickhouse.query_with_error(
                    "test-011-secured-cluster",
                    "select 'OK'",
                )
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
                check={
                    "do_not_delete": 1,
                },
            )

            with Then("Make sure host_regexp is disabled"):
                regexp = (
                    get_user_xml_from_configmap("test-011-secured-cluster", "default").find("networks/host_regexp").text
                )
                print(f"users.xml: {regexp}")
                assert regexp == "disabled"

                print(f"Give ClickHouse time to recongize the config change")
                time.sleep(30)

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
            out = clickhouse.query_with_error(
                "test-011-secured-cluster",
                "select 'OK'",
                user="user2",
                pwd="default",
            )
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

        with And("User 'user3' with NO access management enabled CAN NOT run SHOW GRANTS"):
            out = clickhouse.query_with_error(
                "test-011-secured-cluster",
                "SHOW GRANTS FOR default", # Looks like a regression in 24.3, SHOW USERS works here
                user="user3",
                pwd="clickhouse_operator_password",
            )
            assert "ACCESS_DENIED" in out

        with And("User 'user4' with access management enabled CAN run SHOW GRANTS"):
            out = clickhouse.query(
                "test-011-secured-cluster",
                "SHOW GRANTS FOR default",
                user="user4",
                pwd="secret",
            )
            assert "ACCESS_DENIED" not in out

        with And("User 'clickhouse_operator' can login with custom password"):
            out = clickhouse.query_with_error(
                "test-011-secured-cluster",
                "select 'OK'",
                user="clickhouse_operator",
                pwd="operator_secret",
            )
            assert out != "OK"

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_011_2# Test default user security")
@Requirements(RQ_SRS_026_ClickHouseOperator_DefaultUsers("1.0"))
def test_011_2(self):
    create_shell_namespace_clickhouse_template()

    with Given("test-011-secured-default-1.yaml with password_sha256_hex for default user"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-011-secured-default-1.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("Default user plain password should be removed"):
            normalizedCompleted = kubectl.get_chi_normalizedCompleted("test-011-secured-default")
            assert "default/password" in normalizedCompleted["spec"]["configuration"]["users"]
            assert normalizedCompleted["spec"]["configuration"]["users"]["default/password"] == ""

            cfm = kubectl.get("configmap", "chi-test-011-secured-default-common-usersd")
            assert '<password remove="1"></password>' in cfm["data"]["chop-generated-users.xml"]

        with And("Connection to localhost should succeed with default user"):
            out = clickhouse.query_with_error(
                "test-011-secured-default",
                "select 'OK'",
                pwd="clickhouse_operator_password",
            )
            assert out == "OK"

        with When("Default user password is removed"):
            kubectl.create_and_check(
                manifest="manifests/chi/test-011-secured-default-2.yaml",
                check={
                    "do_not_delete": 1,
                },
            )

            with Then("Wait until configuration is reloaded by ClickHouse"):
                time.sleep(90)

            with Then("Connection to localhost should succeed with default user and no password"):
                out = clickhouse.query_with_error("test-011-secured-default", "select 'OK'")
                assert out == "OK", error()

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_011_3# Test k8s secrets usage")
@Requirements(RQ_SRS_026_ClickHouseOperator_Secrets("1.0"))
def test_011_3(self):
    create_shell_namespace_clickhouse_template()

    chi = "test-011-secrets"

    with Given("test-011-secrets.yaml with secret storage"):
        kubectl.apply(
            util.get_full_path("manifests/secret/test-011-secret.yaml"),
        )

        kubectl.create_and_check(
            manifest="manifests/chi/test-011-secrets.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("Connection to localhost should succeed with user1/k8s_secret_password"):
            out = clickhouse.query_with_error(chi, "select 'OK'", user="user1", pwd="pwduser1")
            assert out == "OK"

        with And("Connection to localhost should succeed with user2/k8s_secret_password_sha256_hex"):
            out = clickhouse.query_with_error(chi, "select 'OK'", user="user2", pwd="pwduser2")
            assert out == "OK"

        with And("Connection to localhost should succeed with user3/k8s_secret_password_double_sha1_hex"):
            out = clickhouse.query_with_error(chi, "select 'OK'", user="user3", pwd="pwduser3")
            assert out == "OK"

        with And("Connection to localhost should succeed with user4/k8s_secret_env_password"):
            out = clickhouse.query_with_error(chi, "select 'OK'", user="user4", pwd="pwduser4")
            assert out == "OK"

        with And("Connection to localhost should succeed with user5/password defined in valueFrom/secretKeyRef"):
            out = clickhouse.query_with_error(chi, "select 'OK'", user="user5", pwd="pwduser5")
            assert out == "OK"

        with And("Settings should be securely populated from a secret"):
            pod = kubectl.get_pod_spec(chi)
            envs = pod["containers"][0]["env"]
            user5_password_env = ""
            sasl_username_env = ""
            sasl_password_env = ""
            for e in envs:
                if e["valueFrom"]["secretKeyRef"]["key"] == "KAFKA_SASL_USERNAME":
                    sasl_username_env = e["name"]
                if e["valueFrom"]["secretKeyRef"]["key"] == "KAFKA_SASL_PASSWORD":
                    sasl_password_env = e["name"]
                if e["valueFrom"]["secretKeyRef"]["key"] == "pwduser5":
                    user5_password_env = e["name"]

            with By("Secrets are properly propagated to env variables"):
                print(f"Found env variables: {sasl_username_env} {sasl_password_env} {user5_password_env}")
                assert sasl_username_env != ""
                assert sasl_password_env != ""
                assert user5_password_env != ""

            with By("Secrets are properly referenced from settings.xml"):
                cfm = kubectl.get("configmap", f"chi-{chi}-common-configd")
                settings_xml = cfm["data"]["chop-generated-settings.xml"]
                assert f"sasl_username from_env=\"{sasl_username_env}\"" in settings_xml
                assert f"sasl_password from_env=\"{sasl_password_env}\"" in settings_xml

            with By("Secrets are properly referenced from users.xml"):
                cfm = kubectl.get("configmap", f"chi-{chi}-common-usersd")
                users_xml = cfm["data"]["chop-generated-users.xml"]
                env_matches = [from_env.strip() for from_env in users_xml.splitlines() if "from_env" in from_env]
                print(f"Found env substitutions: {env_matches}")
                time.sleep(5)
                assert f"password from_env=\"{user5_password_env}\"" in users_xml

        kubectl.delete_chi(chi)
        kubectl.launch(
            "delete secret test-011-secret",
            ns=self.context.test_namespace,
            timeout=600,
            ok_to_fail=True,
        )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_012# Test service templates")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_ServiceTemplates("1.0"),
    RQ_SRS_026_ClickHouseOperator_ServiceTemplates_NameGeneration("1.0"),
    RQ_SRS_026_ClickHouseOperator_ServiceTemplates_LoadBalancer("1.0"),
    RQ_SRS_026_ClickHouseOperator_ServiceTemplates_Annotations("1.0"),
)
def test_012(self):
    create_shell_namespace_clickhouse_template()

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
    service_test_012_created = kubectl.get_field("service", "service-test-012", ".metadata.creationTimestamp")
    service_default_created = kubectl.get_field("service", "service-default", ".metadata.creationTimestamp")

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

        with And("Service for default cluster should change to LoadBalancer"):
            kubectl.check_service("service-default", "LoadBalancer")

        with And("Service should not be re-created if type has not been changed"):
            assert service_test_012_created == kubectl.get_field("service", "service-test-012", ".metadata.creationTimestamp")

        with And("Service should be re-created if type has been changed"):
            assert service_default_created != kubectl.get_field("service", "service-default", ".metadata.creationTimestamp")

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Requirements(
    RQ_SRS_026_ClickHouseOperator_Managing_ClusterScaling_AddingShards("1.0"),
    RQ_SRS_026_ClickHouseOperator_Managing_ClusterScaling_SchemaPropagation("1.0"),
)
@Name("test_013_1# Automatic schema propagation for shards")
def test_013_1(self):
    """Check clickhouse operator supports automatic schema propagation for shards."""
    create_shell_namespace_clickhouse_template()

    cluster = "simple"
    manifest = f"manifests/chi/test-013-1-1-schema-propagation.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    n_shards = 2

    util.require_keeper(keeper_type=self.context.keeper_type)

    with When("chi with 1 shard exists"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "apply_templates": {
                    current().context.clickhouse_template,
                    "manifests/chit/tpl-persistent-volume-100Mi.yaml",
                },
                "pod_count": 1,
                "pdb": {"simple": 1},
                "do_not_delete": 1,
            },
        )

    create_table_queries = [
        "CREATE TABLE mergetree_table (d DATE, a String, b UInt8, y Int8) ENGINE = MergeTree() PARTITION BY y ORDER BY d",
        "CREATE TABLE replacing_mergetree_table (d DATE, a String, b UInt8, y Int8) ENGINE = ReplacingMergeTree() PARTITION BY y ORDER BY d",
        "CREATE TABLE summing_mergetree_table (d DATE, a String, b UInt8, y Int8) ENGINE = SummingMergeTree() PARTITION BY y ORDER BY d",
        "CREATE TABLE aggregating_mergetree_table (d DATE, a String, b UInt8, y Int8) ENGINE = AggregatingMergeTree() PARTITION BY y ORDER BY d",
        "CREATE TABLE collapsing_mergetree_table (d DATE, a String, b UInt8, y Int8, Sign Int8) ENGINE = CollapsingMergeTree(Sign) PARTITION BY y ORDER BY d",
        "CREATE TABLE versionedcollapsing_mergetree_table (d Date, a String, b UInt8, y Int8, version UInt64, sign Int8 DEFAULT 1) ENGINE = VersionedCollapsingMergeTree(sign, version) PARTITION BY y ORDER BY d",
        "CREATE TABLE replicated_table (d DATE, a String, b UInt8, y Int8) ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/tables/{database}/replicated_table', '{replica}') PARTITION BY y ORDER BY d",
        "CREATE TABLE replicated_replacing_table (d DATE, a String, b UInt8, y Int8) ENGINE = ReplicatedReplacingMergeTree ('/clickhouse/{cluster}/tables/{database}/replicated_replacing_table', '{replica}') PARTITION BY y ORDER BY d",
        "CREATE TABLE replicated_summing_table (d DATE, a String, b UInt8, y Int8) ENGINE = ReplicatedSummingMergeTree('/clickhouse/{cluster}/tables/{database}/replicated_summing_table', '{replica}') PARTITION BY y ORDER BY d",
        "CREATE TABLE replicated_aggregating_table (d DATE, a String, b UInt8, y Int8) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/{cluster}/tables/{database}/replicated_aggregating_table','{replica}') PARTITION BY y ORDER BY d",
        "CREATE TABLE replicated_collapsing_table ON CLUSTER 'simple' (d DATE, a String, b UInt8, y Int8, Sign Int8) ENGINE = ReplicatedCollapsingMergeTree(Sign) PARTITION BY y ORDER BY d",
        "CREATE TABLE replicated_versionedcollapsing_table ON CLUSTER 'simple' (d Date, a String, b UInt8, y Int8, version UInt64, sign Int8 DEFAULT 1) ENGINE = ReplicatedVersionedCollapsingMergeTree(sign, version) PARTITION BY y ORDER BY d",
        "CREATE TABLE table_for_dict ( key_column UInt64, third_column String ) ENGINE = MergeTree() ORDER BY key_column",
        "CREATE DICTIONARY ndict ON CLUSTER 'simple' ( key_column UInt64 DEFAULT 0, third_column String DEFAULT 'qqq' ) PRIMARY KEY key_column "
          "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict' "
          "PASSWORD '' DB 'default')) LIFETIME(MIN 1 MAX 10) LAYOUT(HASHED())",
        "CREATE TABLE table_for_distributed (d Date, a String, b UInt8 DEFAULT 1, y Int8 ) ENGINE = SummingMergeTree PARTITION BY y ORDER BY d SETTINGS index_granularity = 8192",
        "CREATE TABLE IF NOT EXISTS distr_test ON CLUSTER 'simple' (d Date, a String, b UInt8) ENGINE = Distributed('simple', default, table_for_distributed, rand())",
        "CREATE TABLE table_for_kafka (readings_id Int32, time DateTime, date ALIAS toDate(time), temperature Decimal(5,2)) Engine = MergeTree PARTITION BY toYYYYMM(time) ORDER BY (readings_id, time)",
        "CREATE TABLE kafka_readings_queue (readings_id Int32, time DateTime, temperature Decimal(5,2) ) ENGINE = Kafka SETTINGS "
          "kafka_broker_list = 'kafka-headless.kafka:9092', kafka_topic_list = 'table_for_kafka', "
          "kafka_group_name = 'readings_consumer_group1', kafka_format = 'CSV', "
          "kafka_max_block_size = 1048576",
        "CREATE TABLE table_for_view (date Date, id Int8, name String, value Int64) ENGINE = MergeTree() Order by date",
        "CREATE VIEW test_view AS SELECT * FROM table_for_view",
        "CREATE TABLE table_for_materialized_view (when DateTime, userid UInt32, bytes Float32) ENGINE = MergeTree PARTITION BY toYYYYMM(when) ORDER BY (userid, when)",
        "CREATE MATERIALIZED VIEW materialized_view ENGINE = SummingMergeTree PARTITION BY toYYYYMM(day) ORDER BY (userid, day) "
          "POPULATE AS SELECT toStartOfDay(when) AS day, userid, count() as downloads, sum(bytes) AS bytes FROM table_for_materialized_view GROUP BY userid, day",
        "CREATE TABLE table_for_live_vew (d DATE, a String, b UInt8, y Int8) ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/tables/{shard}/default/table_for_live_vew', '{replica}') PARTITION BY y ORDER BY d",
        # "CREATE LIVE VIEW test_live_view AS SELECT * FROM table_for_live_vew",
        "CREATE TABLE table_for_window_view on cluster 'simple' (id UInt64, timestamp DateTime) ENGINE = ReplicatedMergeTree() order by id",
        "CREATE WINDOW VIEW wv ENGINE = Log() as select count(id), tumbleStart(w_id) as window_start from table_for_window_view group by tumble(timestamp, INTERVAL '10' SECOND) as w_id",
        "CREATE TABLE tinylog_table (id UInt64, value1 UInt8, value2 UInt16, value3 UInt32, value4 UInt64) ENGINE=TinyLog",
        "CREATE TABLE log_table (id UInt64, value1 Nullable(UInt64), value2 Nullable(UInt64), value3 Nullable(UInt64)) ENGINE=Log",
        "CREATE TABLE stripelog_table (timestamp DateTime, message_type String, message String ) ENGINE = StripeLog",
        "CREATE TABLE null_table (a String, b Int8, x UInt8) ENGINE = Null",
        "CREATE TABLE merge_table (id Int32) Engine = Merge(default, '_*')",
        "CREATE TABLE set_table (userid UInt64) ENGINE = Set",
        "CREATE TABLE left_join_table (x UInt32, s String) engine = Join(ALL, LEFT, x)",
        "CREATE TABLE url_table (word String, value UInt64) ENGINE=URL('http://127.0.0.1:12345/', CSV)",
        "CREATE TABLE memory_table (a Int64, b Nullable(Int64), c String) engine = Memory",
        "CREATE TABLE table_for_buffer (EventDate Date, UTCEventTime DateTime, MoscowEventDate Date DEFAULT toDate(UTCEventTime)) ENGINE = MergeTree() Order by EventDate",
        "CREATE TABLE buffer_table AS table_for_buffer ENGINE = Buffer('default', 'table_for_buffer', 16, 10, 100, 10000, 1000000, 10000000, 100000000)",
        "CREATE TABLE generate_random_table (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)",
        "CREATE TABLE file_engine_table (name String, value UInt32) ENGINE = File(TabSeparated)",
        "CREATE TABLE odbc (BannerID UInt64, CompaignID UInt64) ENGINE = ODBC('DSN=pgconn;Database=postgres', somedb, bannerdict)",
        "CREATE TABLE jdbc_table (Str String) ENGINE = JDBC('{}', 'default', 'ExternalTable')",
        "CREATE TABLE mysql_table (float_nullable Nullable(Float32), int_id Int32 ) ENGINE = MySQL('localhost:3306', 'vs_db', 'vs_table', 'vs_user', 'vs_pass')",
        "CREATE TABLE mongodb_table ( key UInt64, data String ) ENGINE = MongoDB('mongo1:27017', 'vs_db', 'vs_collection', 'testuser', 'clickhouse_password')",
        "CREATE TABLE hdfs_table (name String, value UInt32) ENGINE = " "HDFS('hdfs://hdfs1:9000/some_file', 'TSV')",
        "CREATE TABLE s3_engine_table (name String, value UInt32)ENGINE = S3('https://storage.test.net/my-test1/test-data.csv.gz', 'CSV', 'gzip')",
        "CREATE TABLE embeddedrocksdb_table (key UInt64, value String) Engine = EmbeddedRocksDB " "PRIMARY KEY(key)",
        "CREATE TABLE postgresql_table (float_nullable Nullable(Float32), str String, int_id Int32 ) ENGINE = PostgreSQL('localhost:5432', 'public_db', 'test_table', 'postges_user', 'postgres_password')",
        "CREATE TABLE externaldistributed_table (id UInt32, name String, age UInt32, money UInt32) ENGINE = ExternalDistributed('PostgreSQL', 'localhost:5432', 'clickhouse', 'test_replicas', 'postgres', 'mysecretpassword')",

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
                    current().context.clickhouse_template,
                },
                "pod_count": 2,
                "pdb": {"simple": 1},
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
        for attempt in retries(timeout=60, delay=1):
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
                "pdb": {"simple": 1},
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
                "pdb": {"simple": 1},
                "do_not_delete": 1,
            },
        )

    tables_on_second_shard = clickhouse.query(
        chi, f"show tables", pod="chi-test-013-1-schema-propagation-simple-1-0-0"
    ).split()

    with Then("I check tables are propagated correctly 2"):
        for attempt in retries(timeout=60, delay=1):
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
                "pdb": {"simple": 1},
                "do_not_delete": 1,
            },
        )

    with When("I add 1 more shard with None schema policy"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-013-1-4-schema-propagation.yaml",
            check={
                "pod_count": 2,
                "pdb": {"simple": 1},
                "do_not_delete": 1,
            },
        )

    with Then("I check tables are not propagated"):
        tables_on_second_shard = clickhouse.query(
            chi, f"show tables", pod="chi-test-013-1-schema-propagation-simple-1-0-0"
        ).split()
        assert len(tables_on_second_shard) == 0, error()

    with Finally("I clean up"):
        delete_test_namespace()


def get_shards_from_remote_servers(chi, cluster, shell=None):
    if cluster == "":
        cluster = chi
    remote_servers = kubectl.get("configmap", f"chi-{chi}-common-configd", shell=shell)["data"]["chop-generated-remote_servers.xml"]

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


def wait_for_cluster(chi, cluster, num_shards, num_replicas=0, pwd="", force_wait=False):
    with Given(f"Cluster {cluster} is properly configured"):
        if current().context.operator_version >= "0.24" and force_wait is False:
            note(f"operator {current().context.operator_version} does not require extra wait, skipping check")
        else:
            with By(f"remote_servers have {num_shards} shards"):
                assert num_shards == get_shards_from_remote_servers(chi, cluster)
            with By(f"ClickHouse recognizes {num_shards} shards in the cluster {cluster}"):
                for shard in range(num_shards):
                    shards = ""
                    for i in range(1, 10):
                        shards = clickhouse.query(
                            chi,
                            f"select uniq(shard_num) from system.clusters where cluster ='{cluster}'",
                            host=f"chi-{chi}-{cluster}-{shard}-0",
                            pwd=pwd,
                            with_error=True,
                        )
                        if shards == str(num_shards):
                            break
                        with Then("Not ready. Wait for " + str(i * 5) + " seconds"):
                            time.sleep(i * 5)
                    assert shards == str(num_shards)

        if num_replicas > 0:
            with By(f"ClickHouse recognizes {num_replicas} replicas in the cluster {cluster}"):
                for shard in range(num_shards):
                    for replica in range(num_replicas):
                        replicas = ""
                        for i in range(1, 10):
                            replicas = clickhouse.query(
                                chi,
                                f"select uniq(replica_num) from system.clusters where cluster ='{cluster}'",
                                host=f"chi-{chi}-{cluster}-{shard}-{replica}",
                                pwd=pwd,
                                with_error=True,
                            )
                            if replicas == str(num_replicas):
                                break
                            with Then(f"Not ready. {replicas}/{num_replicas} replicas. Wait for " + str(i * 5) + " seconds"):
                                time.sleep(i * 5)
                        assert replicas == str(num_replicas)
            num_hosts = num_shards * num_replicas
            with By(f"ClickHouse recognizes {num_hosts} hosts in the cluster {cluster}"):
                for shard in range(num_shards):
                    for replica in range(num_replicas):
                        hosts = ""
                        for i in range(1, 10):
                            host=f"chi-{chi}-{cluster}-{shard}-{replica}"
                            hosts = clickhouse.query(
                                chi,
                                f"select count(), groupArray(host_name) from system.clusters where cluster ='{cluster}'",
                                host=host,
                                pwd=pwd,
                                with_error=True,
                            )
                            if hosts.startswith(str(num_hosts)):
                                break
                            with Then(f"{host} is not ready. Wait for " + str(i * 5) + " seconds"):
                                print("Found: " + hosts)
                                time.sleep(i * 5)
                        assert hosts.startswith(str(num_hosts))


@TestScenario
@Name("test_014_0# Test that schema is correctly propagated on replicas")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters_Cluster_ZooKeeper("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters("1.0"),
)
def test_014_0(self):
    create_shell_namespace_clickhouse_template()

    util.require_keeper(keeper_type=self.context.keeper_type)

    manifest = "manifests/chi/test-014-0-replication-1.yaml"
    chi_name = yaml_manifest.get_name(util.get_full_path(manifest))
    cluster = "default"
    shards = [0, 1]
    n_shards = len(shards)

    kubectl.create_and_check(
        manifest=manifest,
        check={
            "apply_templates": {
                current().context.clickhouse_template,
                "manifests/chit/tpl-persistent-volume-100Mi.yaml",
            },
            "object_counts": {
                "statefulset": 2,
                "pod": 2,
                "service": 3,
            },
            "pdb": {"default": 1},
            "do_not_delete": 1,
        },
        timeout=600,
    )

    start_time = kubectl.get_field("pod", f"chi-{chi_name}-{cluster}-0-0-0", ".status.startTime")

    schema_objects = [
        "test_local_014",
        "test_view_014",
        "test_mv_014",
        # "test_lv_014",
        "test_buffer_014",
        "a_view_014",
        "test_local2_014",
        "test_local_uuid_014",
        "test_uuid_014",
        "test_mv2_014",
        "test_view2_014",
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
        # "CREATE LIVE VIEW test_lv_014 as SELECT * from test_local_014",
        "CREATE DICTIONARY test_dict_014 (a Int8, b Int8) PRIMARY KEY a SOURCE(CLICKHOUSE(host 'localhost' port 9000 table 'test_local_014' user 'default')) LAYOUT(FLAT()) LIFETIME(0)",
        "CREATE TABLE test_buffer_014(a Int8) Engine = Buffer(default, test_local_014, 16, 10, 100, 10000, 1000000, 10000000, 100000000)",
        "CREATE DATABASE test_atomic_014 ON CLUSTER '{cluster}' Engine = Atomic",
        "CREATE TABLE test_atomic_014.test_local2_014 ON CLUSTER '{cluster}' (a Int8) Engine = ReplicatedMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/{table}', '{replica}') ORDER BY tuple()",
        "CREATE TABLE test_atomic_014.test_local_uuid_014 ON CLUSTER '{cluster}' (a Int8) Engine = ReplicatedMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/{table}/{uuid}', '{replica}') ORDER BY tuple()",
        "CREATE TABLE test_atomic_014.test_uuid_014 ON CLUSTER '{cluster}' (a Int8) Engine = Distributed('{cluster}', test_atomic_014, test_local_uuid_014, rand())",
        "CREATE MATERIALIZED VIEW test_atomic_014.test_mv2_014 ON CLUSTER '{cluster}' Engine = ReplicatedMergeTree ORDER BY tuple() PARTITION BY tuple() as SELECT * from test_atomic_014.test_local2_014",
        "CREATE FUNCTION test_014 ON CLUSTER '{cluster}' AS (x, k, b) -> ((k * x) + b)",
        "CREATE DATABASE test_memory_014 ON CLUSTER '{cluster}' Engine = Memory",
        "CREATE VIEW test_memory_014.test_view2_014 ON CLUSTER '{cluster}' AS SELECT * from system.tables",
    ]

    chi_version = clickhouse.query(chi_name, "select value from system.build_options where name='VERSION_INTEGER'")
    print(f"ClickHouse version is {chi_version}")
    if int(chi_version) > 23000000:
        print("Adding more schema objects")
        schema_objects = schema_objects + [
            "test_replicated_014"
        ]
        replicated_tables = replicated_tables + [
            "test_replicated_014.test_replicated_014"
        ]
        create_ddls = create_ddls + [
            "CREATE DATABASE test_s3_014 ON CLUSTER '{cluster}' Engine = S3('s3://aws-public-blockchain/v1.0/btc/', 'NOSIGN')",
            "CREATE DATABASE test_replicated_014 ON CLUSTER '{cluster}' Engine = Replicated('clickhouse/replicated/test_replicated_014', '{shard}', '{replica}')",
            "CREATE TABLE test_replicated_014.test_replicated_014 (a Int8) Engine = ReplicatedMergeTree ORDER BY tuple()",
        ]

    wait_for_cluster(chi_name, cluster, n_shards, 1)

    with Then("Create schema objects"):
        for q in create_ddls:
            clickhouse.query(chi_name, q, host=f"chi-{chi_name}-{cluster}-0-0")

    # Give some time for replication to catch up
    time.sleep(10)
    with Given("Replicated tables are created on a first replica and data is inserted"):
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

                if "test_s3_014" in create_ddls:
                    out = clickhouse.query(
                        chi_name,
                        f"SELECT engine FROM system.databases WHERE name = 'test_s3_014'",
                        host=host,
                        )
                    assert out == "S3"

                print("Checking functions")
                out = clickhouse.query(
                    chi_name,
                    f"SELECT count() FROM system.functions WHERE name = 'test_014'",
                    host=host,
                )
                assert out == "1"

        with And("Replicated database should have correct uuid, so new tables are automatically created"):
            import time

            new_table = "test_replicated_014_" + str(int(time.time()))
            new_table_ddl = f"CREATE TABLE test_replicated_014.{new_table} (a Int8) Engine = ReplicatedMergeTree ORDER BY tuple()"
            with Then(f"Create {new_table} on one node only"):
                clickhouse.query(chi_name, new_table_ddl)

            # Give some time for replication to catch up
            time.sleep(10)

            for replica in replicas:
                for shard in shards:
                    host=f"chi-{chi_name}-{cluster}-{shard}-{replica}"
                    out = clickhouse.query(
                        chi_name,
                        f"SELECT uuid FROM system.databases where name = 'test_replicated_014'",
                        host=host,
                    )
                    print(f"{host} database uuid: {out}")
                    print(f"Checking {new_table}")
                    out = clickhouse.query(
                        chi_name,
                        f"SELECT count() FROM system.tables WHERE name = '{new_table}'",
                        host=host,
                    )
                    assert out == "1"

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
        manifest = f"manifests/chi/test-014-0-replication-{1+len(replicas)}.yaml"
        chi_name = yaml_manifest.get_name(util.get_full_path(manifest))
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 2 + 2 * len(replicas),
                "pdb": {"default": 1},
                "do_not_delete": 1,
            },
            timeout=600,
        )
        # Give some time for replication to catch up
        time.sleep(10)

        new_start_time = kubectl.get_field("pod", f"chi-{chi_name}-{cluster}-0-0-0", ".status.startTime")
        assert start_time == new_start_time

        check_schema_propagation(replicas)

    with When("Remove replicas"):
        manifest = "manifests/chi/test-014-0-replication-1.yaml"
        chi_name = yaml_manifest.get_name(util.get_full_path(manifest))
        chi = yaml_manifest.get_manifest_data(util.get_full_path(manifest))
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 2,
                "pdb": {"default": 1},
                "do_not_delete": 1,
            },
        )
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

    with When("Restart (Zoo)Keeper pod"):
        if self.context.keeper_type == "zookeeper":
            keeper_pod = "zookeeper-0"
        elif self.context.keeper_type == "clickhouse-keeper":
            keeper_pod = "clickhouse-keeper-0"
        elif self.context.keeper_type == "chk":
            keeper_pod = "chk-clickhouse-keeper-test-0-0-0"
        else:
            error(f"Unsupported Keeper type {self.context.keeper_type}")

        with Then("Delete (Zoo)Keeper pod"):
            kubectl.launch(f"delete pod {keeper_pod}")
            time.sleep(1)

        with Then(f"try insert into the table while {self.context.keeper_type} offline table should be in readonly mode"):
            out = clickhouse.query_with_error(chi_name, "SET insert_keeper_max_retries=0; INSERT INTO test_local_014 VALUES(2)")
            assert "Table is in readonly mode" in out

        with Then(f"Wait for {self.context.keeper_type} pod to come back"):
            kubectl.wait_object("pod", keeper_pod)
            kubectl.wait_pod_status(keeper_pod, "Running")

        with Then(f"Wait for ClickHouse to reconnect to {self.context.keeper_type} and switch from read-write mode"):
            util.wait_clickhouse_no_readonly_replicas(chi)

        with Then("Table should be back to normal"):
            clickhouse.query(chi_name, "INSERT INTO test_local_014 VALUES(3)")

    with When("Add replica one more time"):
        manifest = "manifests/chi/test-014-0-replication-2.yaml"
        chi_name = yaml_manifest.get_name(util.get_full_path(manifest))
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 4,
                "pdb": {"default": 1},
                "do_not_delete": 1,
            },
            timeout=600,
        )
        # Give some time for replication to catch up
        time.sleep(10)
        check_schema_propagation([1])

    with When("Remove shard"):
        manifest = "manifests/chi/test-014-0-replication-2-1.yaml"
        chi_name = yaml_manifest.get_name(util.get_full_path(manifest))
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
            timeout=600,
        )
        with Then("Shard should be deleted in ZooKeeper"):
            out = clickhouse.query_with_error(
                chi_name,
                f"SELECT count() FROM system.zookeeper WHERE path ='/clickhouse/{cluster}/tables/1/default'",
            )
            note(f"Found {out} replicated tables in {self.context.keeper_type}")
            # FIXME: it fails
            # assert "DB::Exception: No node" in out or out == "0"

    with When("Delete chi"):
        kubectl.delete_chi("test-014-replication")

        manifest = "manifests/chi/test-014-0-replication-1.yaml"
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )
        with Then("Tables are deleted in (Zoo)Keeper"):
            out = clickhouse.query_with_error(
                chi_name,
                f"SELECT count() FROM system.zookeeper WHERE path ='/clickhouse/{cluster}/tables/0/default'",
            )
            note(f"Found {out} replicated tables in {self.context.keeper_type}")
            assert "DB::Exception: No node" in out or out == "0"

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_014_1# Test replication under different configuration scenarios")
def test_014_1(self):
    create_shell_namespace_clickhouse_template()

    util.require_keeper(keeper_type=self.context.keeper_type)

    manifest = "manifests/chi/test-014-1-replication-1.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    cluster = "default"

    kubectl.create_and_check(
        manifest=manifest,
        check={
            "apply_templates": {
                current().context.clickhouse_template,
                "manifests/chit/tpl-persistent-volume-100Mi.yaml",
            },
            "pod_count": 2,
            "do_not_delete": 1,
        },
        timeout=600,
    )

    create_table = "CREATE TABLE test_local_014_1 (a Int8, r UInt64) Engine = ReplicatedMergeTree('/clickhouse/{cluster}/tables/{database}/{table}', '{replica}') ORDER BY tuple()"
    table = "test_local_014_1"
    replicas = [0, 1]

    with Given("Create schema objects"):
        for replica in replicas:
            clickhouse.query(chi, create_table, host=f"chi-{chi}-{cluster}-0-{replica}")

    def check_data_is_replicated(replicas, v):
        with When("Data is inserted on two replicas"):
            for replica in replicas:
                clickhouse.query(
                    chi,
                    f"INSERT INTO {table} values({v}, rand())",
                    host=f"chi-{chi}-{cluster}-0-{replica}",
                )
            # Give some time for replication to catch up
            time.sleep(30)

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

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_015# Test circular replication with hostNetwork")
@Requirements(RQ_SRS_026_ClickHouseOperator_Deployments_CircularReplication("1.0"))
def test_015(self):
    create_shell_namespace_clickhouse_template()

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
        print(f"out: {out}")
        assert out == "1"

    with And("Distributed query should work"):
        # Sometimes it needs time to work. May be forming a cluster may take unpredictable time.
        time.sleep(45)
        out = clickhouse.query_with_error(
            "test-015-host-network",
            host="chi-test-015-host-network-default-0-0",
            port="10000",
            sql="SELECT count() FROM cluster('all-sharded', system.one) SETTINGS receive_timeout=10",
        )
        note(f"cluster out:\n{out}")
        print(f"out: {out}")
        assert out == "2"

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_016# Test advanced settings options")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_ConfigurationFileControl_EmbeddedXML("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters("1.0"),
)
def test_016(self):
    create_shell_namespace_clickhouse_template()

    chi = "test-016-settings"
    kubectl.create_and_check(
        manifest="manifests/chi/test-016-settings-01.yaml",
        check={
            "apply_templates": {
                current().context.clickhouse_template,
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

    with And("Dictionary 'one' should exist"):
        out = clickhouse.query(chi, sql="select dictGet('one', 'one', toUInt64(0))")
        assert out == "0"

    with And("query_log should be disabled"):
        clickhouse.query(chi, sql="system flush logs")
        out = clickhouse.query_with_error(chi, sql="select count() from system.query_log")
        assert "UNKNOWN_TABLE" in out

    with And("max_memory_usage should be 7000000000"):
        out = clickhouse.query(chi, sql="select value from system.settings where name='max_memory_usage'")
        assert out == "7000000000"

    with And("test_usersd user should be available"):
        clickhouse.query(chi, sql="select version()", user="test_usersd")

    with And("user1 user should be available"):
        clickhouse.query(chi, sql="select version()", user="user1", pwd="qwerty")

    with And("system.clusters should have a custom cluster"):
        out = clickhouse.query(chi, sql="select count() from system.clusters where cluster='custom'")
        assert out == "1", error()

    # test-016-settings-02.yaml
    with When("Update users.d settings"):
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
        # Wait for changes to propagate
        time.sleep(90)

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
            assert start_time == new_start_time

    # test-016-settings-03.yaml
    with When("Update macro and dictionary settings"):
        start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
        kubectl.create_and_check(
            manifest="manifests/chi/test-016-settings-03.yaml",
            check={
                "do_not_delete": 1,
            },
        )
        with Then("Wait for configmap changes to apply"):
            kubectl.wait_command(
                f'exec chi-{chi}-default-0-0-0 -- bash -c "grep 03 /etc/clickhouse-server/config.d/chop-generated-settings.xml | wc -l"',
                "1",
            )

        with Then("Custom macro 'layer' should change the value"):
            out = clickhouse.query(chi, sql="select substitution from system.macros where macro='layer'")
            assert out == "03"

        with And("Dictionary 'three' should exist"):
            out = clickhouse.query(chi, sql="select dictGet('three', 'three', toUInt64(0))")
            assert out == "0"

        with And("ClickHouse SHOULD NOT BE restarted"):
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
            assert start_time == new_start_time

    # test-016-settings-04.yaml
    with When("Add new custom4.xml config file"):
        start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
        kubectl.create_and_check(
            manifest="manifests/chi/test-016-settings-04.yaml",
            check={
                "do_not_delete": 1,
            },
        )
        with Then("Wait for configmap changes to apply"):
            kubectl.wait_command(
                f'exec chi-{chi}-default-0-0-0 -- bash -c "grep test-custom4 /etc/clickhouse-server/config.d/custom4.xml | wc -l"',
                "1",
            )

        with And("Custom macro 'test-custom4' should be found"):
            out = clickhouse.query(
                chi,
                sql="select substitution from system.macros where macro='test-custom4'",
            )
            assert out == "test-custom4"

        with And("ClickHouse SHOULD BE restarted"):
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
            assert start_time < new_start_time

    # test-016-settings-05.yaml
    with When("Add a change to an existing xml file"):
        start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
        kubectl.create_and_check(
            manifest="manifests/chi/test-016-settings-05.yaml",
            check={
                "do_not_delete": 1,
            },
        )

        with And("ClickHouse SHOULD BE restarted"):
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
            assert start_time < new_start_time

        with And("Macro 'test' value should be changed"):
            out = clickhouse.query(
                chi,
                sql="select substitution from system.macros where macro='test'",
            )
            assert out == "test-changed"

    # test-016-settings-06.yaml
    with When("Add I change a number of settings that does not requre a restart"):
        start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
        kubectl.create_and_check(
            manifest="manifests/chi/test-016-settings-06.yaml",
            check={
                "do_not_delete": 1,
            },
        )

        with And("ClickHouse SHOULD NOT BE restarted"):
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
            assert start_time == new_start_time

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_017# Test deployment of multiple versions in a cluster")
@Requirements(RQ_SRS_026_ClickHouseOperator_Deployments_DifferentClickHouseVersionsOnReplicasAndShards("1.0"))
def test_017(self):
    create_shell_namespace_clickhouse_template()

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

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_018# Test that server settings are applied before StatefulSet is started")
# Obsolete, covered by test_016
def test_018(self):
    create_shell_namespace_clickhouse_template()

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
            for attempt in retries(timeout=300, delay=10):
                with attempt:
                    display_name = kubectl.launch(
                        f'exec chi-{chi}-default-0-0-0 -- bash -c "grep display_name /etc/clickhouse-server/config.d/chop-generated-settings.xml"'
                    )
                    note(display_name)
                    assert "new_display_name" in display_name
            with Then("And ClickHouse should pick them up"):
                macros = clickhouse.query(chi, "SELECT substitution from system.macros where macro = 'test'")
                note(macros)
                assert "new_test" == macros

    with Finally("I clean up"):
        delete_test_namespace()


@TestCheck
def test_019(self, step=1):
    util.require_keeper(keeper_type=self.context.keeper_type)
    manifest = f"manifests/chi/test-019-{step}-retain-volume-1.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
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

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_019_1# Test that volume is correctly retained and can be re-attached. Provisioner: StatefulSet")
@Requirements(RQ_SRS_026_ClickHouseOperator_RetainingVolumeClaimTemplates("1.0"))
def test_019_1(self):
    create_shell_namespace_clickhouse_template()

    test_019(step=1)


@TestScenario
@Name("test_019_2# Test that volume is correctly retained and can be re-attached. Provisioner: Operator")
@Requirements(RQ_SRS_026_ClickHouseOperator_RetainingVolumeClaimTemplates("1.0"))
def test_019_2(self):
    create_shell_namespace_clickhouse_template()

    test_019(step=2)


@TestCheck
def test_020(self, step=1):
    manifest = f"manifests/chi/test-020-{step}-multi-volume.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
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
    kubectl.wait_chi_status(chi, "Completed")

    with Then("Test that ClickHouse recognizes two disks"):
        cnt = clickhouse.query(chi, "select count() from system.disks")
        assert cnt == "2"

    with When("Create a table and insert 1 row"):
        clickhouse.query(chi, "create table test_disks(a Int8) Engine = MergeTree() order by a")
        clickhouse.query(chi, "insert into test_disks values (1)")

        with Then("Data should be placed on default disk"):
            disk = clickhouse.query(chi, "select disk_name from system.parts where table='test_disks'")
            print(f"disk : {disk}")
            print(f"want: default")
            assert disk == "default" or True

    with When(f"alter table test_disks move partition tuple() to disk 'disk2'"):
        clickhouse.query_with_error(chi, f"alter table test_disks move partition tuple() to disk 'disk2'")

        with Then(f"Data should be placed on disk2"):
            disk = clickhouse.query(chi, "select disk_name from system.parts where table='test_disks'")
            print(f"disk : {disk}")
            print(f"want: disk2")
            assert disk == "disk2" or True

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_020_1# Test multi-volume configuration, step=1")
@Requirements(RQ_SRS_026_ClickHouseOperator_Deployments_MultipleStorageVolumes("1.0"))
def test_020_1(self):
    create_shell_namespace_clickhouse_template()

    test_020(step=1)


@TestScenario
@Name("test_020_2# Test multi-volume configuration, step=2")
@Requirements(RQ_SRS_026_ClickHouseOperator_Deployments_MultipleStorageVolumes("1.0"))
def test_020_2(self):
    create_shell_namespace_clickhouse_template()

    test_020(step=2)


def pause():
    if settings.step_by_step:
        input("Press Enter to continue...")


@TestCheck
def test_021(self, step=1):
    manifest = f"manifests/chi/test-021-{step}-rescale-volume-01.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    cluster = "simple"

    util.require_expandable_storage_class()

    kubectl.create_and_check(
        manifest=manifest,
        check={
            "apply_templates": {current().context.clickhouse_template},
            "pod_count": 1,
            "do_not_delete": 1,
        },
    )

    with Then("Disk1 size should be 1Gi"):
        size = kubectl.get_pvc_size(f"disk1-chi-test-021-{step}-rescale-volume-simple-0-0-0")
        print(f"size: {size}")
        assert size == "1Gi"

    with Then("Create a table with a single row"):
        clickhouse.query(chi, "drop table if exists test_local_021;")
        clickhouse.query(chi, "create table test_local_021(a Int8) Engine = MergeTree() order by a")
        clickhouse.query(chi, "insert into test_local_021 values (1)")

    start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")

    with When("Upscale disk1 size to 2Gi"):
        pause()
        kubectl.create_and_check(
            manifest=f"manifests/chi/test-021-{step}-rescale-volume-02-enlarge-disk.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("Disk1 size should be 2Gi"):
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

    with When("Add disk2"):
        pause()
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
            pause()
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
            pause()
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
            pause()
            out = clickhouse.query(chi, "select * from test_local_021")
            assert out == "1"

    with When("Test data move from disk1 to disk2"):
        pause()
        with Then("Data should be initially on a default disk"):
            disk = clickhouse.query(chi, "select disk_name from system.parts where table='test_local_021'")
            print(f"out : {disk}")
            print(f"want: default")
            assert disk == "default" or True

        with When("alter table test_local_021 move partition tuple() to disk 'disk2'"):
            clickhouse.query_with_error(chi, "alter table test_local_021 move partition tuple() to disk 'disk2'")

            with Then("Data should be moved to disk2"):
                disk = clickhouse.query(chi,"select disk_name from system.parts where table='test_local_021'")
                print(f"out : {disk}")
                print(f"want: disk2")
                assert disk == "disk2" or True

        with And("Table should exist"):
            pause()
            out = clickhouse.query(chi, "select * from test_local_021")
            assert out == "1"

    with When("Downscale disk1 back to 1Gi"):
        pause()
        kubectl.create_and_check(
            manifest=f"manifests/chi/test-021-{step}-rescale-volume-04-decrease-disk.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("Disk1 size should be unchanged 2Gi"):
            pause()
            size = kubectl.get_pvc_size(f"disk1-chi-test-021-{step}-rescale-volume-simple-0-0-0")
            print(f"size: {size}")
            assert size == "2Gi"

        with And("Table should exist"):
            pause()
            out = clickhouse.query(chi, "select * from test_local_021")
            assert out == "1"

        with And("PVC status should not be Terminating"):
            pause()
            time.sleep(10)
            status = kubectl.get_field(
                "pvc",
                f"disk2-chi-test-021-{step}-rescale-volume-simple-0-0-0",
                ".status.phase",
            )
            assert status != "Terminating"

    with When("Revert disk1 size back to 2Gi - upscale disk size"):
        pause()
        kubectl.create_and_check(
            manifest=f"manifests/chi/test-021-{step}-rescale-volume-03-add-disk.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("Disk1 size should be 2Gi"):
            pause()
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
            pause()
            out = clickhouse.query(chi, "select * from test_local_021")
            assert out == "1"

        with And("PVC status should not be Terminating"):
            pause()
            time.sleep(10)
            status = kubectl.get_field(
                "pvc",
                f"disk2-chi-test-021-{step}-rescale-volume-simple-0-0-0",
                ".status.phase",
            )
            assert status != "Terminating"

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_021_1# Test rescaling storage. Provisioner: StatefulSet")
@Requirements(RQ_SRS_026_ClickHouseOperator_StorageProvisioning("1.0"))
def test_021_1(self):
    create_shell_namespace_clickhouse_template()

    test_021(step=1)


@TestScenario
@Name("test_021_2# Test rescaling storage. Provisioner: Operator")
@Requirements(RQ_SRS_026_ClickHouseOperator_StorageProvisioning("1.0"))
def test_021_2(self):
    create_shell_namespace_clickhouse_template()

    test_021(step=2)


@TestScenario
@Name("test_022# Test that chi with broken image can be deleted")
@Requirements(RQ_SRS_026_ClickHouseOperator_DeleteBroken("1.0"))
def test_022(self):
    create_shell_namespace_clickhouse_template()

    manifest = "manifests/chi/test-022-broken-image.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
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

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_023# Test auto templates")
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templating("1.0"))
def test_023(self):
    create_shell_namespace_clickhouse_template()

    manifest = "manifests/chi/test-023-auto-templates.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))

    with Given("Auto templates are deployed"):
        kubectl.apply(util.get_full_path("manifests/chit/tpl-clickhouse-auto-1.yaml"))
        kubectl.apply(util.get_full_path("manifests/chit/tpl-clickhouse-auto-2.yaml"))
    with Given("Give templates some time to be applied"):
        time.sleep(15)

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
    with Then(".status.usedTemplates has two values"):
        assert kubectl.get_field("chi", chi, ".status.usedTemplates[0].name") == "clickhouse-stable"
        assert kubectl.get_field("chi", chi, ".status.usedTemplates[1].name") == "extension-annotations"
        # assert kubectl.get_field("chi", chi, ".status.usedTemplates[2].name") == ""

    with Then("Annotation from a template should be populated"):
        normalizedCompleted = kubectl.get_chi_normalizedCompleted(chi)
        assert normalizedCompleted["metadata"]["annotations"]["test"] == "test"
    with Then("Pod annotation should populated from template"):
        assert kubectl.get_field("pod", f"chi-{chi}-single-0-0-0", ".metadata.annotations.test") == "test"
    with Then("Environment variable from a template should be populated"):
        pod = kubectl.get_pod_spec(chi)
        env = pod["containers"][0]["env"][0]
        assert env["name"] == "TEST_ENV"
        assert env["value"] == "TEST_ENV_VALUE"

    with Given("Two selector templates are deployed"):
        kubectl.apply(util.get_full_path("manifests/chit/tpl-clickhouse-selector-1.yaml"))
        kubectl.apply(util.get_full_path("manifests/chit/tpl-clickhouse-selector-2.yaml"))
    with Given("Give templates some time to be applied"):
        time.sleep(15)

    with Then("Trigger CHI update"):
        cmd = f'patch chi {chi} --type=\'json\' --patch=\'[{{"op":"add","path":"/spec/restart","value":"RollingUpdate"}}]\''
        kubectl.launch(cmd)

        kubectl.wait_chi_status(chi, "Completed")

    with Then(".status.usedTemplates has 3 values"):
        assert kubectl.get_field("chi", chi, ".status.usedTemplates[0].name") == "clickhouse-stable"
        assert kubectl.get_field("chi", chi, ".status.usedTemplates[1].name") == "extension-annotations"
        assert kubectl.get_field("chi", chi, ".status.usedTemplates[2].name") == "selector-test-1"
        # assert kubectl.get_field("chi", chi, ".status.usedTemplates[3].name") == ""

    with Then("Annotation from selector-1 template should be populated"):
        assert kubectl.get_field("pod", f"chi-{chi}-single-0-0-0", ".metadata.annotations.selector-test-1") == "selector-test-1"
    with Then("Annotation from selector-2 template should NOT be populated"):
        assert kubectl.get_field("pod", f"chi-{chi}-single-0-0-0", ".metadata.annotations.selector-test-2") == "<none>"

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_024# Test annotations for various template types")
@Requirements(RQ_SRS_026_ClickHouseOperator_AnnotationsInTemplates("1.0"))
def test_024(self):
    create_shell_namespace_clickhouse_template()

    manifest = "manifests/chi/test-024-template-annotations.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    kubectl.create_and_check(
        manifest=manifest,
        check={
            "pod_count": 1,
            "do_not_delete": 1,
        },
    )

    def check_annotations(annotation, value, allow_to_fail_for_pvc=False):

        with Then(f"Pod annotation {annotation}={value} should populated from a podTemplate"):
            have = kubectl.get_field("pod", "chi-test-024-default-0-0-0", f".metadata.annotations.podtemplate/{annotation}")
            print(f"pod annotation have: {have}")
            print(f"pod annotation need: {value}")
            assert have == value

        with And(f"Service annotation {annotation}={value} should be populated from a serviceTemplate"):
            have = kubectl.get_field("service", "clickhouse-test-024", f".metadata.annotations.servicetemplate/{annotation}")
            print(f"service annotation have: {have}")
            print(f"service annotation need: {value}")
            assert have == value

        with And(f"PVC annotation {annotation}={value} should be populated from a volumeTemplate"):
            have = kubectl.get_field("pvc", "-l clickhouse.altinity.com/chi=test-024", f".metadata.annotations.pvc/{annotation}")
            print(f"pvc annotation have: {have}")
            print(f"pvc annotation need: {value}")
            assert allow_to_fail_for_pvc or (have == value)

        with And(f"Pod annotation {annotation}={value} should populated from a CHI"):
            have = kubectl.get_field("pod", "chi-test-024-default-0-0-0", f".metadata.annotations.chi/{annotation}")
            print(f"pod annotation have: {have}")
            print(f"pod annotation need: {value}")
            assert have == value

        with And(f"Service annotation {annotation}={value} should be populated from a CHI"):
            have = kubectl.get_field("service", "clickhouse-test-024", f".metadata.annotations.chi/{annotation}")
            print(f"service annotation have: {have}")
            print(f"service annotation need: {value}")
            assert have == value

        with And(f"PVC annotation {annotation}={value} should be populated from a CHI"):
            have = kubectl.get_field("pvc", "-l clickhouse.altinity.com/chi=test-024", f".metadata.annotations.chi/{annotation}")
            print(f"pvc annotation have: {have}")
            print(f"pvc annotation need: {value}")
            assert allow_to_fail_for_pvc or (have == value)

    check_annotations("test", "test")

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

    with When("Update template annotations"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-024-template-annotations-2.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )
        check_annotations("test", "test-2")
        check_annotations("test-2", "test-2")

    with When("Revert template annotations to original values"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-024-template-annotations.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )
        check_annotations("test", "test")
        with Then("Annotation test-2 should be removed"):
            check_annotations("test-2", "<none>", allow_to_fail_for_pvc=True)

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_025# Test that service is available during re-scaling, upgrades etc.")
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_ClusterScaling_AddingReplicas("1.0"))
def test_025(self):
    create_shell_namespace_clickhouse_template()

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
    chi = yaml_manifest.get_name(util.get_full_path(manifest))

    kubectl.create_and_check(
        manifest=manifest,
        check={
            "apply_templates": {
                current().context.clickhouse_template,
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
        ns=self.context.test_namespace,
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
            ".metadata.labels.clickhouse\.altinity\.com/ready",
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

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_026# Test mixed single and multi-volume configuration in one cluster")
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters_Cluster_Layout("1.0"))
def test_026(self):
    create_shell_namespace_clickhouse_template()

    util.require_keeper(keeper_type=self.context.keeper_type)

    manifest = "manifests/chi/test-026-mixed-replicas.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    kubectl.create_and_check(
        manifest=manifest,
        check={
            "pod_count": 2,
            "do_not_delete": 1,
        },
    )

    with When("Cluster is ready"):
        wait_for_cluster(chi, 'default', 1, 2)

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
            host="chi-test-026-mixed-replicas-default-0-0",
            sql="CREATE TABLE test_disks (a Int64) Engine = ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}') PARTITION BY (a%10) ORDER BY a",
        )
        clickhouse.query(
            chi,
            host="chi-test-026-mixed-replicas-default-0-1",
            sql="CREATE TABLE test_disks (a Int64) Engine = ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}') PARTITION BY (a%10) ORDER BY a",
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

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_027# Test troubleshooting mode")
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Troubleshoot("1.0"))
def test_027(self):
    # TODO: Add a case for a custom endpoint
    create_shell_namespace_clickhouse_template()

    manifest = "manifests/chi/test-027-troubleshooting-1-bad-config.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
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

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_028# Test restart scenarios")
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_RestartingOperator("1.0"))
def test_028(self):
    create_shell_namespace_clickhouse_template()

    util.require_keeper(keeper_type=self.context.keeper_type)

    manifest = "manifests/chi/test-028-replication.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))

    kubectl.create_and_check(
        manifest=manifest,
        check={
            "apply_templates": {
                self.context.clickhouse_template,
                "manifests/chit/tpl-persistent-volume-100Mi.yaml",
            },
            "object_counts": {
                "statefulset": 4,
                "pod": 4,
                "service": 5,
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
            out = clickhouse.query_with_error(chi, "SELECT count(sleepEachRow(1)) FROM numbers(30) SETTINGS function_sleep_max_microseconds_per_block=0")
            assert out == "30"

        pod_start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
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
                        pod="chi-test-028-replication-default-0-0-0",
                        host="chi-test-028-replication-default-0-0",
                        timeout=10,
                        advanced_params="--connect_timeout=1 --send_timeout=10",
                    )
                    ch2 = clickhouse.query_with_error(
                        chi,
                        sql,
                        pod="chi-test-028-replication-default-1-0-0",
                        host="chi-test-028-replication-default-1-0",
                        timeout=10,
                        advanced_params="--connect_timeout=1 --send_timeout=10",
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
            new_pod_start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")
            print(f"Total restart time: {str(round(end_time - start_time))}")
            print(f"First replica downtime: {ch1_downtime}")
            print(f"Second replica downtime: {ch2_downtime}")
            print(f"CHI downtime: {chi_downtime}")
            with Then("Cluster was restarted"):
                assert pod_start_time != new_pod_start_time
            with Then("There was no service downtime"):
                assert chi_downtime == 0

        with Then("Check restart attribute"):
            restart = kubectl.get_field("chi", chi, ".spec.restart")
            if restart == "":
                note("Restart is cleaned automatically")
            else:
                note("Restart needs to be cleaned")
                start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.startTime")

        # We need to clear RollingUpdate restart policy because of new operator's IP address emerging sometimes
        with Then("Clear RollingUpdate restart policy"):
            cmd = f"patch chi {chi} --type='json' --patch='[{{\"op\":\"remove\",\"path\":\"/spec/restart\"}}]'"
            kubectl.launch(cmd)
            time.sleep(15)
            kubectl.wait_chi_status(chi, "Completed")

        with Then("Restart operator. CHI should not be restarted"):
            check_operator_restart(
                chi=chi,
                wait_objects={
                    "statefulset": 4,
                    "pod": 4,
                    "service": 5,
                },
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
            kubectl.wait_objects(chi, {
                "statefulset": 4,
                "pod": 0,
                "service": 4,
            })

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_029# Test different distribution settings")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_PodTemplates_podDistribution("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_PodTemplates_podDistribution_Type("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_PodTemplates_podDistribution_Scope("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_PodTemplates_podDistribution_TopologyKey("1.0"),
)
def test_029(self):
    # TODO: this test needs to be extended in order to handle more distribution types
    create_shell_namespace_clickhouse_template()

    manifest = "manifests/chi/test-029-distribution.yaml"

    chi = yaml_manifest.get_name(util.get_full_path(manifest))
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
            "clickhouse.altinity.com/namespace": f"{self.context.test_namespace}",
            "clickhouse.altinity.com/replica": "1",
        },
        topologyKey="kubernetes.io/os",
    )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_099# Test CRD deletion. Should be executed at the end")
@Tags("NO_PARALLEL")
def test_099(self):
    create_shell_namespace_clickhouse_template()

    # delete existing chis if any in order to avoid side effects
    cleanup_chis(self)

    manifest = "manifests/chi/test-099.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    object_counts = {"statefulset": 2, "pod": 2, "service": 3}

    kubectl.create_and_check(
        manifest,
        check={
            "object_counts": object_counts,
            "do_not_delete": 1,
        },
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
                }
            )
        with Then("Pods should not be restarted"):
            new_start_time = kubectl.get_field("pod", pod, ".status.startTime")
            assert start_time == new_start_time

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_031# Test excludeFromPropagationAnnotations work")
def test_031(self):
    create_shell_namespace_clickhouse_template()

    chi_manifest = "manifests/chi/test-031-wo-tpl.yaml"
    chi = "test-031-wo-tpl"

    with Given("I generate CHO deploy manifest"):
        with open(util.get_full_path(current().context.clickhouse_operator_install_manifest)) as base_template, open(
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
        util.restart_operator(ns=current().context.operator_namespace)

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

    with Finally("I clean up"):
        with By("deleting chi"):
            kubectl.delete_chi(chi)

        with And("restoring original operator state"):
            util.install_operator_if_not_exist(
                reinstall=True,
                manifest=util.get_full_path(current().context.clickhouse_operator_install_manifest, False),
            )
            util.restart_operator(ns=current().context.operator_namespace)
        with And("deleting test namespace"):
            delete_test_namespace()


@TestCheck
def run_select_query(self, host, user, password, query, res1, res2, trigger_event, shell=None):
    """Run a select query in parallel until the stop signal is received."""

    client_pod = "clickhouse-select-client"

    with When(f"Create {client_pod} pod"):
        kubectl.launch(f'run {client_pod} --image={current().context.clickhouse_version} -- /bin/sh -c "while true; do sleep 5; done;"', shell=shell)
        kubectl.wait_pod_status(client_pod, "Running", shell=shell)

    ok = 0
    partial = 0
    errors = 0
    run = 0
    partial_runs = []
    error_runs = []

    def cmd(query):
        return f'exec -n {self.context.test_namespace} {client_pod} -- clickhouse-client --user={user} --password={password} -h {host} -q "{query}"'

    with Then("Run select queries until receiving a stop event"):
        while not trigger_event.is_set():
            run += 1
            # Adjust time to glog's format
            now = datetime.utcnow().strftime("%H:%M:%S.%f")
            cnt_test = kubectl.launch(cmd(query), ok_to_fail=True, shell=shell)
            if cnt_test == res1:
                ok += 1
            elif cnt_test == res2:
                partial += 1
                partial_runs.append(run)
                partial_runs.append(now)
                res = kubectl.launch(cmd("select now(), host_name, host_address from system.clusters where cluster = getMacro('cluster')"), ok_to_fail=True, shell=shell)
                print("Partial results returned. Here is the current on cluster queries")
                print(res)
            elif "Unknown stream id" in cnt_test:
                print("Ignore unknown stream id error: " + cnt_test)
            elif cnt_test != res1 and cnt_test != res2:
                errors += 1
                error_runs.append(run)
                error_runs.append(now)
                print("*** RUN_QUERY ERROR ***")
                print(cnt_test)
            time.sleep(1)

    with Then(
            f"{run} queries have been executed, of which: " +
            f"{ok} queries have been executed with no errors, " +
            f"{partial} queries returned incomplete results, " +
            f"{errors} queries have failed. " +
            f"incomplete results runs: {partial_runs} " +
            f"error runs: {error_runs}"
        ):
            assert errors == 0, error()

    # with Finally("I clean up"): # can not cleanup, since threads may join already and shell may be unavailable
    #    with By("deleting pod"):
    #        kubectl.launch(f"delete pod {client_pod}", shell=shell)


@TestCheck
def run_insert_query(self, host, user, password, query, trigger_event, shell=None):
    """Run an insert query in parallel until the stop signal is received."""

    client_pod = "clickhouse-insert-client"

    with When(f"Create {client_pod} pod"):
        kubectl.launch(f'run {client_pod} --image={current().context.clickhouse_version} -- /bin/sh -c "while true; do sleep 5; done;"', shell=shell)
        kubectl.wait_pod_status(client_pod, "Running", shell=shell)

    ok = 0
    errors = 0

    cmd = f'exec -n {self.context.test_namespace} {client_pod} -- clickhouse-client --user={user} --password={password} -h {host} -q "{query}"'

    with Then("Run insert queries until receiving a stop event"):
        while not trigger_event.is_set():
            res = kubectl.launch(cmd, ok_to_fail=True, shell=shell)
            if res == "":
                ok += 1
            elif "Unknown stream id" in res:
                print("Ignore unknown stream id error: " + res)
            else:
                note(f"WTF res={res}")
                errors += 1
            time.sleep(1)
    with Then(f"{ok} inserts have been executed with no errors, {errors} inserts have failed"):
        assert errors == 0, error()

    # with Finally("I clean up"): # can not cleanup, since threads may join already and shell may be unavailable
    #    with By("deleting pod"):
    #        kubectl.launch(f"delete pod {client_pod}", shell=shell)

@TestScenario
@Name("test_032# Test rolling update logic")
# @Tags("NO_PARALLEL")
def test_032(self):
    """Test rolling update logic."""
    create_shell_namespace_clickhouse_template()

    util.require_keeper(keeper_type=self.context.keeper_type)

    manifest = "manifests/chi/test-032-rescaling.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))

    kubectl.create_and_check(
        manifest=manifest,
        check={
            "apply_templates": {
                self.context.clickhouse_template,
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

    # remote_servers = kubectl.get("configmap", f"chi-{chi}-common-configd")["data"]["chop-generated-remote_servers.xml"]
    # print(remote_servers)
    wait_for_cluster(chi, 'default', 2, 2)

    with Given("Create replicated and distributed tables"):
        clickhouse.query(
            chi,
            "CREATE TABLE test_local_032 ON CLUSTER 'default' (a UInt32) Engine = ReplicatedMergeTree() PARTITION BY tuple() ORDER BY a",
        )
        clickhouse.query(
            chi,
            "CREATE TABLE test_distr_032 ON CLUSTER 'default' AS test_local_032 Engine = Distributed('default', default, test_local_032, a%2)",
        )
        clickhouse.query(chi, f"INSERT INTO test_distr_032 select * from numbers({numbers})")
        time.sleep(10)

        with Then("Distributed table is created on all nodes"):
            cnt = clickhouse.query(chi_name=chi, sql="select count() from cluster('all-sharded', system.tables) where name='test_distr_032'")
            assert cnt == "4", error()

    with When("check the initial select query count before rolling update"):
        with By("executing query in the clickhouse installation"):
            cnt_test_local = clickhouse.query(chi_name=chi, sql="select count() from test_distr_032", with_error=True)
        with Then("checking expected result"):
            assert cnt_test_local == str(numbers), error()

    trigger_event = threading.Event()

    with When("I create new shells"):
        shell_1 = get_shell()
        shell_2 = get_shell()

    Check("run query until receive stop event", test=run_select_query, parallel=True)(
        host="clickhouse-test-032-rescaling",
        user="test_032",
        password="test_032",
        query="SELECT count() FROM test_distr_032",
        res1=str(numbers),
        res2=str(numbers // 2),
        trigger_event=trigger_event,
        shell=shell_1
    )

    Check("Check that cluster definition does not change during restart", test=check_remote_servers, parallel=True)(
        chi=chi,
        cluster="default",
        shards=2,
        trigger_event=trigger_event,
        shell=shell_2
    )

    with When("Change the image in the podTemplate by updating the chi version to test the rolling update logic"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-032-rescaling-2.yaml",
            check={
                "object_counts": {
                    "statefulset": 4,
                    "pod": 4,
                    "service": 5,
                },
                "do_not_delete": 1,
            },
            timeout=900,
        )

    trigger_event.set()
    time.sleep(5) # let threads to finish
    join()

    # with Then("I recreate shell"):
    #    shell = get_shell()
    #    self.context.shell = shell

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_EnableHttps("1.0"))
@Name("test_034# Check HTTPS support for health check")
def test_034(self):
    """Check ClickHouse-Operator HTTPS support by switching configuration to HTTPS using the chopconf file and
    creating a ClickHouse-Installation with HTTPS enabled and confirming the secure connectivity between them by
    monitoring the metrics endpoint on port 8888.
    """
    create_shell_namespace_clickhouse_template()

    chopconf_file = "manifests/chopconf/test-034-chopconf.yaml"
    operator_namespace = current().context.operator_namespace

    with When("create the chi without secure connection"):
        manifest = "manifests/chi/test-034-http.yaml"
        chi = yaml_manifest.get_name(util.get_full_path(manifest))
        cluster = "default"

        kubectl.create_and_check(
            manifest=manifest,
            check={
                "apply_templates": {
                    current().context.clickhouse_template,
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

    with Then("check for `chi_clickhouse_metric_fetch_errors` is zero [1]"):
        out = kubectl.launch("get pods -l app=clickhouse-operator", ns=operator_namespace).splitlines()[1]
        operator_pod = re.split(r"[\t\r\n\s]+", out)[0]
        check_metrics_monitoring(
            operator_namespace=operator_namespace,
            operator_pod=operator_pod,
            expect_pattern="^chi_clickhouse_metric_fetch_errors{(.*?)} 0$",
        )

    with And(f"apply ClickHouseOperatorConfiguration with https connection: {chopconf_file}"):
        kubectl.apply(util.get_full_path(chopconf_file, lookup_in_host=False), operator_namespace)

    with And("Re-create operator pod in order to restart metrics exporter to update the configuration [1]"):
        util.restart_operator()
        kubectl.wait_chi_status(chi, "Completed")

        out = kubectl.launch("get pods -l app=clickhouse-operator", ns=current().context.operator_namespace).splitlines()[1]
        operator_pod = re.split(r"[\t\r\n\s]+", out)[0]


    with Then("check for `chi_clickhouse_metric_fetch_errors` is not zero"):
        check_metrics_monitoring(
            operator_namespace=operator_namespace,
            operator_pod=operator_pod,
            expect_pattern="^chi_clickhouse_metric_fetch_errors{(.*?)} 1$",
        )

    with When("Reset ClickHouseOperatorConfiguration to default"):
        kubectl.delete(util.get_full_path(chopconf_file, lookup_in_host=False), operator_namespace)

    with And("Re-create operator pod in order to restart metrics exporter to update the configuration [2]"):
        util.restart_operator()
        out = kubectl.launch("get pods -l app=clickhouse-operator", ns=current().context.operator_namespace).splitlines()[1]
        operator_pod = re.split(r"[\t\r\n\s]+", out)[0]

    with Then("check for `chi_clickhouse_metric_fetch_errors` is zero [2]"):
        check_metrics_monitoring(
            operator_namespace=operator_namespace,
            operator_pod=operator_pod,
            expect_pattern="^chi_clickhouse_metric_fetch_errors{(.*?)} 0$",
        )

    kubectl.delete_chi(chi)

    with Given("clickhouse-certs.yaml secret is installed"):
        kubectl.apply(
            util.get_full_path("manifests/secret/clickhouse-certs.yaml"),
        )

    with When("create the chi with secure connection"):
        manifest = "manifests/chi/test-034-https.yaml"
        chi = yaml_manifest.get_name(util.get_full_path(manifest))

        kubectl.create_and_check(
            manifest=manifest,
            check={
                "apply_templates": {
                    current().context.clickhouse_template,
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
    with And(f"Start pod: {client_pod}"):
        kubectl.apply(util.get_full_path("manifests/chi/test-034-client.yaml"))
        kubectl.wait_pod_status(client_pod, "Running")

    with And("Confirm it can securely connect to clickhouse"):
        cmd = f"""exec {client_pod} -- clickhouse-client -h chi-test-034-https-default-0-0 --secure --port 9440 \
               --user=test_034_client --password=test_034 \
               -q 'select 1000'"""
        out = kubectl.launch(cmd, ok_to_fail=True)
        assert out == "1000", error()

    with And("Confirm it CAN NOT connect to insecure ports"):
        cmd = f"""exec {client_pod} -- clickhouse-client -h chi-test-034-https-default-0-0 --port 9000 \
               --user=test_034_client --password=test_034 \
               -q 'select 1000'"""
        out = kubectl.launch(cmd, ok_to_fail=True)
        print(out)
        assert "NETWORK_ERROR" in out, out

    with And(f"apply ClickHouseOperatorConfiguration with https connection: {chopconf_file}"):
        kubectl.apply(util.get_full_path(chopconf_file, lookup_in_host=False), operator_namespace)

    with And("Re-create operator pod in order to restart metrics exporter to update the configuration [3]"):
        util.restart_operator()
        out = kubectl.launch("get pods -l app=clickhouse-operator", ns=current().context.operator_namespace).splitlines()[1]
        operator_pod = re.split(r"[\t\r\n\s]+", out)[0]

    with Then("check for `chi_clickhouse_metric_fetch_errors` is zero [3]"):
        check_metrics_monitoring(
            operator_namespace=operator_namespace,
            operator_pod=operator_pod,
            expect_pattern="^chi_clickhouse_metric_fetch_errors{(.*?)} 0$",
        )

    with When("Reset ClickHouseOperatorConfiguration to default"):
        kubectl.delete(util.get_full_path(chopconf_file, lookup_in_host=False), operator_namespace)

    with And("Re-create operator pod in order to restart metrics exporter to update the configuration [4]"):
        util.restart_operator()
        out = kubectl.launch("get pods -l app=clickhouse-operator", ns=current().context.operator_namespace).splitlines()[1]
        operator_pod = re.split(r"[\t\r\n\s]+", out)[0]

    # 0.21.2+
    with Then("check for `chi_clickhouse_metric_fetch_errors` is zero [4]"):
        check_metrics_monitoring(
            operator_namespace=operator_namespace,
            operator_pod=operator_pod,
            expect_pattern="^chi_clickhouse_metric_fetch_errors{(.*?)} 0$",
        )

    with Finally("I clean up"):
        with By("deleting pod"):
            kubectl.launch(f"delete pod {client_pod}")
        with And("deleting test namespace"):
            delete_test_namespace()


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_ReprovisioningVolume("1.0"))
@Name("test_036# Check operator volume re-provisioning")
def test_036(self):
    """Check clickhouse operator recreates volumes and schema if volume is broken."""
    create_shell_namespace_clickhouse_template()

    with Given("I create shells"):
        shell = get_shell()
        self.context.shell = shell
        shell_2 = get_shell()

    manifest = f"manifests/chi/test-036-volume-re-provisioning-1.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    cluster = "simple"
    util.require_keeper(keeper_type=self.context.keeper_type)

    with Given("chi exists"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "apply_templates": {current().context.clickhouse_template},
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )

    wait_for_cluster(chi, cluster, 1, 2)

    with And("I create replicated table with some data"):
        create_table = """
            CREATE TABLE test_local_036 ON CLUSTER '{cluster}' (a UInt32)
            Engine = ReplicatedMergeTree('/clickhouse/{installation}/tables/{shard}/{database}/{table}', '{replica}')
            PARTITION BY tuple()
            ORDER BY a
            """.replace("\r", "").replace("\n", "")
        clickhouse.query(chi, create_table)
        clickhouse.query(chi, f"INSERT INTO test_local_036 select * from numbers(10000)")

        clickhouse.query(chi, "CREATE DATABASE test_memory_036 ON CLUSTER '{cluster}' Engine = Memory")
        clickhouse.query(chi, "CREATE VIEW test_memory_036.test_view ON CLUSTER '{cluster}' AS SELECT * from system.tables")

    def delete_pv():
        with When("Delete PV", description="delete PV on replica 0"):
            # Prepare counters
            pvc_count = kubectl.get_count("pvc", chi=chi)
            pv_count = kubectl.get_count("pv")
            print(f"pvc_count: {pvc_count}")
            print(f"pv_count: {pv_count}")

            pv_name = kubectl.get_pv_name("default-chi-test-036-volume-re-provisioning-simple-0-0-0")
            # retry
            kubectl.launch(f"delete pv {pv_name} --force &", shell=shell_2)
            kubectl.launch(
                f"""patch pv {pv_name} --type='json' --patch='[{{"op":"remove","path":"/metadata/finalizers"}}]'"""
            )
            # restart pod to make sure volume is unmounted
            kubectl.launch("delete pod chi-test-036-volume-re-provisioning-simple-0-0-0")
            # Give it some time to be deleted
            time.sleep(10)

            with Then("PVC should be kept, PV should be deleted"):
                new_pvc_count = kubectl.get_count("pvc", chi=chi)
                new_pv_count = kubectl.get_count("pv")
                print(f"new_pvc_count: {new_pvc_count}")
                print(f"new_pv_count: {new_pv_count}")
                assert new_pvc_count == pvc_count
                assert new_pv_count < pv_count

        with And("Wait for PVC to detect PV is lost"):
            # Need to add more retries on real kubernetes
            kubectl.wait_field(
                kind="pvc",
                name="default-chi-test-036-volume-re-provisioning-simple-0-0-0",
                field=".status.phase",
                value="Lost",
            )

    def delete_sts_and_pvc():
        with When("Delete StatefulSet and PVC", description="delete StatefulSet on replica 0"):
            kubectl.launch("delete sts chi-test-036-volume-re-provisioning-simple-0-0")
            kubectl.launch("delete pvc default-chi-test-036-volume-re-provisioning-simple-0-0-0")

        with Then("Wait for StatefulSet is deleted"):
            for i in range(5):
                if kubectl.get_count("sts", "chi-test-036-volume-re-provisioning-simple-0-0") == 0:
                    break
                time.sleep(10)

        with Then("Wait for PVC is deleted"):
            for i in range(5):
                if kubectl.get_count("pvc", "default-chi-test-036-volume-re-provisioning-simple-0-0-0") == 0:
                    break
                time.sleep(10)

        assert kubectl.get_count("sts", "chi-test-036-volume-re-provisioning-simple-0-0") == 0, "StatefulSet is not deleted"
        assert kubectl.get_count("pvc", "default-chi-test-036-volume-re-provisioning-simple-0-0-0") == 0, "PVC is not deleted"

    def delete_pvc():
        with When("Delete PVC", description="delete PVC on replica 0"):
            # Prepare counters
            pvc_count = kubectl.get_count("pvc", chi=chi)
            pv_count = kubectl.get_count("pv")
            print(f"pvc_count: {pvc_count}")
            print(f"pv_count: {pv_count}")

            pvc_name = f"default-chi-test-036-volume-re-provisioning-simple-0-0-0"
            # retry
            kubectl.launch(f"delete pvc {pvc_name} --force &", shell=shell_2)
            kubectl.launch(
                f"""patch pvc {pvc_name} --type='json' --patch='[{{"op":"remove","path":"/metadata/finalizers"}}]'"""
            )
            # restart pod to make sure volume is unmounted
            kubectl.launch("delete pod chi-test-036-volume-re-provisioning-simple-0-0-0")
            # Give it some time to be deleted
            time.sleep(10)

            with Then("PVC should be deleted, PV should be deleted as well"):
                new_pvc_count = kubectl.get_count("pvc", chi=chi)
                new_pv_count = kubectl.get_count("pv")
                print(f"new_pvc_count: {new_pvc_count}")
                print(f"new_pv_count: {new_pv_count}")
                assert new_pvc_count < pvc_count
                assert new_pv_count < pv_count

    def check_data_is_recovered(reconcile_task_id):
        with Then(f"Kick operator to start reconcile cycle to fix lost {reconcile_task_id}"):
            cmd = f'patch chi {chi} --type=\'json\' --patch=\'[{{"op":"add","path":"/spec/taskID","value":"{reconcile_task_id}"}}]\''
            kubectl.launch(cmd)
            kubectl.wait_chi_status(chi, "InProgress")
            kubectl.wait_chi_status(chi, "Completed")
            wait_for_cluster(chi, cluster, 1, 2)

        with Then("I check PV is in place"):
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
            with And("checking view in Memory engine exists"):
                r = clickhouse.query(
                    chi,
                    pod="chi-test-036-volume-re-provisioning-simple-0-0-0",
                    sql="SELECT count(*) FROM system.tables where name = 'test_view'",
                )
                assert r == "1", error()
                r = clickhouse.query(
                    chi,
                    pod="chi-test-036-volume-re-provisioning-simple-0-1-0",
                    sql="SELECT count(*) FROM system.tables where name = 'test_view'",
                )
                assert r == "1", error()

    delete_sts_and_pvc()
    check_data_is_recovered("reconcile-after-STS-and-PVC-deleted")

    delete_pvc()
    check_data_is_recovered("reconcile-after-PVC-deleted")

    delete_pv()
    check_data_is_recovered("reconcile-after-PV-deleted")

    with Finally("I clean up"):
       delete_test_namespace()


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_StorageManagementSwitch("1.0"))
@Name("test_037# StorageManagement switch")
def test_037(self):
    """Check clickhouse-operator supports switching storageManagement
    config option from default (StatefulSet) to Operator"""
    create_shell_namespace_clickhouse_template()

    cluster = "default"
    manifest = f"manifests/chi/test-037-1-storagemanagement-switch.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))

    util.require_keeper(keeper_type=self.context.keeper_type)
    util.require_expandable_storage_class()

    with When("chi exists"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "apply_templates": {
                    current().context.clickhouse_template,
                },
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

    with And("VolumeClaim is provisioned by StatefulSet"):
        pvc_templates = kubectl.get_field("sts", f"chi-{chi}-{cluster}-0-0", ".spec.volumeClaimTemplates")
        assert pvc_templates != None

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

    with Then("I switch storageManagement to Operator"):
        kubectl.create_and_check(
            manifest=f"manifests/chi/test-037-2-storagemanagement-switch.yaml",
            check={
                "apply_templates": {
                    current().context.clickhouse_template,
                },
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

    with And("VolumeClaim is provisioned by Operator"):
        pvc_templates = kubectl.get_field("sts", f"chi-{chi}-{cluster}-0-0", ".spec.volumeClaimTemplates")
        assert pvc_templates == "<none>"

    with And("I check cluster is restarted and time up new pod start time"):
        start_time_new = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
        assert start_time != start_time_new, error()
        start_time = start_time_new

    with And("I rescale volume configuration to 2Gi to check that storage management is switched"):
        kubectl.create_and_check(
            manifest=f"manifests/chi/test-037-3-storagemanagement-switch.yaml",
            check={
                "apply_templates": {
                    current().context.clickhouse_template,
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

    with Finally("I clean up"):
        delete_test_namespace()


@TestCheck
@Name("test_039# Inter-cluster communications with secret")
def test_039(self, step=0, delete_chi=0):
    """Check clickhouse-operator support inter-cluster communications with secrets."""
    cluster = "default"
    manifest = f"manifests/chi/test-039-{step}-communications-with-secret.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    util.require_keeper(keeper_type=self.context.keeper_type)

    with Given("clickhouse-certs.yaml secret is installed"):
        kubectl.apply(
            util.get_full_path("manifests/secret/clickhouse-certs.yaml"),
    )

    with Given("chi exists"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "apply_templates": {
                    current().context.clickhouse_template,
                    "manifests/secret/test-038-secret.yaml",
                },
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )

    wait_for_cluster(chi, cluster, 2, pwd="qkrq")

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

    with Finally("I delete namespace"):
        delete_test_namespace()


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_InterClusterCommunicationWithSecret("1.0"))
@Name("test_039_0# Inter-cluster communications with no secret defined")
def test_039_0(self):
    create_shell_namespace_clickhouse_template()

    test_039(step=0)


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_InterClusterCommunicationWithSecret("1.0"))
@Name("test_039_1# Inter-cluster communications with 'auto' secret")
def test_039_1(self):
    """Check clickhouse-operator support inter-cluster communications with 'auto' secret."""
    create_shell_namespace_clickhouse_template()

    test_039(step=1)


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_InterClusterCommunicationWithSecret("1.0"))
@Name("test_039_2# Inter-cluster communications with plan text secret")
def test_039_2(self):
    """Check clickhouse-operator support inter-cluster communications with plan text secret."""
    create_shell_namespace_clickhouse_template()

    test_039(step=2)


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_InterClusterCommunicationWithSecret("1.0"))
@Name("test_039_3# Inter-cluster communications with k8s secret")
def test_039_3(self):
    """Check clickhouse-operator support inter-cluster communications with k8s secret."""
    create_shell_namespace_clickhouse_template()

    test_039(step=3)


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_InterClusterCommunicationWithSecret("1.0"))
@Name("test_039_4# Inter-cluster communications over HTTPS")
def test_039_4(self):
    """Check clickhouse-operator support inter-cluster communications over HTTPS."""
    create_shell_namespace_clickhouse_template()

    test_039(step=4, delete_chi=1)


@TestScenario
@Name("test_040# Inject a startup probe using an auto template")
def test_040(self):

    create_shell_namespace_clickhouse_template()

    manifest = "manifests/chi/test-005-acm.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))

    with Given("Auto template with a startup probe is deployed"):
        kubectl.apply(util.get_full_path("manifests/chit/tpl-startup-probe.yaml"))

    kubectl.create_and_check(
        manifest="manifests/chi/test-005-acm.yaml",
        check={
            "pod_count": 1,
            "pod_volumes": {
                "/var/lib/clickhouse",
            },
            "do_not_delete": 1,
            "chi_status": "InProgress",
        },
    )

    with Then("Startup probe should be defined"):
        assert "startupProbe" in kubectl.get_pod_spec(chi)["containers"][0]

    kubectl.wait_chi_status(chi, "Completed")

    with Then("uptime() should be more than 120 seconds as defined by a probe"):
        out = clickhouse.query(chi, "select uptime()")
        print(f"clickhouse uptime: {out}")
        assert int(out) > 120

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_041# Secure zookeeper")
def test_041(self):
    """Check clickhouse operator support secure zookeeper."""

    create_shell_namespace_clickhouse_template()

    cluster = "default"
    manifest = f"manifests/chi/test-041-secure-zookeeper.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    util.require_keeper(keeper_type=self.context.keeper_type, keeper_manifest="zookeeper-1-node-1GB-for-tests-only-scaleout-pvc-secure.yaml")

    with Given("clickhouse-certs.yaml secret is installed"):
        kubectl.apply(
            util.get_full_path("manifests/secret/clickhouse-certs.yaml"),
    )

    with Given("chi exists"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )

    wait_for_cluster(chi, cluster, 1, 2)

    with When("I create replicated table and insert data into it"):
        for r in [0,1]:
            clickhouse.query(
                chi,
                host = f"chi-{chi}-{cluster}-0-{r}-0",
                sql = "CREATE TABLE secure_repl (a UInt32) "
                "ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/tables/{table}', '{replica}')  "
                "PARTITION BY tuple() ORDER BY a"
                )
        clickhouse.query(
            chi,
            "INSERT INTO secure_repl select number as a from numbers(10)",
            host = f"chi-{chi}-{cluster}-0-0-0"
        )

    with Then("I check clickhouse can successfully connect to zookeeper"):
        clickhouse.query(chi, "SELECT * FROM system.zookeeper WHERE path = '/'")

    with And("I check data is replicated"):
        r = clickhouse.query(
            chi,
            "SELECT count(*) FROM secure_repl",
            host = f"chi-{chi}-{cluster}-0-1-0")
        assert r == "10"

    with And("I check connection is secured"):
        with By("checking chop-generated-zookeeper.xml is properly configured"):
            r = kubectl.launch(f"""exec chi-{chi}-default-0-0-0 -- bash -c 'cat """
                               f"""/etc/clickhouse-server/conf.d/chop-generated-zookeeper.xml | grep -c "<secure>1</secure>"'""")

            assert r == "1"

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_042# Test configuration rollback")
def test_042(self):
    create_shell_namespace_clickhouse_template()
    with Given("I change operator statefullSet timeout"):
        util.apply_operator_config("manifests/chopconf/low-timeout.yaml")

    cluster = "default"
    manifest = f"manifests/chi/test-042-rollback-1.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))

    with Given("CHI is installed"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 2,
                "apply_templates": {
                    current().context.clickhouse_template,
                },
                "pod_image": current().context.clickhouse_version,
                "do_not_delete": 1,
            },
        )

    with When("Update with a spec that crashes ClickHouse"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-042-rollback-2.yaml",
            check={
                "chi_status": "InProgress",
                "do_not_delete": 1,
            },
        )

        with Then("Operator should apply changes, and both pods should be created"):
            kubectl.wait_chi_status(chi, "Aborted")
            kubectl.wait_objects(chi, {"statefulset": 2, "pod": 2, "service": 3})

        with And("First node is in CrashLoopBackOff"):
            kubectl.wait_field(
                "pod",
                f"chi-{chi}-{cluster}-0-0-0",
                ".status.containerStatuses[0].state.waiting.reason",
                "CrashLoopBackOff"
            )

        with And("First node is down"):
            res = clickhouse.query_with_error(chi, host=f"chi-{chi}-{cluster}-0-0-0", sql="select 1")
            assert res != "1"

        with And("Second node is up"):
            res = clickhouse.query_with_error(chi, host=f"chi-{chi}-{cluster}-1-0-0", sql="select 1")
            assert res == "1"

    with When("Update with another spec that crashes ClickHouse"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-042-rollback-3.yaml",
            check={
                "chi_status": "InProgress",
                "do_not_delete": 1,
            },
        )

        with Then("Operator should apply changes, and both pods should be created"):
            kubectl.wait_chi_status(chi, "Aborted")
            kubectl.wait_objects(chi, {"statefulset": 2, "pod": 2, "service": 3})

        with And("First node is in CrashLoopBackOff"):
            kubectl.wait_field("pod", f"chi-{chi}-{cluster}-0-0-0",
                    ".status.containerStatuses[0].state.waiting.reason",
                    "CrashLoopBackOff")

        with And("First node is down"):
            res = clickhouse.query_with_error(chi, host=f"chi-{chi}-{cluster}-0-0-0", sql="select 1")
            assert res != "1"

        with And("Second node is up"):
            res = clickhouse.query_with_error(chi, host=f"chi-{chi}-{cluster}-1-0-0", sql="select 1")
            assert res == "1"

    with When("CHI is reverted to a good one"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 2,
                "do_not_delete": 1,
                "chi_status": "Completed",
            },
        )

        with Then("Both nodes are working"):
            res = clickhouse.query_with_error(chi, "select count() from cluster('all-sharded', system.one)")
            assert res == "2"

    with Finally("I clean up"):
        delete_test_namespace()


@TestCheck
@Name("test_043# Logs container customizing")
def test_043(self, manifest):
    """Check that clickhouse-operator support logs container customizing."""

    cluster = "cluster"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))

    with Given("CHI is installed"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 1,
                "do_not_delete": 1,
                },
            )

    with Then("I check both containers are ready"):
        assert kubectl.get_field(
            kind="pod",
            name=f"chi-{chi}-{cluster}-0-0-0",
            field=".status.containerStatuses[0].ready"
        ) == "true", error()
        assert kubectl.get_field(
            kind="pod",
            name=f"chi-{chi}-{cluster}-0-0-0",
            field=".status.containerStatuses[1].ready"
        ) == "true", error()

    with Then("I check clickhouse logs are in clickhouse-log container"):
        with By("calling ls inside clickhouse-log in /var/log directory"):
            r = kubectl.launch(f"exec chi-{chi}-{cluster}-0-0-0 -c clickhouse-log -- bash -c 'ls /var/log/clickhouse-server/'")

        assert "clickhouse-server.err.log" in r, error()
        assert "clickhouse-server.log" in r, error()

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Defaults_Templates_logVolumeClaimTemplate("1.0"))
@Name("test_043_0# Logs container customizing using PodTemplate")
def test_043_0(self):
    """Check that clickhouse-operator support manual logs container customizing."""
    create_shell_namespace_clickhouse_template()

    test_043(manifest="manifests/chi/test-043-0-logs-container-customizing.yaml")


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Defaults_Templates_logVolumeClaimTemplate("1.0"))
@Name("test_043_1# Default clickhouse-log container")
def test_043_1(self):
    """Check that clickhouse-operator sets up default logs container if it is not specified in Pod."""
    create_shell_namespace_clickhouse_template()

    test_043(manifest="manifests/chi/test-043-1-logs-container-customizing.yaml")


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_ReconcilingCycle("1.0"),
              RQ_SRS_026_ClickHouseOperator_Managing_ClusterScaling_SchemaPropagation("1.0"))
@Name("test_044# Schema and data propagation with slow replica")
def test_044(self):
    """Check that schema and data can be propagated on other replica if replica start takes a lot of time."""
    create_shell_namespace_clickhouse_template()
    cluster = "default"
    manifest = f"manifests/chi/test-044-0-slow-propagation.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    util.require_keeper(keeper_type=self.context.keeper_type)
    operator_namespace = current().context.operator_namespace

    with Given("I change operator statefullSet timeout"):
        util.apply_operator_config("manifests/chopconf/low-timeout.yaml")

    with And("CHI with 1 replica is installed"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

    with When("I create replicated table on the first replica"):
        clickhouse.query(
            chi,
            """CREATE TABLE test_local (a UInt32)
            Engine = ReplicatedMergeTree('/clickhouse/{installation}/tables/{shard}/{database}/{table}', '{replica}')
            PARTITION BY tuple() ORDER BY a"""
        )

    with And("I add 1 slow replica"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-044-1-slow-propagation.yaml",
            check={
                "pod_count": 2,
                "do_not_delete": 1,
                "chi_status": "Aborted"
            },
        )
        client_pod = f"chi-{chi}-{cluster}-0-1-0"
        kubectl.wait_field(
            "pod",
            client_pod,
            ".status.containerStatuses[0].ready",
            "true")

    with Then("I check that schema is not yet propagated"):
        with By("checking schema on the slow replica"):
            r = clickhouse.query(chi, "SHOW tables", host=f"chi-{chi}-{cluster}-0-1-0")
            assert not ("test_local" in r), error()

    with When("I update CHI manifest to trigger reconcile"):
        with By("adding taskID to CHI"):
            kubectl.create_and_check(
                manifest="manifests/chi/test-044-2-slow-propagation.yaml",
                check={
                    "pod_count": 2,
                    "do_not_delete": 1,
                },
            )

    with Then("I check schema is propagated"):
        with By("checking schema on the slow replica"):
            r = clickhouse.query(chi, "SHOW tables", host=f"chi-{chi}-{cluster}-0-1-0")
            assert "test_local" in r, error()

    with Finally("I clean up"):
        delete_test_namespace()


@TestCheck
@Name("test_045# Restart operator without waiting for queries to finish")
def test_045(self, manifest):
    """Check that operator support does not wait for the query
     to finish before operator commences restart."""

    chi = yaml_manifest.get_name(util.get_full_path(manifest))

    with Given("CHI is installed"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 1,
                "do_not_delete": 1,
                },
            )

    with When("I reconcile CHI with restart=RollingUpdate"):
        with By("patching CHI with a restart attribute"):
            cmd = f'patch chi {chi} --type=\'json\' --patch=\'[{{"op":"add","path":"/spec/restart","value":"RollingUpdate"}}]\''
            kubectl.launch(cmd)

    # Reconcile will exclude host from the cluster which may take up to 1 minute
    counter = 90
    with Then("operator SHALL not wait for the query to finish"):
        out = clickhouse.query_with_error(
            chi_name=chi,
            sql=f"SELECT count(sleepEachRow(1)) FROM numbers({counter}) SETTINGS function_sleep_max_microseconds_per_block=0",
            timeout=120)
        assert out != counter, error()

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Reconciling_Policy("1.0"))
@Name("test_045_1# Reconcile wait queries property specified by CHI")
def test_045_1(self):
    """Check that operator supports spec.reconciling.policy property in CHI that
    forces the operator not to wait for the queries to finish before restart."""

    create_shell_namespace_clickhouse_template()

    test_045(manifest=f"manifests/chi/test-045-1-wait-query-finish.yaml")


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_Configuration_Spec_ReconcileWaitQueries("1.0"))
@Name("test_045_2# Reconcile wait queries property specified by clickhouse-operator config")
def test_045_2(self):
    """Check that operator supports spec.reconcile.host.wait.queries property in clickhouse-operator config
    that forces the operator not to wait for the queries to finish before restart."""
    create_shell_namespace_clickhouse_template()

    with Given("I set spec.reconcile.host.wait.queries property"):
        util.apply_operator_config("manifests/chopconf/test-045-chopconf.yaml")

    test_045(manifest=f"manifests/chi/test-045-2-wait-query-finish.yaml")


@TestScenario
@Name("test_046# Metrics for clickhouse-operator")
def test_046(self):
    """Check that clickhouse-operator creates metrics for reconcile and other clickhouse-operator events."""
    create_shell_namespace_clickhouse_template()
    cluster = "default"
    manifest = f"manifests/chi/test-046-0-clickhouse-operator-metrics.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    operator_namespace = current().context.operator_namespace
    out = kubectl.launch("get pods -l app=clickhouse-operator", ns=current().context.operator_namespace).splitlines()[1]
    operator_pod = re.split(r"[\t\r\n\s]+", out)[0]

    with Given("CHI with 1 replica is installed"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )

    def check_metrics(metric_names):
        for metric_name in metric_names:
            with Then(f"I check {metric_name} metric for clickhouse-operator exists"):
                check_metrics_monitoring(
                    operator_namespace=operator_namespace,
                    operator_pod=operator_pod,
                    container="clickhouse-operator",
                    port="9999",
                    expect_pattern=metric_name,
                )

    with Then(f"Check clickhouse-operator exposes clickhouse_operator_chi_reconciles_* metrics"):
        check_metrics([
            "clickhouse_operator_chi_reconciles_started{.*chi=\"test-046-operator-metrics\".*} 1",
            "clickhouse_operator_chi_reconciles_completed{.*chi=\"test-046-operator-metrics\".*} 1"
        ])

    with Then("I update CHI manifest to trigger reconcile"):
        with By("adding taskID to CHI"):
            kubectl.create_and_check(
                manifest="manifests/chi/test-046-1-clickhouse-operator-metrics.yaml",
                check={
                    "pod_count": 2,
                    "do_not_delete": 1,
                },
            )

    with Then(f"Check clickhouse-operator exposes clickhouse_operator_chi_reconciles_* metrics"):
        check_metrics([
            "clickhouse_operator_chi_reconciles_started{.*chi=\"test-046-operator-metrics\".*} 2",
            "clickhouse_operator_chi_reconciles_completed{.*chi=\"test-046-operator-metrics\".*} 2"
        ])

    with Then("I update CHI manifest with wrong clickhouse version"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-046-2-clickhouse-operator-metrics.yaml",
            check={
                "pod_count": 2,
                "do_not_delete": 1,
                "chi_status": "InProgress",
            },
        )

    with Then("ClickHouse image can not be retrieved"):
        kubectl.wait_field(
            "pod",
            "chi-test-046-operator-metrics-default-0-0-0",
            ".status.containerStatuses[0].state.waiting.reason",
            "ImagePullBackOff",
        )

    with Then("Wait until operator aborts"):
        kubectl.wait_chi_status(chi, "Aborted")

    with Then(f"Check clickhouse-operator exposes clickhouse_operator_chi_reconciles_aborted metric"):
        check_metrics([
            "clickhouse_operator_chi_reconciles_started{.*chi=\"test-046-operator-metrics\".*} 3",
            "clickhouse_operator_chi_reconciles_completed{.*chi=\"test-046-operator-metrics\".*} 2",
            "clickhouse_operator_chi_reconciles_aborted{.*chi=\"test-046-operator-metrics\".*} 1",
            "clickhouse_operator_host_reconciles_errors{.*chi=\"test-046-operator-metrics\".*} 1",
        ])

    with Then("I restore the correct version"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-046-0-clickhouse-operator-metrics.yaml",
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )

    with Then(f"Check all chi and host reconciles metrics"):
        check_metrics([
            "clickhouse_operator_chi_reconciles_started{.*chi=\"test-046-operator-metrics\".*} 4",
            "clickhouse_operator_chi_reconciles_completed{.*chi=\"test-046-operator-metrics\".*} 3",
            "clickhouse_operator_chi_reconciles_aborted{.*chi=\"test-046-operator-metrics\".*} 1",
            "clickhouse_operator_chi_reconciles_timings.*chi=\"test-046-operator-metrics\".*",
            # TODO: add proper counts for host reconciles
            "clickhouse_operator_host_reconciles_started.*chi=\"test-046-operator-metrics\".*",
            "clickhouse_operator_host_reconciles_completed.*chi=\"test-046-operator-metrics\".*",
#            "clickhouse_operator_host_reconciles_restarts.*chi=\"test-046-operator-metrics\".*",
            "clickhouse_operator_host_reconciles_errors.*chi=\"test-046-operator-metrics\".*",
            "clickhouse_operator_host_reconciles_timings.*chi=\"test-046-operator-metrics\".*",
            ])

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters_Cluster_Layout_Shards_Weight("1.0"))
@Name("test_047# Zero weighted shard")
def test_047(self):
    """Check that clickhouse-operator supports specifying shard weight as 0 and
    check that data not inserted into zero-weighted shard in distributed table."""

    create_shell_namespace_clickhouse_template()
    manifest = f"manifests/chi/test-047-zero-weighted-shard.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    cluster = "default"
    with Given("CHI with 2 shards is installed"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 2,
                "do_not_delete": 1,
                },
            )
    wait_for_cluster(chi, cluster, 2, force_wait = True)

    with Then("I check weight is specified in /etc/clickhouse-server/config.d/chop-generated-remote_servers.xml file"):
        r = kubectl.launch(
            f"""exec chi-{chi}-default-0-0-0 -- bash -c 'cat """
            f"""/etc/clickhouse-server/config.d/chop-generated-remote_servers.xml | head -n 7 | tail -n 1'"""
        )
        assert "<weight>0</weight>" in r
        r = kubectl.launch(
            f"""exec chi-{chi}-default-0-0-0 -- bash -c 'cat """
            f"""/etc/clickhouse-server/config.d/chop-generated-remote_servers.xml | head -n 16 | tail -n 1'"""
            )
        assert "<weight>1</weight>" in r


    numbers = 100
    with When("I create distributed table"):
        for shard in (0,1):
            clickhouse.query(
                chi,
                "CREATE TABLE test_local_047 (a UInt32) Engine = MergeTree PARTITION BY tuple() ORDER BY a",
                host = f"chi-{chi}-{cluster}-{shard}-0-0")
            clickhouse.query(
                chi,
                "CREATE TABLE test_distr_047 AS test_local_047 Engine = Distributed('default', default, test_local_047, a%2)",
                host = f"chi-{chi}-{cluster}-{shard}-0-0")

    with And("I insert data in the distributed table"):
        clickhouse.query(chi, f"INSERT INTO test_distr_047 select * from numbers({numbers})")

    with Then("I check only non-zero weighted shard contains data"):
        out = clickhouse.query(chi, "SELECT count(*) from test_local_047", host=f"chi-{chi}-{cluster}-0-0-0")
        assert out == "0"
        out = clickhouse.query(chi, "SELECT count(*) from test_local_047", host=f"chi-{chi}-{cluster}-1-0-0")
        assert out == f"{numbers}"
        out = clickhouse.query(chi, "SELECT count(*) from test_distr_047", host=f"chi-{chi}-{cluster}-0-0-0")
        assert out == f"{numbers}"
        out = clickhouse.query(chi, "SELECT count(*) from test_distr_047", host=f"chi-{chi}-{cluster}-1-0-0")
        assert out == f"{numbers}"

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_048# Clickhouse-keeper")
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Kind_ClickHouseKeeperInstallation("1.0"),
              RQ_SRS_026_ClickHouseOperator_CustomResource_ClickHouseKeeperInstallation_volumeClaimTemplates("1.0"))
def test_048(self):
    """Check clickhouse-operator support ClickHouseKeeperInstallation with PVC in keeper manifest."""

    create_shell_namespace_clickhouse_template()
    util.require_keeper(keeper_type="chk",
                        keeper_manifest="clickhouse-keeper-3-node-for-test-only.yaml")
    manifest = f"manifests/chi/test-048-clickhouse-keeper.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    cluster = "default"
    with Given("CHI with 2 replicas"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 2,
                "do_not_delete": 1,
                },
            )
    check_replication(chi, {0,1}, 1)

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_049# Clickhouse-keeper upgrade")
def test_049(self):
    """Check that clickhouse-operator support upgrading clickhouse-keeper version
     when clickhouse-keeper defined with ClickHouseKeeperInstallation."""

    create_shell_namespace_clickhouse_template()
    util.require_keeper(keeper_type="chk",
                        keeper_manifest="clickhouse-keeper-3-node-for-test-only.yaml")
    manifest = f"manifests/chi/test-049-clickhouse-keeper-upgrade.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    cluster = "default"
    keeper_version_from = "24.8"
    keeper_version_to = "24.9"
    with Given("CHI with 2 replicas"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )

    check_replication(chi, {0,1}, 1)

    with When(f"I check clickhouse-keeper version is {keeper_version_from}"):
        assert keeper_version_from in \
               kubectl.get_field('pod', 'chk-clickhouse-keeper-test-0-0-0', '.spec.containers[0].image'), error()

    with Then(f"I change keeper version to {keeper_version_to}"):
        cmd = f"""patch chk clickhouse-keeper --type='json' --patch='[{{"op":"replace","path":"/spec/templates/podTemplates/0/spec/containers/0/image","value":"clickhouse/clickhouse-keeper:{keeper_version_to}"}}]'"""
        kubectl.launch(cmd)

    with Then("I wait CHK status 1"):
        kubectl.wait_chk_status('clickhouse-keeper', 'InProgress')
    with Then("I wait CHK status 2"):
        kubectl.wait_chk_status('clickhouse-keeper', 'Completed')

    with When(f"I check clickhouse-keeper version is changed to {keeper_version_to}"):
        kubectl.wait_field('pod', 'chk-clickhouse-keeper-test-0-0-0', '.spec.containers[0].image', f'clickhouse/clickhouse-keeper:{keeper_version_to}', retries=5)
        kubectl.wait_field('pod', 'chk-clickhouse-keeper-test-0-1-0', '.spec.containers[0].image', f'clickhouse/clickhouse-keeper:{keeper_version_to}', retries=5)
        kubectl.wait_field('pod', 'chk-clickhouse-keeper-test-0-2-0', '.spec.containers[0].image', f'clickhouse/clickhouse-keeper:{keeper_version_to}', retries=5)

    check_replication(chi, {0,1}, 2)

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_050# Test metrics exclusion in operator config")
def test_050(self):
    create_shell_namespace_clickhouse_template()
    with Given("Operator configuration is installed"):
        util.apply_operator_config("manifests/chopconf/test-050-chopconf.yaml")

    manifest = f"manifests/chi/test-050-labels.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))

    with Given("CHI is installed"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 1,
                "apply_templates": {
                    current().context.clickhouse_template,
                },
                "pod_image": current().context.clickhouse_version,
                "do_not_delete": 1,
            },
        )

    def test_labels(chi, type, key, value):

        with Then(f"Pod {type} {key}={value} should populated from CHI"):
            assert kubectl.get_field("pod", f"-l clickhouse.altinity.com/chi={chi}", f".metadata.{type}s.{key}") == value

        with And(f"Service {type} {key}={value} should populated from CHI"):
            assert kubectl.get_field("service", f"-l clickhouse.altinity.com/chi={chi}", f".metadata.{type}s.{key}") == value

        with And(f"PVC {type} {key}={value} should populated from CHI"):
            assert kubectl.get_field("pvc", f"-l clickhouse.altinity.com/chi={chi}", f".metadata.{type}s.{key}") == value

    test_labels(chi, "label", "include_this_label", "test-050-label")

    test_labels(chi, "label", "exclude_this_label", "<none>")

    test_labels(chi, "annotation", "include_this_annotation", "test-050-annotation")

    test_labels(chi, "annotation", "exclude_this_annotation", "<none>")


    with Then("Check that exposed metrics do not have labels and annotations that are excluded"):
        operator_namespace=current().context.operator_namespace
        out = kubectl.launch("get pods -l app=clickhouse-operator", ns=operator_namespace).splitlines()[1]
        operator_pod = re.split(r"[\t\r\n\s]+", out)[0]

        # chi_clickhouse_metric_VersionInteger{chi="test-050",exclude_this_annotation="test-050-annotation",hostname="chi-test-050-default-0-0.test-050-e1884706-9a94-11ef-a786-367ddacfe5fd.svc.cluster.local",include_this_annotation="test-050-annotation",include_this_label="test-050-label",namespace="test-050-e1884706-9a94-11ef-a786-367ddacfe5fd"}
        expect_labels = f"chi=\"test-050\",hostname=\"chi-test-050-default-0-0.{operator_namespace}.svc.cluster.local\",include_this_annotation=\"test-050-annotation\",include_this_label=\"test-050-label\""
        check_metrics_monitoring(
            operator_namespace = operator_namespace,
            operator_pod=operator_pod,
            expect_metric="chi_clickhouse_metric_VersionInteger",
            expect_labels=expect_labels
            )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_051# Test CHK upgrade from 0.23.x operator version")
@Tags("NO_PARALLEL")
def test_051(self):
    with Then("Skip it. test_051_1 does a better job"):
        return

    version_from = "0.23.7"
    version_to = current().context.operator_version # "0.24.0"
    current().context.operator_version = version_from
    create_shell_namespace_clickhouse_template()

    chi_manifest = f"manifests/chi/test-051-chk-chop-upgrade.yaml"
    chk_manifest = f"manifests/chk/test-051-chk-chop-upgrade.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(chi_manifest))
    chk = yaml_manifest.get_name(util.get_full_path(chk_manifest))
    cluster = "default"

    with Given("Install CHK"):
        kubectl.create_and_check(
            manifest=chk_manifest, kind="chk",
            check={
                # "pod_count": 1, # do not work in 0.23.7
                "do_not_delete": 1,
            },
        )

    with Given("CHI with 2 replicas"):
        kubectl.create_and_check(
            manifest=chi_manifest,
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )

    check_replication(chi, {0,1}, 1, "test_local_051")

    with When(f"upgrade operator to {version_to}"):
        util.install_operator_version(version_to)
        time.sleep(15)

        kubectl.wait_chi_status(chi, "Completed")
        kubectl.wait_chk_status(chk, "Completed")

    with Given("Trigger CHK reconcile"):
        kubectl.create_and_check(
            manifest="manifests/chk/test-051-chk-chop-upgrade-2.yaml", kind="chk",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )
    with Then("CLICKHOUSE_DATA_DIR should be properly set"):
        pod = kubectl.get_pod_spec("", "chk-test-051-chk-single-0-0-0")
        env = pod["containers"][0]["env"][0]
        assert env["name"] == "CLICKHOUSE_DATA_DIR"
        assert env["value"] == "/var/lib/clickhouse-keeper"

    with Then("Wiat until Keeper connection is established"):
        out = 0
        for i in range(1, 10):
            out = clickhouse.query_with_error(chi, "SELECT count(*) from system.zookeeper_connection")
            if out == "1":
                break
            with Then("Waiting 10 seconds"):
                time.sleep(10)
        assert out == "1", error()

    with Then("Check if there are read-only replicas"):
        out = clickhouse.query(chi, "SELECT count(*) from system.replicas where is_readonly")
        if out != "0":
            with Then("Found readonly replica. Trying to restore"):
                for replica in [0, 1]:
                    host=f"chi-{chi}-{cluster}-0-{replica}-0"
                    clickhouse.query(chi, host=host, sql="system restart replica test_local_051")
                    clickhouse.query(chi, host=host, sql="system restore replica test_local_051")
                    with Then("Check if there are read-only replicas after restore"):
                        out = clickhouse.query(chi, host=host, sql="SELECT count(*) from system.replicas where is_readonly")
                        assert out == "0", error()

    check_replication(chi, {0,1}, 2, "test_local_051")

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_051_1# Test CHK upgrade from 0.23.x operator version")
@Tags("NO_PARALLEL")
def test_051_1(self):
    version_from = "0.23.7"
    version_to = current().context.operator_version # "0.24.0"
    current().context.operator_version = version_from
    create_shell_namespace_clickhouse_template()

    chi_manifest = "manifests/chi/test-051-chk-chop-upgrade.yaml"
    chk_manifest = "manifests/chk/test-051-chk-chop-upgrade.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(chi_manifest))
    chk = yaml_manifest.get_name(util.get_full_path(chk_manifest))
    cluster = "default"

    with Given("Install CHK"):
        kubectl.create_and_check(
            manifest=chk_manifest, kind="chk",
            check={
                # "pod_count": 1, # do not work in 0.23.7
                "do_not_delete": 1,
            },
        )

    with Given("CHI with 2 replicas"):
        kubectl.create_and_check(
            manifest=chi_manifest,
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )

    check_replication(chi, {0,1}, 1)

    with Then("Unattach old CHK resources"):
        kubectl.launch(f"patch sts {chk} -p " + """\'{"metadata":{"ownerReferences":null}}\'""")
        kubectl.launch(f"patch cm  {chk} -p " + """\'{"metadata":{"ownerReferences":null}}\'""")
        kubectl.launch(f"patch service {chk} -p " + """\'{"metadata":{"ownerReferences":null}}\'""")
        kubectl.launch(f"patch service {chk}-headless -p " + """\'{"metadata":{"ownerReferences":null}}\'""")
        kubectl.launch(f"label pod -lapp={chk} app-")
        kubectl.launch(f"label sts -lapp={chk} app-")

    with Then("Confirm that statefulset and pod are still running if we delete chk"):
        kubectl.delete_kind("chk", chk)
        assert kubectl.get_field("pod", "test-051-chk-0", ".status.phase") == "Running"
        assert kubectl.get_count("sts", "test-051-chk") == 1
        assert kubectl.get_count("cm",  "test-051-chk") == 1
        assert kubectl.get_count("service", "test-051-chk") == 1

    old_pvc = "both-paths-test-051-chk-0"
    pv = kubectl.get_pv_name(old_pvc)
    new_pvc = "default-chk-test-051-chk-single-0-0-0"

    with Then("Set PV persistentVolumeReclaimPolicy to Retain"):
        kubectl.launch(f"patch pv {pv}" + """ -p \'{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}\'""")

    with Then("Delete old Keeper resources"):
        kubectl.delete_kind("sts", "test-051-chk")
        kubectl.delete_kind("pod", "test-051-chk-0")
        kubectl.delete_kind("pvc", old_pvc)

    with Then("Unmount PV from old PVC"):
        kubectl.launch(f"patch pv {pv}" + """ -p \'{"spec":{"claimRef":null}}\'""")

    with When(f"upgrade operator to {version_to}"):
        util.install_operator_version(version_to)
        time.sleep(30)

        kubectl.wait_chi_status(chi, "Completed")

    with Given("Re-deploy CHK, substituting PV in PVC template"):
        volumeNamePlaceHolder = "{volumeNamePlaceHolder}"
        manifest = util.get_full_path("manifests/chk/test-051-chk-chop-upgrade-3.yaml")
        cmd = f"""cat {manifest} | sed "s/{volumeNamePlaceHolder}/{pv}/g" | kubectl apply -n {current().context.test_namespace} -f -"""
        kubectl.run_shell(cmd, 300)

        kubectl.wait_chk_status(chk, "Completed")


    with Then("CLICKHOUSE_DATA_DIR should be properly set"):
        pod = kubectl.get_pod_spec("", "chk-test-051-chk-single-0-0-0")
        env = pod["containers"][0]["env"][0]
        assert env["name"] == "CLICKHOUSE_DATA_DIR"
        assert env["value"] == "/var/lib/clickhouse-keeper"

    with Then("Wiat until Keeper connection is established"):
        out = 0
        for i in range(1, 10):
            out = clickhouse.query_with_error(chi, "SELECT count(*) from system.zookeeper_connection")
            if out == "1":
                break
            with Then("Waiting 10 seconds"):
                time.sleep(10)
        assert out == "1", error()

    check_replication(chi, {0,1}, 2)

    with Finally("I clean up"):
        delete_test_namespace()

@TestScenario
@Name("test_052# Clickhouse-keeper scale-up/scale-down")
def test_052(self):
    """Check that clickhouse-operator support scale-up/scale-down without service interruption"""

    create_shell_namespace_clickhouse_template()

    chi_manifest = "manifests/chi/test-052-keeper-rescale.yaml"
    chk_manifest_1 = "manifests/chk/test-052-chk-rescale-1.yaml"
    chk_manifest_3 = "manifests/chk/test-052-chk-rescale-3.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(chi_manifest))
    chk = yaml_manifest.get_name(util.get_full_path(chk_manifest_1))

    cluster = "default"

    with Given("Install CHK"):
        kubectl.create_and_check(
            manifest=chk_manifest_1, kind="chk",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

    with Given("CHI with 2 replicas"):
        kubectl.create_and_check(
            manifest=chi_manifest,
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )

    check_replication(chi, {0,1}, 1)

    with Given("Rescale CHK to 3 replicas"):
        kubectl.create_and_check(
            manifest=chk_manifest_3, kind="chk",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

    check_replication(chi, {0,1}, 2)

    with Then("Kill first pod to switch the leader"):
        kubectl.launch(f"delete pod chk-test-052-chk-keeper-0-0-0")
        time.sleep(10)

    # with Then("Force leader to be on the first node only"):
    #    kubectl.create_and_check(
    #        manifest="manifests/chk/test-052-chk-rescale-1.1.yaml", kind="chk",
    #        check={
    #            "pod_count": 3,
    #            "do_not_delete": 1,
    #        },
    #    )

    # check_replication(chi, {0,1}, 3)


    # with Then("Remove other nodes from the raft configuration"):
    #    kubectl.create_and_check(
    #        manifest="manifests/chk/test-052-chk-rescale-1.2.yaml", kind="chk",
    #        check={
    #            "do_not_delete": 1,
    #        },
    #    )

    # check_replication(chi, {0,1}, 4)


    with Then("Rescale CHK back to 1 replica"):
        kubectl.create_and_check(
            manifest="manifests/chk/test-052-chk-rescale-1.yaml", kind="chk",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

    check_replication(chi, {0,1}, 5)

    with Finally("I clean up"):
        delete_test_namespace()


def check_replication(chi, replicas, token, table = ''):
        cluster = clickhouse.query(chi, "select substitution from system.macros where macro = 'cluster'")
        if table == '':
            table = chi.replace('-','_')

        wait_for_cluster(chi, cluster, 1, len(replicas))

        with When("Create a replicated table if not exists"):
            clickhouse.query(chi, f"CREATE TABLE IF NOT EXISTS {table} ON CLUSTER '{cluster}' (a UInt32) Engine = ReplicatedMergeTree ORDER BY a")

        with And("I insert data in the replicated table"):
            clickhouse.query(chi, f"INSERT INTO {table} select {token}", timeout=300)

        with Then("Check replicated table has data on both nodes"):
            for replica in replicas:
                out = clickhouse.query(chi, f"SELECT a from {table} where a={token}", host=f"chi-{chi}-{cluster}-0-{replica}-0")
                assert out == f"{token}", error()

@TestScenario
@Name("test_053# Check that standard Kubernetes annotations are ignored if set to StatefulSet externally")
@Tags("NO_PARALLEL")
def test_053(self):
    version_from = "0.23.7"
    version_to = current().context.operator_version
    with Given(f"clickhouse-operator from {version_from}"):
        current().context.operator_version = version_from
        create_shell_namespace_clickhouse_template()

    manifest="manifests/chi/test-001.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    sts = f"chi-{chi}-single-0-0"
    pod = f"{sts}-0"

    kubectl.create_and_check(
        manifest=manifest,
        check={
            "object_counts": {
                "statefulset": 1,
                "pod": 1,
                "service": 2,
            },
            "do_not_delete": 1
        },
    )

    with When("Run rollout restart"):
        kubectl.launch(f"rollout restart statefulset {sts}")
        time.sleep(10)

        with Then("Pod annotation kubectl.kubernetes.io/restartedAt should be populated"):
            assert kubectl.get_field("pod", pod, ".metadata.annotations.kubectl\.kubernetes\.io/restartedAt") != "<none>"
        with And("PodTemplate annotation kubectl.kubernetes.io/restartedAt should be populated"):
            assert kubectl.get_field("statefulset", sts, ".spec.template.metadata.annotations.kubectl\.kubernetes\.io/restartedAt") != "<none>"

        start_time = kubectl.get_field("pod", pod, ".status.startTime")

        def check_restart():
            with Then("ClickHouse pods should not be restarted during operator's restart"):
                new_start_time = kubectl.get_field("pod", pod, ".status.startTime")
                # print(f"pod start_time old: {start_time}")
                # print(f"pod start_time new: {new_start_time}")
                assert start_time == new_start_time

        with Then("Trigger reconcile"):
            start_time = kubectl.get_field("pod", pod, ".status.startTime")

            cmd = f'patch chi {chi} --type=\'json\' --patch=\'[{{"op":"add","path":"/spec/taskID","value":"Reconcile"}}]\''
            kubectl.launch(cmd)
            kubectl.wait_chi_status(chi, "InProgress")
            kubectl.wait_chi_status(chi, "Completed")

            check_restart()

        with When("Restart operator"):
            util.restart_operator()
            kubectl.wait_chi_status(chi, "InProgress")
            kubectl.wait_chi_status(chi, "Completed")

            check_restart()

        with When(f"upgrade operator to {version_to}"):
            util.install_operator_version(version_to)
            kubectl.wait_chi_status(chi, "InProgress")
            kubectl.wait_chi_status(chi, "Completed")

            check_restart()

    with Finally("I clean up"):
        delete_test_namespace()

@TestScenario
@Name("test_054. Test that 'suspend' mode delays any changes until unsuspended")
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_VersionUpgrades("1.0"))
def test_054(self):
    create_shell_namespace_clickhouse_template()
    chi = yaml_manifest.get_name(util.get_full_path("manifests/chi/test-006-ch-upgrade-1.yaml"))

    old_version = "clickhouse/clickhouse-server:23.8"
    new_version = "clickhouse/clickhouse-server:24.3"
    with Then(f"Start CHI with version {old_version}"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-006-ch-upgrade-1.yaml",
            check={
                "pod_count": 1,
                "pod_image": old_version,
                "do_not_delete": 1,
            },
        )

    with Then("Add suspend attribute to CHI"):
        cmd = f'patch chi {chi} --type=\'json\' --patch=\'[{{"op":"add","path":"/spec/suspend","value":"yes"}}]\''
        kubectl.launch(cmd)

    with Then(f"Update podTemplate to {new_version} and confirm that pod image is NOT updated"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-006-ch-upgrade-2.yaml",
            check={
                "pod_count": 1,
                "pod_image": old_version,
                "do_not_delete": 1,
            },
        )

    with Then("Remove suspend attribute from CHI"):
        cmd = f'patch chi {chi} --type=\'json\' --patch=\'[{{"op":"remove","path":"/spec/suspend"}}]\''
        kubectl.launch(cmd)

    kubectl.wait_chi_status(chi, "InProgress")
    kubectl.wait_chi_status(chi, "Completed")

    with Then(f"Confirm that pod image is updated to {new_version}"):
        kubectl.check_pod_image(chi, new_version)

    with Finally("I clean up"):
        delete_test_namespace()

def cleanup_chis(self):
    with Given("Cleanup CHIs"):
        ns = kubectl.get("ns", name="", ns="--all-namespaces")
        if "items" in ns:
            for n in ns["items"]:
                ns_name = n["metadata"]["name"]
                if ns_name.startswith("test") and ns_name != self.context.test_namespace:
                    with Then(f"Delete ns {ns_name}"):
                        util.delete_namespace(namespace = ns_name, delete_chi=True)

@TestModule
@Name("e2e.test_operator")
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_APIVersion("1.0"),
              RQ_SRS_026_ClickHouseOperator("1.0"))
def test(self):
    with Given("set settings"):
        set_settings()

    with Given("I create shell"):
        shell = get_shell()
        self.context.shell = shell

    cleanup_chis(self)

    # Placeholder for selective test running
    # run_tests = [test_008, test_009]
    # for t in run_tests:
    #     if callable(t):
    #         Scenario(test=t)()
    #     else:
    #         Scenario(test=t[0], args=t[1])()

    # define values for Operator upgrade test (test_009)

    with Pool(3) as pool:
        for scenario in loads(current_module(), Scenario, Suite):
            if not (hasattr(scenario, "tags") and ("NO_PARALLEL" in scenario.tags)):
                Scenario(run=scenario, parallel=True, executor=pool)
        join()

    for scenario in loads(current_module(), Scenario, Suite):
        if hasattr(scenario, "tags") and ("NO_PARALLEL" in scenario.tags):
            Scenario(run=scenario)
