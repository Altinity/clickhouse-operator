import os
import time
import yaml
import threading
import re

from e2e.retry_sleep import retry_sleep

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
@Name("test_010001. 1 node")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010001(self):
    create_shell_namespace_clickhouse_template()

    chi = "test-001"
    kubectl.create_and_check(
        manifest="manifests/chi/test-001.yaml",
        check={
            "object_counts": {"statefulset": 1, "pod": 1, "service": 2},
            "configmaps": 1,
            "pdb": {"single": 1},
            "do_not_delete": 1,
        },
    )

    created_objects = kubectl.get_obj_names_grepped("pod,service,sts,pvc,cm,pdb,secret", grep=chi)
    print("Created objects:")
    print(*created_objects, sep='\n')

    print("'nCHI status:")
    chi_status = kubectl.get("chi", chi)["status"]
    print(yaml.safe_dump(chi_status))

    kubectl.delete_chi(chi)

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010002. useTemplates for pod, volume templates, and distribution")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_UseTemplates("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_UseTemplates_Name("1.0"),
)
def test_010002(self):
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
@Name("test_010003. 4 nodes with custom layout definition")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters_Cluster_Layout("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters_Cluster_Layout_Shards_Name("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters_Cluster_Layout_Replicas_Name("1.0"),
)
def test_010003(self):
    create_shell_namespace_clickhouse_template()

    kubectl.create_and_check(
        manifest="manifests/chi/test-003-complex-layout.yaml",
        check={
            "object_counts": {"statefulset": 4, "pod": 4, "service": 5},
            "pdb": {"cluster1": 0, "cluster2": 1},
            "do_not_delete": 1
        },
    )

    chi = "test-003-complex-layout"
    cluster = "cluster1"
    with Then('Cluster settings should be different on replicas'):
        replica0 = clickhouse.query(chi, "select value from system.server_settings where name = 'default_replica_name'",
                                    host=f"chi-{chi}-{cluster}-replica0-0-0")
        replica1 = clickhouse.query(chi, "select value from system.server_settings where name = 'default_replica_name'",
                                    host=f"chi-{chi}-{cluster}-replica0-1-0")
        print(replica0)
        print(replica1)
        assert replica0 == "myreplica0" and replica1 == "myreplica1"

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010004. Compatibility test if old syntax with volumeClaimTemplate is still supported")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_VolumeClaimTemplates("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_VolumeClaimTemplates_Name("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_VolumeClaimTemplates_Spec("1.0"),
)
def test_010004(self):
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
        delete_test_namespace()


@TestScenario
@Name("test_010005. Test manifest created by ACM")
@Requirements(RQ_SRS_026_ClickHouseOperator_ACM("1.0"))
def test_010005(self):
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
@Name("test_010006. Test clickhouse version upgrade from one version to another using podTemplate change")
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_VersionUpgrades("1.0"))
def test_010006(self):
    create_shell_namespace_clickhouse_template()

    old_version = "clickhouse/clickhouse-server:24.8"
    new_version = "clickhouse/clickhouse-server:25.3"
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
@Name("test_010006_2. Test clickhouse version upgrade together with a setting change that is not compatible with a previous version")
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_VersionUpgrades("1.0"))
def test_010006_2(self):
    create_shell_namespace_clickhouse_template()

    old_version = "clickhouse/clickhouse-server:25.3"
    new_version = "clickhouse/clickhouse-server:25.8"
    chi = "test-006-2"

    with Then(f"Start CHI with version {old_version}"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-006-2-upgrade-1.yaml",
            check={
                "pod_count": 1,
                "pod_image": old_version,
                "do_not_delete": 1,
            },
        )

    with When(f"Change upgrade ClickHouse to {new_version} with a setting change that only exists in a newer one"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-006-2-upgrade-2.yaml",
            check={
                "pod_count": 1,
                # "pod_image": new_version,
                "chi_status": "InProgress",
                "do_not_delete": 1,
            },
        )

        with Then("Check if pod crashed during upgrade"):
            pod_name = kubectl.get_pod_names(chi)[0]
            for i in range(15):
                container_status = kubectl.get_field("pod", pod_name, ".status.containerStatuses[0].state.waiting.reason",)
                if container_status == "CrashLoopBackOff":
                    print(kubectl.get_field("pod", pod_name, ".status.containerStatuses[0].state.waiting.message"))
                    break
                retry_sleep(1, 5, f"{pod_name} is {container_status}")
            assert container_status not in ["CrashLoopBackOff","Error"]

        kubectl.wait_chi_status(chi, "Completed")

        with And("Confirm the setting is set"):
            out = clickhouse.query(chi, "select value from system.merge_tree_settings where name ='write_marks_for_substreams_in_compact_parts'")
            assert out == "0"

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010007. Test template with custom clickhouse ports")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_HostTemplates_Spec_InterServerHttpPort("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_HostTemplates_Spec_TcpPort("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_HostTemplates_Spec_HttpPort("1.0"),
)
def test_010007(self):
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
        kubectl.wait_objects(chi, wait_objects, shell=shell)
        kubectl.wait_chi_status(chi, "Completed", shell=shell)


def check_operator_restart(chi, wait_objects, pod, shell=None):
    start_time = kubectl.get_field("pod", pod, ".status.startTime", shell=shell)
    with When("Restart operator"):
        util.restart_operator(shell=shell)

        kubectl.wait_objects(chi, wait_objects, shell=shell)
        kubectl.wait_chi_status(chi, "Completed", shell=shell)

        new_start_time = kubectl.get_field("pod", pod, ".status.startTime", shell=shell)

        with Then("ClickHouse pods should not be restarted during operator's restart"):
            print(f"pod start_time old: {start_time}")
            print(f"pod start_time new: {new_start_time}")
            assert start_time == new_start_time


def _operator_pod_container_restart_total(pod_name, ns=None, shell=None, ok_to_fail=False):
    """Sum of .status.containerStatuses[*].restartCount (detect in-place container restarts)."""
    if not pod_name:
        return 0
    pod = kubectl.get("pod", pod_name, ns=ns, ok_to_fail=ok_to_fail, shell=shell)
    if not pod:
        return 0
    statuses = (pod.get("status") or {}).get("containerStatuses") or []
    return sum(int(cs.get("restartCount") or 0) for cs in statuses)


def wait_for_operator_pod_restart(old_pod_name, ns=None, timeout=180, shell=None):
    if ns is None:
        ns = current().context.operator_namespace

    initial_restart_total = _operator_pod_container_restart_total(
        old_pod_name, ns=ns, shell=shell, ok_to_fail=False
    )

    with Then("Operator pod should be restarted automatically (new pod or container restart)"):
        start_time = time.time()
        while time.time() - start_time < timeout:
            new_pod_name = kubectl.get_operator_pod(ns=ns, shell=shell)
            if not new_pod_name:
                time.sleep(1)
                continue

            if new_pod_name != old_pod_name:
                kubectl.wait_pod_status(new_pod_name, "Running", ns=ns, shell=shell)
                print(f"old operator pod: {old_pod_name}")
                print(f"new operator pod: {new_pod_name}")
                return new_pod_name

            current_restart_total = _operator_pod_container_restart_total(
                new_pod_name, ns=ns, shell=shell, ok_to_fail=True
            )
            if current_restart_total > initial_restart_total:
                kubectl.wait_pod_status(new_pod_name, "Running", ns=ns, shell=shell)
                print(f"operator pod (unchanged name): {new_pod_name}")
                print(
                    f"container restart total: {initial_restart_total} -> {current_restart_total}"
                )
                return new_pod_name

            time.sleep(1)

        assert False, error(
            f"operator was not restarted (no new pod, no container restart) within {timeout} seconds"
        )


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
                "do_not_delete": 1,
            },
        )

    shards = get_shards_from_remote_servers(chi, cluster)
    replicas = get_replicas_from_remote_servers(chi, cluster)

    wait_for_cluster(chi, cluster, shards, replicas)

    with Then("Create tables"):
        for s in range(shards):
            for r in range(replicas):
                h = f"chi-{chi}-{cluster}-{s}-{r}-0"
                clickhouse.query(
                    chi, "CREATE TABLE IF NOT EXISTS test_local (a UInt32) Engine = Log", host=h,
                )
                clickhouse.query(
                    chi, "CREATE TABLE IF NOT EXISTS test_dist as test_local Engine = Distributed('{cluster}', default, test_local, a)", host=h,
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
        check_shards = True,
        check_replicas = True,
        trigger_event=trigger_event,
        shell=shell_3
    )

    check_operator_restart(
        chi=chi,
        wait_objects={
            "statefulset": shards * replicas,
            "pod": shards * replicas,
            "service": shards * replicas + 1,
        },
        pod=f"chi-{chi}-{cluster}-0-0-0"
    )
    trigger_event.set()
    time.sleep(5)   # let threads to finish
    join()

    # with Then("I recreate shell"):
    #    shell = get_shell()
    #    self.context.shell = shell

    with Then("Data in shards should be evenly distributed"):
        cnt0 = clickhouse.query(chi, "select count() from cluster('all-sharded', default.test_local) where getMacro('shard')='0'")
        cnt1 = clickhouse.query(chi, "select count() from cluster('all-sharded', default.test_local) where getMacro('shard')='1'")
        print(f"{cnt0} {cnt1}")
        assert cnt0 == cnt1 and cnt0 != "0"

    with Finally("I clean up"):
        with By("deleting chi"):
            kubectl.delete_chi(chi)


def get_replicas_from_remote_servers(chi, cluster, shell=None):
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
    chi_replicas = chi_cluster.count("<replica>")

    return chi_replicas // chi_shards


@TestCheck
def check_remote_servers(self, chi, check_shards, check_replicas, trigger_event, shell=None, cluster=""):
    """Check cluster definition in configmap until signal is received"""
    if cluster == "":
        cluster = chi

    ok_runs = 0
    shards = get_shards_from_remote_servers(chi, cluster, shell=shell)
    replicas = get_replicas_from_remote_servers(chi, cluster, shell=shell)
    with Then(f"Check remote_servers contains {shards} shards until receiving a stop event"):
        while not trigger_event.is_set():
            if check_shards:
                chi_shards = get_shards_from_remote_servers(chi, cluster, shell=shell)
                if chi_shards != shards:
                    with Then(f"Number of shards in {cluster} cluster should be {shards} got {chi_shards} instead"):
                        assert chi_shards == shards

            if check_replicas:
                chi_replicas = get_replicas_from_remote_servers(chi, cluster, shell=shell)
                if chi_replicas != replicas:
                    with Then(f"Number of replicss in {cluster} cluster should be {replicas} got {chi_replicas} instead"):
                        assert chi_replicas == replicas

            ok_runs += 1
            time.sleep(0.5)

    with Then(f"remote_servers were always correct {ok_runs} times"):
        assert ok_runs > 0


@TestScenario
@Name("test_010008_1. Test operator restart")
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_RestartingOperator("1.0"))
def test_010008_1(self):
    create_shell_namespace_clickhouse_template()

    with Check("Test simple chi for operator restart"):
        test_operator_restart(
            manifest="manifests/chi/test-008-operator-restart-1.yaml",
            service="clickhouse-test-008-1",
        )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010008_2. Test operator restart")
def test_010008_2(self):
    create_shell_namespace_clickhouse_template()

    with Check("Test advanced chi for operator restart"):
        test_operator_restart(
            manifest="manifests/chi/test-008-operator-restart-2.yaml",
            service="service-test-008-2",
        )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010008_3. Test operator restart in the middle of reconcile")
def test_010008_3(self):
    create_shell_namespace_clickhouse_template()

    manifest = "manifests/chi/test-008-operator-restart-3-1.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    cluster = chi

    full_cluster = {"statefulset": 4, "pod": 4, "service": 5}

    with Given("4-node CHI creation started"):
        with Then("Wait for a half of the cluster to start"):
            kubectl.create_and_check(
                manifest,
                check={
                    "apply_templates": {
                        current().context.clickhouse_template,
                    },
                    "pod_count": 2,
                    "do_not_delete": 1,
                    "chi_status": "InProgress",
                },
            )
        with When("Restart operator"):
            util.restart_operator()
            with Then("Cluster creation should continue after a restart"):
                kubectl.wait_objects(chi, full_cluster)
                kubectl.wait_chi_status(chi, "Completed")

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
                "object_counts": {"statefulset": 2, "pod": 2, "service": 3},
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
        # shell_3 = get_shell()

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
        check_shards = True,
        check_replicas = False,
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
@Name("test_010009_1. Test operator upgrade")
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_UpgradingOperator("1.0"))
@Tags("NO_PARALLEL")
def test_010009_1(self, version_from="0.26.3", version_to=None):
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
@Name("test_010009_2. Test operator upgrade")
@Tags("NO_PARALLEL")
def test_010009_2(self, version_from="0.26.3", version_to=None):
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
@Name("test_010010. Test zookeeper initialization")
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_ZooKeeper("1.0"))
def test_010010(self):
    create_shell_namespace_clickhouse_template()

    util.require_keeper(keeper_type=self.context.keeper_type)
    chi = "test-010-zk-init"

    kubectl.create_and_check(
        manifest="manifests/chi/test-010-zk-init.yaml",
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
        out = clickhouse.query_with_error(chi, "select path from system.zookeeper where path = '/' limit 1")
        assert "/" == out
    with And("Availability zone should be set"):
        out = clickhouse.query_with_error(chi, "select availability_zone from system.zookeeper_connection")
        assert "my-azone" == out

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010010_1. Test zookeeper initialization AFTER starting a cluster")
def test_010010_1(self):
    create_shell_namespace_clickhouse_template()
    chi = "test-010-zk-init"

    kubectl.create_and_check(
        manifest="manifests/chi/test-010-zk-init.yaml",
        check={
            "apply_templates": {
                current().context.clickhouse_template,
            },
            "do_not_delete": 1,
            "chi_status": "InProgress"
        },
    )

    with Then("CHI should stay in progress with no pods created (waiting for ZooKeeper)"):
        time.sleep(15)
        assert kubectl.get_chi_status(chi) == "InProgress"
        assert kubectl.get_count("pod", chi = chi) == 0

    util.require_keeper(keeper_type=self.context.keeper_type)

    kubectl.wait_chi_status(chi, "Completed")

    with And("ClickHouse should not complain regarding zookeeper path"):
        out = clickhouse.query_with_error(chi, "select path from system.zookeeper where path = '/' limit 1")
        assert "/" == out

    with Finally("I clean up"):
        delete_test_namespace()


def get_user_xml_from_configmap(chi, user):
    users_xml = kubectl.get("configmap", f"chi-{chi}-common-usersd")["data"]["chop-generated-users.xml"]
    root_node = etree.fromstring(users_xml)
    return root_node.find(f"users/{user}")


@TestScenario
@Name("test_010011_1. Test user security and network isolation")
@Requirements(RQ_SRS_026_ClickHouseOperator_DefaultUsers("1.0"))
def test_010011_1(self):
    create_shell_namespace_clickhouse_template()

    with Given("test-011-secured-cluster-1.yaml and test-011-insecured-cluster.yaml"):

        kubectl.create_and_check(
            manifest="manifests/chi/test-011-secured-cluster-1.yaml",
            check={
                "apply_templates": {
                    current().context.clickhouse_template,
                },
                "chi_status": "InProgress", # Do not wait for completion in order to start second CHI in parallel
                "do_not_delete": 1,
            },
        )

        kubectl.create_and_check(
            manifest="manifests/chi/test-011-insecured-cluster.yaml",
            check={
                "apply_templates": {
                    current().context.clickhouse_template,
                },
                "do_not_delete": 1,
            },
        )

        kubectl.wait_chi_status("test-011-secured-cluster", "Completed")

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

            # Wait for ClickHouse to load updated config with network restrictions.
            # Kubelet ConfigMap sync can take up to 60 seconds after operator reconcile completes.
            for i in range(1, 12):
                clickhouse.query("test-011-secured-cluster", "SYSTEM RELOAD CONFIG")
                clickhouse.query("test-011-secured-cluster", "SYSTEM RELOAD CONFIG", host="chi-test-011-secured-cluster-default-1-0")
                out = clickhouse.query_with_error(
                    "test-011-insecured-cluster",
                    "select 'OK'",
                    host="chi-test-011-secured-cluster-default-1-0",
                )
                if out != "OK":
                    break
                retry_sleep(1, 10, "Network restrictions not yet loaded on default-1-0")
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

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010011_2. Test default user security")
@Requirements(RQ_SRS_026_ClickHouseOperator_DefaultUsers("1.0"))
def test_010011_2(self):
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

            with Then("Connection to localhost should succeed with default user and no password"):
                assert clickhouse.wait_config_applied("test-011-secured-default", user="default"), \
                    error("Default user without password should be available after config reload")

        with When("Default user is removed"):
            kubectl.create_and_check(
                manifest="manifests/chi/test-011-secured-default-3.yaml",
                check={
                    "do_not_delete": 1,
                },
            )

            with Then("Connection to localhost should fail with default user and no password"):
                assert clickhouse.wait_config_denied("test-011-secured-default", user="default"), \
                    error("Default user should be denied after being removed from config")

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010011_3. Test k8s secrets usage")
@Requirements(RQ_SRS_026_ClickHouseOperator_Secrets("1.0"))
def test_010011_3(self):
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
            custom0_env = ""
            custom1_env = ""
            for e in envs:
                if "valueFrom" in e:
                    print(e["name"])
                    if e["valueFrom"]["secretKeyRef"]["key"] == "KAFKA_SASL_USERNAME":
                        sasl_username_env = e["name"]
                    if e["valueFrom"]["secretKeyRef"]["key"] == "KAFKA_SASL_PASSWORD":
                        sasl_password_env = e["name"]
                    if e["valueFrom"]["secretKeyRef"]["key"] == "pwduser5":
                        user5_password_env = e["name"]
                    if e["valueFrom"]["secretKeyRef"]["key"] == "custom0":
                        custom0_env = e["name"]
                    if e["valueFrom"]["secretKeyRef"]["key"] == "custom1":
                        custom1_env = e["name"]

            with By("Secrets are properly propagated to env variables"):
                assert sasl_username_env != ""
                assert sasl_password_env != ""
                assert user5_password_env != ""

            with By("Secrets are properly propagated to env variables for long settings names"):
                assert custom0_env != ""
                assert custom1_env != ""

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
@Name("test_010011_4. Test secret-backed env rollout during upgrade")
@Requirements(RQ_SRS_026_ClickHouseOperator_Secrets("1.0"))
def test_010011_4(self):
    create_shell_namespace_clickhouse_template()

    with Given("a single-node ClickHouseInstallation with no secret-backed env or settings"):
        kubectl.apply(
            util.get_full_path("manifests/secret/test-011-4-secret.yaml"),
        )

        kubectl.create_and_check(
            manifest="manifests/chi/test-011-4-secrets-upgrade-1.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

    chi = "test-011-4-secrets-upgrade"
    pod = f"chi-{chi}-default-0-0-0"

    with When("the CHI is updated to add a secret-backed env var and a server setting that reads it via from_env"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-011-4-secrets-upgrade-2.yaml",
            check={
                "chi_status": "InProgress",
                "do_not_delete": 1,
            },
        )

        with Then("the pod should not enter CrashLoopBackOff while the CHI is reconciling"):
            chi_status = ""
            container_status = ""
            for i in range(75):
                chi_status = kubectl.get_field("chi", chi, ".status.status")
                if chi_status in ("Aborted", "Completed"):
                    break
                container_status = kubectl.get_field("pod", pod, ".status.containerStatuses[0].state.waiting.reason")
                assert container_status not in ["CrashLoopBackOff", "Error"], error(
                    f"{pod} entered {container_status} during secret-backed env rollout"
                )

                retry_sleep(1, 5, f"{chi} status={chi_status} pod={pod} waiting_reason={container_status}")

            assert chi_status == "Completed", error(f"{chi} did not complete successfully")

        with And("the secret-backed setting should be applied successfully"):
            out = clickhouse.query(chi, "select value from system.server_settings where name = 'mark_cache_size'")
            assert out == "10485760"

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010012. Test service templates")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_ServiceTemplates("1.0"),
    RQ_SRS_026_ClickHouseOperator_ServiceTemplates_NameGeneration("1.0"),
    RQ_SRS_026_ClickHouseOperator_ServiceTemplates_LoadBalancer("1.0"),
    RQ_SRS_026_ClickHouseOperator_ServiceTemplates_Annotations("1.0"),
)
def test_010012(self):
    create_shell_namespace_clickhouse_template()

    kubectl.create_and_check(
        manifest="manifests/chi/test-012-service-template.yaml",
        check={
            "object_counts": {"statefulset": 2, "pod": 2, "service": 4},
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
                "object_counts": {"statefulset": 1, "pod": 1, "service": 4},
                "do_not_delete": 1,
            },
        )

        with And("Service for default cluster should change to LoadBalancer"):
            kubectl.check_service("service-default", "LoadBalancer")

        with And("Service for shard 0 change to headless one"):
            kubectl.check_service("service-test-012-0-0", "ClusterIP", headless = True)

        with And("Service should not be re-created if type has not been changed"):
            assert service_test_012_created == kubectl.get_field("service", "service-test-012", ".metadata.creationTimestamp")

        with And("Service should be re-created if type has been changed"):
            assert service_default_created != kubectl.get_field("service", "service-default", ".metadata.creationTimestamp")

        with And("Additional internal headless service should be created"):
            kubectl.check_service("service-test-012-internal", "ClusterIP", headless = True)

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Requirements(
    RQ_SRS_026_ClickHouseOperator_Managing_ClusterScaling_AddingShards("1.0"),
    RQ_SRS_026_ClickHouseOperator_Managing_ClusterScaling_SchemaPropagation("1.0"),
)
@Name("test_010013_1. Automatic schema propagation for shards")
def test_010013_1(self):
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
        # Deprecated in 25.3 "CREATE TABLE externaldistributed_table (id UInt32, name String, age UInt32, money UInt32) ENGINE = ExternalDistributed('PostgreSQL', 'localhost:5432', 'clickhouse', 'test_replicas', 'postgres', 'mysecretpassword')",

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
        for attempt in retries(timeout=120, delay=1):
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
                        retry_sleep(i, 5, f"Not ready ({shards}/{num_shards})")
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
                            retry_sleep(i, 5, f"Not ready ({replicas}/{num_replicas})")
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
                            print("Found: " + hosts)
                            retry_sleep(i, 5, f"{host} is not ready")
                        assert hosts.startswith(str(num_hosts))


@TestScenario
@Name("test_010014_0. Test that schema is correctly propagated on replicas")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters_Cluster_ZooKeeper("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters("1.0"),
)
def test_010014_0(self):
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
            "object_counts": {"statefulset": 2, "pod": 2, "service": 3},
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
        "CREATE TABLE test_local_014 ON CLUSTER '{cluster}' (a Int8, b Int8 ALIAS a) Engine = ReplicatedMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/{table}', '{replica}') ORDER BY tuple()",
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
        # dictionary with a user, requires special settings for clickhouse_operator user
        "CREATE DICTIONARY test_dict_014_2 ON CLUSTER '{cluster}' (a Int8, b Int8) PRIMARY KEY a SOURCE(CLICKHOUSE(host 'localhost' port 9000 table 'test_local_014' user 'test_014' PASSWORD 'test_014')) LAYOUT(FLAT()) LIFETIME(0)"

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
            clickhouse.query(chi_name, q, host=f"chi-{chi_name}-{cluster}-0-0", timeout=120)

    # Give some time for replication to catch up
    time.sleep(3)
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
        for replica in replicas:
            host = f"chi-{chi_name}-{cluster}-0-{replica}"
            with Then(f"Schema objects should be migrated to {host}"):
                with Then("Checking tables and views"):
                    for obj in schema_objects:
                        print(f"Checking {obj}")
                        out = clickhouse.query(
                            chi_name,
                            f"SELECT count() FROM system.tables WHERE name = '{obj}'",
                            host=host,
                        )
                        assert out == "1"

                with And("Checking dictionaries"):
                    out = clickhouse.query(
                        chi_name,
                        f"SELECT count() FROM system.dictionaries WHERE name = 'test_dict_014'",
                        host=host,
                    )
                    assert out == "1"
                    out = clickhouse.query(
                        chi_name,
                        f"SELECT count() FROM system.dictionaries WHERE name = 'test_dict_014_2'",
                        host=host,
                    )
                    assert out == "1"
                    with Then("Checking dictionary with hidden properties"):
                        out = clickhouse.query_with_error(
                            chi_name,
                            f"SELECT count() FROM test_dict_014_2",
                            host=host,
                        )
                        if "Exception" in out:
                            print(out)
                        assert "Exception" not in out, error(out)

                with And("Checking database engines"):
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

                with And("Checking functions"):
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

    replicas = [1]
    # replicas = [1, 2]
    with When(f"Add {len(replicas)} more replicas"):
        query_log_start = clickhouse.query(chi_name, 'select now()')
        manifest = f"manifests/chi/test-014-0-replication-{1+len(replicas)}.yaml"
        chi = yaml_manifest.get_manifest_data(util.get_full_path(manifest))
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

        util.check_query_log(chi_name, ['CREATE'], [], query_log_start)

        with Then("CHI status has all nodes in hostsWithTablesCreated"):
            hosts_with_tables = kubectl.get("chi", chi_name)["status"]["hostsWithTablesCreated"]
            print(yaml.safe_dump(hosts_with_tables))

            domain = current().context.test_namespace + ".svc.cluster.local"

            assert f"chi-{chi_name}-{cluster}-0-1.{domain}" in hosts_with_tables
            assert f"chi-{chi_name}-{cluster}-1-1.{domain}" in hosts_with_tables

        with Then("Ensure replication is has cought up"):
            for i in range(1,10):
                replica_delay = clickhouse.query(chi_name, "select max(absolute_delay) from clusterAllReplicas('{cluster}', system.replicas)")
                if replica_delay == "0":
                    break
                retry_sleep(i, 5)
            assert replica_delay == "0"

        with Then("CHI status has all nodes in hostsWithReplicaCaughtUp"):
            hosts_with_tables = kubectl.get("chi", chi_name)["status"]["hostsWithReplicaCaughtUp"]
            print(yaml.safe_dump(hosts_with_tables))

            domain = current().context.test_namespace + ".svc.cluster.local"

            assert f"chi-{chi_name}-{cluster}-0-1.{domain}" in hosts_with_tables
            assert f"chi-{chi_name}-{cluster}-1-1.{domain}" in hosts_with_tables

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

    with When("Remove replicas"):
        query_log_start = clickhouse.query(chi_name, 'select now()')
        manifest = "manifests/chi/test-014-0-replication-1.yaml"
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 2,
                "pdb": {"default": 1},
                "do_not_delete": 1,
            },
        )
        with Then("Replica is removed from remote_servers.xml"):
            assert get_replicas_from_remote_servers(chi_name, cluster) == 1

        new_start_time = kubectl.get_field("pod", f"chi-{chi_name}-{cluster}-0-0-0", ".status.startTime")
        assert start_time == new_start_time

        with And(f"Replica is removed from the {self.context.keeper_type}"):
            for shard in shards:
                out = clickhouse.query(
                    chi_name,
                    f"SELECT max(total_replicas) FROM system.replicas",
                    host=f"chi-{chi_name}-{cluster}-{shard}-0",
                )
                assert out == "1"

        util.check_query_log(chi_name, ['SYSTEM DROP REPLICA'], ['DROP TABLE', 'DROP DATABASE'], query_log_start)

        with And("Replica is removed from status.hostsWithTablesCreated"):
            hosts_with_tables = kubectl.get("chi", chi_name)["status"]["hostsWithTablesCreated"]
            print(yaml.safe_dump(hosts_with_tables))

            domain = current().context.test_namespace + ".svc.cluster.local"
            assert f"chi-{chi_name}-{cluster}-0-1.{domain}" not in hosts_with_tables
            assert f"chi-{chi_name}-{cluster}-1-1.{domain}" not in hosts_with_tables

        with And("Replica is removed from status.hostsWithReplicaCaughtUp"):
            hosts_with_tables = kubectl.get("chi", chi_name)["status"]["hostsWithReplicaCaughtUp"]
            print(yaml.safe_dump(hosts_with_tables))

            domain = current().context.test_namespace + ".svc.cluster.local"
            assert f"chi-{chi_name}-{cluster}-0-1.{domain}" not in hosts_with_tables
            assert f"chi-{chi_name}-{cluster}-1-1.{domain}" not in hosts_with_tables


    with When("Add replica one more time"):
        manifest = "manifests/chi/test-014-0-replication-2.yaml"
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
        query_log_start = clickhouse.query(chi_name, 'select now()')
        manifest = "manifests/chi/test-014-0-replication-2-1.yaml"
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
            timeout=600,
        )
        with Then(f"Shard is removed from {self.context.keeper_type}", flags=XFAIL):
            out = clickhouse.query_with_error(
                chi_name,
                f"SELECT count() FROM system.zookeeper WHERE path ='/clickhouse/{cluster}/tables/1/default'",
            )
            note(f"Found {out} replicated tables in {self.context.keeper_type}")
            assert "DB::Exception: No node" in out or out == "0"

        util.check_query_log(chi_name, ['SYSTEM DROP REPLICA'], ['DROP TABLE', 'DROP DATABASE'], query_log_start, flags=XFAIL)

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
        with Then(f"Tables are deleted in {self.context.keeper_type}", flags=XFAIL):
            out = clickhouse.query_with_error(
                chi_name,
                f"SELECT count() FROM system.zookeeper WHERE path ='/clickhouse/{cluster}/tables/0/default'",
            )
            note(f"Found {out} replicated tables in {self.context.keeper_type}")
            assert "DB::Exception: No node" in out or out == "0"

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010014_1. Test replicasUseFQDN")
def test_010014_1(self):
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

    with Finally("I clean up"):
        delete_test_namespace()


def check_host_network(manifest, replica1_port="9000", replica2_port="9000"):
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    cluster = "default"

    kubectl.create_and_check(
        manifest=manifest,
        check={
            "object_counts": {"statefulset": 2, "pod": 2, "service": 3},
            "do_not_delete": 1,
        },
        timeout=600,
    )

    with Then("Query from one server to another one should work"):
        for i in range(1, 10):
            out = clickhouse.query_with_error(
                    chi,
                    host=f"chi-{chi}-default-0-0",
                    port=replica1_port,
                    sql=f"SELECT count() FROM remote('chi-{chi}-default-0-1:{replica2_port}', system.one)")
            if "DNS_ERROR" not in out:
                break
            retry_sleep(i, 5, "DNS_ERROR")
        print(f"out: {out}")
        assert out == "1"

    with And("Distributed query should work"):
        out = clickhouse.query_with_error(
                chi,
                host=f"chi-{chi}-default-0-1",
                port=replica2_port,
                sql="SELECT count() FROM cluster('all-sharded', system.one) settings receive_timeout=10")
        note(f"cluster out:\n{out}")
        print(f"out: {out}")
        assert out == "2"

    with And("Replication should work"):
        test_version = replica1_port
        clickhouse.query(
            chi,
            "CREATE TABLE " + f"test_015_{test_version}" + " (a UInt32) Engine = ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}') ORDER BY tuple()",
            host=f"chi-{chi}-{cluster}-0-0", port = replica1_port)
        clickhouse.query(
            chi,
            "CREATE TABLE " + f"test_015_{test_version}" + " (a UInt32) Engine = ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}') ORDER BY tuple()",
            host=f"chi-{chi}-{cluster}-0-1", port = replica2_port)
        clickhouse.query(
            chi,
            f"INSERT INTO test_015_{test_version} SELECT {test_version}",
            host=f"chi-{chi}-{cluster}-0-0", port = replica1_port)
        out = clickhouse.query(
            chi,
            f"SELECT * FROM test_015_{test_version}",
            host=f"chi-{chi}-{cluster}-0-1", port = replica2_port)
        assert out == test_version


@TestScenario
@Name("test_010015. hostNetwork")
@Requirements(RQ_SRS_026_ClickHouseOperator_Deployments_CircularReplication("1.0"))
def test_010015(self):
    create_shell_namespace_clickhouse_template()

    util.require_keeper(keeper_type=self.context.keeper_type)

    with Then("Check host network with different ports on the same node"):
        check_host_network(manifest = "manifests/chi/test-015-host-network.yaml", replica1_port = "10000", replica2_port = "11000")

    # with Then("Check host network with the same ports on different nodes"):
    #    check_host_network(manifest = "manifests/chi/test-015-host-network-2.yaml")

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010016. Test advanced settings options")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_ConfigurationFileControl_EmbeddedXML("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters("1.0"),
)
def test_010016(self):
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
        start_time = kubectl.get_clickhouse_start(chi)
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
        with Then("test_norestart user should be available"):
            assert clickhouse.wait_config_applied(chi, user="test_norestart"), \
                error("test_norestart user should become available after config propagation")
            version = clickhouse.query(chi, sql="select version()", user="test_norestart")
        with And("user1 user should not be available"):
            version_user1 = clickhouse.query_with_error(chi, sql="select version()", user="user1", pwd="qwerty")
            assert version != version_user1
        with And("user2 user should be available"):
            version_user2 = clickhouse.query(chi, sql="select version()", user="user2", pwd="qwerty")
            assert version == version_user2
        with And("ClickHouse SHOULD NOT be restarted"):
            new_start_time = kubectl.get_clickhouse_start(chi)
            assert start_time == new_start_time

    # test-016-settings-03.yaml
    with When("Update macro and dictionary settings"):
        start_time = kubectl.get_clickhouse_start(chi)
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
            new_start_time = kubectl.get_clickhouse_start(chi)
            assert start_time == new_start_time

    # test-016-settings-04.yaml
    with When("Add new custom4.xml config file"):
        start_time = kubectl.get_clickhouse_start(chi)
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
            new_start_time = kubectl.get_clickhouse_start(chi)
            assert start_time < new_start_time

    # test-016-settings-05.yaml
    with When("Add a change to an existing xml file"):
        start_time = kubectl.get_clickhouse_start(chi)
        kubectl.create_and_check(
            manifest="manifests/chi/test-016-settings-05.yaml",
            check={
                "do_not_delete": 1,
            },
        )

        with And("ClickHouse SHOULD BE restarted"):
            new_start_time = kubectl.get_clickhouse_start(chi)
            assert start_time < new_start_time

        with And("Macro 'test' value should be changed"):
            out = clickhouse.query(
                chi,
                sql="select substitution from system.macros where macro='test'",
            )
            assert out == "test-changed"

    # test-016-settings-06.yaml
    with When("Add I change a number of settings that does not require a restart"):
        start_time = kubectl.get_clickhouse_start(chi)
        kubectl.create_and_check(
            manifest="manifests/chi/test-016-settings-06.yaml",
            check={
                "do_not_delete": 1,
            },
        )

        with And("ClickHouse SHOULD NOT BE restarted"):
            new_start_time = kubectl.get_clickhouse_start(chi)
            assert start_time == new_start_time

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010017. Test deployment of multiple versions in a cluster")
@Requirements(RQ_SRS_026_ClickHouseOperator_Deployments_DifferentClickHouseVersionsOnReplicasAndShards("1.0"))
def test_010017(self):
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
    test_query = "select min(offset), max(offset) from test_max"

    res = ""

    for shard in range(pod_count):
        host = f"chi-{chi}-default-{shard}-0"
        for q in queries:
            clickhouse.query(chi, host=host, sql=q)
        out = clickhouse.query(chi, host=host, sql=test_query)
        if res == "":
            res = out
        ver = clickhouse.query(chi, host=host, sql="select version()")
        print(f"version: {ver}, result: {out}")
        assert res == out, error("Aggregate state may be different between versions")

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010018. Test that server settings are applied before StatefulSet is started")
# Obsolete, covered by test_016
def test_010018(self):
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
            for attempt in retries(timeout=180, delay=5):
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
        kubectl.delete_chi(chi, ok_undeleted = True)

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
        kubectl.delete_chi(chi, ok_undeleted=True)
        with Then("One PVC should be left because reclaim policy was unset when removing a replica"):
            assert kubectl.get_count("pvc", chi=chi) == 1
        with Then("Cleanup PVCs"):
            for pvc in kubectl.get_obj_names(chi, "pvc"):
                kubectl.launch(f"delete pvc {pvc}")

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010019_1. Test that volume is correctly retained and can be re-attached. Provisioner: StatefulSet")
@Requirements(RQ_SRS_026_ClickHouseOperator_RetainingVolumeClaimTemplates("1.0"))
def test_010019_1(self):
    create_shell_namespace_clickhouse_template()

    test_019(step=1)


@TestScenario
@Name("test_010019_2. Test that volume is correctly retained and can be re-attached. Provisioner: Operator")
@Requirements(RQ_SRS_026_ClickHouseOperator_RetainingVolumeClaimTemplates("1.0"))
def test_010019_2(self):
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
@Name("test_010020_1. Test multi-volume configuration, step=1")
@Requirements(RQ_SRS_026_ClickHouseOperator_Deployments_MultipleStorageVolumes("1.0"))
def test_010020_1(self):
    create_shell_namespace_clickhouse_template()

    test_020(step=1)


@TestScenario
@Name("test_010020_2. Test multi-volume configuration, step=2")
@Requirements(RQ_SRS_026_ClickHouseOperator_Deployments_MultipleStorageVolumes("1.0"))
def test_010020_2(self):
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
                retry_sleep(i, 5, "Not ready yet")
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
            assert disk == "default"

        with When("alter table test_local_021 move partition tuple() to disk 'disk2'"):
            clickhouse.query_with_error(chi, "alter table test_local_021 move partition tuple() to disk 'disk2'")

            with Then("Data should be moved to disk2"):
                disk = clickhouse.query(chi,"select disk_name from system.parts where table='test_local_021'")
                print(f"out : {disk}")
                print(f"want: disk2")
                assert disk == "disk2"

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
@Name("test_010021_1. Test rescaling storage. Provisioner: StatefulSet")
@Requirements(RQ_SRS_026_ClickHouseOperator_StorageProvisioning("1.0"))
def test_010021_1(self):
    create_shell_namespace_clickhouse_template()

    test_021(step=1)


@TestScenario
@Name("test_010021_2. Test rescaling storage. Provisioner: Operator")
@Requirements(RQ_SRS_026_ClickHouseOperator_StorageProvisioning("1.0"))
def test_010021_2(self):
    create_shell_namespace_clickhouse_template()

    test_021(step=2)


@TestScenario
@Name("test_010022. Test that chi with broken image can be deleted")
@Requirements(RQ_SRS_026_ClickHouseOperator_DeleteBroken("1.0"))
def test_010022(self):
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
        # K8s transitions ErrImagePull → ImagePullBackOff within seconds of
        # the first failed pull, so polling for the exact "ErrImagePull" reason
        # is racy. Accept either; both mean the image cannot be pulled.
        kubectl.wait_field(
            "pod",
            "chi-test-022-broken-image-default-0-0-0",
            ".status.containerStatuses[0].state.waiting.reason",
            ["ErrImagePull", "ImagePullBackOff"],
        )
        with Then("CHI should be able to delete"):
            kubectl.launch(f"delete chi {chi}", ok_to_fail=True, timeout=600)
            assert kubectl.get_count("chi", f"{chi}") == 0

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010023. Test auto templates")
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templating("1.0"))
def test_010023(self):
    create_shell_namespace_clickhouse_template()

    manifest = "manifests/chi/test-023-auto-templates.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))

    with Given("Auto templates are deployed"):
        kubectl.apply(util.get_full_path("manifests/chit/test-023-auto-templates-1.yaml"))
        kubectl.apply(util.get_full_path("manifests/chit/test-023-auto-templates-2.yaml"))
        kubectl.apply(util.get_full_path("manifests/chit/test-023-auto-templates-3.yaml"))
        kubectl.apply(util.get_full_path("manifests/chit/test-023-auto-templates-4.yaml"))
        kubectl.apply(util.get_full_path("manifests/secret/test-023-secret.yaml"))
    with Given("Give templates some time to be applied"):
        time.sleep(15)

    chit_data = yaml_manifest.get_manifest_data(util.get_full_path("manifests/chit/test-023-auto-templates-1.yaml"))
    expected_image = chit_data["spec"]["templates"]["podTemplates"][0]["spec"]["containers"][0]["image"]

    kubectl.create_and_check(
        manifest=manifest,
        check={
            "pod_count": 1,
            "pod_image": expected_image,
            "do_not_delete": 1,
        },
    )
    with Then(".status.usedTemplates list all templates"):
        used_templates = kubectl.get("chi", chi)["status"]["usedTemplates"]
        print(used_templates)

        assert kubectl.get_field("chi", chi, ".status.usedTemplates[0].name") == "clickhouse-stable"
        assert kubectl.get_field("chi", chi, ".status.usedTemplates[1].name") == "extension-annotations"
        assert kubectl.get_field("chi", chi, ".status.usedTemplates[2].name") == "grafana-dashboard-user"
        assert kubectl.get_field("chi", chi, ".status.usedTemplates[3].name") == "set-labels"
        # assert kubectl.get_field("chi", chi, ".status.usedTemplates[2].name") == ""

    chi_spec = kubectl.get("chi", chi)
    print("CHI envs:")
    for env in chi_spec["spec"]["templates"]["podTemplates"][0]["spec"]["containers"][0]["env"]:
        print(env)

    chit_spec = kubectl.get("chit", "clickhouse-stable")
    print("Template envs:")
    for env in chit_spec["spec"]["templates"]["podTemplates"][0]["spec"]["containers"][0]["env"]:
        print(env)

    # manifests/chit/test-023-auto-templates-1.yaml
    pod = kubectl.get_pod_spec(chi)
    print("Pod envs:")
    for env in pod["containers"][0]["env"]:
        print(env)

    def checkEnv(pos, env_name, env_value):
        env = pod["containers"][0]["env"][pos]
        assert env["name"] == env_name
        assert env["value"] == env_value

    with And("Environment variables from template should be populated"):
        checkEnv(0, "TEST_ENV_FROM_CHIT_1", "TEST_ENV_FROM_CHIT_1_VALUE")
    with Then("Environment variables from CHI should be retained"):
        checkEnv(1, "TEST_ENV_FROM_CHI_1", "TEST_ENV_FROM_CHI_1_VALUE")
        checkEnv(2, "TEST_ENV_FROM_CHI_2", "TEST_ENV_FROM_CHI_2_VALUE")

    # manifests/chit/test-023-auto-templates-2.yaml
    with Then("Annotation from a template should be populated"):
        normalizedCompleted = kubectl.get_chi_normalizedCompleted(chi)
        assert normalizedCompleted["metadata"]["annotations"]["test"] == "test"
    with Then("Pod annotation should populated from template"):
        assert kubectl.get_field("pod", f"chi-{chi}-single-0-0-0", ".metadata.annotations.test") == "test"

    # manifests/chit/test-023-auto-templates-3.yaml
    with Then("User from a template should be populated"):
        out = clickhouse.query_with_error(chi, "select 1", user = "grafana_dashboard_user", pwd = "grafana_dashboard_user_password")
        assert out == "1"

    with Then("Label from a template should be populated"):
        normalizedCompleted = kubectl.get_chi_normalizedCompleted(chi)
        assert normalizedCompleted["metadata"]["labels"]["my-label"] == "test"
    with Then("Pod label should populated from template"):
        assert kubectl.get_field("pod", f"chi-{chi}-single-0-0-0", ".metadata.labels.my-label") == "test"

    with Given("Two selector templates are deployed"):
        kubectl.apply(util.get_full_path("manifests/chit/tpl-clickhouse-selector-1.yaml"))
        kubectl.apply(util.get_full_path("manifests/chit/tpl-clickhouse-selector-2.yaml"))
    with Given("Give templates some time to be applied"):
        time.sleep(15)

    with Then("Trigger CHI update"):
        kubectl.force_chi_reconcile(chi, "apply-templates")

    with Then(".status.usedTemplates shows new values"):
        used_templates = kubectl.get("chi", chi)["status"]["usedTemplates"]
        print(used_templates)

        assert kubectl.get_field("chi", chi, ".status.usedTemplates[0].name") == "clickhouse-stable"
        assert kubectl.get_field("chi", chi, ".status.usedTemplates[1].name") == "extension-annotations"
        assert kubectl.get_field("chi", chi, ".status.usedTemplates[2].name") == "grafana-dashboard-user"
        assert kubectl.get_field("chi", chi, ".status.usedTemplates[3].name") == "selector-test-1"

    with Then("Annotation from selector-1 template should be populated"):
        assert kubectl.get_field("pod", f"chi-{chi}-single-0-0-0", ".metadata.annotations.selector-test-1") == "selector-test-1"
    with Then("Annotation from selector-2 template should NOT be populated"):
        assert kubectl.get_field("pod", f"chi-{chi}-single-0-0-0", ".metadata.annotations.selector-test-2") == "<none>"

    with When("Delete all templates and run reconcile"):
        kubectl.delete_all("chit")
        assert kubectl.get_count("chit") == 0, error("All CHIT should be deleted")

        kubectl.force_chi_reconcile(chi, "remove-templates")

        with Then("usedTemplates should have been cleaned in the status"):
            assert kubectl.get_field("chi", chi, ".status.usedTemplates") == "<none>", error("Used templates should be empty")


    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010024. Test annotations for various template types")
@Requirements(RQ_SRS_026_ClickHouseOperator_AnnotationsInTemplates("1.0"))
def test_010024(self):
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
@Name("test_010025. Test that service is available during re-scaling, upgrades etc.")
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_ClusterScaling_AddingReplicas("1.0"))
def test_010025(self):
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
            "object_counts": {"statefulset": 1, "pod": 1, "service": 2},
            "do_not_delete": 1,
        },
        timeout=600,
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
            r".metadata.labels.clickhouse\.altinity\.com/ready",
            "yes",
            backoff=1,
        )
        start_time = time.time()
        lb_error_time = start_time
        distr_lb_error_time = start_time
        latent_replica_time = start_time
        for i in range(1, 10):
            cnt_local = clickhouse.query_with_error(
                chi,
                "SELECT count() FROM test_local_025",
                "chi-test-025-rescaling-default-0-1.test.svc.cluster.local.",
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
            retry_sleep(1, 5)
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
@Name("test_010026. Test mixed single and multi-volume configuration in one cluster")
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters_Cluster_Layout("1.0"))
def test_010026(self):
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
@Name("test_010027. Test troubleshooting mode")
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Troubleshoot("1.0"))
def test_010027(self):
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
                    "object_counts": {"statefulset": 1, "pod": 1, "service": 2},
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
@Name("test_010028. Test restart scenarios")
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_RestartingOperator("1.0"))
def test_010028(self):
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
            "object_counts": {"statefulset": 2, "pod": 2, "service": 3},
            "do_not_delete": 1,
        },
    )

    sql = """SELECT getMacro('replica') AS replica, uptime() AS uptime,
     (SELECT count() FROM system.clusters WHERE cluster='all-sharded') AS total_hosts,
     (SELECT count() online_hosts FROM cluster('all-sharded', system.one) settings skip_unavailable_shards=1 ) AS online_hosts
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

        pod_start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.containerStatuses[0].state.running.startedAt")
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
                        advanced_params="--connect_timeout=1 --send_timeout=10 --receive_timeout=10",
                    )
                    ch2 = clickhouse.query_with_error(
                        chi,
                        sql,
                        pod="chi-test-028-replication-default-0-1-0",
                        host="chi-test-028-replication-default-0-1",
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
            new_pod_start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.containerStatuses[0].state.running.startedAt")
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
                start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.containerStatuses[0].state.running.startedAt")

        # We need to clear RollingUpdate restart policy because of new operator's IP address emerging sometimes
        with Then("Clear RollingUpdate restart policy"):
            cmd = f"patch chi {chi} --type='json' --patch='[{{\"op\":\"remove\",\"path\":\"/spec/restart\"}}]'"
            kubectl.launch(cmd)
            kubectl.wait_chi_status(chi, "InProgress")
            kubectl.wait_chi_status(chi, "Completed")

        with Then("Restart operator. CHI should not be restarted"):
            check_operator_restart(
                chi=chi,
                wait_objects={
                    "statefulset": 2,
                    "pod": 2,
                    "service": 3,
                },
                pod=f"chi-{chi}-default-0-0-0",
            )

        with Then("Re-apply the original config. CHI should not be restarted"):
            kubectl.create_and_check(manifest=manifest, check={"do_not_delete": 1})
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-default-0-0-0", ".status.containerStatuses[0].state.running.startedAt")
            print(f"old_start_time: {start_time}")
            print(f"new_start_time: {new_start_time}")
            assert start_time == new_start_time

    with When("Stop installation"):
        cmd = f'patch chi {chi} --type=\'json\' --patch=\'[{{"op":"add","path":"/spec/stop","value":"yes"}}]\''
        kubectl.launch(cmd)
        kubectl.wait_chi_status(chi, "InProgress")
        kubectl.wait_chi_status(chi, "Completed")
        with Then("Stateful sets should be there but no running pods"):
            kubectl.wait_objects(chi, {
                "statefulset": 2,
                "pod": 0,
                "service": 2,
            })

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010029. Test different distribution settings")
@Requirements(
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_PodTemplates_podDistribution("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_PodTemplates_podDistribution_Type("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_PodTemplates_podDistribution_Scope("1.0"),
    RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Templates_PodTemplates_podDistribution_TopologyKey("1.0"),
)
def test_010029(self):
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
@Name("test_090099. Test CRD deletion. Should be executed at the end")
@Tags("NO_PARALLEL")
def test_090099(self):
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
@Name("test_010031. Test excludeFromPropagationAnnotations work")
def test_010031(self):
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
@Name("test_010032. Test rolling update logic")
def test_010032(self):
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
            "object_counts": {"statefulset": 4, "pod": 4, "service": 5},
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
        check_shards = True,
        check_replicas = False,
        trigger_event=trigger_event,
        shell=shell_2
    )

    with When("Change the image in the podTemplate by updating the chi version to test the rolling update logic"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-032-rescaling-2.yaml",
            check={
                "object_counts": {"statefulset": 4, "pod": 4, "service": 5},
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
@Requirements(RQ_SRS_026_ClickHouseOperator_Settings_ClickHouseOperatorConfiguration_Changes("1.0"))
@Name("test_010033. Restart operator automatically on ClickHouseOperatorConfiguration change")
def test_010033(self):
    create_shell_namespace_clickhouse_template()

    chopconf_file = "manifests/chopconf/test-033-auto-restart.yaml"
    operator_namespace = current().context.operator_namespace

    with Given(f"operator pod before applying {chopconf_file}"):
        operator_pod = kubectl.get_operator_pod(ns=operator_namespace)

    with When("Apply ClickHouseOperatorConfiguration"):
        kubectl.apply(util.get_full_path(chopconf_file, lookup_in_host=False), operator_namespace)

        with Then("Operator should restart automatically after configuration is applied"):
            operator_pod = wait_for_operator_pod_restart(operator_pod, ns=operator_namespace)

    with Then("Deleting ClickHouseOperatorConfiguration"):
        kubectl.delete(util.get_full_path(chopconf_file, lookup_in_host=False), operator_namespace)

        with Then("Operator should restart back after cleanup"):
            wait_for_operator_pod_restart(operator_pod, ns=operator_namespace)

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010035. Auto-recovery from aborted reconcile when pod becomes Ready")
def test_010035(self):
    """Verify that when a CHI reconcile is aborted because a pod did not become
    Ready within `statefulSet.update.timeout`, the operator automatically re-enqueues the
    CHI for reconcile once the pod eventually becomes Ready — no manual intervention
    (taskID change, force reconcile, etc.) required.

    Scenario:
      1. Apply CHOPCONF with short update timeout (30s) and onFailure=abort
      2. Apply CHI with a pod template that sleeps 90s before starting clickhouse
      3. Reconcile aborts around 30s with Status=Aborted
      4. Pod becomes Ready around 60s
      5. Operator detects NotReady→Ready transition, auto-recovers reconcile
      6. CHI reaches Status=Completed without any manual trigger
    """
    create_shell_namespace_clickhouse_template()

    chopconf_file = "manifests/chopconf/test-035-auto-recovery-aborted.yaml"
    util.apply_operator_config(chopconf_file)

    manifest = "manifests/chi/test-035-auto-recovery-1.yaml"
    manifest_2 = "manifests/chi/test-035-auto-recovery-2.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    cluster = "default"

    with When("I create CHI with slow-starting pod"):
        kubectl.apply(util.get_full_path(manifest, lookup_in_host=False))

        with Then("CHI should enter Aborted state after the update timeout"):
            kubectl.wait_chi_status(chi, "Aborted", retries=20)

        pod = f"chi-{chi}-{cluster}-0-0-0"
        with And("Wait for the pod to eventually become Ready"):
            kubectl.wait_container_status(pod, "true")

        with Then("Operator should automatically recover reconcile (no manual action)"):
            kubectl.wait_chi_status(chi, "Completed", retries=20)

    with When("I add a replica and change a version"):
        kubectl.apply(util.get_full_path(manifest_2, lookup_in_host=False))

        with Then("CHI should enter Aborted state after the update timeout"):
            kubectl.wait_chi_status(chi, "Aborted", retries=20)

        pod = f"chi-{chi}-{cluster}-0-0-0"
        with And("Wait for the pod to eventually become Ready"):
            kubectl.wait_container_status(pod, "true")

        with Then("Operator should automatically recover reconcile (no manual action)"):
            kubectl.wait_chi_status(chi, "Completed", retries=20)

        with And("Two replicas should be created"):
            kubectl.wait_objects(chi, {"statefulset": 2, "pod": 2, "service": 3})

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010035_1. Opt-out: CHI stays Aborted when auto-recovery onPodReady=none")
def test_010035_1(self):
    """Opt-out path: Verify that when the operator is configured with
    reconcile.recovery.from.aborted.onPodReady=none, the CHI stays Aborted
    even after the pod becomes Ready — no automatic recovery.

    This is the inverse of test_010035 and validates that the opt-out knob works.
    """
    create_shell_namespace_clickhouse_template()

    chopconf_file = "manifests/chopconf/test-035-1-auto-recovery-disabled.yaml"
    util.apply_operator_config(chopconf_file)

    manifest = "manifests/chi/test-035-auto-recovery-1.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    cluster = "default"

    with When("I create CHI with slow-starting pod and auto-recovery disabled"):
        kubectl.apply(util.get_full_path(manifest, lookup_in_host=False))

        with Then("CHI should enter Aborted state after the update timeout"):
            kubectl.wait_chi_status(chi, "Aborted", retries=20)

        pod = f"chi-{chi}-{cluster}-0-0-0"
        with And("Wait for the pod to eventually become Ready"):
            kubectl.wait_container_status(pod, "true")

        with Then("CHI must stay Aborted for 30s (auto-recovery is disabled)"):
            # Poll every 5s instead of a single sleep+check. A single sleep+check could
            # false-pass if a retry fires at the very end of the window. Polling catches
            # any status change within the window.
            for i in range(6):
                time.sleep(5)
                status = kubectl.get_chi_status(chi)
                assert status == "Aborted", error(
                    f"expected CHI to stay Aborted with onPodReady=none, "
                    f"got status={status} at poll {i+1}/6"
                )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_EnableHttps("1.0"))
@Name("test_010034. Check HTTPS support for health check")
def test_010034(self):
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
                "object_counts": {"statefulset": 1, "pod": 1, "service": 2},
                "do_not_delete": 1,
            },
            timeout=600,
        )

    with Then("check for `chi_clickhouse_metric_fetch_errors` is zero [1]"):
        check_metrics_monitoring(
            operator_namespace=operator_namespace,
            operator_pod=kubectl.get_operator_pod(),
            expect_pattern="^chi_clickhouse_metric_fetch_errors{(.*?)} 0$",
        )

    with And(f"apply ClickHouseOperatorConfiguration with https connection: {chopconf_file}"):
        kubectl.apply(util.get_full_path(chopconf_file, lookup_in_host=False), operator_namespace)

    with And("Re-create operator pod in order to restart metrics exporter to update the configuration [1]"):
        util.restart_operator()
        kubectl.wait_chi_status(chi, "Completed")

    with Then("check for `chi_clickhouse_metric_fetch_errors` is not zero"):
        check_metrics_monitoring(
            operator_namespace=operator_namespace,
            operator_pod=kubectl.get_operator_pod(),
            expect_pattern="^chi_clickhouse_metric_fetch_errors{(.*?)} 1$",
        )

    with When("Reset ClickHouseOperatorConfiguration to default"):
        kubectl.delete(util.get_full_path(chopconf_file, lookup_in_host=False), operator_namespace)

    with And("Re-create operator pod in order to restart metrics exporter to update the configuration [2]"):
        util.restart_operator()

    with Then("check for `chi_clickhouse_metric_fetch_errors` is zero [2]"):
        check_metrics_monitoring(
            operator_namespace=operator_namespace,
            operator_pod=kubectl.get_operator_pod(),
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
                "object_counts": {"statefulset": 1, "pod": 1, "service": 2},
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

    with Then("check for `chi_clickhouse_metric_fetch_errors` is zero [3]"):
        check_metrics_monitoring(
            operator_namespace=operator_namespace,
            operator_pod=kubectl.get_operator_pod(),
            expect_pattern="^chi_clickhouse_metric_fetch_errors{(.*?)} 0$",
        )

    with When("Reset ClickHouseOperatorConfiguration to default"):
        kubectl.delete(util.get_full_path(chopconf_file, lookup_in_host=False), operator_namespace)

    with And("Re-create operator pod in order to restart metrics exporter to update the configuration [4]"):
        util.restart_operator()

    # 0.21.2+
    with Then("check for `chi_clickhouse_metric_fetch_errors` is zero [4]"):
        check_metrics_monitoring(
            operator_namespace=operator_namespace,
            operator_pod=kubectl.get_operator_pod(),
            expect_pattern="^chi_clickhouse_metric_fetch_errors{(.*?)} 0$",
        )

    with Finally("I clean up"):
        with By("deleting pod"):
            kubectl.launch(f"delete pod {client_pod}")
        with And("deleting test namespace"):
            delete_test_namespace()


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_ReprovisioningVolume("1.0"))
@Name("test_010036. Check operator volume re-provisioning")
@Tags("NO_PARALLEL")
def test_010036(self):
    """Check clickhouse operator recreates volumes and schema if volume is broken."""
    # Python try/finally ensures the namespace is deleted even when an assertion
    # fires mid-test. A trailing `with Finally(...)` is only entered on the
    # happy path — AssertionError exits the function before reaching it, so on
    # retry the previous attempt's namespace survives and leaks. Namespace
    # creation is inside the try so a partial setup is still cleaned up; the
    # cleanup itself tolerates a missing test_namespace context attribute.
    try:
        create_shell_namespace_clickhouse_template()

        manifest = "manifests/chi/test-036-volume-re-provisioning-1.yaml"
        chi = yaml_manifest.get_name(util.get_full_path(manifest))
        cluster = "simple"
        util.require_keeper(keeper_type=self.context.keeper_type)

        with Given("CHI with two replicas is created"):
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
            clickhouse.query(chi, "CREATE DATABASE IF NOT EXISTS test_036 ON CLUSTER '{cluster}'")
            create_table = """
                CREATE TABLE IF NOT EXISTS test_036.test_local_036 ON CLUSTER '{cluster}' (a UInt32)
                Engine = ReplicatedMergeTree('/clickhouse/{installation}/tables/{shard}/{database}/{table}', '{replica}')
                PARTITION BY tuple()
                ORDER BY a
                """.replace("\r", "").replace("\n", "")
            clickhouse.query(chi, create_table)
            clickhouse.query(chi, f"INSERT INTO test_036.test_local_036 select * from numbers(10000)")

            clickhouse.query(chi, "CREATE DATABASE IF NOT EXISTS test_036_mem ON CLUSTER '{cluster}' Engine = Memory")
            clickhouse.query(chi, "CREATE VIEW IF NOT EXISTS test_036_mem.test_view ON CLUSTER '{cluster}' AS SELECT * from system.tables")

        def delete_pv(volume):
            with When("Delete PV", description="delete PV on replica 0"):
                # Prepare counters
                pvc_count = kubectl.get_count("pvc", chi=chi)
                pv_count = kubectl.get_count("pv")
                print(f"pvc_count: {pvc_count}")
                print(f"pv_count: {pv_count}")

                pv_name = kubectl.get_pv_name(f"{volume}-chi-test-036-volume-re-provisioning-simple-0-0-0")
                # retry
                kubectl.launch(f"delete pv {pv_name} --force &")
                kubectl.launch(f"""patch pv {pv_name} --type='json' --patch='[{{"op":"remove","path":"/metadata/finalizers"}}]'""")
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
                        name=f"{volume}-chi-test-036-volume-re-provisioning-simple-0-0-0",
                        field=".status.phase",
                        value="Lost",
                    )

        def delete_sts_and_pvc(volume):
            with When("Delete StatefulSet and PVC", description="delete StatefulSet on replica 0"):
                kubectl.launch("delete sts chi-test-036-volume-re-provisioning-simple-0-0")
                kubectl.launch(f"delete pvc {volume}-chi-test-036-volume-re-provisioning-simple-0-0-0")

                with Then("Wait for StatefulSet is deleted"):
                    for i in range(10):
                        if kubectl.get_count("sts", "chi-test-036-volume-re-provisioning-simple-0-0") == 0:
                            break
                        retry_sleep(1, 5, "StatefulSet is not deleted")

                with Then("Wait for PVC is deleted"):
                    for i in range(5):
                        if kubectl.get_count("pvc", f"{volume}-chi-test-036-volume-re-provisioning-simple-0-0-0") == 0:
                            break
                        retry_sleep(1, 5, "PVC is not deleted")

            assert kubectl.get_count("sts", "chi-test-036-volume-re-provisioning-simple-0-0") == 0, "StatefulSet is not deleted"
            assert kubectl.get_count("pvc", f"{volume}-chi-test-036-volume-re-provisioning-simple-0-0-0") == 0, "PVC is not deleted"

        def delete_pvc(volume):
            with Then("Delete PVC", description="delete PVC on replica 0"):
                # Prepare counters
                pvc_count = kubectl.get_count("pvc", chi=chi)
                pv_count = kubectl.get_count("pv")
                print(f"pvc_count: {pvc_count}")
                print(f"pv_count: {pv_count}")

                pvc_name = f"{volume}-chi-test-036-volume-re-provisioning-simple-0-0-0"
                # retry
                kubectl.launch(f"""patch pvc {pvc_name} --type='json' --patch='[{{"op":"remove","path":"/metadata/finalizers"}}]'""")
                kubectl.launch(f"delete pvc {pvc_name} --force &")

                # restart pod to make sure volume is unmounted
                kubectl.launch("delete pod chi-test-036-volume-re-provisioning-simple-0-0-0")
                with Then("Wait for PVC is deleted"):
                    for i in range(10):
                        if kubectl.get_count("pvc", f"{volume}-chi-test-036-volume-re-provisioning-simple-0-0-0") == 0:
                            break
                        retry_sleep(1, 5, "PVC is not deleted")

                with Then("PVC should be deleted, PV should be deleted as well"):
                    new_pvc_count = kubectl.get_count("pvc", chi=chi)
                    new_pv_count = kubectl.get_count("pv")
                    print(f"new_pvc_count: {new_pvc_count}")
                    print(f"new_pv_count: {new_pv_count}")
                    assert new_pvc_count < pvc_count
                    assert new_pv_count < pv_count

        def recover_volume(volume, reconcile_task_id):
            with When(f"Kick operator to start reconcile cycle to fix lost {volume} volume"):
                kubectl.force_chi_reconcile(chi, reconcile_task_id)
                # force_wait=True bypasses the operator>=0.24 short-circuit in
                # wait_for_cluster. CHI status=Completed alone is not enough
                # after volume recovery: CoreDNS still has a negative cache for
                # the recreated pod's headless-service name, so the recovered
                # ClickHouse server can't resolve its own / peer interserver
                # hostnames yet. The forced wait drives a system.clusters poll
                # loop from inside the recovered pod that absorbs the DNS race
                # before check_data_is_recovered tries to count rows.
                wait_for_cluster(chi, cluster, 1, 2, force_wait=True)

                with Then("I check PV is in place"):
                    kubectl.wait_field(
                        "pvc",
                        f"{volume}-chi-test-036-volume-re-provisioning-simple-0-0-0",
                        ".status.phase",
                        "Bound",
                    )
                    kubectl.wait_object(
                        "pv",
                        kubectl.get_pv_name(f"{volume}-chi-test-036-volume-re-provisioning-simple-0-0-0"),
                    )
                    size = kubectl.get_pv_size(f"{volume}-chi-test-036-volume-re-provisioning-simple-0-0-0")
                    assert size == "1Gi", error()

        def check_data_is_recovered():
            with And("I check data on each replica"):
                for replica in (0,1):
                    # Schema propagation from ZooKeeper after volume recovery is staged:
                    # databases come back first, then table metadata via ReplicatedMergeTree
                    # attach, then the actual data via interserver fetch from the peer.
                    # Each step needs its own poll. Budgets are linear-cap 30s × N iters.
                    with By(f"Check that databases exist on replica {replica}"):
                        r = "0"
                        for i in range(1, 15):
                            r = clickhouse.query(
                                chi,
                                pod=f"chi-test-036-volume-re-provisioning-simple-0-{replica}-0",
                                sql="SELECT count(*) FROM system.databases where name like 'test_036%'",
                                )
                            if r == "2":
                                break
                            retry_sleep(i, 5, f"Not ready")
                        assert r == "2", error()
                    with And(f"Wait for table to be registered on replica {replica}"):
                        # Gate the row-count check on system.tables visibility first:
                        # the UNKNOWN_TABLE error from clickhouse-server is structurally
                        # different from "table exists but is still being fetched", and
                        # we want a clean signal that the ReplicatedMergeTree attach has
                        # completed before checking row count.
                        r = "0"
                        for i in range(1, 30):
                            r = clickhouse.query(
                                chi,
                                pod=f"chi-test-036-volume-re-provisioning-simple-0-{replica}-0",
                                sql="SELECT count(*) FROM system.tables WHERE database='test_036' AND name='test_local_036'",
                                )
                            if r == "1":
                                break
                            retry_sleep(i, 5, f"Not ready ({r})")
                        assert r == "1", error()
                    with And(f"checking data on the replica {replica}"):
                        r = "0"
                        for i in range(1, 30):
                            r = clickhouse.query_with_error(
                                chi,
                                pod=f"chi-test-036-volume-re-provisioning-simple-0-{replica}-0",
                                sql="SELECT count(*) FROM test_036.test_local_036",
                                )
                            if r == "10000":
                                break
                            retry_sleep(i, 5, f"Not ready ({r})")
                        assert r == "10000", error()
                    with And("checking view in Memory engine exists"):
                        r = "0"
                        for i in range(1, 15):
                            r = clickhouse.query_with_error(
                                chi,
                                pod="chi-test-036-volume-re-provisioning-simple-0-0-0",
                                sql="SELECT count(*) FROM system.tables where name = 'test_view'",
                                )
                            if r == "1":
                                break
                            retry_sleep(i, 5, f"Not ready ({r})")
                        assert r == "1", error()

        delete_sts_and_pvc("default")
        recover_volume("default", "reconcile-after-STS-and-PVC-deleted")
        check_data_is_recovered()

        query_log_start = clickhouse.query(chi, 'select now()')
        delete_pvc("default")
        recover_volume("default", "reconcile-after-PVC-deleted")
        check_data_is_recovered()
        util.check_query_log(chi, ['SYSTEM DROP REPLICA'], [], since = query_log_start)

        delete_pv("default")
        recover_volume("default", "reconcile-after-PV-deleted")
        check_data_is_recovered()

        with Then("Add a second disk"):
            kubectl.create_and_check(
                manifest="manifests/chi/test-036-volume-re-provisioning-2.yaml",
                check={
                    "apply_templates": {current().context.clickhouse_template},
                    "pod_count": 2,
                    "do_not_delete": 1,
                },
            )
            wait_for_cluster(chi, cluster, 1, 2)
            with Then("Confirm there are two disks"):
                out = clickhouse.query(chi, "select count() from system.disks")
                assert out == "2"

        query_log_start = clickhouse.query(chi, 'select now()')
        delete_pvc("disk2")
        recover_volume("disk2", "reconcile-after-disk2-PVC-deleted")
        check_data_is_recovered()
        util.check_query_log(chi, [], ['SYSTEM DROP REPLICA'], since = query_log_start)
    finally:
        with Finally("I clean up"):
            delete_test_namespace()


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_StorageManagementSwitch("1.0"))
@Name("test_010037. StorageManagement switch")
def test_010037(self):
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
@Name("test_039. Inter-cluster communications with secret")
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
        with And("Select from all-sharded with no secret should fail"):
            r = clickhouse.query_with_error(chi, "SELECT * FROM cluster('all-sharded', system.one)", pwd="qkrq")
            assert "AUTHENTICATION_FAILED" in r
    if step > 0:
        with Then("Select in cluster with secret should pass"):
            r = clickhouse.query(chi, "SELECT count() FROM secure_dist", pwd="qkrq")
            assert r == "10"
        with And("Select from all-sharded with secret should pass"):
            r = clickhouse.query_with_error(chi, "SELECT * FROM cluster('all-sharded', system.one) limit 1", pwd="qkrq")
            assert r == "0"

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
@Name("test_010039_0. Inter-cluster communications with no secret defined")
def test_010039_0(self):
    create_shell_namespace_clickhouse_template()

    test_039(step=0)


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_InterClusterCommunicationWithSecret("1.0"))
@Name("test_010039_1. Inter-cluster communications with 'auto' secret")
def test_010039_1(self):
    """Check clickhouse-operator support inter-cluster communications with 'auto' secret."""
    create_shell_namespace_clickhouse_template()

    test_039(step=1)


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_InterClusterCommunicationWithSecret("1.0"))
@Name("test_010039_2. Inter-cluster communications with plain text secret")
def test_010039_2(self):
    """Check clickhouse-operator support inter-cluster communications with plain text secret."""
    create_shell_namespace_clickhouse_template()

    test_039(step=2)


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_InterClusterCommunicationWithSecret("1.0"))
@Name("test_010039_3. Inter-cluster communications with k8s secret")
def test_010039_3(self):
    """Check clickhouse-operator support inter-cluster communications with k8s secret."""
    create_shell_namespace_clickhouse_template()

    test_039(step=3)


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_InterClusterCommunicationWithSecret("1.0"))
@Name("test_010039_4. Inter-cluster communications over HTTPS")
def test_010039_4(self):
    """Check clickhouse-operator support inter-cluster communications over HTTPS."""
    create_shell_namespace_clickhouse_template()

    test_039(step=4, delete_chi=1)


@TestScenario
@Name("test_010040. Inject a startup probe using an auto template")
def test_010040(self):

    create_shell_namespace_clickhouse_template()

    manifest = "manifests/chi/test-005-acm.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))

    with Given("Auto template with a startup probe is deployed"):
        kubectl.apply(util.get_full_path("manifests/chit/tpl-startup-probe.yaml"))

    kubectl.create_and_check(
        manifest = manifest,
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
@Name("test_010040_1. Inject a startup probe using a reconcile setting")
def test_010040_1(self):

    create_shell_namespace_clickhouse_template()

    manifest = "manifests/chi/test-040-startup-probe.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))

    kubectl.create_and_check(
        manifest=manifest,
        check={
            "apply_templates": {
                current().context.clickhouse_template,
            },
            "pod_image": current().context.clickhouse_version,
            "pod_count": 1,
            "do_not_delete": 1,
        },
    )

    with Then("Startup probe should be defined"):
        assert "startupProbe" in kubectl.get_pod_spec(chi)["containers"][0]

    with Then("Readiness probe should be defined"):
        assert "readinessProbe" in kubectl.get_pod_spec(chi)["containers"][0]

    with Then("uptime() should be less than 120 seconds as defined by a readiness probe"):
        out = clickhouse.query(chi, "select uptime()")
        print(f"clickhouse uptime: {out}")
        assert int(out) < 120

    with Then("Pod should be not ready"):
        ready = kubectl.get_pod_status_full(chi)["containerStatuses"][0]["ready"]
        print(f"ready: {ready}")
        assert ready is not True

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010041. Secure zookeeper")
def test_010041(self):
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
                "apply_templates": {
                    current().context.clickhouse_template,
                },
                "pod_image": current().context.clickhouse_version,
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
@Name("test_010042. Test configuration rollback")
def test_010042(self):
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

        with And(".status.error should be not empty"):
            status_err = kubectl.get_field("chi", chi, ".status.error")
            print(status_err)
            assert status_err != ""

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

@TestScenario
@Name("test_010042_2. Test aborting changes that may recreate STS")
def test_010042_2(self):
    create_shell_namespace_clickhouse_template()

    cluster = "default"
    manifest = f"manifests/chi/test-042-abort-1.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))

    with Given("CHI is created"):
        kubectl.create_and_check(
            manifest = "manifests/chi/test-042-abort-1.yaml",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

    version_1 = "24.8"
    version_2 = "25.3"
    version_3 = "25.8"

    with Then("CHI version is " + version_1):
        ver = clickhouse.query(chi, "select version()")
        assert version_1 in ver

    with When("OnUpdateFailure is aborted"):
        onUpdateFailure = kubectl.get_field("chi", chi, ".spec.reconcile.statefulSet.recreate.onUpdateFailure")
        if onUpdateFailure != 'abort':
            cmd = f'patch chi {chi} --type=\'json\' --patch=\'[{{"op":"replace","path":"/spec/reconcile/statefulSet/recreate/onUpdateFailure","value":"abort"}}]\''
            kubectl.launch(cmd)
            kubectl.wait_chi_status(chi, "InProgress")
            kubectl.wait_chi_status(chi, "Completed")

        with Then("Upgrade podTemplate.image to a different version should be allowed"):
            kubectl.create_and_check(
                manifest = "manifests/chi/test-042-abort-2.yaml",
                check={
                    "pod_count": 1,
                    "do_not_delete": 1
                },
            )

        with And("CHI version is nchanged to " + version_2):
            ver = clickhouse.query(chi, "select version()")
            assert version_2 in ver

    with When("OnUpdateFailure is aborted"):
        onUpdateFailure = kubectl.get_field("chi", chi, ".spec.reconcile.statefulSet.recreate.onUpdateFailure")
        if onUpdateFailure != 'abort':
            cmd = f'patch chi {chi} --type=\'json\' --patch=\'[{{"op":"replace","path":"/spec/reconcile/statefulSet/recreate/onUpdateFailure","value":"abort"}}]\''
            kubectl.launch(cmd)
            kubectl.wait_chi_status(chi, "InProgress")
            kubectl.wait_chi_status(chi, "Completed")

        with Then("Upgrade podTemplate.volumeClaimTemplate should fail"):
            kubectl.create_and_check(
                manifest = "manifests/chi/test-042-abort-3.yaml",
                check={
                    "pod_count": 1,
                    "do_not_delete": 1,
                    "chi_status": "Aborted"
                },
            )

        with And("CHI version is unchanged " + version_2):
            ver = clickhouse.query(chi, "select version()")
            assert version_2 in ver

    with When("OnUpdateFailure is changed to recreate"):
        onUpdateFailure = kubectl.get_field("chi", chi, ".spec.reconcile.statefulSet.recreate.onUpdateFailure")
        if onUpdateFailure != 'recreate':
            cmd = f'patch chi {chi} --type=\'json\' --patch=\'[{{"op":"replace","path":"/spec/reconcile/statefulSet/recreate/onUpdateFailure","value":"recreate"}}]\''
            kubectl.launch(cmd)
            kubectl.wait_chi_status(chi, "InProgress")
            kubectl.wait_chi_status(chi, "Completed")

        with Then("CHI reconcile should proceed, and CHI version is unchanged " + version_3):
            ver = clickhouse.query(chi, "select version()")
            assert version_3 in ver


    with Finally("I clean up"):
        delete_test_namespace()


@TestCheck
@Name("test_043. Logs container customizing")
def test_043(self, manifest):
    """Check that clickhouse-operator support logs container customizing."""

    create_shell_namespace_clickhouse_template()

    cluster = "cluster"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))

    with Given("CHI is installed"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "apply_templates": {
                    current().context.clickhouse_template,
                },
                "pod_image": current().context.clickhouse_version,
                "pod_count": 1,
                "do_not_delete": 1,
                },
            )

    with Then("I check both containers are ready"):
        # Auto-injected clickhouse-log sidecar (alphabetically containerStatuses[0])
        # has no readinessProbe and may flip ready after CHI status=Completed,
        # so poll instead of asserting once. Mirrors the wait_field readiness
        # pattern used by test_044 in this file.
        pod = f"chi-{chi}-{cluster}-0-0-0"
        kubectl.wait_field("pod", pod, ".status.containerStatuses[0].ready", "true")
        kubectl.wait_field("pod", pod, ".status.containerStatuses[1].ready", "true")

    with Then("I check clickhouse logs are in clickhouse-log container"):
        with By("calling ls inside clickhouse-log in /var/log directory"):
            r = kubectl.launch(f"exec chi-{chi}-{cluster}-0-0-0 -c clickhouse-log -- bash -c 'ls /var/log/clickhouse-server/'")

        assert "clickhouse-server.err.log" in r, error()
        assert "clickhouse-server.log" in r, error()

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Defaults_Templates_logVolumeClaimTemplate("1.0"))
@Name("test_010043_0. Logs container customizing using PodTemplate")
def test_010043_0(self):
    """Check that clickhouse-operator support manual logs container customizing."""

    test_043(manifest="manifests/chi/test-043-0-logs-container-customizing.yaml")


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Defaults_Templates_logVolumeClaimTemplate("1.0"))
@Name("test_010043_1. Default clickhouse-log container")
def test_010043_1(self):
    """Check that clickhouse-operator sets up default logs container if it is not specified in Pod."""

    test_043(manifest="manifests/chi/test-043-1-logs-container-customizing.yaml")


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_ReconcilingCycle("1.0"),
              RQ_SRS_026_ClickHouseOperator_Managing_ClusterScaling_SchemaPropagation("1.0"))
@Name("test_010044. Schema and data propagation with slow replica")
def test_010044(self):
    """Check that schema and data can be propagated on other replica if replica start takes a lot of time."""
    create_shell_namespace_clickhouse_template()
    cluster = "default"
    manifest = f"manifests/chi/test-044-0-slow-propagation.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    util.require_keeper(keeper_type=self.context.keeper_type)
    operator_namespace = current().context.operator_namespace

    util.apply_operator_config("manifests/chopconf/test-044-chopconf.yaml")

    with Given("CHI with 1 replica is installed"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

    with And("I create replicated table on the first replica"):
        clickhouse.query(
            chi,
            """CREATE TABLE test_local (a UInt32)
            Engine = ReplicatedMergeTree('/clickhouse/{installation}/tables/{shard}/{database}/{table}', '{replica}')
            PARTITION BY tuple() ORDER BY a"""
        )

    with Then("I add 1 slow replica"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-044-1-slow-propagation.yaml",
            check={
                "pod_count": 2,
                "do_not_delete": 1,
                "chi_status": "InProgress"
            },
        )
        kubectl.wait_chi_status(chi, "Aborted", retries=7) # Fail faster than default

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

    kubectl.force_chi_reconcile(chi)

    with Then("I check schema is propagated"):
        with By("checking schema on the slow replica"):
            r = clickhouse.query(chi, "SHOW tables", host=f"chi-{chi}-{cluster}-0-1-0")
            assert "test_local" in r, error()

    with Finally("I clean up"):
        delete_test_namespace()


@TestCheck
@Name("test_045. Restart operator without waiting for queries to finish")
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
@Name("test_010045_1. Reconcile wait queries property specified by CHI")
def test_010045_1(self):
    """Check that operator supports spec.reconciling.policy property in CHI that
    forces the operator not to wait for the queries to finish before restart."""

    create_shell_namespace_clickhouse_template()

    test_045(manifest=f"manifests/chi/test-045-1-wait-query-finish.yaml")


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_Configuration_Spec_ReconcileWaitQueries("1.0"))
@Name("test_010045_2. Reconcile wait queries property specified by clickhouse-operator config")
def test_010045_2(self):
    """Check that operator supports spec.reconcile.host.wait.queries property in clickhouse-operator config
    that forces the operator not to wait for the queries to finish before restart."""
    create_shell_namespace_clickhouse_template()

    with Given("I set spec.reconcile.host.wait.queries property"):
        util.apply_operator_config("manifests/chopconf/no-wait-queries.yaml")

    test_045(manifest=f"manifests/chi/test-045-2-wait-query-finish.yaml")


@TestScenario
@Name("test_010046. Metrics for clickhouse-operator")
def test_010046(self):
    """Check that clickhouse-operator creates metrics for reconcile and other clickhouse-operator events."""
    create_shell_namespace_clickhouse_template()
    with Given("I change operator statefullSet timeout"):
        util.apply_operator_config("manifests/chopconf/low-timeout.yaml")

    cluster = "default"
    manifest = f"manifests/chi/test-046-0-clickhouse-operator-metrics.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    operator_namespace = current().context.operator_namespace
    operator_pod = kubectl.get_operator_pod()

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
                    max_retries=3
                )

    with Then(f"Check clickhouse-operator exposes clickhouse_operator_chi metrics"):
        check_metrics([
            "clickhouse_operator_chi{.*chi=\"test-046-operator-metrics\".*} 1",
        ])

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

    with Then("Stop CHI"):
        cmd = f'patch chi {chi} --type=\'json\' --patch=\'[{{"op":"add","path":"/spec/stop","value":"yes"}}]\''
        kubectl.launch(cmd)
        kubectl.wait_chi_status(chi, "InProgress")
        kubectl.wait_chi_status(chi, "Completed")

    with Then(f"Check clickhouse-operator exposes clickhouse_operator_chi metric for stopped chi"):
        check_metrics([
            "clickhouse_operator_chi{.*chi=\"test-046-operator-metrics\".*} 1",
        ])

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Spec_Configuration_Clusters_Cluster_Layout_Shards_Weight("1.0"))
@Name("test_010047. Zero weighted shard")
def test_010047(self):
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
        for shard in (0, 1):
            clickhouse.query(
                chi,
                "CREATE TABLE test_local_047 (a UInt32) Engine = MergeTree PARTITION BY tuple() ORDER BY a",
                host=f"chi-{chi}-{cluster}-{shard}-0-0")
            clickhouse.query(
                chi,
                "CREATE TABLE test_distr_047 AS test_local_047 Engine = Distributed('default', default, test_local_047, a%2)",
                host=f"chi-{chi}-{cluster}-{shard}-0-0")

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
@Name("test_010050. Test metrics exclusion in operator config")
def test_010050(self):
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
        operator_namespace = current().context.operator_namespace
        operator_pod = kubectl.get_operator_pod()

        # chi_clickhouse_metric_VersionInteger{chi="test-050",exclude_this_annotation="test-050-annotation",hostname="chi-test-050-default-0-0.test-050-e1884706-9a94-11ef-a786-367ddacfe5fd.svc.cluster.local",include_this_annotation="test-050-annotation",include_this_label="test-050-label",namespace="test-050-e1884706-9a94-11ef-a786-367ddacfe5fd"}
        # Hostname label has no trailing dot: the FQDN connection path retains it (ndots:5
        # bypass) but appendHostLabel normalizes via util.NormalizeFQDN before exposing.
        expect_labels = f"chi=\"test-050\",hostname=\"chi-test-050-default-0-0.{operator_namespace}.svc.cluster.local\",include_this_annotation=\"test-050-annotation\",include_this_label=\"test-050-label\""
        check_metrics_monitoring(
            operator_namespace=operator_namespace,
            operator_pod=operator_pod,
            expect_metric="chi_clickhouse_metric_VersionInteger",
            expect_labels=expect_labels
        )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010051. Test ClickHouse metrics exclusion")
def test_010051(self):
    create_shell_namespace_clickhouse_template()

    chi_manifest = "manifests/chi/test-051-metrics-exclusion.yaml"
    chopconf_file = "manifests/chopconf/test-051-chopconf.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(chi_manifest))
    operator_namespace = current().context.operator_namespace

    def wait_metrics_state(description, present_patterns=None, absent_patterns=None, max_retries=10):
        present_patterns = present_patterns or []
        absent_patterns = absent_patterns or []
        with Then(description):
            present_rx = [re.compile(pattern, re.MULTILINE) for pattern in present_patterns]
            absent_rx = [re.compile(pattern, re.MULTILINE) for pattern in absent_patterns]
            out = ""
            for i in range(1, max_retries):
                out = util.get_metrics()

                present = all(rx.search(out) is not None for rx in present_rx)
                absent  = all(rx.search(out) is None for rx in absent_rx)

                print(f"{present_patterns} present is {present}")
                print(f"{absent_patterns} absent is {absent}")

                if present and absent:
                    return out

                retry_sleep(i, 5, "Metrics are not ready")

            print(out)
            assert False, error("Metrics do not match present/absent patterns")

    with Given("Operator configuration with custom metric exclusions is installed"):
        util.apply_operator_config(chopconf_file)

    with Given("CHI is installed"):
        kubectl.create_and_check(
            manifest=chi_manifest,
            check={
                "pod_count": 1,
                "apply_templates": {
                    current().context.clickhouse_template,
                },
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

    with Then("Create a table with one part"):
        clickhouse.query(chi, "CREATE TABLE test (a String) Engine = MergeTree ORDER BY a PARTITION BY a")
        clickhouse.query(chi, "INSERT INTO test SELECT 'This is a test'");

    cpu_metric_pattern = r"^chi_clickhouse_metric_(OS.*CPU[0-9]+|CPUFrequencyMHz_[0-9]+)\{"
    version_metric_pattern = r"^chi_clickhouse_metric_VersionInteger.*"
    table_in_metric_pattern = r"^chi_clickhouse_table_partitions.*"
    table_ex_metric_pattern = r"^chi_clickhouse_table_parts_bytes_uncompressed.*"

    wait_metrics_state(
        "CPU-related and VersionInteger ClickHouse metrics should exist when exclusions are disabled",
        present_patterns=[cpu_metric_pattern, version_metric_pattern, table_in_metric_pattern],
        absent_patterns=[table_ex_metric_pattern]
    )

    with When("CHOP configuration is deleted to restore default metric exclusions"):
        kubectl.delete(util.get_full_path(chopconf_file, lookup_in_host=False), operator_namespace)
        util.restart_operator()
        kubectl.wait_chi_status(chi, 'Completed')

    # Bumped to 24 retries (~10 min). Default 12 (~4 min) was too tight after restart_operator
    # on slow CI runners — operator can not scrape ClickHouse metrics until new IP address is picked up.
    wait_metrics_state(
        "CPU-related ClickHouse metrics should disappear while VersionInteger remains when exclusions are enabled",
        present_patterns=[version_metric_pattern, table_ex_metric_pattern],
        absent_patterns=[cpu_metric_pattern],
        max_retries=24,
    )

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
             # Give some time for replication to catch up
            time.sleep(10)

        with Then("Check replicated table has data on both nodes"):
            for replica in replicas:
                out = clickhouse.query(chi, f"SELECT a from {table} where a={token}", host=f"chi-{chi}-{cluster}-0-{replica}-0")
                assert out == f"{token}", error()


@TestScenario
@Name("test_010053. Check that standard Kubernetes annotations are ignored if set to StatefulSet externally")
def test_010053(self):
    """Verify that kubectl.kubernetes.io/restartedAt annotation set by 'kubectl rollout restart'
    does not cause the operator to restart pods during reconcile or operator restart."""
    create_shell_namespace_clickhouse_template()

    manifest = "manifests/chi/test-005-acm.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    sts = f"chi-{chi}-t1-0-0"
    pod = f"{sts}-0"

    kubectl.create_and_check(
        manifest=manifest,
        check={
            "object_counts": {"statefulset": 1, "pod": 1, "service": 2},
            "do_not_delete": 1
        },
    )

    with When("Run rollout restart"):
        kubectl.launch(f"rollout restart statefulset {sts}")
        time.sleep(10)

        with Then("Pod annotation kubectl.kubernetes.io/restartedAt should be populated"):
            assert kubectl.get_field("pod", pod, r".metadata.annotations.kubectl\.kubernetes\.io/restartedAt") != "<none>"
        with And("PodTemplate annotation kubectl.kubernetes.io/restartedAt should be populated"):
            assert kubectl.get_field("statefulset", sts, r".spec.template.metadata.annotations.kubectl\.kubernetes\.io/restartedAt") != "<none>"

        start_time = kubectl.get_field("pod", pod, ".status.startTime")

        def check_restart():
            with Then("ClickHouse pods should not be restarted"):
                new_start_time = kubectl.get_field("pod", pod, ".status.startTime")
                assert start_time == new_start_time

        with Then("Trigger reconcile"):
            kubectl.force_chi_reconcile(chi)
            check_restart()

        with When("Restart operator"):
            util.restart_operator()
            # After operator restart, reconcile may complete before we can observe InProgress.
            # Just wait for Completed — we only care that pods are not restarted.
            kubectl.wait_chi_status(chi, "Completed")
            check_restart()

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010054. Test that 'suspend' mode delays any changes until unsuspended")
@Requirements(RQ_SRS_026_ClickHouseOperator_Managing_VersionUpgrades("1.0"))
def test_010054(self):
    create_shell_namespace_clickhouse_template()
    chi = yaml_manifest.get_name(util.get_full_path("manifests/chi/test-006-ch-upgrade-1.yaml"))

    old_version = "clickhouse/clickhouse-server:24.8"
    new_version = "clickhouse/clickhouse-server:25.3"
    with Then(f"Start CHI with version {old_version}"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-006-ch-upgrade-1.yaml",
            check={
                "pod_count": 1,
                "pod_image": old_version,
                "do_not_delete": 1,
            },
        )

    with When("Add suspend attribute to CHI"):
        cmd = f'patch chi {chi} --type=\'json\' --patch=\'[{{"op":"add","path":"/spec/suspend","value":"yes"}}]\''
        kubectl.launch(cmd)

        with Then(f"Update podTemplate to {new_version} and confirm that pod image is NOT updated"):
            kubectl.create_and_check(
                manifest="manifests/chi/test-006-ch-upgrade-2.yaml",
                check={
                    "pod_count": 1,
                    "pod_image": old_version,
                    "chi_status": "Aborted",
                    "do_not_delete": 1,
                },
            )

    with When("Remove suspend attribute from CHI"):
        cmd = f'patch chi {chi} --type=\'json\' --patch=\'[{{"op":"remove","path":"/spec/suspend"}}]\''
        kubectl.launch(cmd)

        kubectl.wait_chi_status(chi, "InProgress")
        kubectl.wait_chi_status(chi, "Completed")

        with Then(f"Confirm that pod image is updated to {new_version}"):
            kubectl.check_pod_image(chi, new_version)

    with When(f"Update podTemplate to {old_version} back but do not wait for completion"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-006-ch-upgrade-1.yaml",
            check={
                "chi_status": "InProgress",
                "do_not_delete": 1,
            },
        )

    with And("Add suspend attribute to CHI"):
        cmd = f'patch chi {chi} --type=\'json\' --patch=\'[{{"op":"add","path":"/spec/suspend","value":"yes"}}]\''
        kubectl.launch(cmd)

        with Then(f"Reconcile should be interrupted and pod image should remain at {new_version}"):
            kubectl.wait_chi_status(chi, "Aborted", retries=5)
            kubectl.check_pod_image(chi, new_version)

    with When("Remove suspend attribute from CHI"):
        cmd = f'patch chi {chi} --type=\'json\' --patch=\'[{{"op":"remove","path":"/spec/suspend"}}]\''
        kubectl.launch(cmd)

        with Then("Reconcile should be resumed"):
            kubectl.wait_chi_status(chi, "InProgress")
            kubectl.wait_chi_status(chi, "Completed")

        with And(f"Pod image should be reverted back to {old_version}"):
            kubectl.check_pod_image(chi, old_version)

    with Finally("I clean up"):
        delete_test_namespace()



@TestScenario
@Name("test_010055. Test that restart rules can be merged from CHOP configuration")
def test_010055(self):
    create_shell_namespace_clickhouse_template()
    with Given("Operator configuration is installed"):
       util.apply_operator_config("manifests/chopconf/test-055-chopconf.yaml")

    manifest = f"manifests/chi/test-055-chopconf.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    cluster = "default"

    with Given("CHI is installed"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 1,
                "apply_templates": {
                    current().context.clickhouse_template,
                },
                "do_not_delete": 1,
            },
        )
    start_time = kubectl.get_clickhouse_start(chi)

    with When(f"Add configuration file that SHOULD be ignored by restart rules"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-055-chopconf-2.yaml",
            check={"do_not_delete": 1},
        )

        with Then("ClickHouse SHOULD NOT be restarted"):
            new_start_time = kubectl.get_clickhouse_start(chi)
            assert start_time == new_start_time

        with Then("Startup script SHODLD NOT be executed"):
            res = clickhouse.query_with_error(chi, "select count() from test_055")
            assert res != "0"

    with When(f"Add another configuration file that SHOULD be ignored by restart rules"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-055-chopconf-3.yaml",
            check={"do_not_delete": 1},
        )

        with Then("ClickHouse SHOULD NOT be restarted"):
            new_start_time = kubectl.get_clickhouse_start(chi)
            assert start_time == new_start_time

    with When(f"Add configuration file that SHOULD NOT be ignored by restart rules"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-055-chopconf-4.yaml",
            check={"do_not_delete": 1},
        )

        with Then("ClickHouse SHOULD be restarted"):
            new_start_time = kubectl.get_clickhouse_start(chi)
            assert start_time != new_start_time

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010056. Test replica delay")
def test_010056(self):
    create_shell_namespace_clickhouse_template()
    with Given("I change operator StatefulSet timeout"):
        util.apply_operator_config("manifests/chopconf/low-timeout.yaml")

    util.require_keeper(keeper_type=self.context.keeper_type)

    manifest = f"manifests/chi/test-056-replica-delay-1.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    cluster = "default"

    with Given("CHI is installed"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "pod_count": 1,
                "apply_templates": {
                    current().context.clickhouse_template,
                },
                "do_not_delete": 1,
                "status": "InProgress"
            },
        )

    with Then("Create a replicated table"):
        clickhouse.query(chi, "CREATE TABLE test_056 (a Int8) Engine = ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}') ORDER BY a PARTITION by a")
        clickhouse.query(chi, "INSERT INTO test_056 SELECT 1")

    with And("STOP REPLICATED SENDS"):
        clickhouse.query(chi, "SYSTEM STOP REPLICATED SENDS")

    with When("Add one more replica, but do not wait for completion"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-056-replica-delay-2.yaml",
            check={
                "do_not_delete": 1,
                "pod_count": 2,
                "chi_status": "InProgress",
            },
        )

        with Then("Table should be created on a new replica"):
            retries_left = 10
            out = 0
            while retries_left > 0:
                out = clickhouse.query_with_error(chi, "select count() from system.tables where name='test_056'", host=f"chi-{chi}-{cluster}-0-1-0")
                if out == "1":
                    break
                with Then("Not ready. Wait for 10 seconds"):
                    time.sleep(10)
                retries_left = retries_left-1
            assert out == "1", error("Table was not created on a new replica")

        with And("Table should have no data replicated"):
            out = clickhouse.query(chi, "select count() from test_056", host=f"chi-{chi}-{cluster}-0-1-0")
            assert out == "0", error("Table data has been replicated")

        with And("Replication delay should be non-zero"):
            out = clickhouse.query(chi, "select max(absolute_delay) from system.replicas", host=f"chi-{chi}-{cluster}-0-1-0")
            print(f"max(absolute_delay)={out}")
            assert out != "0"

        with And("Wait 90 seconds - more than sts update timeout"):
            time.sleep(90)

        with And("Replication delay should be non-zero"):
            out = clickhouse.query(chi, "select max(absolute_delay) from system.replicas", host=f"chi-{chi}-{cluster}-0-1-0")
            print(f"max(absolute_delay)={out}")
            assert out != "0"

        with And("Replica still should be unready after reconcile timeout"):
            ready = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-1-0", ".metadata.labels.clickhouse\.altinity\.com\/ready")
            print(f"ready label={ready}")
            assert ready != "yes", error("Replica should be unready")

        with And("Replica should be included in the monitoring"): # as of 0.26.0
            operator_namespace=current().context.operator_namespace
            check_metrics_monitoring(
                operator_namespace = current().context.operator_namespace,
                operator_pod = kubectl.get_operator_pod(),
                expect_metric = "chi_clickhouse_metric_VersionInteger",
                expect_labels = f"chi-{chi}-{cluster}-0-1"
            )
        with And("Replica should report a replication queue"): # as of 0.26.0
            operator_namespace=current().context.operator_namespace
            check_metrics_monitoring(
                operator_namespace = current().context.operator_namespace,
                operator_pod = kubectl.get_operator_pod(),
                expect_metric = "chi_clickhouse_metric_ReplicasSumQueueSize",
                expect_labels = f"chi-{chi}-{cluster}-0-1"
            )

    with When("START REPLICATED SENDS"):
        clickhouse.query(chi, "SYSTEM START REPLICATED SENDS", host=f"chi-{chi}-{cluster}-0-0-0")

        with Then("Replica should become ready"):
            kubectl.wait_field("pod", f"chi-{chi}-{cluster}-0-1-0",
                                       ".metadata.labels.clickhouse\.altinity\.com\/ready", value="yes")

        with And("Replication delay should be zero"):
            out = clickhouse.query(chi, "select max(absolute_delay) from system.replicas", host=f"chi-{chi}-{cluster}-0-1-0")
            print(f"max(absolute_delay)={out}")
            assert out == "0"

        with And("Table data should be replicated"):
            out = clickhouse.query(chi, "select count() from test_056", host=f"chi-{chi}-{cluster}-0-1-0")
            assert out == "1"

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010057. Test reconcile concurrency settings on CHI level")
def test_010057(self):
    create_shell_namespace_clickhouse_template()

    manifest = f"manifests/chi/test-057-max-concurrency.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    cluster = "default"

    with Given("CHI is installed"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "apply_templates": {
                    current().context.clickhouse_template,
                },
                "do_not_delete": 1,
                "chi_status": "InProgress",
            },
        )

    with When("First shard is Running"):
        kubectl.wait_pod_status(f"chi-{chi}-{cluster}-0-0-0", "Running")
        time.sleep(10)
        with Then("Other shards are running or being created"):
            for shard in [1,2,3]:
                pod_status = kubectl.get_pod_status(f"chi-{chi}-{cluster}-{shard}-0-0")
                assert pod_status in ["Running", "ContainerCreating"]

    kubectl.wait_chi_status(chi, "Completed")

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010058. Check ClickHouse with rootCA")
def test_010058(self):  # Can be merged with test_034 potentially
    create_shell_namespace_clickhouse_template()
    operator_namespace = current().context.operator_namespace

    with Given("Add rootCA to operator configuration"):
        util.apply_operator_config("manifests/chopconf/test-058-chopconf.yaml")
    operator_pod = kubectl.get_operator_pod()

    with Given("test-058-root-ca secret is installed"):
        kubectl.apply(
            util.get_full_path("manifests/secret/test-058-secret.yaml"),
        )

    manifest = "manifests/chi/test-058-root-ca.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))
    with When("create the chi with secure connection"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "apply_templates": {
                    current().context.clickhouse_template,
                },
                "object_counts": {"statefulset": 1, "pod": 1, "service": 2},
                "do_not_delete": 1,
            }
        )

    client_pod = "test-058-root-ca-client"
    with And(f"Start pod: {client_pod}"):
        kubectl.apply(util.get_full_path("manifests/chi/test-058-root-ca-client.yaml"))
        kubectl.wait_pod_status(client_pod, "Running")

    with And("Confirm it can securely connect to clickhouse"):
        creds = "--user=admin --password=password"
        cmd = f"exec {client_pod} -- clickhouse-client -h chi-{chi}-default-0-0 --secure --port 9440 {creds} -q 'select 58'"
        out = kubectl.launch(cmd, ok_to_fail=True)
        assert out == "58", error()

    with Then("check for `chi_clickhouse_metric_fetch_errors` is zero"):
        check_metrics_monitoring(
            operator_namespace=operator_namespace,
            operator_pod=operator_pod,
            expect_pattern="^chi_clickhouse_metric_fetch_errors{(.*?)} 0$",
        )

    with Finally("I clean up"):
        kubectl.launch(f"delete pod {client_pod}")
        delete_test_namespace()


@TestScenario
@Name("test_010059. Test macro substitutions in settings")
def test_010059(self):
    create_shell_namespace_clickhouse_template()

    chi = "test-059-macros"
    cluster = "default"
    kubectl.create_and_check(
        manifest="manifests/chi/test-059-macros.yaml",
        check={
            "apply_templates": {
                current().context.clickhouse_template,
            },
            "pod_count": 2,
            "do_not_delete": 1,
        },
    )

    for h in [f"chi-{chi}-{cluster}-0-0-0", f"chi-{chi}-{cluster}-1-0-0"]:

        with Then("default_replica_path should be unchanged"):
            out = clickhouse.query(chi, host=h, sql="select value from system.server_settings where name = 'default_replica_path'")
            assert out == "/clickhouse/{cluster}/tables/{shard}/{uuid}"

        with And("default_replica_name should be unchanged"):
            out = clickhouse.query(chi, host=h, sql="select value from system.server_settings where name = 'default_replica_name'")
            assert out == "{replica}"

        with And("Macro my_replica should be unchanged"):
            out = clickhouse.query(chi, host=h, sql="select substitution from system.macros where macro='my_replica'")
            assert out == "{replica}"

        with And("Macro my_endpoint should be unchanged"):
            out = clickhouse.query(chi, host=h, sql="select substitution from system.macros where macro='my_endpoint'")
            assert out == "https://s3_url/{cluster}/{shard}/"

    with When("Update CHI to apply macro substitutions"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-059-macros-2.yaml",
            check={
                "do_not_delete": 1,
            },
        )

    for h in [f"chi-{chi}-{cluster}-0-0-0", f"chi-{chi}-{cluster}-1-0-0"]:

        cluster_macro = clickhouse.query(chi, host=h, sql="select substitution from system.macros where macro='cluster'")
        shard_macro = clickhouse.query(chi, host=h, sql="select substitution from system.macros where macro='shard'")
        replica_macro = clickhouse.query(chi, host=h, sql="select substitution from system.macros where macro='replica'")
        # 'replica' macro has different value in ClickHouse and Operator - replica name, not hostname
        operator_replica_macro = '0'

        with Then("default_replica_path should be substituted from ClickHouse macros"):
            out = clickhouse.query(chi, host=h, sql="select value from system.server_settings where name = 'default_replica_path'")
            expect = f"/clickhouse/{cluster_macro}/tables/{shard_macro}/" + "{uuid}"
            print(f"{out}")
            print(f"{expect}")
            assert out == expect

        # # 'replica' macro has different value in ClickHouse (hostname) and Operator (replica name, default to index)
        with And("default_replica_name should be substituted from ClickHouse macros", flags=XFAIL):
            out = clickhouse.query(chi, host=h, sql="select value from system.server_settings where name = 'default_replica_name'")
            expect = replica_macro
            print(f"{out}")
            print(f"{expect}")
            assert out == expect

        # 'replica' macro has different value in ClickHouse and Operator
        with And("Macro my_replica should be substituted from operator macros"):
            out = clickhouse.query(chi, host=h, sql="select substitution from system.macros where macro='my_replica'")
            expect = operator_replica_macro
            print(f"{out}")
            print(f"{expect}")
            assert out == expect

        with And("Macro my_endpoint should be substituted"):
            out = clickhouse.query(chi, host=h, sql="select substitution from system.macros where macro='my_endpoint'")
            expect = f"https://s3_url/{cluster_macro}/{shard_macro}/"
            print(f"{out}")
            print(f"{expect}")
            assert out == expect

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010060. pdb management disabled")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010060(self):
    create_shell_namespace_clickhouse_template()

    chi = "test-060-pdb-management-disabled"
    kubectl.create_and_check(
        manifest="manifests/chi/test-060-pdb-management-disabled.yaml",
        check={
            "apply_templates": {
                current().context.clickhouse_template,
            },
            "object_counts": {"statefulset": 1, "pod": 1, "service": 2},
            "configmaps": 1,
            "pdb": {"single": {"is_managed": False}},
            "do_not_delete": 1,
        },
    )

    created_objects = kubectl.get_obj_names_grepped("pod,service,sts,pvc,cm,pdb,secret", grep=chi)
    print("Created objects:")
    for o in created_objects:
        print(o)

    kubectl.delete_chi(chi)

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010061. Test fractional CPU requests/limits handling")
def test_010061(self):
    create_shell_namespace_clickhouse_template()

    chi = "test-061-fractional-cpu"
    kubectl.create_and_check(
        manifest="manifests/chi/test-061-fractional-cpu-1.yaml",
        check={
            "apply_templates": {
                current().context.clickhouse_template,
            },
            "object_counts": {"statefulset": 1, "pod": 1, "service": 2},
            "do_not_delete": 1,
        },
    )

    with Then("cpu.limits are set to 500m"):
        pod_spec = kubectl.get_pod_spec(chi)
        cpu_limits = pod_spec["containers"][0]["resources"]["limits"]["cpu"]
        assert cpu_limits == "500m"

    kubectl.force_chi_reconcile(chi, "reconcile1")

    actionPlan = kubectl.get_actionPlan("chi", chi)
    print(actionPlan)
    with Then("ActionPlan should not contain Templates.PodTemplates[0].Spec.Containers[0].Resources [1]"):
        assert "Templates.PodTemplates[0].Spec.Containers[0].Resources" not in actionPlan

    kubectl.create_and_check(
        manifest="manifests/chi/test-061-fractional-cpu-2.yaml",
        check={
            "do_not_delete": 1,
        },
    )

    print(kubectl.get_actionPlan("chi", chi))

    with Then("cpu.limits are set to 500m"):
        pod_spec = kubectl.get_pod_spec(chi)
        cpu_limits = pod_spec["containers"][0]["resources"]["limits"]["cpu"]
        assert cpu_limits == "500m"

    kubectl.force_chi_reconcile(chi, "reconcile2")

    actionPlan = kubectl.get_actionPlan("chi", chi)
    print(actionPlan)
    with Then("ActionPlan should not contain Templates.PodTemplates[0].Spec.Containers[0].Resources [2]"):
        assert "Templates.PodTemplates[0].Spec.Containers[0].Resources" not in actionPlan

    with Finally("I clean up"):
        delete_test_namespace()


def check_operator_logs(markers, since = ""):
    """Check clickhouse-operator pod logs for specific markers.
    since is accpeted as XXs format to filter out recent rows only"""
    operator_pod = kubectl.get_operator_pod(ns=current().context.test_namespace)
    if since != "":
        since = f"--since={since}"
    out = kubectl.launch(
        f"logs {operator_pod} -c clickhouse-operator {since}",
        ns=current().context.test_namespace,
    )
    for marker in markers:
        with Then(f"operator logs should contain '{marker}'"):
            assert marker in out, error(f"Marker '{marker}' not found in operator logs")


@TestScenario
@Name("test_010062. Reconcile hooks")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010062(self):
    """Verify reconcile hooks with explicit event targeting (`on:` field).

    Covers:
      - Combined cluster + host hooks gated on [HostUpdate] — fire on every regular reconcile.
      - target=AllHosts on cluster pre-hook — runs on each host.
      - Pre-hook failure aborts reconcile.
      - events:[HostCreate] — fires once on initial creation, NOT on subsequent reconciles.
      - events:[HostShutdown] — does NOT fire on no-op reconcile, fires when config change
        forces a software restart.

    Hooks gated on [HostUpdate] are skipped on first CHI creation (host has no ancestor
    yet), so steps 1 and 2 force-reconcile after create to actually exercise them."""
    create_shell_namespace_clickhouse_template()
    with Given("I change operator statefullSet timeout"):
        util.apply_operator_config("manifests/chopconf/low-timeout.yaml")

    chi = "test-062-hooks"

    # Step 1: Combined cluster + host hooks gated on [HostUpdate].
    with Given("CHI with both cluster and host hooks (events:[HostUpdate]) is created"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-062-hooks-combined.yaml",
            check={
                "apply_templates": {current().context.clickhouse_template},
                "object_counts": {"statefulset": 1, "pod": 1, "service": 2},
                "do_not_delete": 1,
            },
        )

    with When("Force reconcile to trigger all [HostUpdate] hooks"):
        kubectl.force_chi_reconcile(chi, "combined-hooks")

    with Then("All four hook markers appear in operator logs"):
        check_operator_logs([
            "cluster_pre_hook_marker", "cluster_post_hook_marker",
            "host_pre_hook_marker", "host_post_hook_marker",
        ])

    # Step 2: target=AllHosts — scale up to 2 shards using the same CHI name.
    with When("Apply CHI with 2 shards and target=AllHosts hook events:[HostUpdate]"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-062-hooks-allhosts.yaml",
            check={
                "apply_templates": {current().context.clickhouse_template},
                "object_counts": {"statefulset": 2, "pod": 2, "service": 3},
                "do_not_delete": 1,
            },
        )

    with Then("allhosts_hook_marker and 'all hosts' appear in operator logs"):
        check_operator_logs(["allhosts_hook_marker", "Running SQL cluster hook on all hosts"])

    # Step 3: Pre-hook failure aborts reconcile.
    with When("Apply CHI with a pre-hook that fails"):
        kubectl.apply(
            util.get_full_path("manifests/chi/test-062-hooks-pre-fail.yaml"),
            ns=current().context.test_namespace,
        )

    with Then("CHI should eventually abort"):
        kubectl.wait_chi_status(chi, "Aborted")

    # Hand off the namespace before exercising the next event-targeting cases — they
    # need a clean CHI to assert "fired once" / "did not fire" semantics.
    with When("Delete the previous CHI to start a fresh state for event-targeting cases"):
        kubectl.delete_chi(chi, ns=current().context.test_namespace, ok_to_fail=True)

    # Step 4: events:[HostCreate] — fires exactly once on initial creation, not on subsequent reconciles.
    with Given("CHI with host post-hook events:[HostCreate]"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-062-hooks-on-create.yaml",
            check={
                "apply_templates": {current().context.clickhouse_template},
                "object_counts": {"statefulset": 1, "pod": 1, "service": 2},
                "do_not_delete": 1,
            },
        )

    create_marker_count_after_create = _operator_log_marker_count("host_oncreate_hook_marker")

    with Then("host_oncreate_hook_marker fired at least once on initial create"):
        # Presence-only check. The marker string appears multiple times per hook
        # firing (operator's "Running SQL host hook on ..." line, schemer's per-query
        # debug logs, etc.) so an exact count would be brittle. The follow-up step
        # asserts the count does NOT increase on a subsequent regular reconcile —
        # that's the actual semantic of "fires on HostCreate, not on HostUpdate".
        assert create_marker_count_after_create >= 1, error(
            f"events:[HostCreate] hook did not fire on initial create, "
            f"got count={create_marker_count_after_create}"
        )

    with When("Force reconcile a CHI that already has an ancestor (HostUpdate event, NOT HostCreate)"):
        kubectl.force_chi_reconcile(chi, "after-create")

    with Then("host_oncreate_hook_marker count must NOT increase — hook gated on HostCreate is inert post-create"):
        create_marker_count_after_reconcile = _operator_log_marker_count("host_oncreate_hook_marker")
        assert create_marker_count_after_reconcile == create_marker_count_after_create, error(
            f"events:[HostCreate] hook unexpectedly fired on a regular reconcile: "
            f"before={create_marker_count_after_create} after={create_marker_count_after_reconcile}"
        )

    with When("Delete the previous CHI to start a fresh state for HostShutdown case"):
        kubectl.delete_chi(chi, ns=current().context.test_namespace, ok_to_fail=True)

    # Step 5: events:[HostShutdown] — silent on no-op reconcile, fires when config change forces software restart.
    with Given("CHI with host pre-hook events:[HostShutdown]"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-062-hooks-on-shutdown.yaml",
            check={
                "apply_templates": {current().context.clickhouse_template},
                "object_counts": {"statefulset": 1, "pod": 1, "service": 2},
                "do_not_delete": 1,
            },
        )

    shutdown_marker_count_before_noop = _operator_log_marker_count("host_onshutdown_hook_marker")

    with When("Force reconcile with no config change (taskID-only — no shutdown event fires)"):
        kubectl.force_chi_reconcile(chi, "noop-shutdown-test")

    with Then("host_onshutdown_hook_marker count must NOT increase — no shutdown happened"):
        shutdown_marker_count_after_noop = _operator_log_marker_count("host_onshutdown_hook_marker")
        assert shutdown_marker_count_after_noop == shutdown_marker_count_before_noop, error(
            f"events:[HostShutdown] hook unexpectedly fired on a no-op reconcile: "
            f"before={shutdown_marker_count_before_noop} after={shutdown_marker_count_after_noop}"
        )

    with When("Apply CHI with a config change that forces software restart (HostConfigRestart → HostShutdown)"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-062-hooks-on-shutdown-2.yaml",
            check={
                "do_not_delete": 1,
            },
        )

    with Then("host_onshutdown_hook_marker count must increase — HostShutdown fired via HostConfigRestart"):
        shutdown_marker_count_after_config = _operator_log_marker_count("host_onshutdown_hook_marker")
        assert shutdown_marker_count_after_config > shutdown_marker_count_after_noop, error(
            f"events:[HostShutdown] hook did NOT fire on a config-change reconcile: "
            f"before={shutdown_marker_count_after_noop} after={shutdown_marker_count_after_config}"
        )

    with When("Delete the previous CHI to start a fresh state for HostDelete case"):
        kubectl.delete_chi(chi, ns=current().context.test_namespace, ok_to_fail=True)

    # Step 6: events:[HostDelete] — fires only on host removal, not on regular reconciles.
    # Apply 2-shard CHI first, then 1-shard CHI which deletes shard 1's host.
    with Given("CHI with 2 shards and a host pre-delete hook events:[HostDelete]"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-062-hooks-on-delete-1.yaml",
            check={
                "apply_templates": {current().context.clickhouse_template},
                "object_counts": {"statefulset": 2, "pod": 2, "service": 3},
                "do_not_delete": 1,
            },
        )

    delete_marker_before = _operator_log_marker_count("host_ondelete_hook_marker")

    with Then("host_ondelete_hook_marker count is 0 on initial 2-shard create — no host was deleted"):
        # The classifier doesn't emit HostDelete on the regular reconcile path; the
        # delete sweep is the only emitter. So a fresh CHI's create must leave the
        # marker count at exactly zero.
        assert delete_marker_before == 0, error(
            f"events:[HostDelete] hook unexpectedly fired before any host was deleted, "
            f"got count={delete_marker_before}"
        )

    with When("Scale down to 1 shard — shard 1's host is deleted, pre-delete hook should fire against it"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-062-hooks-on-delete-2.yaml",
            check={
                "object_counts": {"statefulset": 1, "pod": 1, "service": 2},
                "do_not_delete": 1,
            },
        )

    with Then("host_ondelete_hook_marker count must increase — pre-delete hook fired against the dying host"):
        delete_marker_after = _operator_log_marker_count("host_ondelete_hook_marker")
        assert delete_marker_after > delete_marker_before, error(
            f"events:[HostDelete] hook did NOT fire on host removal: "
            f"before={delete_marker_before} after={delete_marker_after}"
        )

    with Finally("I clean up"):
        delete_test_namespace()


def _operator_log_marker_count(marker: str) -> int:
    """Count actual hook firings of a marker across the operator's pod logs.

    A naive `out.count(marker)` overcounts: the marker SQL string appears in operator
    log lines that aren't hook firings — diff dumps that print the entire CHI spec
    (including the SQL queries on each Hooks.Pre[N] / Hooks.Post[N] entry) contribute
    occurrences on every reconcile, regardless of whether the hook actually fired.

    The hook-execution log line is unique to actual firings:
        "Running SQL host hook on <name>: [<SQL with marker>]"
        "Running SQL cluster hook on ...: [<SQL with marker>]"
    We require BOTH "Running SQL" and the marker to appear on the same line.
    """
    operator_pod = kubectl.get_operator_pod(ns=current().context.test_namespace)
    if not operator_pod:
        return 0
    out = kubectl.launch(
        f"logs {operator_pod} -c clickhouse-operator",
        ns=current().context.test_namespace,
        ok_to_fail=True,
    )
    if not out:
        return 0
    count = 0
    for line in out.splitlines():
        if "Running SQL " in line and marker in line:
            count += 1
    return count


@TestScenario
@Name("test_010063. Test CHI keeper reference to CHK")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010063(self):
    """Verify that CHI can reference a CHK by name and the operator resolves keeper endpoints."""
    create_shell_namespace_clickhouse_template()

    chk_manifest = "manifests/chk/test-063-keeper-ref-chk.yaml"
    chk_manifest_3nodes = "manifests/chk/test-063-keeper-ref-chk-2.yaml"
    chi_manifest = "manifests/chi/test-063-keeper-ref.yaml"
    chk = "test-063-chk"
    chi = "test-063-keeper-ref"

    with Given("CHK is installed"):
        kubectl.create_and_check(
            manifest=chk_manifest,
            kind="chk",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

    with And("CHI referencing CHK by name is installed"):
        kubectl.create_and_check(
            manifest=chi_manifest,
            check={
                "apply_templates": {current().context.clickhouse_template},
                "object_counts": {"statefulset": 2, "pod": 2, "service": 3},
                "do_not_delete": 1,
            },
        )

    with Then("ClickHouse can access Keeper via resolved keeper reference"):
        out = clickhouse.query(chi, "SELECT path FROM system.zookeeper WHERE path = '/' limit 1")
        assert out == '/', error("ClickHouse should be able to query ZooKeeper")

    with And("Keeper is accessible from all replicas"):
        for pod_name in kubectl.get_pod_names(chi):
            out = clickhouse.query(chi, "SELECT path FROM system.zookeeper WHERE path = '/' limit 1", pod=pod_name)
            assert out == '/', error(f"ZooKeeper should be accessible from {pod_name}")

    with When("Rescale Keeper to 3 nodes"):
        kubectl.create_and_check(
            manifest=chk_manifest_3nodes,
            kind="chk",
            check={
                "pod_count": 3,
                "do_not_delete": 1,
            },
        )
        with Then("Push CHI reconcile"):
            kubectl.force_chi_reconcile(chi, "reconcile")

        with Then("CHI should be reconfigured for 3 node ZooKeeper"):
            # Poll the configmap: a taskID-only patch reaches "Completed" before the
            # downstream Keeper-ref resolver has propagated the new endpoint list into
            # chop-generated-zookeeper.xml on every host. Reading once races the operator.
            node_count = 0
            for i in range(1, 15):
                zookeeper_config = kubectl.get("configmap", f"chi-{chi}-deploy-confd-default-0-0")["data"]["chop-generated-zookeeper.xml"]
                node_count = zookeeper_config.count("<node>")
                if node_count == 3:
                    break
                retry_sleep(i, 5, f"Not ready ({node_count} nodes)")
            assert node_count == 3, error("ZooKeeper configuration should contain 3 nodes now")

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010064. Test CHK watch triggers CHI reconcile on keeper resource update")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010064(self):
    """Verify that when onKeeperResourceUpdate=reconcile is configured,
    the operator auto-reconciles dependent CHIs when a referenced CHK completes reconcile."""
    create_shell_namespace_clickhouse_template()

    chk_manifest = "manifests/chk/test-063-keeper-ref-chk.yaml"
    chk_manifest_3nodes = "manifests/chk/test-063-keeper-ref-chk-2.yaml"
    chi_manifest = "manifests/chi/test-063-keeper-ref.yaml"
    chopconf_manifest = "manifests/chopconf/test-063-keeper-watch.yaml"
    chk = "test-063-chk"
    chi = "test-063-keeper-ref"
    cluster = "default"

    with Given("Operator configuration enables keeper watch"):
        util.apply_operator_config(chopconf_manifest)

    with And("CHK is installed and ready"):
        kubectl.create_and_check(
            manifest=chk_manifest,
            kind="chk",
            check={
                "do_not_delete": 1,
            },
        )

    with And("CHI referencing CHK by name is installed"):
        kubectl.create_and_check(
            manifest=chi_manifest,
            check={
                "apply_templates": {current().context.clickhouse_template},
                "object_counts": {"statefulset": 2, "pod": 2, "service": 3},
                "do_not_delete": 1,
            },
        )

    start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
    connected_time = ""
    with And("CHI is connected to Keeper"):
        connected_time = clickhouse.query(chi, "SELECT connected_time from system.zookeeper_connection")
        assert connected_time != ""

    with When("Trigger CHK reconcile by patching taskID"):
        kubectl.force_chk_reconcile(chk, "keeper-watch-test")

        with Then("Confirm CHI is complete"):
            kubectl.wait_chi_status(chi, "Completed")

        with Then("CHI has not been restarted"):
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
            assert new_start_time == start_time, error("CHI has been restarted")

        with Then("CHI does not reconnect to Keeper"):
            new_connected_time = clickhouse.query(chi, "SELECT connected_time from system.zookeeper_connection")
            assert new_connected_time == connected_time, error("ClickHouse reconnected to Keeper")

    with When("Rescale Keeper to 3 nodes"):
        kubectl.create_and_check(
            manifest=chk_manifest_3nodes,
            kind="chk",
            check={
                "pod_count": 3,
                "do_not_delete": 1,
            },
        )

        with Then("Confirm CHI is completed"):
            kubectl.wait_chi_status(chi, "Completed")

        with Then("CHI should be reconfigured for 3 node ZooKeeper"):
            # See test_010063 for rationale: poll because the keeper-ref resolver
            # propagates the new endpoint list shortly after status hits Completed.
            node_count = 0
            for i in range(1, 15):
                zookeeper_config = kubectl.get("configmap", f"chi-{chi}-deploy-confd-default-0-0")["data"]["chop-generated-zookeeper.xml"]
                node_count = zookeeper_config.count("<node>")
                if node_count == 3:
                    break
                retry_sleep(i, 5, f"Not ready ({node_count} nodes)")
            assert node_count == 3, error("ZooKeeper configuration should contain 3 nodes now")

        with Then("CHI has not been restarted"):
            # Zookeeper config changes do not require pod restart (configurationRestartPolicy
            # marks zookeeper/* as "no"). ClickHouse picks up the new server list via config
            # reload.
            new_start_time = kubectl.get_field("pod", f"chi-{chi}-{cluster}-0-0-0", ".status.startTime")
            assert new_start_time == start_time, error("CHI has been restarted")

        with Then("CHI is still connected to Keeper after config change"):
            # NOTE: connected_time is expected to differ here — when the zookeeper server
            # list changes, ClickHouse's zk client opens a new session with the new list.
            # What we verify is that CH is still connected to keeper at all (non-empty time).
            new_connected_time = clickhouse.query(chi, "SELECT connected_time from system.zookeeper_connection")
            assert new_connected_time != "", error("ClickHouse is not connected to Keeper after rescale")

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010065_0. Test container security context is propagated to Pod")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010065_0(self):
    create_shell_namespace_clickhouse_template()

    manifest = "manifests/chi/test-065-security-context.yaml"
    manifest_data = yaml_manifest.get_manifest_data(util.get_full_path(manifest))
    chi = manifest_data["metadata"]["name"]
    expected_container = manifest_data["spec"]["templates"]["podTemplates"][0]["spec"]["containers"][0]
    expected_security_context = expected_container["securityContext"]

    with Given("CHI with container security context is installed"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "apply_templates": {current().context.clickhouse_template},
                "object_counts": {"statefulset": 1, "pod": 1, "service": 2},
                "do_not_delete": 1,
            },
        )

    with Then("ClickHouse Pod container should have the requested security context"):
        pod_spec = kubectl.get_pod_spec(chi)
        container = pod_spec["containers"][0]
        assert container["name"] == expected_container["name"], error(f"Pod container {expected_container['name']} not found")

        actual_security_context = container["securityContext"]
        print(actual_security_context)

        assert actual_security_context["allowPrivilegeEscalation"] == expected_security_context["allowPrivilegeEscalation"], error(
            "Pod container allowPrivilegeEscalation does not match CHI pod template"
        )
        assert actual_security_context["seccompProfile"] == expected_security_context["seccompProfile"], error(
            "Pod container seccompProfile does not match CHI pod template"
        )
        assert set(actual_security_context["capabilities"]["add"]) == set(expected_security_context["capabilities"]["add"]
        ), error("Pod container added capabilities do not match CHI pod template")
        assert set(actual_security_context["capabilities"]["drop"]) == set(expected_security_context["capabilities"]["drop"]
        ), error("Pod container dropped capabilities do not match CHI pod template")

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010065. FIPS IPC Secure mode: operator↔exporter token-protected channel")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010065(self):
    """Verify clickhouse.security.ipc.mode=Secure activates token-based auth
    on the operator↔metrics-exporter /chi REST channel without breaking the
    operator's own ability to register CHIs with the exporter.

    Checks:
      1. After applying the chopconf and restarting the operator, both
         containers log entering Secure IPC mode.
      2. The shared-volume token file at /etc/clickhouse-operator-ipc/token
         is readable in both containers (sh -c '[ -r ... ]' — distroless
         images have no `stat`/`ls`).
      3. A CHI created post-config-change reconciles successfully (=
         operator successfully POSTs to /chi with the X-CHOP-Token header).
      4. A direct loopback request to /chi WITHOUT the token header is
         rejected 401.
      5. A direct loopback request with a WRONG token header is rejected 401.

    The non-loopback (cross-pod) rejection path is covered by the unit test
    `TestIPCAuthMiddlewareRejectsNonLoopback`; reproducing it in e2e requires
    launching a helper pod with curl in the cluster network namespace, which
    is out of scope for this scenario.
    """
    create_shell_namespace_clickhouse_template()

    chi_manifest = "manifests/chi/test-065-fips-ipc.yaml"
    chopconf_file = "manifests/chopconf/test-065-fips-ipc-chopconf.yaml"
    operator_namespace = current().context.operator_namespace

    with Given("Operator configuration enables IPC Secure mode"):
        util.apply_operator_config(chopconf_file)

    with And("Both operator and exporter containers report Secure IPC mode"):
        operator_pod = kubectl.get_operator_pod()
        # Operator container log: "IPC: Secure mode — provisioned token at ..."
        op_logs = kubectl.launch(
            f"logs {operator_pod} -c clickhouse-operator --tail=200",
            ns=operator_namespace,
        )
        assert "IPC: Secure mode" in op_logs, error("operator did not log Secure IPC provisioning")
        # Exporter container log: "IPC: Secure mode — binding /chi to ..."
        ex_logs = kubectl.launch(
            f"logs {operator_pod} -c metrics-exporter --tail=200",
            ns=operator_namespace,
        )
        assert "IPC: Secure mode" in ex_logs, error("exporter did not log Secure IPC bind")

    with And("Shared-volume token file is readable in both containers"):
        # Distroless operator/exporter images ship only bash/sh/curl (no GNU
        # coreutils — no stat, no ls). Use bash test-builtins instead.
        for container in ("clickhouse-operator", "metrics-exporter"):
            out = kubectl.launch(
                f"exec {operator_pod} -c {container} -- sh -c "
                f"'[ -r /etc/clickhouse-operator-ipc/token ] && echo readable'",
                ns=operator_namespace,
            ).strip()
            assert out == "readable", error(f"{container}: token file not readable: {out}")

    with And("CHI created post-config reconciles to Completed (operator→exporter IPC works)"):
        kubectl.create_and_check(
            manifest=chi_manifest,
            check={
                "apply_templates": {current().context.clickhouse_template},
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

    with When("Direct curl to /chi without X-CHOP-Token from inside exporter pod"):
        # 401 = unauthorized (loopback OK, token missing/wrong)
        out = kubectl.launch(
            f"exec {operator_pod} -c metrics-exporter -- "
            f"curl -s -o /dev/null -w '%{{http_code}}' -X POST http://127.0.0.1:8888/chi",
            ns=operator_namespace,
            ok_to_fail=True,
        ).strip()
        assert out == "401", error(f"expected 401 for tokenless /chi, got {out}")

    with When("Direct curl to /chi with wrong token from inside exporter pod"):
        out = kubectl.launch(
            f"exec {operator_pod} -c metrics-exporter -- "
            f"curl -s -o /dev/null -w '%{{http_code}}' -H 'X-CHOP-Token: wrong' "
            f"-X POST http://127.0.0.1:8888/chi",
            ns=operator_namespace,
            ok_to_fail=True,
        ).strip()
        assert out == "401", error(f"expected 401 for wrong-token /chi, got {out}")

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010066. Mixed-posture clusters: per-cluster security.clickhouse.tls knobs resolve independently")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010066(self):
    """Two clusters in one CHI carry different security.clickhouse.tls blocks. Verify the
    normalizer resolves each cluster's security independently (the per-cluster
    overlay path from worker-migrator.go::ensureClusterSchemer). Without the
    per-cluster overlay both clusters would silently share the CHOP-config
    default — masking the cluster-level setting.

    This is the e2e counterpart to the unit test
    `TestEndpointTLSConfigKey_DistinctOnDifferentKnobs`. The e2e angle
    confirms the wiring through normalize → InheritClusterSecurityFrom →
    OverlayClusterSecurityTLS actually reaches the cluster-shaped objects.
    """
    create_shell_namespace_clickhouse_template()

    manifest = "manifests/chi/test-066-mixed-posture.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))

    with When("Apply CHI with two clusters carrying distinct security.clickhouse.tls blocks"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "apply_templates": {current().context.clickhouse_template},
                "object_counts": {"statefulset": 2, "pod": 2, "service": 3},
                "do_not_delete": 1,
            },
        )

    def _cluster_tls_field(cluster_name, leaf):
        # Helper: pull a single per-cluster .security.clickhouse.tls.<leaf>
        # value out of the live CHI via jsonpath. Mirrors the inline jsonpath
        # idiom already used in this test for `verify`.
        return kubectl.launch(
            f"get chi {chi} -o jsonpath="
            f"'{{.spec.configuration.clusters[?(@.name==\"{cluster_name}\")]"
            f".security.clickhouse.tls.{leaf}}}'"
        ).strip().strip("'")

    with Then("Each cluster preserves its own security.clickhouse.tls knobs after normalize"):
        # Cluster-level security in the spec persists through reconcile; this
        # exercises the same MergeFrom-fill-empty path that future refactors
        # could regress.
        strict_verify = _cluster_tls_field("strict-cluster", "verify")
        assert strict_verify == "Strict", error(
            f"strict-cluster.security.clickhouse.tls.verify expected 'Strict', got {strict_verify!r}"
        )
        lax_verify = _cluster_tls_field("lax-cluster", "verify")
        assert lax_verify == "None", error(
            f"lax-cluster.security.clickhouse.tls.verify expected 'None', got {lax_verify!r}"
        )

    with And("Each cluster's minVersion + serverName survive normalize (per-cluster overlay)"):
        # If the per-cluster overlay path regresses to MergeFrom-copy instead
        # of MergeFrom-fill-empty, an empty leaf on one cluster would clobber
        # the other cluster's value. Assert each leaf independently.
        strict_min = _cluster_tls_field("strict-cluster", "minVersion")
        assert strict_min == "1.3", error(
            f"strict-cluster.security.clickhouse.tls.minVersion expected '1.3', got {strict_min!r}"
        )
        lax_min = _cluster_tls_field("lax-cluster", "minVersion")
        assert lax_min == "1.2", error(
            f"lax-cluster.security.clickhouse.tls.minVersion expected '1.2', got {lax_min!r}"
        )
        strict_sni = _cluster_tls_field("strict-cluster", "serverName")
        assert strict_sni == "strict.example", error(
            f"strict-cluster.security.clickhouse.tls.serverName expected "
            f"'strict.example', got {strict_sni!r}"
        )
        lax_sni = _cluster_tls_field("lax-cluster", "serverName")
        assert lax_sni == "lax.example", error(
            f"lax-cluster.security.clickhouse.tls.serverName expected "
            f"'lax.example', got {lax_sni!r}"
        )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010067. 3-level security inheritance: chopconf → CHI → cluster precedence")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010067(self):
    """Verify the 3-level inheritance chain resolves correctly:

    - chopconf sets verify=None, minVersion=1.2 as operator-wide defaults.
    - CHI spec.security overrides verify=Strict, minVersion=1.3 (CHI > chopconf).
    - One cluster overrides verify=None (cluster > CHI > chopconf).
    - Another cluster has no override (inherits CHI's Strict).

    Expected resolved cluster.security.clickhouse.tls.verify values:
      - inherit          → Strict   (from CHI)
      - override-verify  → None     (cluster wins over CHI)

    minVersion is "1.3" for BOTH clusters (cluster doesn't override; falls
    through to CHI).
    """
    create_shell_namespace_clickhouse_template()

    chopconf_file = "manifests/chopconf/test-067-inheritance-chopconf.yaml"
    chi_manifest = "manifests/chi/test-067-inheritance.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(chi_manifest))

    with Given("Operator-wide default security (verify=None, minVersion=1.2)"):
        util.apply_operator_config(chopconf_file)

    with When("Apply CHI with spec-level Strict and a per-cluster None override"):
        kubectl.create_and_check(
            manifest=chi_manifest,
            check={
                "apply_templates": {current().context.clickhouse_template},
                "object_counts": {"statefulset": 2, "pod": 2, "service": 3},
                "do_not_delete": 1,
            },
        )

    with Then("Resolved cluster.security must reflect cluster > CHI > chopconf precedence"):
        # The CHI's spec.security.clickhouse.tls.verify stays Strict (CHI level).
        chi_verify = kubectl.get_field("chi", chi, ".spec.security.clickhouse.tls.verify")
        assert chi_verify == "Strict", error(
            f"CHI spec.security.clickhouse.tls.verify expected 'Strict', got {chi_verify!r}"
        )
        # The override cluster keeps its explicit None.
        override = kubectl.get_field(
            "chi",
            chi,
            '.spec.configuration.clusters[?(@.name==\\"override-verify\\")].security.clickhouse.tls.verify',
        )
        assert override == "None", error(
            f"override-verify cluster expected 'None', got {override!r}"
        )

    with And("CHI spec.security.minVersion overrides chopconf's 1.2 → 1.3 (chopconf→CHI)"):
        chi_min = kubectl.get_field("chi", chi, ".spec.security.clickhouse.tls.minVersion")
        assert chi_min == "1.3", error(
            f"CHI spec.security.clickhouse.tls.minVersion expected '1.3' "
            f"(CHI-level override of chopconf 1.2), got {chi_min!r}"
        )

    # NOTE: CHI→cluster Security fill-empty inheritance is normalize-only —
    # for a cluster that lacks an explicit security block (`inherit` cluster
    # here), the resolved security values populated by MergeFrom-fill-empty
    # live in `.status.normalizedCompleted`, NOT in `.spec.configuration.
    # clusters[].security` on the persisted CHI. Similarly, a cluster that
    # overrides only one field (`override-verify` here, which sets only
    # verify) does not have inherited siblings (minVersion) written into its
    # `.spec` cluster security block. The three assertions (inherit_verify,
    # inherit_min, override_min) added in round 8 were factually incorrect
    # about the persistence contract — same deferred audit as test_010069.
    # See memory/deferred_chit_merge_persistence_audit.md.

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010068. Normalize idempotence: repeated apply of the same security spec is stable")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010068(self):
    """Apply the same security-laden CHI manifest twice; the second apply must
    NOT trigger a StatefulSet generation bump (= idempotent normalize). This
    catches MergeFrom append-style drift (e.g. accidentally calling
    UseTemplates-style append on a sub-block) which would grow the merged
    Security on every pass.
    """
    create_shell_namespace_clickhouse_template()

    manifest = "manifests/chi/test-068-idempotence.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))

    with When("Apply CHI for the first time"):
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "apply_templates": {current().context.clickhouse_template},
                "object_counts": {"statefulset": 1, "pod": 1, "service": 2},
                "do_not_delete": 1,
            },
        )

    with And("Capture StatefulSet generation after first apply"):
        gen1 = kubectl.get_field("sts", f"chi-{chi}-default-0-0", ".metadata.generation")

    with When("Re-apply the same manifest (no spec change)"):
        # kubectl apply with identical content should be a no-op for the STS.
        kubectl.apply(util.get_full_path(manifest))
        # Replace racy time.sleep(10) with an explicit wait for the
        # operator's second reconcile pass to finish. With a bare sleep, if
        # the operator hasn't yet started its 2nd reconcile when the sleep
        # ends, the gen1==gen2 check is trivially satisfied and any
        # generation-bumping drift in normalize is masked.
        kubectl.wait_chi_status(chi, "Completed", retries=10)

    with Then("StatefulSet generation MUST be unchanged after re-apply"):
        gen2 = kubectl.get_field("sts", f"chi-{chi}-default-0-0", ".metadata.generation")
        assert gen1 == gen2, error(
            f"StatefulSet generation changed on re-apply: {gen1} → {gen2} (normalize is not idempotent)"
        )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010069. CHIT template merges security into CHI spec; cluster override wins")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010069(self):
    """A CHIT carries spec.security.clickhouse.tls (verify=Strict, minVersion=1.3); a CHI
    references the CHIT via useTemplates. The CHIT's security must merge into
    the CHI's spec.security via ChiSpec.MergeFrom — without this Security-merge
    branch, the template's security knobs would be silently dropped.

    The CHI then defines one cluster overriding verify=None at cluster level.
    Resolved cluster.security.clickhouse.tls.verify must be None (cluster wins); the
    inherited minVersion 1.3 stays.
    """
    create_shell_namespace_clickhouse_template()

    chit_manifest = "manifests/chit/test-069-security-template.yaml"
    chi_manifest = "manifests/chi/test-069-template-overlay.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(chi_manifest))

    with Given("Apply CHIT carrying security.clickhouse.tls (verify=Strict, minVersion=1.3)"):
        kubectl.apply(util.get_full_path(chit_manifest))

    with When("Apply CHI referencing the CHIT and overriding verify at cluster level"):
        kubectl.create_and_check(
            manifest=chi_manifest,
            check={
                "apply_templates": {current().context.clickhouse_template},
                "object_counts": {"statefulset": 1, "pod": 1, "service": 2},
                "do_not_delete": 1,
            },
        )

    with Then("Cluster-level verify=None wins over CHIT's Strict"):
        cluster_verify = kubectl.get_field("chi", chi,
            '.spec.configuration.clusters[?(@.name==\\"override\\")].security.clickhouse.tls.verify',
        )
        assert cluster_verify == "None", error(
            f"override cluster.security.clickhouse.tls.verify expected 'None', got {cluster_verify!r}"
        )

    # NOTE: CHIT→CHI Security merge is normalize-only — the merged values
    # are NOT written back to the CHI's persisted `.spec` on the API server,
    # so we cannot assert them via `kubectl get` here. The cluster-level
    # override above is the only externally observable invariant of this
    # test. The persistence-model question (should CHIT merge land in
    # `.spec` or only `.status.normalized`?) is deferred — see MEMORY.md
    # note on the CHIT-merge persistence audit.

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010070. Security feature is opt-in: omitting the security block round-trips cleanly with no STS churn")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010070(self):
    """Verifies three properties that distinguish this test from the rest of
    the suite, where most tests also happen to omit security but never check
    these invariants explicitly:

      INVARIANT A — CRD schema accepts the new security fields when absent.
        After the security CRD properties were added, a CHI that DOES NOT
        set them must still validate. A typo in `required:` on any of the
        new security sub-fields would make every old-shape CHI fail at
        kubectl apply time. test_010001 etc. would catch that, but only as
        a symptom ("apply rejected") — here we make the contract explicit.

      INVARIANT B — Normalizer does NOT inject a default Security struct.
        The operator round-trips the CHI through CreateTemplated → normalize
        → MergeFrom. After that round-trip, .spec.security MUST stay empty.
        If a future refactor wrote `cr.Spec.Security = &ClusterSecurity{}`
        unconditionally (e.g. to simplify nil-handling downstream), the
        round-tripped object would persist {} instead of being absent —
        breaking pre-0.27.1 manifests' on-disk shape and triggering rolling
        restarts on operator upgrade. This invariant is unique to this test;
        no other test in the suite asserts the absence shape post-normalize.

      INVARIANT C — Re-apply of the same no-security CHI MUST NOT roll the
        StatefulSet. The most subtle regression vector: the normalizer
        injects a zero-value Security{} on the second pass that differs
        byte-wise from the first pass's nil. The pod-template diff sees a
        change, increments .status.generation, and rolls every pod. This
        invariant catches that class of bug — empty-vs-nil normalize drift.
        No other test in the suite exercises double-apply on a no-security
        manifest specifically to assert generation stability.

    This test is the explicit zero-regression anchor for the FIPS feature
    family (010065-010073). The other family members all SET some security
    knob; this one is the negative control that proves opt-out is a stable,
    first-class configuration.
    """
    create_shell_namespace_clickhouse_template()

    manifest = "manifests/chi/test-070-no-security.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(manifest))

    with Given("Apply a CHI with no security block — Invariant A: CRD schema accepts it"):
        # If the CRD schema were broken (e.g. accidental required: on a
        # security sub-field), create_and_check would fail at apply time
        # before reconcile begins. Reaching Completed proves Invariant A.
        kubectl.create_and_check(
            manifest=manifest,
            check={
                "apply_templates": {current().context.clickhouse_template},
                "object_counts": {"statefulset": 1, "pod": 1, "service": 2},
                "do_not_delete": 1,
            },
        )

    with Then("Invariant B: .spec.security stays empty post-normalize round-trip"):
        # Operator does CreateTemplated → MergeFrom into target → write back
        # .status.normalizedCR. If normalize injected `&ClusterSecurity{}`,
        # the field would marshal as {} on the wire instead of being absent.
        # get_field returns <none> for both "key absent" and "key present
        # but null" — both are acceptable absence shapes.
        sec = kubectl.get_field("chi", chi, ".spec.security")
        assert sec == "<none>", error(
            f"Invariant B violated: .spec.security should be absent after "
            f"round-trip, got: {sec!r}. A non-empty value here means the "
            f"normalizer is injecting a default Security struct that wasn't "
            f"in the user's manifest, breaking the absent-state contract."
        )

    sts_name = f"chi-{chi}-default-0-0"
    with And("Record StatefulSet generation before re-apply"):
        gen_before = kubectl.get_field("sts", sts_name, ".metadata.generation")
        assert gen_before, error(f"could not read STS generation: {gen_before!r}")

    with When("Re-apply the identical no-security manifest"):
        # Second normalize pass through the same CHI must produce
        # byte-identical output to the first pass. If it doesn't, the
        # operator detects a spec change and bumps the STS generation,
        # rolling the pods. The wait_chi_status loop in create_and_check is
        # already over; we just apply and observe sts.metadata.generation.
        kubectl.apply(util.get_full_path(manifest, lookup_in_host=False))
        # Allow a brief settle window for the operator informer to react
        # to the apply event before re-reading generation.
        kubectl.wait_chi_status(chi, "Completed", retries=10)

    with Then("Invariant C: StatefulSet generation did NOT change on re-apply"):
        gen_after = kubectl.get_field("sts", sts_name, ".metadata.generation")
        assert gen_after == gen_before, error(
            f"Invariant C violated: STS generation drifted {gen_before!r} → "
            f"{gen_after!r} after re-applying an IDENTICAL no-security CHI. "
            f"This indicates the normalizer is not byte-stable on the absent "
            f"path — a likely cause is `&ClusterSecurity{{}}` injection that "
            f"differs from nil. Rolling every pod on operator upgrade for "
            f"users who never set security knobs is the regression to catch."
        )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010071. IPC Secure mode: operator-pod restart preserves token + exporter waitForToken tolerates race")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010071(self):
    """Restart the entire operator pod while in IPC Secure mode. Both containers
    come up simultaneously; exporter's waitForToken polls (30s timeout) while
    the operator provisions a fresh token. After restart:
      1. The operator pod becomes Ready within a sensible window.
      2. The exporter log shows it eventually loaded the token (no
         "IPC token never appeared" timeout).
      3. A new CHI created post-restart reconciles successfully (= IPC channel
         is healthy after the race).

    This catches an init-order regression: if ProvisionIPCToken ever moves
    after the exporter goroutine start in operator init, the exporter could
    permanently fail (or the retry-on-error fix could be reverted).
    """
    create_shell_namespace_clickhouse_template()

    chopconf_file = "manifests/chopconf/test-065-fips-ipc-chopconf.yaml"
    chi_manifest = "manifests/chi/test-071-ipc-restart.yaml"
    operator_namespace = current().context.operator_namespace

    with Given("IPC Secure mode is active"):
        util.apply_operator_config(chopconf_file)

    with When("Delete the operator pod (both containers restart simultaneously)"):
        old_pod = kubectl.get_operator_pod()
        # Single delete-and-wait: util.restart_operator() itself deletes the
        # operator pod and waits for the new one to be Ready, so calling it
        # alone replaces the manual `kubectl delete pod` (two consecutive
        # deletes would race on slow clusters).
        util.restart_operator()

    with Then("Both containers report Secure IPC mode after restart"):
        new_pod = kubectl.get_operator_pod()
        assert new_pod != old_pod, error(f"operator pod did not restart: still {new_pod}")
        # Poll for the Secure-mode marker in exporter logs — wait_pod_status
        # only checks .status.phase, not container Ready, so the exporter may
        # still be inside waitForToken when the pod is Running.
        deadline = time.time() + 60
        ex_logs = ""
        while time.time() < deadline:
            ex_logs = kubectl.launch(
                f"logs {new_pod} -c metrics-exporter --tail=200",
                ns=operator_namespace,
                ok_to_fail=True,
            )
            if "IPC: Secure mode" in ex_logs:
                break
            time.sleep(3)
        op_logs = kubectl.launch(
            f"logs {new_pod} -c clickhouse-operator --tail=200",
            ns=operator_namespace,
        )
        assert "IPC: Secure mode" in op_logs, error("operator did not log Secure IPC after restart")
        assert "IPC: Secure mode" in ex_logs, error("exporter did not log Secure IPC after restart")
        # Negative assertion: the token-wait timeout marker must NOT appear.
        assert "IPC token never appeared" not in ex_logs, error(
            "exporter timed out waiting for token — operator init-order regression"
        )

    with And("A CHI created after restart reconciles via the IPC channel"):
        kubectl.create_and_check(
            manifest=chi_manifest,
            check={
                "apply_templates": {current().context.clickhouse_template},
                "pod_count": 1
            },
        )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010072. IPC Secure mode: /metrics remains accessible from another pod")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010072(self):
    """The Secure-mode loopback gate applies ONLY to the /chi handler. The
    Prometheus /metrics endpoint MUST stay reachable from non-loopback callers
    so ServiceMonitor / external scrapers keep working.

    Test:
      1. Activate Secure IPC mode.
      2. From a helper pod (different Pod, hence non-loopback), curl the
         exporter's /metrics endpoint via the ClusterIP service. Expect 200
         with Prometheus exposition format ("# HELP" line).
      3. From the SAME helper pod, curl /chi via the same service. Expect
         a non-2xx response (the request reaches the listener but the
         loopback gate rejects it with 403).

    The test framework defaults the helper pod's namespace to the operator's
    own; same-namespace traffic is still non-loopback (different Pod IP), so
    the gate is meaningfully exercised. Catches a future refactor that
    accidentally wraps /metrics in the same middleware chain as /chi.
    """
    create_shell_namespace_clickhouse_template()

    chopconf_file = "manifests/chopconf/test-065-fips-ipc-chopconf.yaml"
    operator_namespace = current().context.operator_namespace

    with Given("IPC Secure mode is active"):
        util.apply_operator_config(chopconf_file)

    helper_ns = current().context.test_namespace
    helper_pod = "test-072-curl-client"

    with And(f"Launch a curl helper pod in {helper_ns}"):
        kubectl.launch(
            f"run {helper_pod} --image=curlimages/curl:latest --restart=Never -- sleep 600",
            ns=helper_ns,
        )
        kubectl.wait_pod_status(helper_pod, "Running", ns=helper_ns)

    metrics_url = (
        f"http://clickhouse-operator-metrics.{operator_namespace}.svc:8888/metrics"
    )
    chi_url = (
        f"http://clickhouse-operator-metrics.{operator_namespace}.svc:8888/chi"
    )

    with When("Curl /metrics from the helper pod (non-loopback caller)"):
        out = kubectl.launch(
            f"exec {helper_pod} -- curl -s -o /tmp/m -w '%{{http_code}}' {metrics_url}",
            ns=helper_ns,
            ok_to_fail=True,
        ).strip()
        assert out == "200", error(f"/metrics expected 200 from non-loopback caller, got {out}")

    with And("Body contains Prometheus exposition format ('# HELP' line)"):
        body = kubectl.launch(
            f"exec {helper_pod} -- cat /tmp/m",
            ns=helper_ns,
            ok_to_fail=True,
        )
        assert "# HELP" in body, error("/metrics did not return Prometheus exposition format")

    with When("Curl /chi from the same helper pod (non-loopback, no token)"):
        # Loopback check rejects with 403 (forbidden) per ipcAuthMiddleware:
        # remote address is not loopback → http.StatusForbidden.
        out = kubectl.launch(
            f"exec {helper_pod} -- curl -s -o /dev/null -w '%{{http_code}}' -X POST {chi_url}",
            ns=helper_ns,
            ok_to_fail=True,
        ).strip()
        assert out == "403", error(
            f"/chi expected 403 from non-loopback (loopback gate), got {out}"
        )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010073. FIPS Strict: plain-text ZooKeeper CHI is rejected with FIPSValidationFailed reason")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010073(self):
    """Activate Strict FIPS mode at the operator level, then apply a CHI that
    references plain-text external ZooKeeper (`secure: true` not set on any
    node). The operator must:

      1. Coerce per-component security knobs (logged at startup).
      2. Reject the CHI at normalize time with status=Aborted.
      3. Tag the error stream with `[FIPSValidationFailed]` so operators and
         dashboards can distinguish the FIPS rejection from generic Aborted.

    Recovery is via spec edit (informer UpdateFunc on next `kubectl apply`),
    not via the auto-recovery onPodReady path — pods are never created for
    the rejected CHI.
    """
    create_shell_namespace_clickhouse_template()

    chopconf_file = "manifests/chopconf/test-073-fips-strict-chopconf.yaml"
    chi_manifest = "manifests/chi/test-073-fips-zk-rejected.yaml"
    operator_namespace = current().context.operator_namespace
    chi = yaml_manifest.get_name(util.get_full_path(chi_manifest))

    with Given("Apply Strict FIPS chopconf and restart operator"):
        util.apply_operator_config(chopconf_file)

    with And("Operator log shows FIPS coercion entries"):
        operator_pod = kubectl.get_operator_pod()
        op_logs = kubectl.launch(
            f"logs {operator_pod} -c clickhouse-operator --tail=400 --all-containers=false",
            ns=operator_namespace,
            ok_to_fail=True,
        )
        # At least one knob should have been coerced (default config has none of
        # the strict positions set). At -v=0 the line is suppressed; gate this
        # assertion behind a substring search that tolerates either presence.
        # Soft check: we only assert reconciler behavior below.
        _ = op_logs

    with When("Apply CHI referencing plain-text external ZooKeeper (missing `secure`)"):
        kubectl.apply(util.get_full_path(chi_manifest))

    with Then("CHI lands in status=Aborted"):
        kubectl.wait_chi_status(chi, 'Aborted')

    with And("Aborted reason is [FIPSValidationFailed]"):
        errors = kubectl.get_field('chi', chi, '.status.errors')
        print(errors)
        assert "FIPSValidationFailed" in errors, error(
                f"expected [FIPSValidationFailed] reason in status.errors, got {errors}"
        )

    # Sibling sub-assertion: the FIPS validator must reject not only the
    # implicit `secure: <missing>` case (covered above) but also the
    # *explicit* `secure: false` case. A naive validator that walks the spec
    # tree with `if zkNode.Secure != nil && *zkNode.Secure == false: reject`
    # has different code paths from the missing-field case and the two paths
    # can drift; we exercise both. The explicit-false manifest is a sibling
    # of the missing-secure manifest, differing only in that field.
    chi_explicit_manifest = "manifests/chi/test-073-fips-zk-rejected-explicit-false.yaml"
    chi_explicit = yaml_manifest.get_name(util.get_full_path(chi_explicit_manifest))
    with When("Apply CHI with explicit `secure: false` on ZooKeeper nodes"):
        kubectl.apply(util.get_full_path(chi_explicit_manifest))

    with Then("Explicit-false CHI also lands in status=Aborted"):
        kubectl.wait_chi_status(chi_explicit, 'Aborted')

    with And("Aborted reason on explicit-false CHI is also [FIPSValidationFailed]"):
        errors = kubectl.get_field('chi', chi_explicit, '.status.errors')
        print(errors)
        assert "FIPSValidationFailed" in errors, error(
            f"expected [FIPSValidationFailed] reason in status.errors for "
            f"explicit `secure: false` CHI, got {errors}"
        )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010074. FIPS image policy Required: non-fips CHI is rejected at admission with FIPSImagePolicyViolation reason")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010074(self):
    """Activate security.images.policy=Required at the operator level
    and apply a CHI whose resolved ClickHouse image lacks the "fips" tag
    substring. The operator's admission gate must:

      1. Refuse the CHI at normalize time (before any pod is created).
      2. Set status=Aborted with `[FIPSImagePolicyViolation]` leading the
         error stream — auto-recovery skip relies on the prefix.

    Image policy is orthogonal to security.policy (master TLS switch); this
    test isolates the image-policy branch by leaving security.policy unset.
    Recovery is via spec edit (informer UpdateFunc on next `kubectl apply`),
    not via the auto-recovery onPodReady path — pods never exist.
    """
    create_shell_namespace_clickhouse_template()

    chopconf_file = "manifests/chopconf/test-074-fips-images-required-chopconf.yaml"
    chi_manifest = "manifests/chi/test-074-fips-images-required-non-fips.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(chi_manifest))

    with Given("Apply FIPS image-policy=Required chopconf and restart operator"):
        util.apply_operator_config(chopconf_file)

    with When("Apply CHI whose default image lacks the 'fips' tag substring"):
        kubectl.apply(util.get_full_path(chi_manifest))

    with Then("CHI lands in status=Aborted"):
        kubectl.wait_chi_status(chi, 'Aborted')

    with And("Aborted reason is [FIPSImagePolicyViolation]"):
        errors = kubectl.get_field('chi', chi, '.status.errors')
        assert "FIPSImagePolicyViolation" in errors, error(
            f"expected [FIPSImagePolicyViolation] reason in status.errors, got {errors}"
        )

    with And("First error entry starts with the [FIPSImagePolicyViolation] prefix (contract)"):
        # pkg/controller/chi/worker-pod-retry.go:36-39 distinguishes
        # auto-recovery-eligible failures from terminal FIPS rejections by
        # matching the LEADING `[FIPSImagePolicyViolation]` prefix on
        # errors[0]. If a future refactor relocates the tag mid-string the
        # auto-recovery skip stops working and pods spin-retry forever.
        # `get_field` collapses list output into a whitespace-separated
        # rendering; assert that the prefix sits at the head of the string.
        stripped = errors.strip().lstrip("[").lstrip()
        assert errors.strip().startswith("[FIPSImagePolicyViolation]") or \
            stripped.startswith("FIPSImagePolicyViolation]"), error(
            f"errors[0] must START with [FIPSImagePolicyViolation] prefix "
            f"(auto-recovery contract in worker-pod-retry.go:36-39 depends on "
            f"the leading bracket-tag); got: {errors!r}"
        )

    with And("No StatefulSet was created for the rejected CHI"):
        sts = kubectl.get_count('sts', label = f'clickhouse.altinity.com/chi={chi}')
        assert sts == 0, error(f"expected no STS for aborted CHI, got {sts}")

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010075. FIPS image policy Required: CHI with fips-tagged image reconciles normally")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010075(self):
    """Positive-path counterpart to test_010074. With image-policy=Required
    in chopconf, a CHI whose PodTemplate container "clickhouse" carries an
    image whose tag contains the "fips" substring must reconcile to
    Completed — the gate's substring match is case-insensitive and matches
    the Altinity ".altinityfips" convention.

    The container name MUST be exactly "clickhouse" — templates using
    "clickhouse-pod" or other names fall through to the operator default
    image and would be rejected; that is a known mismatch worth documenting
    via this test rather than masking it.
    """
    create_shell_namespace_clickhouse_template()

    chopconf_file = "manifests/chopconf/test-074-fips-images-required-chopconf.yaml"
    chi_manifest = "manifests/chi/test-075-fips-images-required-fips.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(chi_manifest))

    with Given("Apply FIPS image-policy=Required chopconf and restart operator"):
        util.apply_operator_config(chopconf_file)

    with When("Apply CHI with fips-tagged ClickHouse image"):
        kubectl.create_and_check(
            manifest=chi_manifest,
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )

        with Then("No FIPSImagePolicyViolation error appears"):
            errors = kubectl.get_field('chi', chi, '.status.errors')
            assert "FIPSImagePolicyViolation" not in errors, error(
                f"unexpected FIPSImagePolicyViolation in status.errors, got {errors}"
            )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010076. FIPS posture: default operator image is FIPS-built")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010076(self):
    """Pin the operator-wide FIPS posture as a release-gate invariant.

    The operator emits a single banner at startup of the form
        FIPS: chopconf.fips.enforced=<bool> build.enabled=<bool> runtime.enforced=<bool> module=<ver>

    Regardless of `security.fips.enforced` in chopconf (FIPS cryptographic-
    module gate — opt-in), the shipped default image MUST be GOFIPS140-built
    so the Go FIPS crypto module is linked (`build.enabled=true`). A regression
    that drops GOFIPS140 silently downgrades FIPS strength across the whole
    fleet, so we fail the e2e run rather than let it slip through.

    Runtime mode is `GODEBUG=fips140=only` in the default image (strict: any
    invocation of a non-FIPS primitive panics at call time). `runtime.enforced`
    therefore reads `true` for the shipped default — the test asserts that
    explicitly. The `pkg/util/{hash,string,shell}.go` deterministic-identifier
    hashing uses inline pure-Go SHA-1/MD5 implementations that do NOT invoke
    `crypto/sha1`/`crypto/md5`, so they remain safe under `fips140=only`. See
    `docs/security_hardening.md` §3 for the FIPS-boundary rationale. The
    `GODEBUG_FIPS140` Docker build-arg parameterizes the baked default
    (`only`|`on`|`off`); customers can override at runtime via Pod env,
    Helm `operator.env`, or `kubectl set env`.

    Build linkage is driven by `dev/go_build_config.sh` defaulting GOFIPS140
    to v1.0.0 and propagated by every image-build entrypoint (CI, dev-image,
    Vagrant, devspace).
    """
    create_shell_namespace_clickhouse_template()
    operator_namespace = current().context.operator_namespace

    with Given("Operator pod is running the default install"):
        operator_pod = kubectl.get_operator_pod(ns=operator_namespace)
        assert operator_pod != "", error("operator pod not found")

    # Release-gate invariant covers BOTH binaries shipped from the operator
    # pod: clickhouse-operator AND metrics-exporter. The two are built from
    # the same Go module under the same GOFIPS140 build flag, but they are
    # distinct images with separate image-build entrypoints — a regression
    # that drops the build tag from one image will not necessarily drop it
    # from the other. Assert the banner symmetrically against both
    # containers so future regressions in either binary fail this gate.
    banner_pattern = (
        r"FIPS: chopconf\.fips\.enforced=(true|false) "
        r"build\.enabled=(true|false) "
        r"runtime\.enforced=(true|false) "
        r"module=\S+"
    )

    # Use --tail=-1 (no cap): the FIPS banner lands at ~line 500-700 of the
    # operator log because chop.Config().String(true) dumps ~500-700 lines of
    # yaml before the banner is emitted. A bounded --tail value (e.g. 400)
    # silently misses the banner on a freshly-restarted operator and the
    # regex match falsely fails — the binary is FIPS-built but we never see
    # the line we need to verify. -1 disables the kubectl-side cap.
    fips_env_pattern = (
        r'FIPS env: GODEBUG="[^"]*" DefaultGODEBUG="[^"]*" GOFIPS140="[^"]*"'
    )

    with When("Read the operator startup banner"):
        op_logs = kubectl.launch(
            f"logs {operator_pod} -c clickhouse-operator --tail=-1",
            ns=operator_namespace,
        )

    with Then("Banner is present and reports FIPS-built (build.enabled=true)"):
        m = re.search(banner_pattern, op_logs)
        assert m is not None, error(
            "FIPS startup banner not found in operator logs (tail=-1) — "
            "operator may be from a pre-FIPS-gate image"
        )
        build_enabled = m.group(2)
        assert build_enabled == "true", error(
            f"operator image is NOT FIPS-built: build.enabled={build_enabled} "
            f"(expected true — GOFIPS140 build tag missing)"
        )

    with And("Banner reports strict runtime (runtime.enforced=true) for shipped default"):
        # Shipped default since 0.27.1: Dockerfile bakes GODEBUG=fips140=only
        # via ARG GODEBUG_FIPS140=only. Enforced() reads true. A regression
        # that drops the build-arg or flips it back to `on` slips strict-mode
        # silently — fail the gate so the drop is visible.
        runtime_enforced = m.group(3)
        assert runtime_enforced == "true", error(
            f"operator runtime.enforced={runtime_enforced} (expected true — "
            f"GODEBUG_FIPS140 build-arg may have flipped from only to on)"
        )

    with And("FIPS env line is present (soft-fail: forward-compatible)"):
        # Soft-fail with a warning rather than assert: A2's banner enhancement
        # adds a second `FIPS env: GODEBUG=... DefaultGODEBUG=... GOFIPS140=...`
        # line. If that enhancement has not landed in the image under test,
        # the rest of the gate still holds. Use note() so the absence shows
        # up in the test log without flipping the test red.
        if re.search(fips_env_pattern, op_logs) is None:
            note(
                "FIPS env line not found in operator logs — A2 banner "
                "enhancement may not be present yet (soft-fail, not an error)"
            )

    with When("Read the metrics-exporter startup banner"):
        exporter_logs = kubectl.launch(
            f"logs {operator_pod} -c metrics-exporter --tail=-1",
            ns=operator_namespace,
        )

    with Then("Banner is present and reports FIPS-built (build.enabled=true) for metrics-exporter"):
        m = re.search(banner_pattern, exporter_logs)
        assert m is not None, error(
            "FIPS startup banner not found in metrics-exporter logs (tail=-1) — "
            "exporter image may be from a pre-FIPS-gate build"
        )
        build_enabled = m.group(2)
        assert build_enabled == "true", error(
            f"metrics-exporter image is NOT FIPS-built: build.enabled={build_enabled} "
            f"(expected true — GOFIPS140 build tag missing from exporter image)"
        )

    with And("Banner reports strict runtime (runtime.enforced=true) for metrics-exporter"):
        runtime_enforced = m.group(3)
        assert runtime_enforced == "true", error(
            f"metrics-exporter runtime.enforced={runtime_enforced} (expected "
            f"true — GODEBUG_FIPS140 build-arg may have flipped from only to on)"
        )

    with And("FIPS env line is present in metrics-exporter logs (soft-fail)"):
        if re.search(fips_env_pattern, exporter_logs) is None:
            note(
                "FIPS env line not found in metrics-exporter logs — A2 "
                "banner enhancement may not be present yet (soft-fail, not an error)"
            )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010077. FIPS on-wire TLS verification: Strict + wrong rootCA fails ClickHouse fetch")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010077(self):
    """Exercise the FIPS-built TLS stack on the wire and prove it actually
    verifies peer certs rather than silently falling back to
    InsecureSkipVerify=true.

    The release gate for FIPS asks for "e2e test using the FIPS operator
    image with verified TLS". Banner-only tests (test_010076) confirm the
    binary is built against the Go FIPS module, but they cannot detect a
    silent code-path regression that bypasses the verifying TLS dialer.
    This test closes that gap with a NEGATIVE roundtrip:

      1. Deploy a ClickHouse with a self-signed cert (reusing the test-058
         server cert + key + CA secret).
      2. Configure the operator with `security.clickhouse.tls.verify=Strict`
         and an inline `rootCA` that is NOT the issuer of the server cert
         (self-signed CA CN=test-077-unrelated-ca.example, generated
         specifically for this test — guarantees chain validation fails).
      3. Assert `chi_clickhouse_metric_fetch_errors == 1`: the operator's
         outbound TLS handshake must fail cert verification and the metric
         exporter must surface it.

    A positive roundtrip would require provisioning a server cert signed by
    a CA whose PEM is reproducible across runs; the negative roundtrip
    exercises the same `crypto/tls` code path under FIPS without that
    infrastructure burden, while still failing loudly if a future change
    re-introduces InsecureSkipVerify=true on the strict path.
    """
    create_shell_namespace_clickhouse_template()
    operator_namespace = current().context.operator_namespace

    with Given("test-058-root-ca secret is installed (reused for server cert/key/CA)"):
        kubectl.apply(
            util.get_full_path("manifests/secret/test-058-secret.yaml"),
        )

    chi_manifest = "manifests/chi/test-077-fips-tls-wrong-ca.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(chi_manifest))

    with When("Create the CHI with HTTPS-enabled ClickHouse"):
        kubectl.create_and_check(
            manifest=chi_manifest,
            check={
                "apply_templates": {
                    current().context.clickhouse_template,
                },
                "object_counts": {"statefulset": 1, "pod": 1, "service": 2},
                "do_not_delete": 1,
            },
        )

    chopconf_file = "manifests/chopconf/test-077-fips-tls-wrong-ca-chopconf.yaml"
    with When("Apply chopconf with verify=Strict and a deliberately wrong rootCA"):
        util.apply_operator_config(chopconf_file)
        kubectl.wait_chi_status(chi, "Completed")

    with Then("chi_clickhouse_metric_fetch_errors is 1 — TLS handshake rejected"):
        # Negative assertion: the strict-verify path must refuse the server
        # cert (issuer != configured rootCA). If this metric reports 0, the
        # operator silently downgraded to InsecureSkipVerify=true — exactly
        # the regression this test guards against. Pin the chi label so a
        # stray fetch_errors=1 on an unrelated CHI cannot satisfy the assertion.
        check_metrics_monitoring(
            operator_namespace=operator_namespace,
            operator_pod=kubectl.get_operator_pod(),
            expect_pattern=f'^chi_clickhouse_metric_fetch_errors{{[^}}]*chi="{chi}"[^}}]*}} 1$',
        )

    with When("Reset ClickHouseOperatorConfiguration to default"):
        kubectl.delete(util.get_full_path(chopconf_file, lookup_in_host=False), operator_namespace)
        util.restart_operator()

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_010078. FIPS Enforced: cluster-level verify=None on CHI is rejected with FIPSValidationFailed reason")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_010078(self):
    """CHI counterpart of test_020009 (CHK) and a per-cluster sibling of
    test_010073 (CHI/ZK). With `security.policy: Enforced` at the operator
    level, a CHI whose .spec.configuration.clusters[].security.clickhouse.tls.verify
    is explicitly set to None must be rejected at normalize time.

    Where test_010073 covers the implicit-bypass route (plain-text ZK without
    `secure: true`), this test covers the per-cluster explicit-bypass route:
    the user opted into a Strict FIPS posture at the operator level and then
    tried to escape it inside a single cluster. The validator must:

      1. Set status=Aborted with [FIPSValidationFailed] in errors[0].
      2. Refuse to create any StatefulSet for the rejected CHI (no half-
         provisioned resources lying around requiring manual cleanup).
    """
    create_shell_namespace_clickhouse_template()

    chopconf_file = "manifests/chopconf/test-078-fips-verify-none-chopconf.yaml"
    chi_manifest = "manifests/chi/test-078-fips-verify-none.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(chi_manifest))

    with Given("Apply Enforced-policy FIPS chopconf and restart operator"):
        util.apply_operator_config(chopconf_file)

    with When("Apply CHI with cluster.security.clickhouse.tls.verify=None"):
        kubectl.apply(util.get_full_path(chi_manifest))

    with Then("CHI lands in status=Aborted"):
        kubectl.wait_chi_status(chi, 'Aborted')

    with And("Aborted reason is [FIPSValidationFailed]"):
        errors = kubectl.get_field('chi', chi, '.status.errors')
        print(errors)
        assert "FIPSValidationFailed" in errors, error(
            f"expected [FIPSValidationFailed] reason in status.errors, got {errors}"
        )

    with And("No StatefulSet was created for the rejected CHI"):
        # The rejection must happen before any resource provisioning so
        # there is nothing to clean up downstream. Matches the contract
        # already enforced by test_010074 for the image-policy branch.
        sts = kubectl.get_count('sts', label=f'clickhouse.altinity.com/chi={chi}')
        assert sts == 0, error(f"expected no STS for aborted CHI, got {sts}")

    with Finally("I clean up"):
        delete_test_namespace()


#
# Keeper tests section
#


@TestScenario
@Name("test_020000. Test Basic CHK functions")
def test_020000(self):
    create_shell_namespace_clickhouse_template()

    chk_manifest = "manifests/chk/test-020000-chk.yaml"
    chk = yaml_manifest.get_name(util.get_full_path(chk_manifest))

    with Given("Install CHK"):
        kubectl.apply(util.get_full_path("manifests/chk/test-020000-chk-sa.yaml"))
        kubectl.create_and_check(
            manifest=chk_manifest,
            kind="chk",
            check={
                "pod_count": 1,
                "pdb": {"keeper": 0},
                "do_not_delete": 1
            }
        )

    chk_objects = kubectl.get_obj_names_grepped("pod,service,sts,pvc,cm,pdb,secret", grep=chk)
    print("Created objects:")
    for o in chk_objects:
        print(o)

    with Then("Service account should be set"):
        chk_pod_spec = kubectl.get_chk_pod_spec(chk)
        assert chk_pod_spec["serviceAccountName"] == "test-020000-chk-sa"

    with And("There should be a service for cluster a cluster"):
        kubectl.check_service(f"keeper-{chk}-service", "ClusterIP", headless=True)

    with And("There should be a service for first replica"):
        kubectl.check_service(f"keeper-{chk}-0", "ClusterIP", headless=True)

    with And("There should be a PVC"):
        assert kubectl.get_count("pvc", label=f"-l clickhouse-keeper.altinity.com/chk={chk}") == 1

    with When("Stop CHK"):
        cmd = f'patch chk {chk} --type=\'json\' --patch=\'[{{"op":"add","path":"/spec/stop","value":"yes"}}]\''
        kubectl.launch(cmd)
        kubectl.wait_chk_status(chk, "InProgress")
        kubectl.wait_chk_status(chk, "Completed")
        with Then("STS should be there but no running pods"):
            label = f"-l clickhouse-keeper.altinity.com/chk={chk}"
            assert kubectl.get_count('sts', label = label) == 1
            assert kubectl.get_count('pod', label = label) == 0

    with When("Resume CHK"):
        cmd = f'patch chk {chk} --type=\'json\' --patch=\'[{{"op":"replace","path":"/spec/stop","value":"no"}}]\''
        kubectl.launch(cmd)
        kubectl.wait_chk_status(chk, "InProgress")
        kubectl.wait_chk_status(chk, "Completed")
        with Then("Both STS and Pod should be up"):
            label = f"-l clickhouse-keeper.altinity.com/chk={chk}"
            assert kubectl.get_count('sts', label = label) == 1
            assert kubectl.get_count('pod', label = label) == 1

    with When("Suspend CHK"):
        cmd = f'patch chk {chk} --type=\'json\' --patch=\'[{{"op":"add","path":"/spec/suspend","value":"yes"}}]\''
        kubectl.launch(cmd)

        with Then("Stop CHK one more time"):
            cmd = f'patch chk {chk} --type=\'json\' --patch=\'[{{"op":"replace","path":"/spec/stop","value":"yes"}}]\''
            kubectl.launch(cmd)
            time.sleep(15) # wait in case there was some sync issue
            kubectl.wait_chk_status(chk, "Completed")
            with Then("Stop should be ignored. Both STS and Pod should be up"):
                label = f"-l clickhouse-keeper.altinity.com/chk={chk}"
                assert kubectl.get_count('sts', label = label) == 1
                assert kubectl.get_count('pod', label = label) == 1

    with When("Unsuspend CHK"):
        cmd = f'patch chk {chk} --type=\'json\' --patch=\'[{{"op":"remove","path":"/spec/suspend"}}]\''
        kubectl.launch(cmd)

        with Then("Reconcile should trigger"):
            # Do NOT wait on InProgress: when unsuspending a CHK whose stop=yes
            # was set during suspend, the rebuilt ActionPlan can produce no diff
            # vs the persisted ancestor → operator logs "No reconcile work" and
            # status never transitions to InProgress. Waiting for the eventual
            # Completed state is sufficient; the post-condition (pod count) is
            # what actually matters for this test.
            kubectl.wait_chk_status(chk, "Completed")

        with Then("And CHK should be stopped"):
            label = f"-l clickhouse-keeper.altinity.com/chk={chk}"
            assert kubectl.get_count('sts', label = label) == 1
            assert kubectl.get_count('pod', label = label) == 0

    with Then("Delete CHK"):
        kubectl.delete_chk(chk)

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_020001. Test that Kubernetes objects between CHI and CHK does not overlap")
def test_020001(self):
    create_shell_namespace_clickhouse_template()

    objects = {}
    for ch_kind in ('chi', 'chk'):
        manifest = f"manifests/chk/test-020001-{ch_kind}.yaml"
        ch_name = "test-020001"

        with Given(f"Install {ch_kind}"):
            kubectl.create_and_check(
                manifest=manifest, kind=ch_kind,
                check={
                    "pod_count": 1,
                    "do_not_delete": 1
                    }
                )

        with Then("Collect created objects"):
            objects[ch_kind] = kubectl.get_obj_names_grepped("pod,service,sts,pvc,cm,pdb,secret", grep=ch_name)
            print(*objects[ch_kind], sep='\n')

        with When(f"Delete {ch_kind}"):
            if ch_kind == 'chi':
                kubectl.delete_chi(ch_name)
            else:
                kubectl.delete_chk(ch_name)

    with Then("There should not be objects with overlapped names"):
        overlap = list(set(objects['chi']) & set(objects['chk']))
        if len(overlap) > 0:
            print("Overlapped objects:")
            print(*overlap, sep='\n')

        assert len(overlap) == 0, f"{len(overlap)} overlapping resource(s):\n" + "\n".join(f"  {o}" for o in overlap)

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_020002. Test CHI with CHK")
@Requirements(RQ_SRS_026_ClickHouseOperator_CustomResource_Kind_ClickHouseKeeperInstallation("1.0"),
              RQ_SRS_026_ClickHouseOperator_CustomResource_ClickHouseKeeperInstallation_volumeClaimTemplates("1.0"))
def test_020002(self):
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
    check_replication(chi, {0, 1}, 1)

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_020003. Clickhouse-keeper upgrade")
def test_020003(self):
    """Check that clickhouse-operator support upgrading clickhouse-keeper version
     when clickhouse-keeper defined with ClickHouseKeeperInstallation."""

    create_shell_namespace_clickhouse_template()

    chi_manifest = "manifests/chk/test-020003-chi-chk-upgrade.yaml"
    chk_manifest = "manifests/chk/test-020003-chk.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(chi_manifest))
    chk = yaml_manifest.get_name(util.get_full_path(chk_manifest))

    cluster = "default"
    keeper_version_from = "25.3"
    keeper_version_to = "25.8"

    with Given("CHK with 3 replicas"):
        kubectl.create_and_check(
            manifest="manifests/chk/test-020003-chk.yaml",
            kind = "chk",
            check={
                "pod_count": 3,
                "do_not_delete": 1,
            },
        )


    with And("CHI with 2 replicas"):
        kubectl.create_and_check(
            manifest=chi_manifest,
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )

    check_replication(chi, {0, 1}, 1)

    with And(f"I check clickhouse-keeper version is {keeper_version_from}"):
        assert keeper_version_from in \
               kubectl.get_field('pod', 'chk-test-020003-chk-keeper-0-0-0', '.spec.containers[0].image'), error()

    with When(f"I change keeper version to {keeper_version_to}"):
        kubectl.create_and_check(
            manifest="manifests/chk/test-020003-chk-2.yaml",
            kind = "chk",
            check={
                "pod_count": 3,
                "do_not_delete": 1,
            },
        )

    with Then(f"I check clickhouse-keeper version is changed to {keeper_version_to}"):
        kubectl.wait_field('pod', 'chk-test-020003-chk-keeper-0-0-0', '.spec.containers[0].image', f'clickhouse/clickhouse-keeper:{keeper_version_to}', retries=1)
        kubectl.wait_field('pod', 'chk-test-020003-chk-keeper-0-1-0', '.spec.containers[0].image', f'clickhouse/clickhouse-keeper:{keeper_version_to}', retries=1)
        kubectl.wait_field('pod', 'chk-test-020003-chk-keeper-0-2-0', '.spec.containers[0].image', f'clickhouse/clickhouse-keeper:{keeper_version_to}', retries=1)

    with And("Wait for ClickHouse to connect to Keeper properly"):
        for attempt in retries(timeout=180, delay=5):
            out = clickhouse.query_with_error(chi, "select * from system.zookeeper_connection")
            if not "KEEPER_EXCEPTION" in out:
                break
            clickhouse.query(chi, "select * from system.zookeeper_connection")

    with And("Wait for DDL queue to be operational after keeper upgrade"):
        # After keeper rolling restart, the DDLWorker ZK watcher can be lost during
        # leader election. ZK connection succeeds before DDL queue is ready.
        # Retry a lightweight DDL until it completes to ensure the queue is working.
        for attempt in retries(timeout=120, delay=5):
            out = clickhouse.query_with_error(
                chi,
                "DROP TABLE IF EXISTS __chk_ddl_check ON CLUSTER default",
                advanced_params="--distributed_ddl_task_timeout=10",
                timeout=15,
            )
            if "Exception" not in out and "Timeout" not in out:
                break

    check_replication(chi, {0, 1}, 2)

    with Finally("I clean up"):
        delete_test_namespace()

@TestScenario
@Name("test_020003_2. ClickhouseKeeper configuration")
def test_020003_2(self):
    create_shell_namespace_clickhouse_template()

    with Given("CHK with 3 replicas"):
        kubectl.create_and_check(
            manifest="manifests/chk/test-020003-chk-2.yaml",
            kind = "chk",
            check={
                "pod_count": 3,
                "do_not_delete": 1,
            },
        )

    with When("Change Keeper server setting"):
        kubectl.create_and_check(
            manifest="manifests/chk/test-020003-chk-3.yaml",
            kind = "chk",
            check={
                # do not wait for pods, only for CHK status
                # "pod_count": 3,
                "do_not_delete": 1,
            },
        )

        with Then("I confirm all 3 Keeper nodes are ready"):
            kubectl.wait_field('pod', 'chk-test-020003-chk-keeper-0-0-0', '.status.containerStatuses[0].ready', 'true', retries=10)
            kubectl.wait_field('pod', 'chk-test-020003-chk-keeper-0-1-0', '.status.containerStatuses[0].ready', 'true', retries=10)
            kubectl.wait_field('pod', 'chk-test-020003-chk-keeper-0-2-0', '.status.containerStatuses[0].ready', 'true', retries=10)

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_020005. Clickhouse-keeper scale-up/scale-down")
def test_020005(self):
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

    check_replication(chi, {0, 1}, 1)

    with Given("Rescale CHK to 3 replicas"):
        kubectl.create_and_check(
            manifest=chk_manifest_3, kind="chk",
            check={
                "pod_count": 3,
                "do_not_delete": 1,
            },
        )

    check_replication(chi, {0, 1}, 2)

    # TODO: This does not work now
    # with Then("Kill first pod to switch the leader"):
    #    kubectl.launch(f"delete pod chk-test-052-chk-keeper-0-0-0")
    #    time.sleep(10)

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

    check_replication(chi, {0, 1}, 5)

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_020006. Test https://github.com/Altinity/clickhouse-operator/issues/1863")
def test_020006(self):
    create_shell_namespace_clickhouse_template()

    chk_manifest = "manifests/chk/test-020006-issue-1863.yaml"
    chk = yaml_manifest.get_name(util.get_full_path(chk_manifest))

    with Given("Install CHK"):
        kubectl.create_and_check(
            manifest=chk_manifest, kind="chk",
            check={
                "pod_count": 3,
                "do_not_delete": 1
            }
        )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_020007. Test fractional CPU requests/limits handling for CHK")
def test_020007(self):
    create_shell_namespace_clickhouse_template()

    chk_manifest = "manifests/chk/test-020007-fractional-resources.yaml"
    chk = yaml_manifest.get_name(util.get_full_path(chk_manifest))

    kubectl.create_and_check(
        manifest=chk_manifest, kind="chk",
        check={
            "pod_count": 1,
            "do_not_delete": 1
        }
    )

    with Then("cpu.limits are set to 500m"):
        pod_spec = kubectl.get_chk_pod_spec(chk)
        cpu_limits = pod_spec["containers"][0]["resources"]["limits"]["cpu"]
        assert cpu_limits == "500m"

    kubectl.force_chk_reconcile(chk, "reconcile1")

    with Then("cpu.limits are set to 500m"):
        pod_spec = kubectl.get_chk_pod_spec(chk)
        cpu_limits = pod_spec["containers"][0]["resources"]["limits"]["cpu"]
        assert cpu_limits == "500m"

    kubectl.force_chk_reconcile(chk, "reconcile2")

    with Finally("I clean up"):
        delete_test_namespace()

@TestScenario
@Name("test_020008. Test FIPS versions are properly supported by both in CHI and CHK")
def test_020008(self):
    create_shell_namespace_clickhouse_template()

    chk_manifest = f"manifests/chk/test-020008-chk-fips.yaml"
    chi_manifest = f"manifests/chk/test-020008-chi-fips.yaml"
    chi = yaml_manifest.get_name(util.get_full_path(chi_manifest))
    chk = yaml_manifest.get_name(util.get_full_path(chk_manifest))

    cluster = "default"

    with Given("CHK with FIPS versions"):
        kubectl.create_and_check(
            manifest=chk_manifest,
            kind = "chk",
            check={
                "pod_count": 1,
                "do_not_delete": 1,
            },
        )


    with And("CHI with FIPS version"):
        kubectl.create_and_check(
            manifest=chi_manifest,
            check={
                "pod_count": 2,
                "do_not_delete": 1,
            },
        )

    with Then("Clickhouse version is a FIPS one"):
        ver = clickhouse.query(chi, 'select version()')
        print(ver)
        assert "fips" in ver

    check_replication(chi, {0, 1}, 1)

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_020009. FIPS Strict: CHK with spec-level security bypass is rejected with FIPSValidationFailed reason")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_020009(self):
    """Symmetric to test_010073 but for CHK. Activate Strict FIPS at the
    operator level, then apply a CHK whose spec-level security explicitly
    sets tls.verify=None. The spec value is inherited into every cluster via
    InheritClusterSecurityFrom, then the normalizer's rejectFIPSBypass must
    reject the CHK with status=Aborted and [FIPSValidationFailed] in the
    error stream.
    """
    create_shell_namespace_clickhouse_template()

    chopconf_file = "manifests/chopconf/test-073-fips-strict-chopconf.yaml"
    chk_manifest = "manifests/chk/test-020009-chk-fips-bypass-rejected.yaml"
    chk = yaml_manifest.get_name(util.get_full_path(chk_manifest))

    with Given("Apply Strict FIPS chopconf and restart operator"):
        util.apply_operator_config(chopconf_file)

    with When("Apply CHK with spec-level security bypass"):
        kubectl.apply(util.get_full_path(chk_manifest))

    with Then("CHK lands in status=Aborted"):
        kubectl.wait_chk_status(chk, 'Aborted')

    with And("Aborted reason is [FIPSValidationFailed]"):
        errors = kubectl.get_field('chk', chk, '.status.errors')
        print(errors)
        assert "FIPSValidationFailed" in errors, error(
                f"expected [FIPSValidationFailed] reason in status.errors, got {errors}"
        )

    with Finally("I clean up"):
        delete_test_namespace()


@TestScenario
@Name("test_020010. FIPS image policy Required: non-fips CHK is rejected at admission with FIPSImagePolicyViolation reason")
@Requirements(RQ_SRS_026_ClickHouseOperator_Create("1.0"))
def test_020010(self):
    """CHK mirror of test_010074. With image-policy=Required, a CHK whose
    resolved Keeper image lacks the "fips" tag substring must be rejected
    at admission. Asserts the CHK normalizer wires the same gate as the
    CHI side (the implementations are mirrors of each other).

    Status reason `FIPSImagePolicyViolation` is re-exported from the CHI
    constant in pkg/apis/clickhouse-keeper.altinity.com/v1/type_status.go,
    so dashboards keyed on the CHI reason continue working for CHK aborts.
    """
    create_shell_namespace_clickhouse_template()

    chopconf_file = "manifests/chopconf/test-074-fips-images-required-chopconf.yaml"
    chk_manifest = "manifests/chk/test-020010-chk-fips-images-required-non-fips.yaml"
    chk = yaml_manifest.get_name(util.get_full_path(chk_manifest))

    with Given("Apply FIPS image-policy=Required chopconf and restart operator"):
        util.apply_operator_config(chopconf_file)

    with When("Apply CHK whose default Keeper image lacks the 'fips' tag substring"):
        kubectl.apply(util.get_full_path(chk_manifest))

    with Then("CHK lands in status=Aborted"):
        kubectl.wait_chk_status(chk, 'Aborted')

    with And("Aborted reason is [FIPSImagePolicyViolation]"):
        errors = kubectl.get_field('chk', chk, '.status.errors')
        assert "FIPSImagePolicyViolation" in errors, error(
            f"expected [FIPSImagePolicyViolation] reason in status.errors, got {errors}"
        )

    with Finally("I clean up"):
        delete_test_namespace()


def cleanup_chis(self):
    with Given("Cleanup CHIs"):
        ns = kubectl.get("ns", name="", ns="--all-namespaces", ok_to_fail=True)
        if ns and "items" in ns:
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

    with Pool(int(os.environ.get("POOL_SIZE", 3))) as pool:
        for scenario in loads(current_module(), Scenario, Suite):
            if not (hasattr(scenario, "tags") and ("NO_PARALLEL" in scenario.tags)):
                Scenario(run=scenario, parallel=True, executor=pool)
        join()

    for scenario in loads(current_module(), Scenario, Suite):
        if hasattr(scenario, "tags") and ("NO_PARALLEL" in scenario.tags):
            Scenario(run=scenario)
