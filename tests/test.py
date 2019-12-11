import json
import json
import os
import time

from testflows.core import TestScenario, Name, When, Then, Given, And, main, run, Module, TE
from testflows.asserts import error
from testflows.connect import Shell

from kubectl import *
from clickhouse import *
from test_examples import *

@TestScenario
@Name("1 node")
def test_001():
    create_and_check("configs/test-001.yaml", {"object_counts": [1,1,2]})
    
@TestScenario
@Name("useTemplates for pod and volume templates")
def test_002():
    create_and_check("configs/test-002-tpl.yaml", 
                     {"pod_count": 1,
                      "apply_templates": {"configs/tpl-clickhouse-stable.yaml", "configs/tpl-log-volume.yaml"},
                      "pod_image": "yandex/clickhouse-server:19.11.8.46",
                      "pod_volumes": {"/var/log/clickhouse-server"}})

@TestScenario
@Name("useTemplates for pod and distribution templates")
def test_003():
    create_and_check("configs/test-003-tpl.yaml", 
                     {"pod_count": 1,
                      "apply_templates": {"configs/tpl-clickhouse-stable.yaml", "configs/tpl-one-per-host.yaml"},
                      "pod_image": "yandex/clickhouse-server:19.11.8.46",
                      "pod_podAntiAffinity": 1})

@TestScenario
@Name("Compatibility test if old syntax with volumeClaimTemplate is still supported")
def test_004():
    create_and_check("configs/test-004-tpl.yaml", 
                     {"pod_count": 1,
                      "pod_volumes": {"/var/lib/clickhouse"}})

@TestScenario
@Name("Test manifest created by ACM")
def test_005():
    create_and_check("configs/test-005-acm.yaml", 
                     {"pod_count": 1,
                      "pod_image": "yandex/clickhouse-server:19.11.8.46",
                      "pod_volumes": {"/var/lib/clickhouse"}})

@TestScenario
@Name("Test clickhouse version upgrade from one version to another using podTemplate change")
def test_006():
    create_and_check("configs/test-006-ch-upgrade-1.yaml", 
                     {"pod_count": 2,
                      "pod_image": "yandex/clickhouse-server:19.11.8.46",
                      "do_not_delete": 1})
    with Then("Use different podTemplate and confirm that pod image is updated"):  
        create_and_check("configs/test-006-ch-upgrade-2.yaml", 
                         {"pod_count": 2,
                          "pod_image": "yandex/clickhouse-server:latest",
                          "do_not_delete": 1})
        with Then("Change image in podTemplate itself and confirm that pod image is updated"):
            create_and_check("configs/test-006-ch-upgrade-3.yaml", 
                             {"pod_count": 2,
                              "pod_image": "yandex/clickhouse-server:19.11.8.46"})

@TestScenario
@Name("Test template with custom clickhouse ports")
def test_007():
    create_and_check("configs/test-007-custom-ports.yaml", 
                     {"pod_count": 1,
                      "apply_templates": {"configs/tpl-custom-ports.yaml"},
                      "pod_image": "yandex/clickhouse-server:19.11.8.46",
                      "pod_ports": [8124,9001,9010]})

@TestScenario
@Name("Test operator upgrade from 0.6.0 to 0.7.0 version")
def test_009():
    version_from="0.6.0"
    version_to="dev"
    with Given(f"clickhouse-operator {version_from}"):
        set_operator_version(version_from)
        config=get_full_path("configs/test-009-long-name.yaml")
        chi_full_name=get_chi_name(config)
        chi_cut_name=chi_full_name[0:15]

        kube_apply(config)
        kube_wait_objects(chi_cut_name, [1,1,2])
        kube_wait_chi_status(chi_full_name, "Completed")
        
        assert kube_get_count("statefulset", label = "-l clickhouse.altinity.com/app=chop")==1, error()
        
        with Then(f"upgrade operator to {version_to}"):
            set_operator_version(version_to)
            with And("Wait 20 seconds"):
                time.sleep(20)
                with Then("No new statefulsets should be created"):
                    assert kube_get_count("statefulset", label = "-l clickhouse.altinity.com/app=chop")==1, error()

        
        
def set_operator_version(version):
    kubectl(f"set image deployment.v1.apps/clickhouse-operator clickhouse-operator=altinity/clickhouse-operator:{version} -n kube-system")
    kubectl("rollout status deployment.v1.apps/clickhouse-operator -n kube-system")
    # assert kube_get_count("pod", ns = "kube-system", label = f"-l app=clickhouse-operator,version={version}")>0, error()

@TestScenario
@Name("Test zookeeper initialization")
def test_010():
    with Given("Install Zookeeper if missing"):
        if kube_get_count("service", name = "zookeepers") == 0:
            config=get_full_path("../deploy/zookeeper/quick-start-volume-emptyDir/zookeeper-1-node.yaml")
            kube_apply(config)
            kube_wait_object("service", "zookeepers")

    create_and_check("configs/test-010-zkroot.yaml", 
                     {"pod_count": 1,
                      "do_not_delete": 1})
    with And("ClickHouse should complain regarding zookeeper path"):
        out = clickhouse_query_with_error("test-010-zkroot", "select * from system.zookeeper where path = '/'")
        assert "You should create root node /clickhouse/test-010-zkroot before start" in out, error()
    
    create_and_check("configs/test-010-zkroot.yaml",{})

@TestScenario
@Name("Test access isolation")    
def test_011():
    create_and_check("configs/test-011-secure-user.yaml", 
                     {"pod_count": 2,
                      "do_not_delete": 1})
    create_and_check("configs/test-011-insecure-user.yaml", 
                     {"pod_count": 1,
                      "do_not_delete": 1})
    with Then("Query between hosts should succeed"):
        out = clickhouse_query_with_error("test-011-secure-user", 
                "select 'OK' from remote('chi-test-011-secure-user-default-1-0', system.one)")
        assert out == 'OK', error()
    with And("Query from other host should fail"):
        out = clickhouse_query_with_error("test-011-insecure-user", 
                "select 'OK' from remote('chi-test-011-secure-user-default-1-0', system.one)")
        assert out != 'OK', error()

    
kubectl.namespace="test"

if main():
    with Module("main"):
        with Given("clickhouse-operator is installed"):
            assert kube_get_count("pod", ns = "kube-system", label = "-l app=clickhouse-operator")>0, error()
            with And(f"Clean namespace {kubectl.namespace}"): 
                kube_deletens(kubectl.namespace)
                with And(f"Create namespace {kubectl.namespace}"):
                    kube_createns(kubectl.namespace)
    
        with Module("examples", flags=TE):
            examples=[test_examples01_1, test_examples01_2, test_examples02_1, test_examples02_2]
            for t in examples:
                run(test=t, flags=TE)
        
        with Module("regression", flags=TE):
            tests = [test_001, 
                     test_002, 
                     test_003,
                     test_004, 
                     test_005, 
                     test_006, 
                     test_007, 
                     test_010,
                     test_011]
        
            # all_tests = tests
            all_tests = [test_011]
        
            for t in all_tests:
                run(test=t, flags=TE)

