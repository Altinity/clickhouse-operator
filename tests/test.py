import json
import json
import os
import time

from testflows.core import TestScenario, Name, When, Then, Given, And, main, run, Module, TE
from testflows.asserts import error
from testflows.connect import Shell

from kubectl import *
from clickhouse import *
from test import kube_count_resources

namespace="test"

def get_full_path(test_file):
    return os.path.join(current_dir, f"{test_file}")

def get_chi_name(path):
    return yaml.safe_load(open(path,"r"))["metadata"]["name"]

def create_and_check(test_file, checks):
    config=get_full_path(test_file)
    chi_name=get_chi_name(config)
    
    if "apply_templates" in checks:
        for t in checks["apply_templates"]:
            kube_apply(get_full_path(t), namespace)

    kube_apply(config, namespace)
    
    if "object_counts" in checks:
        kube_wait_objects(chi_name, namespace, checks["object_counts"])
        
    if "chi_status" in checks:
        kube_wait_chi_status(chi_name, namespace, checks["chi_status"])
    else:
        kube_wait_chi_status(chi_name, namespace, "Completed")
        
    if "pod_image" in checks:
        kube_check_pod_image(chi_name, namespace, checks["pod_image"])
    
    if "pod_volumes" in checks:
        kube_check_pod_volumes(chi_name, namespace, checks["pod_volumes"])
        
    if "pod_podAntiAffinity" in checks:
        kube_check_pod_antiaffinity(chi_name, namespace)
        
    if "pod_ports" in checks:
        kube_check_pod_ports(chi_name, namespace, checks["pod_ports"])
    
    if "do_not_delete" not in checks:
        kube_delete(config, namespace)
        kube_wait_objects(chi_name, namespace, [0,0,0])


@TestScenario
@Name("Empty installation, creates 1 node")
def test_examples01_1():
    create_and_check("../docs/chi-examples/01-simple-layout-01-1shard-1repl.yaml", {"object_counts": [1,1,2]})

@TestScenario
@Name("1 shard 2 replicas")
def test_examples01_2():
    create_and_check("../docs/chi-examples/01-simple-layout-02-1shard-2repl.yaml", {"object_counts": [2,2,3]})

@TestScenario
@Name("Persistent volume mapping via defaults")
def test_examples02_1():
    create_and_check("../docs/chi-examples/02-persistent-volume-01-default-volume.yaml", 
                     {"object_counts": [1,1,2],
                      "pod_volumes": {"/var/lib/clickhouse", "/var/log/clickhouse-server"}})

@TestScenario
@Name("Persistent volume mapping via podTemplate")
def test_examples02_2():
    create_and_check("../docs/chi-examples/02-persistent-volume-02-pod-template.yaml", 
                     {"object_counts": [1,1,2],
                      "pod_image": "yandex/clickhouse-server:19.3.7",
                      "pod_volumes": {"/var/lib/clickhouse", "/var/log/clickhouse-server"}})
@TestScenario
@Name("1 node")
def test_001():
    create_and_check("configs/test-001.yaml", {"object_counts": [1,1,2]})
    
@TestScenario
@Name("useTemplates for pod and volume templates")
def test_002():
    create_and_check("configs/test-002-tpl.yaml", 
                     {"object_counts": [1,1,2],
                      "apply_templates": {"configs/tpl-clickhouse-stable.yaml", "configs/tpl-log-volume.yaml"},
                      "pod_image": "yandex/clickhouse-server:19.11.8.46",
                      "pod_volumes": {"/var/log/clickhouse-server"}})

@TestScenario
@Name("useTemplates for pod and distribution templates")
def test_003():
    create_and_check("configs/test-003-tpl.yaml", 
                     {"object_counts": [1,1,2],
                      "apply_templates": {"configs/tpl-clickhouse-stable.yaml", "configs/tpl-one-per-host.yaml"},
                      "pod_image": "yandex/clickhouse-server:19.11.8.46",
                      "pod_podAntiAffinity": 1})

@TestScenario
@Name("Compatibility test if old syntax with volumeClaimTemplate is still supported")
def test_004():
    create_and_check("configs/test-004-tpl.yaml", 
                     {"object_counts": [1,1,2],
                      "pod_volumes": {"/var/lib/clickhouse"}})

@TestScenario
@Name("Test manifest created by ACM")
def test_005():
    create_and_check("configs/test-005-acm.yaml", 
                     {"object_counts": [1,1,2],
                      "pod_image": "yandex/clickhouse-server:19.11.8.46",
                      "pod_volumes": {"/var/lib/clickhouse"}})

@TestScenario
@Name("Test clickhouse version upgrade from one version to another using podTemplate change")
def test_006():
    create_and_check("configs/test-006-ch-upgrade-1.yaml", 
                     {"object_counts": [2,2,3],
                      "pod_image": "yandex/clickhouse-server:19.11.8.46",
                      "do_not_delete": 1})
    with Then("Use different podTemplate and confirm that pod image is updated"):  
        create_and_check("configs/test-006-ch-upgrade-2.yaml", 
                         {"object_counts": [2,2,3],
                          "pod_image": "yandex/clickhouse-server:latest",
                          "do_not_delete": 1})
        with Then("Change image in podTemplate itself and confirm that pod image is updated"):
            create_and_check("configs/test-006-ch-upgrade-3.yaml", 
                             {"object_counts": [2,2,3],
                              "pod_image": "yandex/clickhouse-server:19.11.8.46"})

@TestScenario
@Name("Test template with custom clickhouse ports")
def test_007():
    create_and_check("configs/test-007-custom-ports.yaml", 
                     {"object_counts": [1,1,2],
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

        kube_apply(config, namespace)
        kube_wait_objects(chi_cut_name, namespace, [1,1,2])
        kube_wait_chi_status(chi_full_name, namespace, "Completed")
        
        assert kube_get_count("statefulset", namespace, "-l clickhouse.altinity.com/app=chop")==1, error()
        
        with Then(f"upgrade operator to {version_to}"):
            set_operator_version(version_to)
            with And("Wait 20 seconds"):
                time.sleep(20)
                with Then("No new statefulsets should be created"):
                    assert kube_get_count("statefulset", namespace, "-l clickhouse.altinity.com/app=chop")==1, error()

        
        
def set_operator_version(version):
    kubectl(f"set image deployment.v1.apps/clickhouse-operator clickhouse-operator=altinity/clickhouse-operator:{version} -n kube-system")
    kubectl("rollout status deployment.v1.apps/clickhouse-operator -n kube-system")
    # assert kube_get_count("pod", "kube-system", f"-l app=clickhouse-operator,version={version}")>0, error()

@TestScenario
@Name("Test zookeeper initialization")
def test_010():
    with Given("Zookeeper is installed"):
        assert kube_get("service", "zookeeper", "zoo1ns"), error()
    create_and_check("configs/test-010-zkroot.yaml", 
                     {"object_counts": [1,1,2],
                      "do_not_delete": 1})
    with And("ClickHouse should complain regarding zookeeper path"):
        out = clickhouse_query_with_error("test-010-zkroot", namespace, "select * from system.zookeeper where path = '/'")
        assert "You should create root node /clickhouse/test-010-zkroot before start" in out, error()
    
    create_and_check("configs/test-010-zkroot.yaml",{})

if main():
    with Module("init"):
        with Given("clickhouse-operator is installed"):
            assert kube_get_count("pod", "kube-system", "-l app=clickhouse-operator")>0, error()
            with And(f"Clean namespace {namespace}"): 
                kube_deletens(namespace)
                with And(f"Create namespace {namespace}"):
                    kube_createns(namespace)
    
        with Module("examples", flags=TE):
            examples=[test_examples01_1, test_examples01_2, test_examples02_1, test_examples02_2]
            for t in examples:
                run(test=t, flags=TE)
        
        with Module("regression", flags=TE):
            tests = [test_001, 
                     test_002, 
                     test_003, # can fail in dev 0.8.0 version 
                     test_004, 
                     test_005, 
                     test_006, 
                     test_007, 
                     test_010]
        
            all_tests = tests
            # all_tests = [test_010]
        
            for t in all_tests:
                run(test=t, flags=TE)

