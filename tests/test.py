import json
import json
import os
import time

from testflows.core import TestScenario, Name, When, Then, Given, And, main, run, Module
from testflows.asserts import error
from testflows.connect import Shell

from kubectl import *
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
    
    if "do_not_delete" not in checks:
        kube_delete(config, namespace)
        kube_wait_objects(chi_name, namespace, [0,0,0])

@TestScenario
@Name("1 node")
def test1():
    create_and_check("configs/test-001.yaml", {"object_counts": [1,1,2]})

@TestScenario
@Name("Empty installation, creates 1 node")
def test2():
    create_and_check("../docs/chi-examples/01-simple-layout-01-1shard-1repl.yaml", {"object_counts": [1,1,2]})

@TestScenario
@Name("1 shard 2 replicas")
def test3():
    create_and_check("../docs/chi-examples/01-simple-layout-02-1shard-2repl.yaml", {"object_counts": [2,2,3]})

@TestScenario
@Name("Persistent volume mapping via defaults")
def test4():
    create_and_check("../docs/chi-examples/02-persistent-volume-01-default-volume.yaml", 
                     {"object_counts": [1,1,2],
                      "pod_volumes": {"/var/lib/clickhouse", "/var/log/clickhouse-server"}})

@TestScenario
@Name("Persistent volume mapping via podTemplate")
def test5():
    create_and_check("../docs/chi-examples/02-persistent-volume-02-pod-template.yaml", 
                     {"object_counts": [1,1,2],
                      "pod_image": "yandex/clickhouse-server:19.3.7",
                      "pod_volumes": {"/var/lib/clickhouse", "/var/log/clickhouse-server"}})

@TestScenario
@Name("useTemplates for pod and volume templates")
def test6():
    create_and_check("configs/test-002-tpl.yaml", 
                     {"object_counts": [1,1,2],
                      "apply_templates": {"configs/tpl-clickhouse-stable.yaml", "configs/tpl-log-volume.yaml"},
                      "pod_image": "yandex/clickhouse-server:19.11.8.46",
                      "pod_volumes": {"/var/log/clickhouse-server"}})

@TestScenario
@Name("useTemplates for pod and distribution templates")
def test7():
    create_and_check("configs/test-003-tpl.yaml", 
                     {"object_counts": [1,1,2],
                      "apply_templates": {"configs/tpl-clickhouse-stable.yaml", "configs/tpl-one-per-host.yaml"},
                      "pod_image": "yandex/clickhouse-server:19.11.8.46",
                      "pod_podAntiAffinity": 1})
@TestScenario
@Name("Compatibility test if old syntax with volumeClaimTemplate is still supported")
def test8():
    create_and_check("configs/test-004-tpl.yaml", 
                     {"object_counts": [1,1,2],
                      "pod_volumes": {"/var/lib/clickhouse"}})
            
if main():
    with Module("regression"):
        with Given("clickhouse-operator in installed"):
            assert kube_get_count("pod", "kube-system", "-l app=clickhouse-operator")>0, error()
            with And(f"Clean namespace {namespace}"): 
                kube_deletens(namespace)
                with And(f"Create namespace {namespace}"):
                    kube_createns(namespace)

        all_tests = [test1, test2, test3, test4, test5, test6, test7, test8]
        # all_tests = [test8]
        
        for t in all_tests:
            run(test=t)

