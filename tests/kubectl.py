import json
import os
import time
import yaml

from testflows.core import TestScenario, Name, When, Then, Given, And, main, run, Module
from testflows.asserts import error
from testflows.connect import Shell

current_dir = os.path.dirname(os.path.abspath(__file__))
max_retries=10

shell = Shell()

def kube_get(type, name, ns="default"):
    cmd = shell(f"kubectl get {type} {name} -n {ns} -o json")
    assert cmd.exitcode == 0, error()
    return json.loads(cmd.output.strip())

def kube_createns(ns):
    cmd = shell(f"kubectl create ns {ns}")
    assert cmd.exitcode == 0, error()
    time.sleep(5)

def kube_deletens(ns):
    shell(f"kubectl delete ns {ns}", timeout = 60)
    
def kube_get_count(type, ns="default", label=""):
    cmd = shell(f"kubectl get {type} -n {ns} -o=custom-columns=kind:kind,name:.metadata.name {label}")
    if cmd.exitcode == 0:
        return len(cmd.output.splitlines())-1
    else:
        return 0

def kube_count_resources(ns="default", label=""):
    sts = kube_get_count("sts", ns, label)
    pod = kube_get_count("pod", ns, label)
    service = kube_get_count("service", ns, label)
    return [sts, pod, service]

def kube_apply(config, ns):
    with When(f"{config} is applied"):
        cmd = shell(f"kubectl apply -f {config} -n {ns}")
    with Then("exitcode should be 0"):
        assert cmd.exitcode == 0, error()

def kube_delete(config, ns):
    with When(f"{config} is deleted"):
        cmd = shell(f"kubectl delete -f {config} -n {ns}")
    with Then("exitcode should be 0"):
        assert cmd.exitcode == 0, error()

def kube_wait_objects(chi, ns, objects):
    with Then(f"{objects[0]} statefulsets, {objects[1]} pods and {objects[2]} services should be created"):
        for i in range(1,max_retries):
            counts = kube_count_resources(ns, f"-l clickhouse.altinity.com/chi={chi}")
            if counts == objects:
                break
            with Then("Not ready. Wait for " + str(i*5) + " seconds"):
                time.sleep(i*5)
        assert counts == objects, error()

def kube_wait_chi_status(chi, ns, status):        
    with Then(f"Installation status is {status}"):
        for i in range(1,max_retries):
            state = kube_get("chi", chi, ns)
            try:
                if (state["status"]["status"] == status):
                    break
            except KeyError:
                pass
            with Then("Not ready. Wait for " + str(i*5) + " seconds"):
                time.sleep(i*5)
        assert state["status"]["status"] == status, error()

def kube_get_pod_spec(chi_name, ns):
    chi = kube_get("chi", chi_name, ns)
    pod = kube_get("pod", chi["status"]["pods"][0], ns)
    return pod["spec"]

def kube_get_pod_image(chi_name, ns):
    pod_image = kube_get_pod_spec(chi_name, ns)["containers"][0]["image"]
    return pod_image

def kube_get_pod_volumes(chi_name, ns):
    volumeMounts = kube_get_pod_spec(chi_name, ns)["containers"][0]["volumeMounts"]
    return volumeMounts


def kube_check_pod_image(chi_name, ns, image):
    pod_image = kube_get_pod_image(chi_name, ns)
    with Then(f"Expect pod image to match {image}"):
        assert pod_image == image

def kube_check_pod_volumes(chi_name, ns, volumes):
    pod_volumes = kube_get_pod_volumes(chi_name, ns)
    for v in volumes:
        with Then(f"Expect pod has volume mount {v}"):
            found = 0
            for vm in pod_volumes:
                if vm["mountPath"] == v:
                    found = 1
                    break
            assert found == 1

def kube_check_pod_antiaffinity(chi_name, ns):
    pod_spec = kube_get_pod_spec(chi_name, ns)
    expected = {"requiredDuringSchedulingIgnoredDuringExecution": [
                    {
                        "labelSelector": {
                            "matchExpressions": [
                                {
                                    "key": "clickhouse.altinity.com/app",
                                    "operator": "In",
                                    "values": [
                                        "chop"
                                    ]
                                }
                            ]
                        },
                        "topologyKey": "kubernetes.io/hostname"
                    }
                    ]
                }
    with Then(f"Expect podAntiAffinity to exist and match {expected}"):
        assert "affinity" in pod_spec
        assert "podAntiAffinity" in pod_spec["affinity"]
        assert pod_spec["affinity"]["podAntiAffinity"] == expected
