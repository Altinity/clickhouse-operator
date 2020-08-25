import json
import os
import time
import yaml

from settings import kubectlcmd
from settings import test_namespace

from testflows.core import TestScenario, Name, When, Then, Given, And, main, run, Module
from testflows.asserts import error
from testflows.connect import Shell

current_dir = os.path.dirname(os.path.abspath(__file__))
max_retries=10

shell = Shell()
namespace = test_namespace


def get_full_path(test_file):
    return os.path.join(current_dir, f"{test_file}")

def get_chi_name(path):
    return yaml.safe_load(open(path,"r"))["metadata"]["name"]

def kubectl(command, ok_to_fail=False, ns = namespace, timeout=60):
    cmd = shell(f"{kubectlcmd} -n {ns} {command}", timeout=timeout)
    code = cmd.exitcode
    if not ok_to_fail:
        assert code == 0, error()
    return cmd.output

def kube_delete_chi(chi, ns = namespace):
    with When(f"Delete chi {chi}"):
        shell(f"{kubectlcmd} delete chi {chi} -n {ns}", timeout=900)
        kube_wait_objects(chi, [0,0,0], ns)

def kube_delete_all_chi(ns = namespace):
    crds = kubectl("get crds -o=custom-columns=name:.metadata.name", ns=ns).splitlines()
    if "clickhouseinstallations.clickhouse.altinity.com" in crds:
        chis = kube_get("chi", "", ns = ns)["items"]
        for chi in chis:
            # kubectl(f"patch chi {chi} --type=merge -p '\{\"metadata\":\{\"finalizers\": [null]\}\}'", ns = ns)
            kube_delete_chi(chi["metadata"]["name"], ns)

def create_and_check(test_file, checks, ns = namespace):
    config=get_full_path(test_file)
    chi_name=get_chi_name(config)
    
    if "apply_templates" in checks:
        for t in checks["apply_templates"]:
            kube_apply(get_full_path(t), ns)
        time.sleep(1)

    kube_apply(config, ns)
    
    if "object_counts" in checks:
        kube_wait_objects(chi_name, checks["object_counts"], ns)
    
    if "pod_count" in checks:
        kube_wait_object("pod", "", label = f"-l clickhouse.altinity.com/chi={chi_name}", count = checks["pod_count"], ns = ns)
        
    if "chi_status" in checks:
        kube_wait_chi_status(chi_name, checks["chi_status"], ns)
    else:
        kube_wait_chi_status(chi_name, "Completed", ns)

    if "pod_image" in checks:
        kube_check_pod_image(chi_name, checks["pod_image"], ns)

    if "pod_volumes" in checks:
        kube_check_pod_volumes(chi_name, checks["pod_volumes"], ns)

    if "pod_podAntiAffinity" in checks:
        kube_check_pod_antiaffinity(chi_name, ns)

    if "pod_ports" in checks:
        kube_check_pod_ports(chi_name, checks["pod_ports"], ns)

    if "service" in checks:
        kube_check_service(checks["service"][0], checks["service"][1], ns)

    if "configmaps" in checks:
        kube_check_configmaps(chi_name, ns)

    if "do_not_delete" not in checks:
        kube_delete_chi(chi_name, ns)

def kube_get(type, name, label="", ns = namespace):
    cmd = shell(f"{kubectlcmd} get {type} {name} {label} -o json -n {ns}")
    assert cmd.exitcode == 0, error()
    return json.loads(cmd.output.strip())

def kube_createns(ns):
    cmd = shell(f"{kubectlcmd} create ns {ns}")
    assert cmd.exitcode == 0, error()
    cmd = shell(f"{kubectlcmd} get ns {ns}")
    assert cmd.exitcode == 0, error()

def kube_deletens(ns):
    shell(f"{kubectlcmd} delete ns {ns}", timeout=60)
    
def kube_get_count(type, name="", label="", ns = namespace):
    if ns is None:
        ns = '--all-namespaces'
    elif '-n' not in ns and '--namespace' not in ns:
        ns = f"-n {ns}"
    cmd = shell(f"{kubectlcmd} get {type} {name} {ns} -o=custom-columns=kind:kind,name:.metadata.name {label}")
    if cmd.exitcode == 0:
        return len(cmd.output.splitlines())-1
    else:
        return 0

def kube_count_resources(label="", ns = namespace):
    sts = kube_get_count("sts", ns=ns, label=label)
    pod = kube_get_count("pod", ns=ns, label=label)
    service = kube_get_count("service", ns=ns, label=label)
    return [sts, pod, service]

def kube_apply(config, ns=namespace, validate=True, timeout=30):
    with When(f"{config} is applied"):
        cmd = f"{kubectlcmd} apply --validate={validate} -n {ns} -f {config}"
        cmd = shell(cmd, timeout=timeout)
    with Then("exitcode should be 0"):
        assert cmd.exitcode == 0, error()

def kube_delete(config, ns=namespace, timeout=30):
    with When(f"{config} is deleted"):
        cmd = shell(f"{kubectlcmd} delete -n {ns} -f {config}", timeout=timeout)
    with Then("exitcode should be 0"):
        assert cmd.exitcode == 0, error()

def kube_wait_objects(chi, objects, ns=namespace):
    with Then(f"{objects[0]} statefulsets, {objects[1]} pods and {objects[2]} services should be created"):
        for i in range(1,max_retries):
            counts = kube_count_resources(label = f"-l clickhouse.altinity.com/chi={chi}", ns = ns)
            if counts == objects:
                break
            with Then("Not ready. Wait for " + str(i*5) + " seconds"):
                time.sleep(i*5)
        assert counts == objects, error()

def kube_wait_object(type, name, label="", count = 1, ns = namespace, retries = max_retries):
    with Then(f"{count} {type}(s) {name} should be created"):
        for i in range(1,retries):
            counts = kube_get_count(type, ns = ns, name = name, label = label)
            if counts >= count:
                break
            with Then("Not ready. Wait for " + str(i*5) + " seconds"):
                time.sleep(i*5)
        assert counts >= count, error()

def kube_wait_chi_status(chi, status, ns = namespace, retries = max_retries):
    kube_wait_field("chi", chi, ".status.status", status, ns, retries)

def kube_get_chi_status(chi, ns = namespace):
    kube_get_field("chi", chi, ".status.status", ns)

def kube_wait_pod_status(pod, status, ns = namespace):
    kube_wait_field("pod", pod, ".status.phase", status, ns)

def kube_wait_field(object, name, field, value, ns = namespace, retries = max_retries):
    with Then(f"{object} {name} {field} should be {value}"):
        for i in range(1,retries):
            obj_status = kubectl(f"get {object} {name} -o=custom-columns=field:{field}", ns=ns).splitlines()
            if obj_status[1] == value:
                break
            with Then("Not ready. Wait for " + str(i*5) + " seconds"):
                time.sleep(i*5)
        assert obj_status[1] == value, error()

def kube_wait_jsonpath(object, name, field, value, ns=namespace, retries = max_retries):
    with Then(f"{object} {name} -o jsonpath={field} should be {value}"):
        for i in range(1,retries):
            obj_status = kubectl(f"get {object} {name} -o jsonpath=\"{field}\"", ns=ns).splitlines()
            if obj_status[0] == value:
                break
            with Then("Not ready. Wait for " + str(i*5) + " seconds"):
                time.sleep(i*5)
        assert obj_status[0] == value, error()

def kube_get_field(object, name, field, ns = namespace):
    out = kubectl(f"get {object} {name} -o=custom-columns=field:{field}", ns=ns).splitlines()
    return out[1]

def kube_get_default_storage_class(ns = namespace):
    out = kubectl(f"get storageclass -o=custom-columns=DEFAULT:\".metadata.annotations.storageclass\.kubernetes\.io/is-default-class\",NAME:.metadata.name", ns=ns).splitlines()
    for line in out[1:]:
        if line.startswith("true"):
            parts = line.split(maxsplit=1)
            return parts[1].strip()

def kube_get_pod_spec(chi_name, ns = namespace):
    pod = kube_get("pod", "", ns = ns, label = f"-l clickhouse.altinity.com/chi={chi_name}")["items"][0]
    return pod["spec"]

def kube_get_pod_image(chi_name, ns = namespace):
    pod_image = kube_get_pod_spec(chi_name, ns)["containers"][0]["image"]
    return pod_image

def kube_get_pod_names(chi_name, ns = namespace):
    pod_names = kubectl(f"get pods -o=custom-columns=name:.metadata.name -l clickhouse.altinity.com/chi={chi_name}", ns=ns).splitlines();
    return pod_names[1:]

def kube_get_pod_volumes(chi_name, ns = namespace):
    volumeMounts = kube_get_pod_spec(chi_name, ns)["containers"][0]["volumeMounts"]
    return volumeMounts

def kube_get_pod_ports(chi_name, ns = namespace):
    port_specs = kube_get_pod_spec(chi_name, ns)["containers"][0]["ports"]
    ports = []
    for p in port_specs:
        ports.append(p["containerPort"])
    return ports

def kube_check_pod_ports(chi_name, ports, ns = namespace):
    pod_ports = kube_get_pod_ports(chi_name, ns)
    with Then(f"Expect pod ports {pod_ports} to match {ports}"):
        assert pod_ports.sort() == ports.sort()

def kube_check_pod_image(chi_name, image, ns = namespace):
    pod_image = kube_get_pod_image(chi_name, ns)
    with Then(f"Expect pod image {pod_image} to match {image}"):
        assert pod_image == image

def kube_check_pod_volumes(chi_name, volumes, ns = namespace):
    pod_volumes = kube_get_pod_volumes(chi_name, ns)
    for v in volumes:
        with Then(f"Expect pod has volume mount {v}"):
            found = 0
            for vm in pod_volumes:
                if vm["mountPath"] == v:
                    found = 1
                    break
            assert found == 1

def kube_get_pvc_size(pvc_name, ns = namespace):
    return kube_get_field("pvc", pvc_name, ".spec.resources.requests.storage", ns)

def kube_check_pod_antiaffinity(chi_name, ns = namespace):
    pod_spec = kube_get_pod_spec(chi_name, ns)
    expected = {"requiredDuringSchedulingIgnoredDuringExecution": [
                    {
                        "labelSelector": {
                            "matchLabels": {
                                    "clickhouse.altinity.com/app": "chop",
                                    "clickhouse.altinity.com/chi": f"{chi_name}",
                                    "clickhouse.altinity.com/namespace": f"{ns}"
                            }
                        },
                        "topologyKey": "kubernetes.io/hostname"
                    }
                    ]
                }
    with Then(f"Expect podAntiAffinity to exist and match {expected}"):
        assert "affinity" in pod_spec
        assert "podAntiAffinity" in pod_spec["affinity"]
        assert pod_spec["affinity"]["podAntiAffinity"] == expected

def kube_check_service(service_name, service_type, ns = namespace):
    with When(f"{service_name} is available"):
        service = kube_get("service", service_name, ns = ns)
        with Then(f"Service type is {service_type}"):
            assert service["spec"]["type"] == service_type

def kube_check_configmaps(chi_name, ns = namespace):
    kube_check_configmap(f"chi-{chi_name}-common-configd",
                         ["01-clickhouse-listen.xml",
                          "02-clickhouse-logger.xml",
                          "03-clickhouse-querylog.xml"], ns = ns)

    kube_check_configmap(f"chi-{chi_name}-common-usersd",
                         ["01-clickhouse-user.xml",
                          "02-clickhouse-default-profile.xml"],
                         ns = ns)

def kube_check_configmap(cfg_name, values, ns = namespace):
    cfm =  kube_get("configmap", cfg_name, ns = ns)
    for v in values:
        with Then(f"{cfg_name} should contain {v}"):
            assert v in cfm["data"]


