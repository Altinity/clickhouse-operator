import json
import os
import time
import manifest
import util

from testflows.core import TestScenario, Name, When, Then, Given, And, main, run, Module
from testflows.asserts import error
from testflows.connect import Shell

import settings

current_dir = os.path.dirname(os.path.abspath(__file__))
max_retries = 10

shell = Shell()
namespace = settings.test_namespace
kubectl_cmd = settings.kubectl_cmd


def run(command, ok_to_fail=False, ns=namespace, timeout=60):
    cmd = shell(f"{kubectl_cmd} -n {ns} {command}", timeout=timeout)
    code = cmd.exitcode
    if not ok_to_fail:
        if code != 0:
            print("command failed, output:")
            print(cmd.output)
        assert code == 0, error()
    return cmd.output


def delete_chi(chi, ns=namespace):
    with When(f"Delete chi {chi}"):
        shell(f"{kubectl_cmd} delete chi {chi} -n {ns}", timeout=900)
        wait_objects(chi, [0, 0, 0], ns)


def delete_all_chi(ns=namespace):
    crds = run("get crds -o=custom-columns=name:.metadata.name", ns=ns).splitlines()
    if "clickhouseinstallations.clickhouse.altinity.com" in crds:
        chis = get("chi", "", ns=ns)["items"]
        for chi in chis:
            # kubectl(f"patch chi {chi} --type=merge -p '\{\"metadata\":\{\"finalizers\": [null]\}\}'", ns = ns)
            delete_chi(chi["metadata"]["name"], ns)


def create_and_check(config, check, ns=namespace):
    config = util.get_full_path(config)
    chi_name = manifest.get_chi_name(config)

    if "apply_templates" in check:
        for t in check["apply_templates"]:
            apply(util.get_full_path(t), ns)
        time.sleep(1)

    apply(config, ns)

    if "object_counts" in check:
        wait_objects(chi_name, check["object_counts"], ns)

    if "pod_count" in check:
        wait_object("pod", "", label=f"-l clickhouse.altinity.com/chi={chi_name}", count=check["pod_count"], ns=ns)

    if "chi_status" in check:
        wait_chi_status(chi_name, check["chi_status"], ns)
    else:
        wait_chi_status(chi_name, "Completed", ns)

    if "pod_image" in check:
        check_pod_image(chi_name, check["pod_image"], ns)

    if "pod_volumes" in check:
        check_pod_volumes(chi_name, check["pod_volumes"], ns)

    if "pod_podAntiAffinity" in check:
        check_pod_antiaffinity(chi_name, ns)

    if "pod_ports" in check:
        check_pod_ports(chi_name, check["pod_ports"], ns)

    if "service" in check:
        check_service(check["service"][0], check["service"][1], ns)

    if "configmaps" in check:
        check_configmaps(chi_name, ns)

    if "do_not_delete" not in check:
        delete_chi(chi_name, ns)


def get(_type, name, label="", ns=namespace):
    cmd = shell(f"{kubectl_cmd} get {_type} {name} {label} -o json -n {ns}")
    assert cmd.exitcode == 0, error()
    return json.loads(cmd.output.strip())


def create_ns(ns):
    cmd = shell(f"{kubectl_cmd} create ns {ns}")
    assert cmd.exitcode == 0, error()
    cmd = shell(f"{kubectl_cmd} get ns {ns}")
    assert cmd.exitcode == 0, error()


def delete_ns(ns):
    shell(f"{kubectl_cmd} delete ns {ns}", timeout=60)


def get_count(_type, name="", label="", ns=namespace):
    if ns is None:
        ns = '--all-namespaces'
    elif '-n' not in ns and '--namespace' not in ns:
        ns = f"-n {ns}"
    cmd = shell(f"{kubectl_cmd} get {_type} {name} {ns} -o=custom-columns=kind:kind,name:.metadata.name {label}")
    if cmd.exitcode == 0:
        return len(cmd.output.splitlines()) - 1
    else:
        return 0


def count_objects(label="", ns=namespace):
    sts = get_count("sts", ns=ns, label=label)
    pod = get_count("pod", ns=ns, label=label)
    service = get_count("service", ns=ns, label=label)
    return [sts, pod, service]


def apply(config, ns=namespace, validate=True, timeout=30):
    with When(f"{config} is applied"):
        cmd = f"{kubectl_cmd} apply --validate={validate} -n {ns} -f {config}"
        cmd = shell(cmd, timeout=timeout)
    with Then("exitcode should be 0"):
        assert cmd.exitcode == 0, error()


def delete(config, ns=namespace, timeout=30):
    with When(f"{config} is deleted"):
        cmd = shell(f"{kubectl_cmd} delete -n {ns} -f {config}", timeout=timeout)
    with Then("exitcode should be 0"):
        assert cmd.exitcode == 0, error()


def wait_objects(chi, objects, ns=namespace):
    with Then(f"{objects[0]} statefulsets, {objects[1]} pods and {objects[2]} services should be created"):
        for i in range(1, max_retries):
            counts = count_objects(label=f"-l clickhouse.altinity.com/chi={chi}", ns=ns)
            if counts == objects:
                break
            with Then("Not ready. Wait for " + str(i * 5) + " seconds"):
                time.sleep(i * 5)
        assert counts == objects, error()


def wait_object(_type, name, label="", count=1, ns=namespace, retries=max_retries):
    with Then(f"{count} {_type}(s) {name} should be created"):
        for i in range(1, retries):
            cur_count = get_count(_type, ns=ns, name=name, label=label)
            if cur_count >= count:
                break
            with Then("Not ready. Wait for " + str(i * 5) + " seconds"):
                time.sleep(i * 5)
        assert cur_count >= count, error()


def wait_chi_status(chi, status, ns=namespace, retries=max_retries):
    wait_field("chi", chi, ".status.status", status, ns, retries)


def get_chi_status(chi, ns=namespace):
    get_field("chi", chi, ".status.status", ns)


def wait_pod_status(pod, status, ns=namespace):
    wait_field("pod", pod, ".status.phase", status, ns)


def wait_field(_object, name, field, value, ns=namespace, retries=max_retries):
    with Then(f"{_object} {name} {field} should be {value}"):
        for i in range(1, retries):
            cur_value = get_field(_object, name, field, ns)
            if cur_value == value:
                break
            with Then("Not ready. Wait for " + str(i * 5) + " seconds"):
                time.sleep(i * 5)
        assert cur_value == value, error()


def wait_jsonpath(_object, name, field, value, ns=namespace, retries=max_retries):
    with Then(f"{_object} {name} -o jsonpath={field} should be {value}"):
        for i in range(1, retries):
            cur_value = get_jsonpath(_object, name, field, ns)
            if cur_value == value:
                break
            with Then("Not ready. Wait for " + str(i * 5) + " seconds"):
                time.sleep(i * 5)
        assert cur_value == value, error()


def get_field(_object, name, field, ns=namespace):
    out = run(f"get {_object} {name} -o=custom-columns=field:{field}", ns=ns).splitlines()
    return out[1]


def get_jsonpath(_object, name, field, ns=namespace):
    out = run(f"get {_object} {name} -o jsonpath=\"{field}\"", ns=ns).splitlines()
    return out[0]


def get_default_storage_class(ns=namespace):
    out = run(
        f"get storageclass -o=custom-columns=DEFAULT:\".metadata.annotations.storageclass\.kubernetes\.io/is-default-class\",NAME:.metadata.name",
        ns=ns).splitlines()
    for line in out[1:]:
        if line.startswith("true"):
            parts = line.split(maxsplit=1)
            return parts[1].strip()
    out = run(
        f"get storageclass -o=custom-columns=DEFAULT:\".metadata.annotations.storageclass\.beta\.kubernetes\.io/is-default-class\",NAME:.metadata.name",
        ns=ns).splitlines()
    for line in out[1:]:
        if line.startswith("true"):
            parts = line.split(maxsplit=1)
            return parts[1].strip()


def get_pod_spec(chi_name, ns=namespace):
    pod = get("pod", "", ns=ns, label=f"-l clickhouse.altinity.com/chi={chi_name}")["items"][0]
    return pod["spec"]


def get_pod_image(chi_name, ns=namespace):
    pod_image = get_pod_spec(chi_name, ns)["containers"][0]["image"]
    return pod_image


def get_pod_names(chi_name, ns=namespace):
    pod_names = run(
        f"get pods -o=custom-columns=name:.metadata.name -l clickhouse.altinity.com/chi={chi_name}",
        ns=ns
    ).splitlines()
    return pod_names[1:]


def get_pod_volumes(chi_name, ns=namespace):
    volume_mounts = get_pod_spec(chi_name, ns)["containers"][0]["volumeMounts"]
    return volume_mounts


def get_pod_ports(chi_name, ns=namespace):
    port_specs = get_pod_spec(chi_name, ns)["containers"][0]["ports"]
    ports = []
    for p in port_specs:
        ports.append(p["containerPort"])
    return ports


def check_pod_ports(chi_name, ports, ns=namespace):
    pod_ports = get_pod_ports(chi_name, ns)
    with Then(f"Expect pod ports {pod_ports} to match {ports}"):
        assert pod_ports.sort() == ports.sort()


def check_pod_image(chi_name, image, ns=namespace):
    pod_image = get_pod_image(chi_name, ns)
    with Then(f"Expect pod image {pod_image} to match {image}"):
        assert pod_image == image


def check_pod_volumes(chi_name, volumes, ns=namespace):
    pod_volumes = get_pod_volumes(chi_name, ns)
    for v in volumes:
        with Then(f"Expect pod has volume mount {v}"):
            found = 0
            for vm in pod_volumes:
                if vm["mountPath"] == v:
                    found = 1
                    break
            assert found == 1


def get_pvc_size(pvc_name, ns=namespace):
    return get_field("pvc", pvc_name, ".spec.resources.requests.storage", ns)


def check_pod_antiaffinity(chi_name, ns=namespace):
    pod_spec = get_pod_spec(chi_name, ns)
    expected = {
        "requiredDuringSchedulingIgnoredDuringExecution": [
            {
                "labelSelector": {
                    "matchLabels": {
                        "clickhouse.altinity.com/app": "chop",
                        "clickhouse.altinity.com/chi": f"{chi_name}",
                        "clickhouse.altinity.com/namespace": f"{ns}",
                    },
                },
                "topologyKey": "kubernetes.io/hostname",
            },
        ],
    }
    with Then(f"Expect podAntiAffinity to exist and match {expected}"):
        assert "affinity" in pod_spec
        assert "podAntiAffinity" in pod_spec["affinity"]
        assert pod_spec["affinity"]["podAntiAffinity"] == expected


def check_service(service_name, service_type, ns=namespace):
    with When(f"{service_name} is available"):
        service = get("service", service_name, ns=ns)
        with Then(f"Service type is {service_type}"):
            assert service["spec"]["type"] == service_type


def check_configmaps(chi_name, ns=namespace):
    check_configmap(
        f"chi-{chi_name}-common-configd",
        [
            "01-clickhouse-listen.xml",
            "02-clickhouse-logger.xml",
            "03-clickhouse-querylog.xml",
        ],
        ns=ns,
    )

    check_configmap(
        f"chi-{chi_name}-common-usersd",
        [
            "01-clickhouse-user.xml",
            "02-clickhouse-default-profile.xml",
        ],
        ns=ns,
    )


def check_configmap(cfg_name, values, ns=namespace):
    cfm = get("configmap", cfg_name, ns=ns)
    for v in values:
        with Then(f"{cfg_name} should contain {v}"):
            assert v in cfm["data"]
