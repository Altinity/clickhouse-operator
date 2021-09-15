import json
import os
import time

import tests.manifest as manifest
import tests.util as util

from testflows.core import *
from testflows.asserts import error
from testflows.connect import Shell

import tests.settings as settings

current_dir = os.path.dirname(os.path.abspath(__file__))
max_retries = 20

shell = Shell()
shell.timeout = 300
namespace = settings.test_namespace
kubectl_cmd = settings.kubectl_cmd


def launch(node, command, ok_to_fail=False, ns=namespace, timeout=6000):
    # Build command
    cmd = f"{kubectl_cmd} "
    cmd_args = command.split(" ")
    if ns is not None and ns != "" and ns != "--all-namespaces":
        cmd += f"{cmd_args[0]} --namespace={ns} "
    elif ns == "--all-namespaces":
        cmd += f"{cmd_args[0]} {ns} "
    else:
        cmd += f"{cmd_args[0]} "

    if len(cmd_args) > 1:
        cmd += " ".join(cmd_args[1:])

    # Run command
    cmd = node.cmd(cmd, timeout=timeout, no_checks=ok_to_fail)
    # cmd = shell(cmd, timeout=timeout)

    # Check command failure
    if not ok_to_fail:
        if cmd.exitcode != 0:
            debug(f"command failed, output:\n{cmd.output}")
        assert cmd.exitcode == 0, error()
    # Command test result
    return cmd.output if (cmd.exitcode == 0) or ok_to_fail else ""


def delete_chi(node, chi, ns=namespace, wait = True, ok_to_fail=False):
    with When(f"Delete chi {chi}"):
        launch(node, f"delete chi {chi}", ns=ns, timeout=6000, ok_to_fail=ok_to_fail)
        if wait:
            wait_objects(node, chi,
                {
                    "statefulset": 0,
                    "pod": 0,
                    "service": 0,
                    },
                ns,
                )


def delete_all_chi(node, ns=namespace):
    crds = launch(node, "get crds -o=custom-columns=name:.metadata.name", ns=ns).splitlines()
    if "clickhouseinstallations.clickhouse.altinity.com" in crds:
        chis = get(node, "chi", "", ns=ns, ok_to_fail=True)["items"]
        for chi in chis:
            # kubectl(f"patch chi {chi} --type=merge -p '\{\"metadata\":\{\"finalizers\": [null]\}\}'", ns = ns)
            delete_chi(node, chi["metadata"]["name"], ns)


def create_and_check(node, config, check, ns=namespace, timeout=10000):
    chi_name = manifest.get_chi_name(util.get_full_path(f'{config}'))

    if "apply_templates" in check:
        debug("Need to apply additional templates")
        for t in check["apply_templates"]:
            debug(f"Applying template: {util.get_full_path(t, False)} \n{t}")
            apply(node, util.get_full_path(t, False), ns)
        time.sleep(5)

    apply(node, util.get_full_path(config, False), ns=ns, timeout=timeout)

    if "object_counts" in check:
        wait_objects(node, chi_name, check["object_counts"], ns)

    if "pod_count" in check:
        wait_object(node, "pod", "", label=f"-l clickhouse.altinity.com/chi={chi_name}", count=check["pod_count"], ns=ns)

    if "chi_status" in check:
        wait_chi_status(node, chi_name, check["chi_status"], ns)
    else:
        wait_chi_status(node, chi_name, "Completed", ns)

    if "pod_image" in check:
        check_pod_image(node, chi_name, check["pod_image"], ns)

    if "pod_volumes" in check:
        check_pod_volumes(node, chi_name, check["pod_volumes"], ns)

    if "pod_podAntiAffinity" in check:
        check_pod_antiaffinity(node, chi_name, ns)

    if "pod_ports" in check:
        check_pod_ports(node, chi_name, check["pod_ports"], ns)

    if "service" in check:
        check_service(node, check["service"][0], check["service"][1], ns)

    if "configmaps" in check:
        check_configmaps(node, chi_name, ns)

    if "do_not_delete" not in check:
        delete_chi(node, chi_name, ns)


def get(node, kind, name, label="", ns=namespace, ok_to_fail=False):
    out = launch(node, f"get {kind} {name} {label} -o json", ns=ns, ok_to_fail=ok_to_fail)
    return json.loads(out.strip())


def create_ns(node, ns):
    launch(node, f"create ns {ns}", ns=None)
    launch(node, f"get ns {ns}", ns=None)


def delete_ns(node, ns, ok_to_fail=False, timeout=6000):
    launch(node, f"delete ns {ns}", ns=None, ok_to_fail=ok_to_fail, timeout=timeout)


def get_count(node, kind, name="", label="", ns=namespace):
    out = launch(node, f"get {kind} {name} -o=custom-columns=kind:kind,name:.metadata.name {label}", ns=ns, ok_to_fail=True)
    if (out is None) or (len(out) == 0):
        return 0
    return len(out.splitlines()) - 1


def count_objects(node, label="", ns=namespace):
    return {
        "statefulset": get_count(node, "sts", ns=ns, label=label),
        "pod": get_count(node, "pod", ns=ns, label=label),
        "service": get_count(node, "service", ns=ns, label=label),
    }


def apply(node, config, ns=namespace, validate=True, timeout=120):
    with When(f"{config} is applied"):
        launch(node, f"apply --validate={validate} -f {config}", ns=ns, timeout=timeout)


def delete(node, config, ns=namespace, timeout=120):
    with When(f"{config} is deleted"):
        launch(node, f"delete -f {config}", ns=ns, timeout=timeout)


def wait_objects(node, chi, object_counts, ns=namespace):
    with Then(
            f"Waiting for: "
            f"{object_counts['statefulset']} statefulsets, "
            f"{object_counts['pod']} pods and "
            f"{object_counts['service']} services "
            f"to be available"
    ):
        for i in range(1, max_retries):
            cur_object_counts = count_objects(node, label=f"-l clickhouse.altinity.com/chi={chi}", ns=ns)
            if cur_object_counts == object_counts:
                break
            with Then(
                    f"Not ready yet. [ "
                    f"statefulset: {cur_object_counts['statefulset']} "
                    f"pod: {cur_object_counts['pod']} "
                    f"service: {cur_object_counts['service']} ]. "
                    f"Wait for {i * 5} seconds"
            ):
                time.sleep(i * 5)
        assert cur_object_counts == object_counts, error()


def wait_object(node, kind, name, label="", count=1, ns=namespace, retries=max_retries, backoff = 5):
    with Then(f"{count} {kind}(s) {name} should be created"):
        for i in range(1, retries):
            cur_count = get_count(node, kind, ns=ns, name=name, label=label)
            if cur_count >= count:
                break
            with Then("Not ready. Wait for " + str(i * backoff) + " seconds"):
                time.sleep(i * backoff)
        assert cur_count >= count, error()


def wait_command(node, command, result, count=1, ns=namespace, retries=max_retries):
    with Then(f"{command} should return {result}"):
        for i in range(1, retries):
            res = launch(node, command, ok_to_fail=True, ns=ns)
            if res == result:
                break
            with Then("Not ready. Wait for " + str(i * 5) + " seconds"):
                time.sleep(i * 5)
        assert res == result, error()


def wait_chi_status(node, chi, status, ns=namespace, retries=max_retries):
    wait_field(node, "chi", chi, ".status.status", status, ns, retries)


def get_chi_status(node, chi, ns=namespace):
    get_field(node, "chi", chi, ".status.status", ns)


def wait_pod_status(node, pod, status, ns=namespace):
    wait_field(node, "pod", pod, ".status.phase", status, ns)


def wait_field(node, kind, name, field, value, ns=namespace, retries=max_retries, backoff = 5):
    with Then(f"{kind} {name} {field} should be {value}"):
        for i in range(1, retries):
            cur_value = get_field(node, kind, name, field, ns)
            if cur_value == value:
                break
            with Then("Not ready. Wait for " + str(i * backoff) + " seconds"):
                time.sleep(i * backoff)
        assert cur_value == value, error()


def wait_jsonpath(node, kind, name, field, value, ns=namespace, retries=max_retries):
    with Then(f"{kind} {name} -o jsonpath={field} should be {value}"):
        for i in range(1, retries):
            cur_value = get_jsonpath(node, kind, name, field, ns)
            if cur_value == value:
                break
            with Then("Not ready. Wait for " + str(i * 5) + " seconds"):
                time.sleep(i * 5)
        assert cur_value == value, error()


def get_field(node, kind, name, field, ns=namespace):
    out = launch(node, f"get {kind} {name} -o=custom-columns=field:{field}", ns=ns).splitlines()
    return out[1]


def get_jsonpath(node, kind, name, field, ns=namespace):
    out = launch(node, f"get {kind} {name} -o jsonpath=\"{field}\"", ns=ns).splitlines()
    return out[0]


def get_default_storage_class(node, ns=namespace):
    out = launch(node,
        f"get storageclass "
        f"-o=custom-columns="
        f"DEFAULT:\".metadata.annotations.storageclass\.kubernetes\.io/is-default-class\",NAME:.metadata.name",
        ns=ns,
    ).splitlines()
    for line in out[1:]:
        if line.startswith("true"):
            parts = line.split(maxsplit=1)
            return parts[1].strip()
    out = launch(node,
        f"get storageclass "
        f"-o=custom-columns="
        f"DEFAULT:\".metadata.annotations.storageclass\.beta\.kubernetes\.io/is-default-class\",NAME:.metadata.name",
        ns=ns,
    ).splitlines()
    for line in out[1:]:
        if line.startswith("true"):
            parts = line.split(maxsplit=1)
            return parts[1].strip()


def get_pod_spec(node, chi_name, ns=namespace):
    pod = get(node, "pod", "", ns=ns, label=f"-l clickhouse.altinity.com/chi={chi_name}")["items"][0]
    return pod["spec"]


def get_pod_image(node, chi_name, ns=namespace):
    pod_image = get_pod_spec(node, chi_name, ns)["containers"][0]["image"]
    return pod_image


def get_pod_names(node, chi_name, ns=namespace):
    pod_names = launch(node,
        f"get pods -o=custom-columns=name:.metadata.name -l clickhouse.altinity.com/chi={chi_name}",
        ns=ns,
    ).splitlines()
    return pod_names[1:]


def get_pod_volumes(node, chi_name, ns=namespace):
    volume_mounts = get_pod_spec(node, chi_name, ns)["containers"][0]["volumeMounts"]
    return volume_mounts


def get_pod_ports(node, chi_name, ns=namespace):
    port_specs = get_pod_spec(node, chi_name, ns)["containers"][0]["ports"]
    ports = []
    for p in port_specs:
        ports.append(p["containerPort"])
    return ports


def check_pod_ports(node, chi_name, ports, ns=namespace):
    pod_ports = get_pod_ports(node, chi_name, ns)
    with Then(f"Expect pod ports {pod_ports} to match {ports}"):
        assert pod_ports.sort() == ports.sort()

def check_pod_image(node, chi_name, image, ns=namespace):
    pod_image = get_pod_image(node, chi_name, ns)
    with Then(f"Expect pod image {pod_image} to match {image}"):
        assert pod_image == image


def check_pod_volumes(node, chi_name, volumes, ns=namespace):
    pod_volumes = get_pod_volumes(node, chi_name, ns)
    for v in volumes:
        with Then(f"Expect pod has volume mount {v}"):
            found = 0
            for vm in pod_volumes:
                if vm["mountPath"] == v:
                    found = 1
                    break
            assert found == 1


def get_pvc_size(node, pvc_name, ns=namespace):
    return get_field(node, "pvc", pvc_name, ".spec.resources.requests.storage", ns)


def check_pod_antiaffinity(node, chi_name, ns=namespace):
    pod_spec = get_pod_spec(node, chi_name, ns)
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


def check_service(node, service_name, service_type, ns=namespace):
    with When(f"{service_name} is available"):
        service = get(node, "service", service_name, ns=ns)
        with Then(f"Service type is {service_type}"):
            assert service["spec"]["type"] == service_type


def check_configmaps(node, chi_name, ns=namespace):
    check_configmap(node,
        f"chi-{chi_name}-common-configd",
        [
            "01-clickhouse-01-listen.xml",
            "01-clickhouse-02-logger.xml",
            "01-clickhouse-03-query_log.xml",
        ],
        ns=ns,
    )

    check_configmap(node,
        f"chi-{chi_name}-common-usersd",
        [
            "01-clickhouse-user.xml",
            "02-clickhouse-default-profile.xml",
        ],
        ns=ns,
    )


def check_configmap(node, cfg_name, values, ns=namespace):
    cfm = get(node, "configmap", cfg_name, ns=ns)
    for v in values:
        with Then(f"{cfg_name} should contain {v}"):
            assert v in cfm["data"]
