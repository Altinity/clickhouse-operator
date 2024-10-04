import json
import os
import time
import threading

from testflows.core import *
from testflows.asserts import error
# from testflows.connect import Shell

# import e2e.settings as settings
import e2e.yaml_manifest as yaml_manifest
import e2e.util as util

current_dir = os.path.dirname(os.path.abspath(__file__))
max_retries = 20


def launch(command, ok_to_fail=False, ns=None, timeout=600, shell=None):
    # Build commanddef launch

    if ns is None:
        if hasattr(current().context, "test_namespace"):
            ns = current().context.test_namespace

    cmd = f"{current().context.kubectl_cmd} "
    cmd_args = command.split(" ")
    if ns is not None and ns != "" and ns != "--all-namespaces":
        cmd += f"{cmd_args[0]} --namespace={ns} "
    elif ns == "--all-namespaces":
        cmd += f"{cmd_args[0]} {ns} "
    else:
        cmd += f"{cmd_args[0]} "

    if len(cmd_args) > 1:
        cmd += " ".join(cmd_args[1:])

    # save command for debug purposes
    # command = cmd
    # print(f"run command: {cmd}")

    return run_shell(cmd, timeout, ok_to_fail, shell=shell)


def run_shell(cmd, timeout=600, ok_to_fail=False, shell=None):
    # Run command

    if shell is None:
        res_cmd = current().context.shell(cmd, timeout=timeout)
    else:
        res_cmd = shell(cmd, timeout=timeout)

    # Check command failure
    code = res_cmd.exitcode
    if not ok_to_fail:
        if code != 0:
            print(f"command failed, command:\n{cmd}")
            print(f"command failed, exit code:\n{code}")
            print(f"command failed, output :\n{res_cmd.output}")
        assert code == 0, error()
    # Command test result
    return res_cmd.output if (code == 0) or ok_to_fail else ""


def delete_kind(kind, name, ns=None, ok_to_fail=False, shell=None):
    with When(f"Delete {kind} {name}"):
        launch(
            f"delete {kind} {name} -v 5 --now --timeout=600s",
            ns=ns,
            timeout=600,
            ok_to_fail=ok_to_fail,
            shell=shell
        )

def delete_chi(chi, ns=None, wait=True, ok_to_fail=False, shell=None):
    delete_kind("chi", chi, ns=ns, ok_to_fail=ok_to_fail, shell=shell)
    if wait:
            wait_objects(
                chi,
                {
                    "statefulset": 0,
                    "pod": 0,
                    "service": 0,
                },
                ns,
                shell=shell
            )


def delete_chk(chk, ns=None, wait=True, ok_to_fail=False, shell=None):
    delete_kind("chk", chk, ns=ns, ok_to_fail=ok_to_fail, shell=shell)

def delete_all_chi(ns=None):
    delete_all("chi", ns=ns)

def delete_all_chk(ns=None):
    delete_all("chk", ns=ns)

def delete_all(kind, ns=None):
    crds = launch("get crds -o=custom-columns=name:.spec.names.shortNames[0]", ns=ns).splitlines()
    if kind in crds:
        try:
            to_delete = get(kind, "", ns=ns, ok_to_fail=True)
        except Exception:
            to_delete = {}
        if "items" in to_delete:
            for i in to_delete["items"]:
                delete_kind(kind, i["metadata"]["name"], ns=ns)
                wait_object(kind, i["metadata"]["name"], ns=ns, count=0)


def delete_all_keeper(ns=None):
    for keeper_type in ("zookeeper-operator", "zookeeper", "clickhouse-keeper"):
        expected_resource_types = (
            ("zookeepercluster",) if keeper_type == "zookeeper-operator" else ("sts", "pvc", "cm", "svc")
        )
        for resource_type in expected_resource_types:
            try:
                item_list = get(
                    resource_type,
                    "",
                    label=f"-l app={keeper_type}",
                    ns=ns,
                    ok_to_fail=True,
                )
            except Exception as e:
                item_list = {}
            if "items" in item_list:
                for item in item_list["items"]:
                    name = item["metadata"]["name"]
                    launch(f"delete {resource_type} -n {current().context.test_namespace} {name}", ok_to_fail=True)


def create_and_check(manifest, check, kind="chi", ns=None, shell=None, timeout=1800):
    chi_name = yaml_manifest.get_name(util.get_full_path(manifest))

    if kind == "chi":
        label = f"-l clickhouse.altinity.com/chi={chi_name}"
    elif kind == "chk":
        label = f"-l clickhouse-keeper.altinity.com/chk={chi_name}"
    else:
        assert False, error(f"Unknown kind {kind}")

    if "apply_templates" in check:
        debug("Need to apply additional templates")
        for t in check["apply_templates"]:
            debug(f"Applying template: {util.get_full_path(t, False)} \n{t}")
            apply(util.get_full_path(t, False), ns=ns, shell=shell)
        time.sleep(5)

    apply_chi(util.get_full_path(manifest, False), ns=ns, timeout=timeout, shell=shell)

    if "chi_status" in check:
        wait_chi_status(chi_name, check["chi_status"], ns=ns, shell=shell)
    elif "chk_status" in check:
        wait_chk_status(chi_name, check["chk_status"], ns=ns, shell=shell)
    else:
        # Wait for reconcile to start before performing other checks. In some cases it does not start, so we can pass
        # wait_field_changed("chi", chi_name, state_field, prev_state, ns)
        wait_field(kind=kind, name=chi_name, field=".status.status", value="InProgress"
                   , ns=ns, retries=3, throw_error=False, shell=shell)
        wait_field(kind=kind, name=chi_name, field=".status.status", value="Completed"
                   , ns=ns, shell=shell)

    if "object_counts" in check:
        wait_objects(chi_name, check["object_counts"], ns=ns, shell=shell)

    if "pod_count" in check:
        wait_object(
            "pod",
            "",
            label=label,
            count=check["pod_count"],
            ns=ns,
            shell=shell
        )

    if "pod_image" in check:
        check_pod_image(chi_name, check["pod_image"], ns=ns, shell=shell)

    if "pod_volumes" in check:
        check_pod_volumes(chi_name, check["pod_volumes"], ns=ns, shell=shell)

    if "pod_podAntiAffinity" in check:
        check_pod_antiaffinity(chi_name, ns=ns, shell=shell)

    if "pod_ports" in check:
        check_pod_ports(chi_name, check["pod_ports"], ns=ns, shell=shell)

    if "service" in check:
        check_service(check["service"][0], check["service"][1], ns=ns, shell=shell)

    if "configmaps" in check:
        check_configmaps(chi_name, ns=ns, shell=shell)

    if "pdb" in check:
        check_pdb(chi_name, check["pdb"], ns=ns, shell=shell)

    if "do_not_delete" not in check:
        delete_chi(chi_name, ns=ns, shell=shell)


def get(kind, name, label="", ns=None, ok_to_fail=False, shell=None):
    out = launch(f"get {kind} {name} {label} -o json", ns=ns, ok_to_fail=ok_to_fail, shell=shell)
    return json.loads(out.strip())


def create_ns(ns):
    if ns is None:
        launch(f"create ns {current().context.test_namespace}", ns=None)
        launch(f"get ns {current().context.test_namespace}", ns=None)
    else:
        launch(f"create ns {ns}", ns=None)
        launch(f"get ns {ns}", ns=None)


def delete_ns(ns = None, delete_chi=False, ok_to_fail=False, timeout=1000):
    if ns is None:
        ns = current().context.test_namespace
    if delete_chi:
        delete_all_chi(ns)
        delete_all_chk(ns)
    launch(
        f"delete ns {ns} -v 5 --now --timeout={timeout}s",
        ns=None,
        ok_to_fail=ok_to_fail,
        timeout=timeout,
    )
    for attempt in retries(timeout=300, delay=10):
        with attempt:
            out = launch(f"get namespace {ns}", ok_to_fail=True)
            assert "Error" in out


def get_count(kind, name="", label="", chi="", ns=None, shell=None):
    if chi != "" and label == "":
        label = f"-l clickhouse.altinity.com/chi={chi}"

    if ns is None:
        ns = current().context.test_namespace

    if kind == "pv":
        # pv is not namespaced so need to search namespace in claimRef
        out = launch(f'get pv {label} -o yaml | grep "namespace: {ns}"', ok_to_fail=True, shell=shell)
    else:
        out = launch(
            f"get {kind} {name} -o=custom-columns=kind:kind,name:.metadata.name {label}",
            ns=ns,
            ok_to_fail=True,
            shell=shell
        )

    if (out is None) or (len(out) == 0):
        return 0
    else:
        return len(out.splitlines()) - 1


def count_objects(label="", ns=None, shell=None):
    return {
        "statefulset": get_count("sts", ns=ns, label=label, shell=shell),
        "pod": get_count("pod", ns=ns, label=label, shell=shell),
        "service": get_count("service", ns=ns, label=label, shell=shell),
    }


def apply(manifest, ns=None, validate=True, timeout=600, shell=None):
    for attempt in retries(timeout=500, delay=1):
        with attempt:
            with When(f"{manifest} is applied"):
                if " | " not in manifest:
                    manifest = f'"{manifest}"'
                    launch(f"apply --validate={validate} -f {manifest}", ns=ns, timeout=timeout, shell=shell)
                else:
                    run_shell(
                        f"set -o pipefail && {manifest} | {current().context.kubectl_cmd} apply --namespace={current().context.test_namespace} --validate={validate} -f -",
                        timeout=timeout,
                        shell=shell
                    )


def apply_chi(manifest, ns=None, validate=True, timeout=600, shell=None):
    if ns is None:
        ns = current().context.test_namespace
    chi_name = yaml_manifest.get_name(manifest)
    with When(f"CHI {chi_name} is applied"):
        if current().context.kubectl_mode == "replace":
            if get_count("chi", chi_name, ns=ns) == 0:
                create(manifest, ns=ns, validate=validate, timeout=timeout)
            else:
                replace(manifest, ns=ns, validate=validate, timeout=timeout)
        else:
            apply(manifest, ns=ns, validate=validate, timeout=timeout, shell=shell)


def create(manifest, ns=None, validate=True, timeout=600):
    with When(f"{manifest} is created"):
        if "<(" not in manifest:
            manifest = f'"{manifest}"'
        launch(f"create --validate={validate} -f {manifest}", ns=ns, timeout=timeout)


def replace(manifest, ns=None, validate=True, timeout=600):
    with When(f"{manifest} is replaced"):
        if "<(" not in manifest:
            manifest = f'"{manifest}"'
        launch(f"replace --validate={validate} -f {manifest}", ns=ns, timeout=timeout)


def delete(manifest, ns=None, timeout=600):
    with When(f"{manifest} is deleted"):
        if " | " not in manifest:
            manifest = f'"{manifest}"'
            return launch(f"delete -f {manifest}", ns=ns, timeout=timeout)
        else:
            run_shell(f"{manifest} | {current().context.kubectl_cmd} delete -f -", timeout=timeout)


def wait_objects(chi, object_counts, ns=None, shell=None, retries=max_retries):
    with Then(
        f"Waiting for: "
        f"{object_counts['statefulset']} statefulsets, "
        f"{object_counts['pod']} pods and "
        f"{object_counts['service']} services "
        f"to be available"
    ):
        for i in range(1, retries):
            cur_object_counts = count_objects(label=f"-l clickhouse.altinity.com/chi={chi}", ns=ns, shell=shell)
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


def wait_object(kind, name, names=[], label="", count=1, ns=None, retries=max_retries, backoff=5, shell=None):
    with Then(f"{count} {kind}(s) {name} should be created"):
        for i in range(1, retries):
            cur_count = get_count(kind, ns=ns, name=name, label=label, shell=shell)
            if cur_count >= count:
                break
            with Then(f"Not ready yet. {cur_count}/{count}. Wait for {i * backoff} seconds"):
                time.sleep(i * backoff)
        assert cur_count >= count, error()


def wait_command(command, result, count=1, ns=None, retries=max_retries):
    with Then(f"{command} should return {result}"):
        for i in range(1, retries):
            res = launch(command, ok_to_fail=True, ns=ns)
            if res == result:
                break
            with Then("Not ready. Wait for " + str(i * 5) + " seconds"):
                time.sleep(i * 5)
        assert res == result, error()


def wait_chi_status(chi, status, ns=None, retries=max_retries, throw_error=True, shell=None):
    wait_field("chi", chi, ".status.status", status, ns, retries, throw_error=throw_error, shell=shell)


def wait_chk_status(chk, status, ns=None, retries=max_retries, throw_error=True, shell=None):
    wait_field("chk", chk, ".status.status", status, ns, retries, throw_error=throw_error, shell=shell)


def get_chi_status(chi, ns=None):
    get_field("chi", chi, ".status.status", ns)


def wait_pod_status(pod, status,shell=None, ns=None):
    wait_field("pod", pod, ".status.phase", status, ns, shell=shell)


def wait_container_status(pod, status, ns=None):
    wait_field("pod", pod, ".status.containerStatuses[0].ready", status, ns)


def wait_field(
    kind,
    name,
    field,
    value,
    ns=None,
    retries=max_retries,
    backoff=5,
    throw_error=True,
    shell=None,
):
    with Then(f"{kind} {name} {field} should be {value}"):
        for i in range(1, retries):
            cur_value = get_field(kind, name, field, ns, shell=shell)
            if cur_value == value:
                break
            with Then("Not ready. Wait for " + str(i * backoff) + " seconds"):
                time.sleep(i * backoff)
        assert cur_value == value or throw_error is False, error()


def wait_field_changed(
    kind,
    name,
    field,
    prev_value,
    ns=None,
    retries=max_retries,
    backoff=5,
    throw_error=True,
):
    with Then(f"{kind} {name} {field} should be different from {prev_value}"):
        for i in range(1, retries):
            cur_value = get_field(kind, name, field, ns)
            if cur_value != "" and cur_value != prev_value:
                break
            with Then("Not ready. Wait for " + str(i * backoff) + " seconds"):
                time.sleep(i * backoff)
        assert cur_value != "" and cur_value != prev_value or throw_error == False, error()


def wait_jsonpath(kind, name, field, value, ns=None, retries=max_retries):
    with Then(f"{kind} {name} -o jsonpath={field} should be {value}"):
        for i in range(1, retries):
            cur_value = get_jsonpath(kind, name, field, ns)
            if cur_value == value:
                break
            with Then("Not ready. Wait for " + str(i * 5) + " seconds"):
                time.sleep(i * 5)
        assert cur_value == value, error()


def get_field(kind, name, field, ns=None, shell=None):
    out = launch(f"get {kind} {name} -o=custom-columns=field:{field}", ns=ns, ok_to_fail=True, shell=shell).splitlines()
    if len(out) > 1:
        return out[1]
    else:
        return ""


def get_jsonpath(kind, name, field, ns=None):
    out = launch(f'get {kind} {name} -o jsonpath="{field}"', ns=ns).splitlines()
    return out[0]


def get_default_storage_class(ns=None):
    out = launch(
        f"get storageclass "
        f"-o=custom-columns="
        f'DEFAULT:".metadata.annotations.storageclass\.kubernetes\.io/is-default-class",NAME:.metadata.name',
        ns=ns,
    ).splitlines()
    for line in out[1:]:
        if line.startswith("true"):
            parts = line.split(maxsplit=1)
            return parts[1].strip()
    out = launch(
        f"get storageclass "
        f"-o=custom-columns="
        f'DEFAULT:".metadata.annotations.storageclass\.beta\.kubernetes\.io/is-default-class",NAME:.metadata.name',
        ns=ns,
    ).splitlines()
    for line in out[1:]:
        if line.startswith("true"):
            parts = line.split(maxsplit=1)
            return parts[1].strip()


def get_pod_spec(chi_name, pod_name="", ns=None, shell=None):
    label = f"-l clickhouse.altinity.com/chi={chi_name}"
    if pod_name == "":
        pod = get("pod", "", ns=ns, label=label, shell=shell)["items"][0]
    else:
        pod = get("pod", pod_name, ns=ns, shell=shell)
    return pod["spec"]


def get_pod_image(chi_name, pod_name="", ns=None, shell=None):
    pod_image = get_pod_spec(chi_name, pod_name, ns, shell=shell)["containers"][0]["image"]
    return pod_image


def get_pod_names(chi_name, ns=None, shell=None):
    pod_names = launch(
        f"get pods -o=custom-columns=name:.metadata.name -l clickhouse.altinity.com/chi={chi_name}",
        ns=ns,
        shell=shell
    ).splitlines()
    return pod_names[1:]


def get_obj_names(chi_name, obj_type="pods", ns=None):
    pod_names = launch(
        f"get {obj_type} -o=custom-columns=name:.metadata.name -l clickhouse.altinity.com/chi={chi_name}",
        ns=ns,
    ).splitlines()
    return pod_names[1:]


def get_pod_volumes(chi_name, pod_name="", ns=None, shell=None):
    volume_mounts = get_pod_spec(chi_name, pod_name, ns, shell=shell)["containers"][0]["volumeMounts"]
    return volume_mounts


def get_pod_ports(chi_name, pod_name="", ns=None, shell=None):
    port_specs = get_pod_spec(chi_name, pod_name, ns, shell=shell)["containers"][0]["ports"]
    ports = []
    for p in port_specs:
        ports.append(p["containerPort"])
    return ports


def check_pod_ports(chi_name, ports, ns=None, shell=None):
    pod_ports = get_pod_ports(chi_name, ns=ns, shell=shell)
    with Then(f"Expect pod ports {pod_ports} to match {ports}"):
        assert sorted(pod_ports) == sorted(ports)


def check_pod_image(chi_name, image, ns=None, shell=None):
    pod_image = get_pod_image(chi_name, ns=ns, shell=shell)
    with Then(f"Expect pod image {pod_image} to match {image}"):
        assert pod_image == image


def check_pod_volumes(chi_name, volumes, ns=None, shell=None):
    pod_volumes = get_pod_volumes(chi_name, ns=ns, shell=shell)
    for v in volumes:
        with Then(f"Expect pod has volume mount {v}"):
            found = 0
            for vm in pod_volumes:
                if vm["mountPath"] == v:
                    found = 1
                    break
            assert found == 1


def get_pvc_size(pvc_name, ns=None):
    return get_field("pvc", pvc_name, ".spec.resources.requests.storage", ns)


def get_pv_name(pvc_name, ns=None):
    return get_field("pvc", pvc_name, ".spec.volumeName", ns)


def get_pv_size(pvc_name, ns=None):
    return get_field("pv", get_pv_name(pvc_name, ns), ".spec.capacity.storage", ns)


def check_pod_antiaffinity(
    chi_name,
    pod_name="",
    match_labels={},
    topologyKey="kubernetes.io/hostname",
    ns=None,
    shell=None
):
    pod_spec = get_pod_spec(chi_name, pod_name, ns, shell=shell)
    if match_labels == {}:
        match_labels = {
            "clickhouse.altinity.com/app": "chop",
            "clickhouse.altinity.com/chi": f"{chi_name}",
            "clickhouse.altinity.com/namespace": f"{current().context.test_namespace}",
        }
    expected = {
        "requiredDuringSchedulingIgnoredDuringExecution": [
            {
                "labelSelector": {
                    "matchLabels": match_labels,
                },
                "topologyKey": f"{topologyKey}",
            },
        ],
    }
    with Then(f"Expect podAntiAffinity to exist and match {expected}"):
        assert "affinity" in pod_spec
        assert "podAntiAffinity" in pod_spec["affinity"]
        assert pod_spec["affinity"]["podAntiAffinity"] == expected


def check_service(service_name, service_type, ns=None, shell=None):
    with When(f"{service_name} is available"):
        service = get("service", service_name, ns=ns, shell=shell)
        with Then(f"Service type is {service_type}"):
            assert service["spec"]["type"] == service_type


def check_configmaps(chi_name, ns=None, shell=None):
    check_configmap(
        f"chi-{chi_name}-common-configd",
        [
            "01-clickhouse-01-listen.xml",
            "01-clickhouse-02-logger.xml",
            "01-clickhouse-03-query_log.xml",
        ],
        ns=ns,
        shell=shell
    )

    check_configmap(
        f"chi-{chi_name}-common-usersd",
        [
            "01-clickhouse-operator-profile.xml",
            "02-clickhouse-default-profile.xml",
        ],
        ns=ns,
        shell=shell
    )


def check_configmap(cfg_name, values, ns=None, shell=None):
    cfm = get("configmap", cfg_name, ns=ns, shell=shell)
    for v in values:
        with Then(f"{cfg_name} should contain {v}"):
            assert v in cfm["data"], error()


def check_pdb(chi, clusters, ns=None, shell=None):
    for c in clusters.keys():
        with Then(f"PDB is configured for cluster {c}"):
            pdb = get("pdb", chi + "-" + c, shell=shell)
            labels = pdb["spec"]["selector"]["matchLabels"]
            assert labels["clickhouse.altinity.com/app"] == "chop"
            assert labels["clickhouse.altinity.com/chi"] == chi
            assert labels["clickhouse.altinity.com/cluster"] == c
            assert labels["clickhouse.altinity.com/namespace"] == current().context.test_namespace
            assert pdb["spec"]["maxUnavailable"] == clusters[c]
