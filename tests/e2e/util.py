import os
import time

import e2e.clickhouse as clickhouse
import e2e.kubectl as kubectl
import e2e.settings as settings
import e2e.yaml_manifest as yaml_manifest

from testflows.core import fail, Given, Then, But, current, message


current_dir = os.path.dirname(os.path.abspath(__file__))
operator_label = "-l app=clickhouse-operator"


def get_full_path(test_file, lookup_in_host=True):
    # this must be substituted if ran in docker
    if current().context.native or lookup_in_host:
        return os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), test_file))
    else:
        return os.path.abspath(f"/home/master/clickhouse-operator/tests/e2e/{test_file}")


def set_operator_version(version, ns=None, timeout=600, shell=None):
    if ns is None:
        ns = current().context.operator_namespace
    if current().context.operator_install != "yes":
        return
    operator_image = f"{current().context.operator_docker_repo}:{version}"
    metrics_exporter_image = f"{current().context.metrics_exporter_docker_repo}:{version}"

    kubectl.launch(
        f"set image deployment.v1.apps/clickhouse-operator clickhouse-operator={operator_image}",
        ns=ns,
        shell=shell
    )
    kubectl.launch(
        f"set image deployment.v1.apps/clickhouse-operator metrics-exporter={metrics_exporter_image}",
        ns=ns,
        shell=shell
    )
    kubectl.launch("rollout status deployment.v1.apps/clickhouse-operator", ns=ns, timeout=timeout, shell=shell)
    if kubectl.get_count("pod", ns=ns, label=operator_label, shell=shell) == 0:
        fail("invalid clickhouse-operator pod count")


def set_metrics_exporter_version(version, ns=None):
    if ns is None:
        ns = current().context.operator_namespace
    kubectl.launch(
        f"set image deployment.v1.apps/clickhouse-operator metrics-exporter=altinity/metrics-exporter:{version}",
        ns=ns,
    )
    kubectl.launch("rollout status deployment.v1.apps/clickhouse-operator", ns=ns)


def restart_operator(ns=None, timeout=600, shell=None):
    if ns is None:
        ns = current().context.operator_namespace
    if current().context.operator_install != "yes":
        return
    pod = kubectl.get("pod", name="", ns=ns, label=operator_label, shell=shell)["items"][0]
    old_pod_name = pod["metadata"]["name"]
    old_pod_ip = pod["status"]["podIP"]
    kubectl.launch(f"delete pod {old_pod_name}", ns=ns, timeout=timeout, shell=shell)
    kubectl.wait_object("pod", name="", ns=ns, label=operator_label, shell=shell)
    pod = kubectl.get("pod", name="", ns=ns, label=operator_label, shell=shell)["items"][0]
    new_pod_name = pod["metadata"]["name"]
    kubectl.wait_pod_status(new_pod_name, "Running", ns=ns, shell=shell)
    pod = kubectl.get("pod", name="", ns=ns, label=operator_label, shell=shell)["items"][0]
    new_pod_ip = pod["status"]["podIP"]
    print(f"old operator pod: {old_pod_name} ip: {old_pod_ip}")
    print(f"new operator pod: {new_pod_name} ip: {new_pod_ip}")


def require_keeper(keeper_manifest="", keeper_type="zookeeper", force_install=False):
    if force_install or kubectl.get_count("service", name=keeper_type) == 0:

        if keeper_type == "zookeeper":
            keeper_manifest = "zookeeper-1-node-1GB-for-tests-only.yaml" if keeper_manifest == "" else keeper_manifest
            keeper_manifest = f"../../deploy/zookeeper/quick-start-persistent-volume/{keeper_manifest}"
        if keeper_type == "clickhouse-keeper":
            keeper_manifest = (
                "clickhouse-keeper-1-node-256M-for-test-only.yaml" if keeper_manifest == "" else keeper_manifest
            )
            keeper_manifest = f"../../deploy/clickhouse-keeper/{keeper_manifest}"
        if keeper_type == "zookeeper-operator":
            keeper_manifest = "zookeeper-operator-1-node.yaml" if keeper_manifest == "" else keeper_manifest
            keeper_manifest = f"../../deploy/zookeeper-operator/{keeper_manifest}"

        multi_doc = yaml_manifest.get_multidoc_manifest_data(get_full_path(keeper_manifest, lookup_in_host=True))
        keeper_nodes = 1
        docs_count = 0
        for doc in multi_doc:
            docs_count += 1
            if doc["kind"] in ("StatefulSet", "ZookeeperCluster"):
                keeper_nodes = doc["spec"]["replicas"]
        expected_docs = {
            "zookeeper": 5 if "scaleout-pvc" in keeper_manifest else 4,
            "clickhouse-keeper": 6,
            "zookeeper-operator": 3 if "probes" in keeper_manifest else 1,
        }
        expected_pod_prefix = {
            "zookeeper": "zookeeper",
            "zookeeper-operator": "zookeeper",
            "clickhouse-keeper": "clickhouse-keeper",
        }
        assert (
            docs_count == expected_docs[keeper_type]
        ), f"invalid {keeper_type} manifest, expected {expected_docs[keeper_type]}, actual {docs_count} documents in {keeper_manifest} file"
        with Given(f"Install {keeper_type} {keeper_nodes} nodes"):
            kubectl.apply(get_full_path(keeper_manifest, lookup_in_host=False))
            for pod_num in range(keeper_nodes):
                kubectl.wait_object("pod", f"{expected_pod_prefix[keeper_type]}-{pod_num}")
            for pod_num in range(keeper_nodes):
                kubectl.wait_pod_status(f"{expected_pod_prefix[keeper_type]}-{pod_num}", "Running")
                kubectl.wait_container_status(f"{expected_pod_prefix[keeper_type]}-{pod_num}", "true")


def wait_clickhouse_cluster_ready(chi):
    with Given("All expected pods present in system.clusters"):
        all_pods_ready = False
        while all_pods_ready is False:
            all_pods_ready = True

            for pod in chi["status"]["pods"]:
                cluster_response = clickhouse.query(
                    chi["metadata"]["name"],
                    "SELECT host_name FROM system.clusters WHERE cluster='all-sharded'",
                    pod=pod,
                )
                for host in chi["status"]["fqdns"]:
                    svc_short_name = host.replace(f".{current().context.test_namespace}.svc.cluster.local", "")
                    if svc_short_name not in cluster_response:
                        with Then("Not ready, sleep 5 seconds"):
                            all_pods_ready = False
                            time.sleep(5)


def install_clickhouse_and_keeper(
    chi_file,
    chi_template_file,
    chi_name,
    keeper_type="zookeeper",
    keeper_manifest="",
    force_keeper_install=False,
    clean_ns=True,
    keeper_install_first=True,
    make_object_count=True,
):
    if keeper_manifest == "":
        if keeper_type == "zookeeper":
            keeper_manifest = "zookeeper-1-node-1GB-for-tests-only.yaml"
        if keeper_type == "clickhouse-keeper":
            keeper_manifest = "clickhouse-keeper-1-node-256M-for-test-only.yaml"
        if keeper_type == "zookeeper-operator":
            keeper_manifest = "zookeeper-operator-1-node.yaml"

    with Given("install zookeeper/clickhouse-keeper + clickhouse"):
        if clean_ns:
            kubectl.delete_all_chi(current().context.test_namespace)
            kubectl.delete_all_keeper(current().context.test_namespace)

        # when create clickhouse, need install ZK before CH
        if keeper_install_first:
            require_keeper(
                keeper_type=keeper_type,
                keeper_manifest=keeper_manifest,
                force_install=force_keeper_install,
            )

        chi_manifest_data = yaml_manifest.get_manifest_data(get_full_path(chi_file))
        layout = chi_manifest_data["spec"]["configuration"]["clusters"][0]["layout"]
        expected_nodes = 1 * layout["shardsCount"] * layout["replicasCount"]
        check = {
            "apply_templates": [
                chi_template_file,
                "manifests/chit/tpl-persistent-volume-100Mi.yaml",
            ],
            "do_not_delete": 1,
        }
        if make_object_count:
            check["object_counts"] = {
                "statefulset": expected_nodes,
                "pod": expected_nodes,
                "service": expected_nodes + 1,
            }
        kubectl.create_and_check(
            manifest=chi_file,
            check=check,
        )
        clickhouse_operator_spec = kubectl.get(
            "pod",
            name="",
            ns=current().context.operator_namespace,
            label="-l app=clickhouse-operator",
        )
        chi = kubectl.get("chi", ns=current().context.test_namespace, name=chi_name)

        # when re-scale clickhouse, need install Keeper after CH to follow ACM logic
        if not keeper_install_first:
            require_keeper(
                keeper_type=keeper_type,
                keeper_manifest=keeper_manifest,
                force_install=force_keeper_install,
            )

        return clickhouse_operator_spec, chi


def clean_namespace(delete_chi=False, delete_keeper=False, namespace=None):
    if namespace is None:
        namespace = current().context.test_namespace
    with Given(f"Clean namespace {namespace}"):
        if delete_keeper:
            kubectl.delete_all_keeper(namespace)
        if delete_chi:
            kubectl.delete_all_chi(namespace)
        kubectl.delete_ns(namespace, ok_to_fail=True)
        kubectl.create_ns(namespace)


def delete_namespace(namespace, delete_chi=False):
    kubectl.delete_ns(namespace, delete_chi, ok_to_fail=True)


def create_namespace(namespace):
    kubectl.create_ns(namespace)


def make_http_get_request(host, port, path):
    # thanks to https://github.com/falzm/burl
    # return f"wget -O- -q http://{host}:{port}{path}"
    cmd = "export LC_ALL=C; unset headers_read; "
    cmd += f"exec 3<>/dev/tcp/{host}/{port} ; "
    cmd += f'printf "GET {path} HTTP/1.1\\r\\nHost: {host}\\r\\nUser-Agent: bash\\r\\n\\r\\n" >&3 ; '
    cmd += "IFS=; while read -r -t 1 line 0<&3; do "
    cmd += "line=${line//$'\"'\"'\\r'\"'\"'}; "
    cmd += "if [[ ! -v headers_read ]]; then if [[ -z $line ]]; then headers_read=yes; fi; continue; fi; "
    cmd += 'echo "$line"; done; '
    cmd += "exec 3<&- ;"
    return f"bash -c '{cmd}'"


def install_operator_if_not_exist(
    reinstall=False,
    manifest=None,
    shell=None,
):
    if manifest is None:
        manifest = get_full_path(current().context.clickhouse_operator_install_manifest)

    if current().context.operator_install != "yes":
        return

    with Given(f"clickhouse-operator version {current().context.operator_version} is installed"):
        if (
            kubectl.get_count(
                "pod",
                ns=current().context.operator_namespace,
                label="-l app=clickhouse-operator",
                shell=shell
            )
            == 0
            or reinstall
        ):
            kubectl.apply(
                ns=current().context.operator_namespace,
                manifest=f"cat {manifest} | "
                f'OPERATOR_NAMESPACE="{current().context.operator_namespace}" '
                f'OPERATOR_IMAGE="{current().context.operator_docker_repo}:{current().context.operator_version}" '
                f'OPERATOR_IMAGE_PULL_POLICY="{current().context.image_pull_policy}" '
                f'METRICS_EXPORTER_NAMESPACE="{current().context.operator_namespace}" '
                f'METRICS_EXPORTER_IMAGE="{current().context.metrics_exporter_docker_repo}:{current().context.operator_version}" '
                f'METRICS_EXPORTER_IMAGE_PULL_POLICY="{current().context.image_pull_policy}" '
                f"envsubst",
                validate=False,
                shell=shell
            )
        set_operator_version(current().context.operator_version, shell=shell)


def install_operator_version(version, shell=None):
    if version == current().context.operator_version:
        manifest = get_full_path(current().context.clickhouse_operator_install_manifest)
        manifest = f"cat {manifest}"
    else:
        manifest = f"https://github.com/Altinity/clickhouse-operator/raw/{version}/deploy/operator/clickhouse-operator-install-template.yaml"
        manifest = f"curl -sL {manifest}"

    kubectl.apply(
        ns=current().context.operator_namespace,
        manifest=f"{manifest} | "
        f'OPERATOR_NAMESPACE="{current().context.operator_namespace}" '
        f'OPERATOR_IMAGE="{current().context.operator_docker_repo}:{version}" '
        f'OPERATOR_IMAGE_PULL_POLICY="{current().context.image_pull_policy}" '
        f'METRICS_EXPORTER_NAMESPACE="{current().context.operator_namespace}" '
        f'METRICS_EXPORTER_IMAGE="{current().context.metrics_exporter_docker_repo}:{version}" '
        f'METRICS_EXPORTER_IMAGE_PULL_POLICY="{current().context.image_pull_policy}" '
        f"envsubst",
        validate=False,
        shell=shell
    )


def wait_clickhouse_no_readonly_replicas(chi, retries=20):
    expected_replicas = 1
    layout = chi["spec"]["configuration"]["clusters"][0]["layout"]

    if "replicasCount" in layout:
        expected_replicas = layout["replicasCount"]
    if "shardsCount" in layout:
        expected_replicas = expected_replicas * layout["shardsCount"]

    expected_replicas = "[" + ",".join(["0"] * expected_replicas) + "]"
    for i in range(retries):
        readonly_replicas = clickhouse.query(
            chi["metadata"]["name"],
            "SELECT groupArray(if(value<0,0,value)) FROM cluster('all-sharded',system.metrics) WHERE metric='ReadonlyReplica'",
        )
        if readonly_replicas == expected_replicas:
            message(f"OK ReadonlyReplica actual={readonly_replicas}, expected={expected_replicas}")
            break
        else:
            with But(
                    f"CHECK ReadonlyReplica actual={readonly_replicas}, expected={expected_replicas}, Wait for {i * 3} seconds"
            ):
                time.sleep(i * 3)
        if i >= (retries - 1):
            raise RuntimeError(f"FAIL ReadonlyReplica failed, actual={readonly_replicas}, expected={expected_replicas}")
