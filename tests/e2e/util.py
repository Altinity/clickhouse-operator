import os
import time

import e2e.clickhouse as clickhouse
import e2e.kubectl as kubectl
import e2e.settings as settings
import e2e.yaml_manifest as yaml_manifest

from testflows.core import fail, Given, Then, current


current_dir = os.path.dirname(os.path.abspath(__file__))
operator_label = "-l app=clickhouse-operator"


def get_full_path(test_file, lookup_in_host=True):
    # this must be substituted if ran in docker
    if current().context.native or lookup_in_host:
        return os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), test_file))
    else:
        return os.path.abspath(f"/home/master/clickhouse-operator/tests/e2e/{test_file}")


def set_operator_version(version, ns=settings.operator_namespace, timeout=600):
    if settings.operator_install != 'yes':
        return
    operator_image = f"{settings.operator_docker_repo}:{version}"
    metrics_exporter_image = f"{settings.metrics_exporter_docker_repo}:{version}"

    kubectl.launch(f"set image deployment.v1.apps/clickhouse-operator clickhouse-operator={operator_image}", ns=ns)
    kubectl.launch(f"set image deployment.v1.apps/clickhouse-operator metrics-exporter={metrics_exporter_image}", ns=ns)
    kubectl.launch("rollout status deployment.v1.apps/clickhouse-operator", ns=ns, timeout=timeout)
    if kubectl.get_count("pod", ns=ns, label=operator_label) == 0:
        fail("invalid clickhouse-operator pod count")


def set_metrics_exporter_version(version, ns=settings.operator_namespace):
    kubectl.launch(f"set image deployment.v1.apps/clickhouse-operator metrics-exporter=altinity/metrics-exporter:{version}", ns=ns)
    kubectl.launch("rollout status deployment.v1.apps/clickhouse-operator", ns=ns)


def restart_operator(ns=settings.operator_namespace, timeout=600):
    if settings.operator_install != 'yes':
        return
    pod_name = kubectl.get("pod", name="", ns=ns, label=operator_label)["items"][0]["metadata"]["name"]
    kubectl.launch(f"delete pod {pod_name}", ns=ns, timeout=timeout)
    kubectl.wait_object("pod", name="", ns=ns, label=operator_label)
    pod_name = kubectl.get("pod", name="", ns=ns, label=operator_label)["items"][0]["metadata"]["name"]
    kubectl.wait_pod_status(pod_name, "Running", ns=ns)


def require_zookeeper(zk_manifest='zookeeper-1-node-1GB-for-tests-only.yaml', force_install=False):
    if force_install or kubectl.get_count("service", name="zookeeper") == 0:
        zk_manifest = f"../../deploy/zookeeper/quick-start-persistent-volume/{zk_manifest}"
        zk = yaml_manifest.get_multidoc_manifest_data(get_full_path(zk_manifest, lookup_in_host=True))
        zk_nodes = 1
        i = 0
        for doc in zk:
            i += 1
            if i == 4:
                zk_nodes = doc["spec"]["replicas"]
        assert i == 4, "invalid zookeeper manifest, expected 4 documents in yaml file"
        with Given(f"Install Zookeeper {zk_nodes} nodes"):
            kubectl.apply(get_full_path(zk_manifest, lookup_in_host=False))
            for i in range(zk_nodes):
                kubectl.wait_object("pod", f"zookeeper-{i}")
            for i in range(zk_nodes):
                kubectl.wait_pod_status(f"zookeeper-{i}", "Running")


def wait_clickhouse_cluster_ready(chi):
    with Given("All expected pods present in system.clusters"):
        all_pods_ready = False
        while all_pods_ready is False:
            all_pods_ready = True

            for pod in chi['status']['pods']:
                cluster_response = clickhouse.query(
                    chi["metadata"]["name"],
                    "SYSTEM RELOAD CONFIG; SELECT host_name FROM system.clusters WHERE cluster='all-sharded'",
                    pod=pod
                )
                for host in chi['status']['fqdns']:
                    svc_short_name = host.replace(f'.{settings.test_namespace}.svc.cluster.local', '')
                    if svc_short_name not in cluster_response:
                        with Then("Not ready, sleep 5 seconds"):
                            all_pods_ready = False
                            time.sleep(5)


def install_clickhouse_and_zookeeper(chi_file, chi_template_file, chi_name,
                                     zk_manifest='zookeeper-1-node-1GB-for-tests-only.yaml', force_zk_install=False, clean_ns=True, zk_install_first=True,
                                     make_object_count=True):
    with Given("install zookeeper+clickhouse"):
        if clean_ns:
            kubectl.delete_ns(settings.test_namespace, ok_to_fail=True, timeout=600)
            kubectl.create_ns(settings.test_namespace)
        # when create clickhouse, need install ZK before CH
        if zk_install_first:
            require_zookeeper(zk_manifest=zk_manifest, force_install=force_zk_install)

        chi_manifest_data = yaml_manifest.get_manifest_data(get_full_path(chi_file))
        layout = chi_manifest_data["spec"]["configuration"]["clusters"][0]["layout"]
        expected_nodes = 1 * layout["shardsCount"] * layout["replicasCount"]
        check = {
            "apply_templates": [
                chi_template_file,
                "manifests/chit/tpl-persistent-volume-100Mi.yaml"
            ],
            "do_not_delete": 1
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
            "pod", name="", ns=settings.operator_namespace, label="-l app=clickhouse-operator"
        )
        chi = kubectl.get("chi", ns=settings.test_namespace, name=chi_name)

        # when re-scale clickhouse, need install ZK after CH to follow ACM logic
        if not zk_install_first:
            require_zookeeper(zk_manifest=zk_manifest, force_install=force_zk_install)

        return clickhouse_operator_spec, chi


def clean_namespace(delete_chi=False):
    with Given(f"Clean namespace {settings.test_namespace}"):
        if delete_chi:
            kubectl.delete_all_chi(settings.test_namespace)
        kubectl.delete_ns(settings.test_namespace, ok_to_fail=True)
        kubectl.create_ns(settings.test_namespace)


def make_http_get_request(host, port, path):
    # thanks to https://github.com/falzm/burl
    # return f"wget -O- -q http://{host}:{port}{path}"
    cmd = "export LC_ALL=C; unset headers_read; "
    cmd += f"exec 3<>/dev/tcp/{host}/{port} ; "
    cmd += f"printf \"GET {path} HTTP/1.1\\r\\nHost: {host}\\r\\nUser-Agent: bash\\r\\n\\r\\n\" >&3 ; "
    cmd += 'IFS=; while read -r -t 1 line 0<&3; do '
    cmd += "line=${line//$'\"'\"'\\r'\"'\"'}; "
    cmd += 'if [[ ! -v headers_read ]]; then if [[ -z $line ]]; then headers_read=yes; fi; continue; fi; '
    cmd += 'echo "$line"; done; '
    cmd += 'exec 3<&- ;'
    return f"bash -c '{cmd}'"


def install_operator_if_not_exist(reinstall = False):
    if settings.operator_install != 'yes':
        return
    with Given(f"clickhouse-operator version {settings.operator_version} is installed"):
        if kubectl.get_count("pod", ns=settings.operator_namespace, label="-l app=clickhouse-operator") == 0 or reinstall == True:
            manifest = get_full_path(settings.clickhouse_operator_install_manifest)
            kubectl.apply(
                ns=settings.operator_namespace,
                manifest=f"<(cat {manifest} | "
                       f"OPERATOR_IMAGE=\"{settings.operator_docker_repo}:{settings.operator_version}\" "
                       f"OPERATOR_NAMESPACE=\"{settings.operator_namespace}\" "
                       f"METRICS_EXPORTER_IMAGE=\"{settings.metrics_exporter_docker_repo}:{settings.operator_version}\" "
                       f"METRICS_EXPORTER_NAMESPACE=\"{settings.operator_namespace}\" "
                       f"envsubst)",
                validate=False
            )
        set_operator_version(settings.operator_version)
