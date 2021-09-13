import os
import time

import tests.clickhouse as clickhouse
import tests.kubectl as kubectl
import tests.settings as settings
import tests.util as util

from testflows.core import fail, Given, Then


current_dir = os.path.dirname(os.path.abspath(__file__))
operator_label = "-l app=clickhouse-operator"


def get_full_path(test_file, baremetal=True):
    if baremetal:
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), f"../{test_file}")
    else:
        return "/home/master/clickhouse-operator/tests/" + test_file


def set_operator_version(node, version, ns=settings.operator_namespace, timeout=6000):
    operator_image = f"{settings.operator_docker_repo}:{version}"
    metrics_exporter_image = f"{settings.metrics_exporter_docker_repo}:{version}"
    kubectl.launch(node, f"set image deployment.v1.apps/clickhouse-operator clickhouse-operator={operator_image}", ns=ns)
    kubectl.launch(node, f"set image deployment.v1.apps/clickhouse-operator metrics-exporter={metrics_exporter_image}", ns=ns)
    kubectl.launch(node, "rollout status deployment.v1.apps/clickhouse-operator", ns=ns, timeout=timeout)
    if kubectl.get_count(node, "pod", ns=ns, label=operator_label) == 0:
        fail("invalid clickhouse-operator pod count")


def set_metrics_exporter_version(node, version, ns=settings.operator_namespace):
    kubectl.launch(node, f"set image deployment.v1.apps/clickhouse-operator metrics-exporter=altinity/metrics-exporter:{version}", ns=ns)
    kubectl.launch(node, "rollout status deployment.v1.apps/clickhouse-operator", ns=ns)


def restart_operator(node, ns=settings.operator_namespace, timeout=6000):
    pod_name = kubectl.get(node, "pod", name="", ns=ns, label=operator_label)["items"][0]["metadata"]["name"]
    kubectl.launch(node, f"delete pod {pod_name}", ns=ns, timeout=timeout)
    kubectl.wait_object(node, "pod", name="", ns=ns, label=operator_label)
    pod_name = kubectl.get(node, "pod", name="", ns=ns, label=operator_label)["items"][0]["metadata"]["name"]
    kubectl.wait_pod_status(node, pod_name, "Running", ns=ns)


def require_zookeeper(node, manifest='zookeeper-1-node-1GB-for-tests-only.yaml', force_install=False):
    if force_install or kubectl.get_count(node, "service", name="zookeeper") == 0:
        with Given("Zookeeper is missing, installing"):
            config = util.get_full_path(f"../deploy/zookeeper/quick-start-persistent-volume/{manifest}", False)
            kubectl.apply(node, config)
            kubectl.wait_object(node, "pod", "zookeeper-0")
            kubectl.wait_pod_status(node, "zookeeper-0", "Running")


def wait_clickhouse_cluster_ready(node, chi):
    with Given("All expected pods present in system.clusters"):
        all_pods_ready = False
        while all_pods_ready is False:
            all_pods_ready = True
            for pod in chi['status']['pods']:
                cluster_response = clickhouse.query(
                    node,
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


def install_clickhouse_and_zookeeper(node, chi_file, chi_template_file, chi_name):
    with Given("install zookeeper+clickhouse"):
        kubectl.delete_ns(node, settings.test_namespace, ok_to_fail=True, timeout=6000)
        kubectl.create_ns(node, settings.test_namespace)
        util.require_zookeeper(node)
        kubectl.create_and_check(
            node,
            config=chi_file,
            check={
                "apply_templates": [
                    chi_template_file,
                    "templates/tpl-persistent-volume-100Mi.yaml"
                ],
                "object_counts": {
                    "statefulset": 2,
                    "pod": 2,
                    "service": 3,
                },
                "do_not_delete": 1
            }
        )
        clickhouse_operator_spec = kubectl.get(node,
            "pod", name="", ns=settings.operator_namespace, label="-l app=clickhouse-operator"
        )
        chi = kubectl.get(node, "chi", ns=settings.test_namespace, name=chi_name)
        return clickhouse_operator_spec, chi
