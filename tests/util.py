import os
import settings
import kubectl
import util

from testflows.core import fail, Given


current_dir = os.path.dirname(os.path.abspath(__file__))
operator_label = "-l app=clickhouse-operator"


def get_full_path(test_file):
    return os.path.join(current_dir, f"{test_file}")


def set_operator_version(version, ns=settings.operator_namespace, timeout=60):
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
    pod_name = kubectl.get("pod", name="", ns=ns, label=operator_label)["items"][0]["metadata"]["name"]
    kubectl.launch(f"delete pod {pod_name}", ns=ns, timeout=timeout)
    kubectl.wait_object("pod", name="", ns=ns, label=operator_label)
    pod_name = kubectl.get("pod", name="", ns=ns, label=operator_label)["items"][0]["metadata"]["name"]
    kubectl.wait_pod_status(pod_name, "Running", ns=ns)


def require_zookeeper():
    with Given("Install Zookeeper if missing"):
        if kubectl.get_count("service", name="zookeeper") == 0:
            config = util.get_full_path(
                "../deploy/zookeeper/quick-start-persistent-volume/zookeeper-1-node-1GB-for-tests-only.yaml"
            )
            kubectl.apply(config)
            kubectl.wait_object("pod", "zookeeper-0")
            kubectl.wait_pod_status("zookeeper-0", "Running")
