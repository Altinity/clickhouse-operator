import settings
from kubectl import *


def set_operator_version(version, ns="kube-system", timeout=60):
    kubectl(
        f"set image deployment.v1.apps/clickhouse-operator clickhouse-operator=altinity/clickhouse-operator:{version}",
        ns=ns
    )
    kubectl(
        f"set image deployment.v1.apps/clickhouse-operator metrics-exporter=altinity/metrics-exporter:{version}",
        ns=ns
    )
    kubectl("rollout status deployment.v1.apps/clickhouse-operator", ns=ns, timeout=timeout)
    assert kube_get_count("pod", ns=ns, label="-l app=clickhouse-operator") > 0, error()


def restart_operator(ns="kube-system", timeout=60):
    pod_name = kube_get("pod", name="", ns=ns, label="-l app=clickhouse-operator")["items"][0]["metadata"]["name"]
    kubectl(f"delete pod {pod_name}", ns=ns, timeout=timeout)
    kube_wait_object("pod", name="", ns=ns, label="-l app=clickhouse-operator")
    pod_name = kube_get("pod", name="", ns=ns, label="-l app=clickhouse-operator")["items"][0]["metadata"]["name"]
    kube_wait_pod_status(pod_name, "Running", ns=ns)
    time.sleep(5)


def require_zookeeper():
    with Given("Install Zookeeper if missing"):
        if kube_get_count("service", name="zookeepers") == 0:
            config = get_full_path("../deploy/zookeeper/quick-start-volume-emptyDir/zookeeper-1-node.yaml")
            kube_apply(config)
            kube_wait_object("pod", "zookeeper-0")
            kube_wait_pod_status("zookeeper-0", "Running")


def test_operator_upgrade(config, version_from, version_to=settings.version):
    with Given(f"clickhouse-operator {version_from}"):
        set_operator_version(version_from)
        config = get_full_path(config)
        chi = get_chi_name(config)

        create_and_check(config, {"object_counts": [1, 1, 2], "do_not_delete": 1})

        with When(f"upgrade operator to {version_to}"):
            set_operator_version(version_to, timeout=120)
            kube_wait_chi_status(chi, "Completed", retries=5)
            kube_wait_objects(chi, [1, 1, 2])

        kube_delete_chi(chi)


def test_operator_restart(config, version=settings.version):
    with Given(f"clickhouse-operator {version}"):
        set_operator_version(version)
        config = get_full_path(config)
        chi = get_chi_name(config)

        create_and_check(config, {"object_counts": [1, 1, 2], "do_not_delete": 1})

        with When("Restart operator"):
            restart_operator()
            kube_wait_chi_status(chi, "Completed")
            kube_wait_objects(chi, [1, 1, 2])

        kube_delete_chi(chi)
