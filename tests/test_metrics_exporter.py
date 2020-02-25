import time

from testflows.core import TestScenario, Name, When, Then, Given, And, main, run, Module, TE
from testflows.asserts import error
import re
import json
import kubectl

version = '0.9.1'

def set_metrics_exporter_version(version, ns="kube-system"):
    kubectl.kubectl(f"set image deployment.v1.apps/clickhouse-operator metrics-exporter=altinity/metrics-exporter:{version}", ns=ns)
    kubectl.kubectl("rollout status deployment.v1.apps/clickhouse-operator", ns=ns)


@TestScenario
@Name("Check metrics server setup and version")
def test_metrics_exporter_setup():
    with Given("clickhouse-operator is installed"):
        assert kubectl.kube_get_count("pod", ns='--all-namespaces', label="-l app=clickhouse-operator") > 0, error()
        with And(f"Set metrics-exporter version {version}"):
            set_metrics_exporter_version(version)


@TestScenario
@Name("Check metrics server state after reboot")
def test_metrics_exporter_reboot():
    def check_monitoring_chi(operator_namespace, operator_pod, expect_result):
        with And(f"metrics-exporter /chi enpoint result should return {expect_result}"):
            out = kubectl.kubectl(
                f"exec {operator_pod} -c metrics-exporter wget -- -O- -q http://127.0.0.1:8888/chi",
                ns=operator_namespace
            )
            out = json.loads(out)
            assert out == expect_result, error()

    with Given("clickhouse-operator is installed"):
        assert kubectl.kube_get_count("pod", ns='--all-namespaces', label="-l app=clickhouse-operator") > 0, error()
        out = kubectl.kubectl("get pods -l app=clickhouse-operator", ns='kube-system').splitlines()[1]
        operator_pod = re.split(r'[\t\r\n\s]+', out)[0]
        operator_namespace = "kube-system"
        kubectl.kube_deletens(kubectl.namespace)
        kubectl.kube_createns(kubectl.namespace)
        check_monitoring_chi(operator_namespace, operator_pod, [])
        with And("created simple clickhouse installation"):
            config = kubectl.get_full_path("../docs/chi-examples/01-simple-layout-01-1shard-1repl.yaml")
            kubectl.create_and_check(config, {"object_counts": [1, 1, 2], "do_not_delete": True})
            expected_chi = [{
                "namespace": "test", "name": "simple-01",
                "hostnames": ["chi-simple-01-cluster-0-0.test.svc.cluster.local"]
            }]
            check_monitoring_chi(operator_namespace, operator_pod, expected_chi)
            with When("reboot metrics exporter"):
                kubectl.kubectl(f"exec -n {operator_namespace} {operator_pod} -c metrics-exporter reboot")
                time.sleep(5)
                kubectl.kube_wait_field("pods", "-l app=clickhouse-operator", ".status.containerStatuses[*].started", "true,true", ns="kube-system")
                with Then("check metrics exporter still contains chi objects"):
                    check_monitoring_chi(operator_namespace, operator_pod, expected_chi)
                    kubectl.kube_delete(config)
                    time.sleep(5)
                    check_monitoring_chi(operator_namespace, operator_pod, [])


if main():
    with Module("metrics_exporter", flags=TE):
        examples = [test_metrics_exporter_setup, test_metrics_exporter_reboot]
        for t in examples:
            run(test=t, flags=TE)
