import time

from testflows.core import TestScenario, Name, When, Then, Given, And, main, run, Module, TE
from testflows.asserts import error
import re
import json
import kubectl
import settings

def set_metrics_exporter_version(version, ns=settings.operator_namespace):
    kubectl.kubectl(f"set image deployment.v1.apps/clickhouse-operator metrics-exporter=altinity/metrics-exporter:{version}", ns=ns)
    kubectl.kubectl("rollout status deployment.v1.apps/clickhouse-operator", ns=ns)


@TestScenario
@Name("Check metrics server setup and version")
def test_metrics_exporter_setup():
    with Given("clickhouse-operator is installed"):
        assert kubectl.kube_get_count("pod", ns='--all-namespaces', label="-l app=clickhouse-operator") > 0, error()
        with And(f"Set metrics-exporter version {settings.operator_version}"):
            set_metrics_exporter_version(settings.operator_version)


@TestScenario
@Name("Check metrics server state after reboot")
def test_metrics_exporter_reboot():
    def check_monitoring_chi(operator_namespace, operator_pod, expect_result, max_retries=10):
        with And(f"metrics-exporter /chi enpoint result should return {expect_result}"):
            for i in range(1, max_retries):
                # check /metrics for try to refresh monitored instances
                kubectl.kubectl(
                    f"exec {operator_pod} -c metrics-exporter -- wget -O- -q http://127.0.0.1:8888/metrics",
                    ns=operator_namespace
                )
                # check /chi after refresh monitored instances
                out = kubectl.kubectl(
                    f"exec {operator_pod} -c metrics-exporter -- wget -O- -q http://127.0.0.1:8888/chi",
                    ns=operator_namespace
                )
                out = json.loads(out)
                if out == expect_result:
                    break
                with Then("Not ready. Wait for " + str(i * 5) + " seconds"):
                    time.sleep(i * 5)
            assert out == expect_result, error()

    with Given("clickhouse-operator is installed"):
        kubectl.kube_wait_field("pods", "-l app=clickhouse-operator", ".status.containerStatuses[*].ready", "true,true",
                                ns=settings.operator_namespace)
        assert kubectl.kube_get_count("pod", ns='--all-namespaces', label="-l app=clickhouse-operator") > 0, error()

        out = kubectl.kubectl("get pods -l app=clickhouse-operator", ns=settings.operator_namespace).splitlines()[1]
        operator_pod = re.split(r'[\t\r\n\s]+', out)[0]
        operator_namespace = settings.operator_namespace
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
                kubectl.kubectl(f"exec -n {operator_namespace} {operator_pod} -c metrics-exporter -- reboot")
                time.sleep(15)
                kubectl.kube_wait_field("pods", "-l app=clickhouse-operator",
                                        ".status.containerStatuses[*].ready", "true,true",
                                        ns=settings.operator_namespace)
                with Then("check metrics exporter still contains chi objects"):
                    check_monitoring_chi(operator_namespace, operator_pod, expected_chi)
                    kubectl.kube_delete(config)
                    check_monitoring_chi(operator_namespace, operator_pod, [])

@TestScenario
@Name("Check metrics server help with different clickhouse version")
def test_metrics_exporter_with_multiple_clickhouse_version():
    def check_monitoring_metrics(operator_namespace, operator_pod, expect_result, max_retries=10):
        with And(f"metrics-exporter /metrics enpoint result should match with {expect_result}"):
            for i in range(1, max_retries):
                out = kubectl.kubectl(
                    f"exec {operator_pod} -c metrics-exporter -- wget -O- -q http://127.0.0.1:8888/metrics",
                    ns=operator_namespace
                )
                all_strings_expected_done = True
                for string, exists in expect_result.items():
                    all_strings_expected_done = (exists == (string in out))
                    if not all_strings_expected_done:
                        break

                if all_strings_expected_done:
                    break
                with Then("Not ready. Wait for " + str(i * 5) + " seconds"):
                    time.sleep(i * 5)
            assert all_strings_expected_done, error()

    with Given("clickhouse-operator pod exists"):
        out = kubectl.kubectl("get pods -l app=clickhouse-operator", ns='kube-system').splitlines()[1]
        operator_pod = re.split(r'[\t\r\n\s]+', out)[0]
        operator_namespace = "kube-system"

        with Then("check empty /metrics"):
            kubectl.kube_deletens(kubectl.namespace)
            kubectl.kube_createns(kubectl.namespace)
            check_monitoring_metrics(operator_namespace, operator_pod, expect_result={
                'chi_clickhouse_metric_VersionInteger': False,
            })

        with Then("Install multiple clickhouse version"):
            config = kubectl.get_full_path("configs/test-017-multi-version.yaml")
            kubectl.create_and_check(config, {"object_counts": [4, 4, 5], "do_not_delete": True})
            with And("Check not empty /metrics"):
                check_monitoring_metrics(operator_namespace, operator_pod, expect_result={
                    '# HELP chi_clickhouse_metric_VersionInteger': True,
                    '# TYPE chi_clickhouse_metric_VersionInteger gauge': True,
                    'chi_clickhouse_metric_VersionInteger{chi="test-017-multi-version",hostname="chi-test-017-multi-version-default-0-0': True,
                    'chi_clickhouse_metric_VersionInteger{chi="test-017-multi-version",hostname="chi-test-017-multi-version-default-1-0': True,
                    'chi_clickhouse_metric_VersionInteger{chi="test-017-multi-version",hostname="chi-test-017-multi-version-default-2-0': True,
                    'chi_clickhouse_metric_VersionInteger{chi="test-017-multi-version",hostname="chi-test-017-multi-version-default-3-0': True,

                })

        with Then("check empty /metrics after delete namespace"):
            kubectl.kube_deletens(kubectl.namespace)
            check_monitoring_metrics(operator_namespace, operator_pod, expect_result={
                'chi_clickhouse_metric_VersionInteger': False,
            })


if main():
    with Module("metrics_exporter", flags=TE):
        test_cases = [
            test_metrics_exporter_setup,
            test_metrics_exporter_reboot,
            test_metrics_exporter_with_multiple_clickhouse_version,
        ]
        for t in test_cases:
            run(test=t, flags=TE)
