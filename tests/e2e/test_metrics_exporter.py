import time
import re
import json

from testflows.core import *
from testflows.asserts import error

import e2e.kubectl as kubectl
import e2e.settings as settings
import e2e.util as util


@TestScenario
@Name("Check metrics server setup and version")
def test_metrics_exporter_setup(self):
    with Given("clickhouse-operator is installed"):
        assert kubectl.get_count("pod", ns="--all-namespaces", label=util.operator_label) > 0, error()
        with Then(f"Set metrics-exporter version {settings.operator_version}"):
            util.set_metrics_exporter_version(settings.operator_version)


@TestScenario
@Name("Check metrics server state after reboot")
def test_metrics_exporter_reboot(self):
    def check_monitoring_chi(operator_namespace, operator_pod, expect_result, max_retries=10):
        with Then(f"metrics-exporter /chi endpoint result should return {expect_result}"):
            for i in range(1, max_retries):
                # check /metrics for try to refresh monitored instances
                url_cmd = util.make_http_get_request("127.0.0.1", "8888", "/metrics")
                kubectl.launch(
                    f"exec {operator_pod} -c metrics-exporter -- {url_cmd}",
                    ns=operator_namespace,
                )
                # check /chi after refresh monitored instances
                url_cmd = util.make_http_get_request("127.0.0.1", "8888", "/chi")
                out = kubectl.launch(
                    f"exec {operator_pod} -c metrics-exporter -- {url_cmd}",
                    ns=operator_namespace,
                )
                out = json.loads(out)
                if out == expect_result:
                    break
                with Then("Not ready. Wait for " + str(i * 5) + " seconds"):
                    time.sleep(i * 5)
            assert out == expect_result, error()

    with Given("clickhouse-operator is installed"):
        kubectl.wait_field(
            "pods",
            util.operator_label,
            ".status.containerStatuses[*].ready",
            "true,true",
            ns=settings.operator_namespace,
        )
        assert kubectl.get_count("pod", ns="--all-namespaces", label=util.operator_label) > 0, error()

        out = kubectl.launch("get pods -l app=clickhouse-operator", ns=settings.operator_namespace).splitlines()[1]
        operator_pod = re.split(r"[\t\r\n\s]+", out)[0]
        kubectl.delete_all_chi(settings.test_namespace)
        check_monitoring_chi(settings.operator_namespace, operator_pod, [])
        with And("created simple clickhouse installation"):
            manifest = "../../docs/chi-examples/01-simple-layout-01-1shard-1repl.yaml"
            kubectl.create_and_check(
                manifest=manifest,
                check={
                    "object_counts": {
                        "statefulset": 1,
                        "pod": 1,
                        "service": 2,
                    },
                    "do_not_delete": True,
                },
            )
            expected_chi = [
                {
                    "namespace": "test",
                    "name": "simple-01",
                    "clusters": [
                        {
                            "name": "simple",
                            "hosts": [
                                {
                                    "name": "0-0",
                                    "hostname": "chi-simple-01-simple-0-0.test.svc.cluster.local",
                                    "tcpPort": 9000,
                                    "httpPort": 8123
                                }
                            ]
                        }
                    ]
                }
            ]
            check_monitoring_chi(settings.operator_namespace, operator_pod, expected_chi)
            with When("reboot metrics exporter"):
                kubectl.launch(f"exec -n {settings.operator_namespace} {operator_pod} -c metrics-exporter -- bash -c 'kill 1'")
                time.sleep(15)
                kubectl.wait_field(
                    "pods",
                    util.operator_label,
                    ".status.containerStatuses[*].ready",
                    "true,true",
                    ns=settings.operator_namespace,
                )
                with Then("check metrics exporter still contains chi objects"):
                    check_monitoring_chi(settings.operator_namespace, operator_pod, expected_chi)
                    kubectl.delete(util.get_full_path(manifest, lookup_in_host=False), timeout=600)
                    check_monitoring_chi(settings.operator_namespace, operator_pod, [])


@TestScenario
@Name("Check metrics server help with different clickhouse version")
def test_metrics_exporter_with_multiple_clickhouse_version(self):
    def check_monitoring_metrics(operator_namespace, operator_pod, expect_result, max_retries=10):
        with Then(f"metrics-exporter /metrics endpoint result should match with {expect_result}"):
            for i in range(1, max_retries):
                url_cmd = util.make_http_get_request("127.0.0.1", "8888", "/metrics")
                out = kubectl.launch(
                    f"exec {operator_pod} -c metrics-exporter -- {url_cmd}",
                    ns=operator_namespace,
                )
                all_strings_expected_done = True
                for string, exists in expect_result.items():
                    all_strings_expected_done = exists == (string in out)
                    if not all_strings_expected_done:
                        break

                if all_strings_expected_done:
                    break
                with Then("Not ready. Wait for " + str(i * 5) + " seconds"):
                    time.sleep(i * 5)
            assert all_strings_expected_done, error()

    with Given("clickhouse-operator pod exists"):
        out = kubectl.launch("get pods -l app=clickhouse-operator", ns=settings.operator_namespace).splitlines()[1]
        operator_pod = re.split(r"[\t\r\n\s]+", out)[0]

        with Then("check empty /metrics"):
            kubectl.delete_all_chi(settings.test_namespace)
            check_monitoring_metrics(
                settings.operator_namespace,
                operator_pod,
                expect_result={
                    "chi_clickhouse_metric_VersionInteger": False,
                },
            )

        with Then("Install multiple clickhouse version"):
            manifest = "manifests/chi/test-017-multi-version.yaml"
            kubectl.create_and_check(
                manifest=manifest,
                check={
                    "object_counts": {
                        "statefulset": 2,
                        "pod": 2,
                        "service": 3,
                    },
                    "do_not_delete": True,
                },
            )
            with And("Check not empty /metrics"):
                check_monitoring_metrics(
                    settings.operator_namespace,
                    operator_pod,
                    expect_result={
                        "# HELP chi_clickhouse_metric_VersionInteger": True,
                        "# TYPE chi_clickhouse_metric_VersionInteger gauge": True,
                        'chi_clickhouse_metric_VersionInteger{chi="test-017-multi-version",hostname="chi-test-017-multi-version-default-0-0': True,
                        'chi_clickhouse_metric_VersionInteger{chi="test-017-multi-version",hostname="chi-test-017-multi-version-default-1-0': True,
                    },
                )

        with Then("check empty /metrics after delete namespace"):
            kubectl.delete_all_chi(settings.test_namespace)
            check_monitoring_metrics(
                settings.operator_namespace,
                operator_pod,
                expect_result={
                    "chi_clickhouse_metric_VersionInteger": False,
                },
            )


@TestFeature
@Name("e2e.test_metrics_exporter")
def test(self):
    util.clean_namespace(delete_chi=True)
    util.install_operator_if_not_exist()
    test_cases = [
        test_metrics_exporter_setup,
        test_metrics_exporter_reboot,
        test_metrics_exporter_with_multiple_clickhouse_version,
    ]
    for t in test_cases:
        Scenario(test=t)()
