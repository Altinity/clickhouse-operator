import json
import os
os.environ["TEST_NAMESPACE"]="test-metrics-exporter"
import json

from e2e.steps import *
from testflows.core import *
from testflows.asserts import error

import e2e.kubectl as kubectl
import e2e.settings as settings
import e2e.util as util
import e2e.clickhouse as clickhouse
import e2e.steps as steps


@TestScenario
@Name("test_metrics_exporter_setup: Check metrics server setup and version")
def test_metrics_exporter_setup(self):
    with Given("clickhouse-operator is installed"):
        assert kubectl.get_count("pod", ns=self.context.operator_namespace, label=util.operator_label) > 0, error()
        with Then(f"Set metrics-exporter version {settings.operator_version}"):
            util.set_metrics_exporter_version(settings.operator_version)


@TestScenario
@Name("test_metrics_exporter_chi: Test basic metrics exporter functionality")
def test_metrics_exporter_chi(self):
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
                print(out)
                print(expect_result)
                with Then("Not ready. Wait for " + str(i * 5) + " seconds"):
                    time.sleep(i * 5)
            assert out == expect_result, error()

    def check_monitoring_metrics(operator_namespace, operator_pod, expect_result, max_retries=10):
        with Then(f"metrics-exporter /metrics endpoint result should match with {expect_result}"):
            found = 0
            for i in range(1, max_retries):
                url_cmd = util.make_http_get_request("127.0.0.1", "8888", "/metrics")
                out = kubectl.launch(
                    f"exec {operator_pod} -c metrics-exporter -- {url_cmd}",
                    ns=operator_namespace,
                )
                found = 0
                for string, exists in expect_result.items():
                    if exists == (string in out):
                        found = found + 1
                        print(f"FOUND: {string}")
                if found == len(expect_result.items()):
                    break
                with Then("Not ready. Wait for " + str(i * 5) + " seconds"):
                    time.sleep(i * 5)
            assert found == len(expect_result.items()), error()

    with Given("clickhouse-operator is installed"):
        kubectl.wait_field(
            "pods",
            util.operator_label,
            ".status.containerStatuses[*].ready",
            "true,true",
            ns=self.context.operator_namespace,
        )
        assert kubectl.get_count("pod", ns=self.context.operator_namespace, label=util.operator_label) > 0, error()

        out = kubectl.launch("get pods -l app=clickhouse-operator", ns=self.context.operator_namespace).splitlines()[1]
        operator_pod = re.split(r"[\t\r\n\s]+", out)[0]
        kubectl.delete_all_chi(self.context.test_namespace)
        check_monitoring_chi(self.context.operator_namespace, operator_pod, [])
        with And("created simple clickhouse installation"):
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
            expected_chi = [
                {
                    "namespace": self.context.test_namespace,
                    "name": "test-017-multi-version",
                    "labels": {"clickhouse.altinity.com/chi": "test-017-multi-version"},
                    "annotations": {"clickhouse.altinity.com/email": "myname@mydomain.com, yourname@yourdoman.com"},
                    "clusters": [
                        {
                            "name": "default",
                            "hosts": [
                                {
                                    "name": "0-0",
                                    "hostname": f"chi-test-017-multi-version-default-0-0.{self.context.test_namespace}.svc.cluster.local",
                                    "tcpPort": 9000,
                                    "httpPort": 8123
                                },
                                {
                                    "name": "1-0",
                                    "hostname": f"chi-test-017-multi-version-default-1-0.{self.context.test_namespace}.svc.cluster.local",
                                    "tcpPort": 9000,
                                    "httpPort": 8123
                                }
                            ]
                        }
                    ]
                }
            ]

        with Then("Add system.custom_metrics"):
            clickhouse.query("test-017-multi-version", "CREATE VIEW system.custom_metrics as SELECT 'MyCustomMetric' as metric, 1 as value")


        with Then("Check both pods are monitored"):
            check_monitoring_chi(self.context.operator_namespace, operator_pod, expected_chi)
        labels = ','.join([
                  'chi="test-017-multi-version"',
                  'clickhouse_altinity_com_chi="test-017-multi-version"',
                  'clickhouse_altinity_com_email="myname@mydomain.com, yourname@yourdoman.com"'
                  ])

        with Then("Check not empty /metrics"):
            check_monitoring_metrics(
                self.context.operator_namespace,
                operator_pod,
                expect_result={
                    "# HELP chi_clickhouse_metric_VersionInteger": True,
                    "# TYPE chi_clickhouse_metric_VersionInteger gauge": True,
                    "chi_clickhouse_metric_VersionInteger{" + labels +",hostname=\"chi-test-017-multi-version-default-0-0": True,
                    "chi_clickhouse_metric_VersionInteger{" + labels +",hostname=\"chi-test-017-multi-version-default-1-0": True,
                },
            )

        with Then("Check that custom_metrics is properly monitored"):
            steps.check_metrics_monitoring(
                operator_namespace = self.context.operator_namespace,
                operator_pod = operator_pod,
                expect_pattern="^chi_clickhouse_metric_MyCustomMetric{(.*?)} 1$"
            )

        with When("reboot metrics exporter"):
            kubectl.launch(f"exec -n {self.context.operator_namespace} {operator_pod} -c metrics-exporter -- bash -c 'kill 1'")
            time.sleep(15)
            kubectl.wait_field(
                    "pods",
                    util.operator_label,
                    ".status.containerStatuses[*].ready",
                    "true,true",
                    ns=self.context.operator_namespace,
            )
            with Then("check metrics exporter still contains chi objects"):
                    check_monitoring_chi(self.context.operator_namespace, operator_pod, expected_chi)
                    kubectl.delete(util.get_full_path(manifest, lookup_in_host=False), timeout=600)
                    check_monitoring_chi(self.context.operator_namespace, operator_pod, [])

        with Then("check empty /metrics after delete namespace"):
            kubectl.delete_all_chi(self.context.test_namespace)
            check_monitoring_metrics(
                self.context.operator_namespace,
                operator_pod,
                expect_result={
                    "chi_clickhouse_metric_VersionInteger": False,
                },
            )


@TestFeature
@Name("e2e.test_metrics_exporter")
def test(self):
    with Given("set settings"):
        set_settings()
        self.context.test_namespace = "test-metrics-exporter"
        self.context.operator_namespace = "test-metrics-exporter"
    with Given("I create shell"):
        shell = get_shell()
        self.context.shell = shell

    util.clean_namespace(delete_chi=True)
    util.install_operator_if_not_exist()
    test_cases = [
        test_metrics_exporter_setup,
        test_metrics_exporter_chi,
    ]
    for t in test_cases:
        Scenario(test=t)()
