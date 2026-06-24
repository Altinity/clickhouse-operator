import time
import re
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
@Name("Check metrics server setup and version")
def test_metrics_exporter_setup(self):
    with Given("clickhouse-operator is installed"):
        assert kubectl.get_count("pod", ns="--all-namespaces", label=util.operator_label) > 0, error()
        with Then(f"Set metrics-exporter version {settings.operator_version}"):
            util.set_metrics_exporter_version(settings.operator_version)


@TestScenario
@Name("Test basic metrics exporter functionality")
def test_metrics_exporter_chi(self):
    def check_monitoring_chi(operator_namespace, operator_pod, expect_result, max_retries=10):
        with Then(f"metrics-exporter /chi endpoint result should return {expect_result}"):
            for i in range(1, max_retries):
                # check /metrics for try to refresh monitored instances
                util.get_metrics(operator_pod, operator_namespace)
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
                out = util.get_metrics(operator_pod, operator_namespace)
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
        assert kubectl.get_count("pod", ns="--all-namespaces", label=util.operator_label) > 0, error()

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
                    "namespace": "test",
                    "name": "test-017-multi-version",
                    "labels": {"clickhouse.altinity.com/chi": "test-017-multi-version"},
                    "annotations": {"clickhouse.altinity.com/email": "myname@mydomain.com, yourname@yourdoman.com"},
                    "clusters": [
                        {
                            "name": "default",
                            "hosts": [
                                {
                                    "name": "0-0",
                                    "hostname": "chi-test-017-multi-version-default-0-0.test.svc.cluster.local.",
                                    "tcpPort": 9000,
                                    "httpPort": 8123
                                },
                                {
                                    "name": "1-0",
                                    "hostname": "chi-test-017-multi-version-default-1-0.test.svc.cluster.local.",
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


@TestScenario
@Name("Metrics-exporter restarts and reloads on ClickHouseOperatorConfiguration change")
def test_metrics_exporter_chopconf_restart(self):
    """The metrics-exporter sibling container must restart (and thus reload the
    merged chopconf) when a ClickHouseOperatorConfiguration changes — not only
    the clickhouse-operator container. Before the fix only the operator
    container os.Exit'd, leaving the exporter on its stale startup config until
    the whole pod was recreated.

    Discriminator: the metrics-exporter container's OWN restartCount must
    increment. The pod-TOTAL restart count is insufficient — it moves pre-fix
    too because the operator container always restarts. Gated by the shipped
    watch.configuration.onChange=restart default; the benign
    test-033-auto-restart chopconf (timeout only, no FIPS) avoids any crashloop.
    """
    operator_namespace = self.context.operator_namespace
    chopconf_file = "manifests/chopconf/test-033-auto-restart.yaml"
    container = "metrics-exporter"

    def wait_exporter_restart(pod, baseline, reason, timeout=180):
        with Then(f"the {container} container restartCount must increment after chopconf {reason}"):
            start = time.time()
            current = baseline
            while time.time() - start < timeout:
                current = kubectl.get_container_restart_count(pod, container, ns=operator_namespace)
                if current is not None and current > baseline:
                    break
                time.sleep(2)
            assert current is not None and current > baseline, error(
                f"{container} restartCount did not increment within {timeout}s after chopconf {reason} "
                f"(baseline={baseline}, current={current}); pre-fix the sibling kept stale config"
            )
            return current

    def wait_both_ready():
        kubectl.wait_field(
            "pods", util.operator_label,
            ".status.containerStatuses[*].ready", "true,true",
            ns=operator_namespace,
        )

    with Given("clickhouse-operator is installed and both containers are ready"):
        wait_both_ready()
        out = kubectl.launch("get pods -l app=clickhouse-operator", ns=operator_namespace).splitlines()[1]
        operator_pod = re.split(r"[\t\r\n\s]+", out)[0]

    with And(f"baseline {container} restartCount"):
        baseline = kubectl.get_container_restart_count(operator_pod, container, ns=operator_namespace)
        assert baseline is not None, error(f"{container} container not found in pod {operator_pod}")

    try:
        with When(f"I apply {chopconf_file} (chopconf added)"):
            kubectl.apply(util.get_full_path(chopconf_file, lookup_in_host=False), operator_namespace)
            after_add = wait_exporter_restart(operator_pod, baseline, "add")

        with And("the exporter recovers ready and serves /metrics again (reloaded, not crashlooping)"):
            wait_both_ready()
            metrics = util.get_metrics(operator_pod, operator_namespace)
            assert metrics, error("metrics-exporter served empty /metrics after reload — not healthy")

        with When(f"I delete {chopconf_file} (chopconf deleted)"):
            kubectl.delete(util.get_full_path(chopconf_file, lookup_in_host=False), operator_namespace)
            # The DeleteFunc handler must also restart the exporter.
            wait_exporter_restart(operator_pod, after_add, "delete")
            wait_both_ready()
    finally:
        # Leave the operator in the shipped-default state for any later run.
        kubectl.launch(
            f"delete -f {util.get_full_path(chopconf_file, lookup_in_host=False)}",
            ns=operator_namespace, ok_to_fail=True,
        )


@TestFeature
@Name("e2e.test_metrics_exporter")
def test(self):
    with Given("set settings"):
        set_settings()
        self.context.test_namespace = "test"
        self.context.operator_namespace = "test"
        # Metrics-exporter functional suite is not a FIPS test — its FIPS posture is
        # covered by test_acvp. Skip the global GODEBUG=fips140=only override so the
        # operator/exporter deployment is not forced into strict FIPS mode here.
        self.context.skip_fips = True
    with Given("I create shell"):
        shell = get_shell()
        self.context.shell = shell

    util.clean_namespace(delete_chi=True)
    util.install_operator_if_not_exist()
    test_cases = [
        test_metrics_exporter_setup,
        test_metrics_exporter_chi,
        # Last: mutates the shared operator (chopconf apply/delete + container
        # restarts), so it runs after the monitoring scenarios and restores
        # baseline in its finally block.
        test_metrics_exporter_chopconf_restart,
    ]
    try:
        for t in test_cases:
            Scenario(test=t)()
    finally:
        # Tear down the fixed `test` namespace (operator + any leftover CHIs)
        # on the way out. The scenarios here use a SHARED fixed namespace with
        # no per-test Finally, so without this the namespace + installed
        # operator leak after the suite finishes (clean_namespace above only
        # clears the PREVIOUS run's residue at start). Runs on success and
        # failure; honored NO_CLEANUP keeps the namespace for debugging.
        with Finally("Delete the test namespace"):
            if settings.no_cleanup:
                print(f"NO_CLEANUP is set, skipping namespace deletion: {self.context.test_namespace}")
            else:
                util.delete_namespace(namespace=self.context.test_namespace, delete_chi=True)
