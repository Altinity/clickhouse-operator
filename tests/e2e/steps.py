from testflows.core import *
from testflows.connect import Shell
from testflows.core.name import basename
import e2e.util as util
import uuid
import os
import re
import shlex
import yaml
import time
import inspect
import pathlib
import sys
from testflows.core import current
from testflows.asserts import error

import e2e.kubectl as kubectl
import e2e.settings as settings


def _test_run_failed(test=None):
    """True when the current scenario (or a sibling step) already failed."""
    if sys.exc_info()[0] is not None:
        return True

    node = test if test is not None else current()
    while node is not None:
        result = getattr(node, "result", None)
        if isinstance(result, (Fail, Error)):
            return True
        parent = getattr(node, "parent", None)
        if parent is not None:
            for subtest in getattr(parent, "subtests", {}).values():
                subtest_result = getattr(subtest, "result", None)
                if isinstance(subtest_result, (Fail, Error)):
                    return True
        node = parent
    return False


def _dump_failed_test_namespace(test, ns, shell):
    """Best-effort cluster snapshot before namespace teardown."""
    operator_ns = getattr(test.context, "operator_namespace", None) or ns
    print(f"\n=== DEBUG DUMP (test failed) namespace={ns} ===")

    for kind in ("pods", "chi", "chk"):
        try:
            print(f"\n--- {kind.upper()} ---")
            items = kubectl.launch(f"get {kind}", ns=ns, ok_to_fail=True, shell=shell)
            print(items or f"(no {kind})")
        except Exception as exc:
            print(f"failed to list {kind}: {exc}")

    try:
        print("\n--- Operator log (last 10 lines) ---")
        operator_pod = kubectl.get_operator_pod(ns=operator_ns, shell=shell)
        if not operator_pod:
            print(f"(operator pod not found in namespace {operator_ns})")
        else:
            logs = kubectl.launch(
                f"logs {operator_pod} -c clickhouse-operator --tail=10",
                ns=operator_ns,
                ok_to_fail=True,
                shell=shell,
            )
            print(logs or "(empty operator log)")
    except Exception as exc:
        print(f"failed to fetch operator logs: {exc}")

    print("=== END DEBUG DUMP ===\n")


@TestStep(Given)
def get_shell(self, timeout=600):
    """Create shell terminal."""
    try:
        shell = Shell()
        shell.timeout = timeout
        yield shell
    finally:
        shell.close()


@TestStep(Given)
def create_shell(self):
    """Create shell only, without a namespace."""
    # For host-only scenarios, which need a shell but no cluster. Without their own
    # they fall back to the module-level shell created once in test(), which every
    # scenario in the parallel pool shares. A testflows Shell drives a single pty and
    # does not lock it, so two scenarios sending commands at the same time consume
    # each other's prompts and both park in expect() with no way out.
    shell = get_shell()
    self.context.shell = shell


@TestStep(Given)
def create_test_namespace(self, force=False):
    """Create unique test namespace for test."""

    random_namespace = self.name[self.name.find('test_0'):self.name.find('. ')].replace("_", "-") + "-" + str(uuid.uuid1())

    if not force: # (self.cflags & PARALLEL) and not force:
        self.context.test_namespace = random_namespace

    self.context.operator_namespace = self.context.test_namespace
    util.create_namespace(self.context.test_namespace)
    util.install_operator_if_not_exist()

    return self.context.test_namespace


@TestStep(Finally)
def delete_test_namespace(self):
    # Tolerate a context that never reached the point of setting test_namespace
    # (e.g. shell creation failed before create_test_namespace ran). Tests that
    # wrap their body in Python try/finally for retry-safety call this even on
    # early setup failures, so a missing attribute must not mask the original error.
    ns = getattr(self.context, "test_namespace", None)
    if not ns:
        print("No test_namespace recorded on context; skipping namespace deletion")
        return
    if settings.no_cleanup:
        print(f"NO_CLEANUP is set, skipping namespace deletion: {ns}")
        return
    shell = get_shell()
    self.context.shell = shell
    if _test_run_failed(self):
        _dump_failed_test_namespace(self, ns, shell)
    util.delete_namespace(namespace=ns, delete_chi=True)
    shell.close()


@TestStep(Given)
def get_ch_version(self, test_file):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return yaml.safe_load(open(os.path.join(current_dir, test_file), "r"))["spec"]["templates"]["podTemplates"][0][
        "spec"
    ]["containers"][0]["image"]


@TestStep(Given)
def get_docker_compose_path(self):
    caller_dir = os.path.dirname(os.path.abspath(inspect.currentframe().f_back.f_globals["__file__"]))
    docker_compose_project_dir = os.path.join(caller_dir, "../docker-compose")
    docker_compose_file_path = os.path.join(docker_compose_project_dir, "docker-compose.yml")
    return docker_compose_file_path, docker_compose_project_dir


@TestStep(Given)
def set_settings(self):
    """Set settings inside test context."""
    # apply | replace
    self.context.kubectl_mode = define("kubectl_mode", os.getenv("KUBECTL_MODE") if "KUBECTL_MODE" in os.environ else "apply")

    self.context.kubectl_cmd = (
        "kubectl"
        if current().context.native
        else f"docker-compose -f {get_docker_compose_path()[0]} exec -T runner kubectl"
    )

    self.context.kubectl_cmd = define("kubectl_cmd", os.getenv("KUBECTL_CMD") if "KUBECTL_CMD" in os.environ else self.context.kubectl_cmd)

    # Dual-cluster e2e: extract the --context / --kubeconfig flags from kubectl_cmd so
    # direct subprocess kubectl calls (e.g. the port-forward helpers in steps_fips.py)
    # hit the SAME cluster as kubectl.launch(). Empty list for single-cluster runs.
    # Only the --flag=value form is recognized (the dual wrapper emits exactly that);
    # space-separated "--context foo" would drop the value and is not used.
    self.context.kubectl_context_args = [
        arg for arg in shlex.split(self.context.kubectl_cmd)
        if arg.startswith(("--context=", "--kubeconfig="))
    ]
    # minikube profile for direct `minikube` invocations (e.g. decoy image load).
    self.context.minikube_profile = define(
        "minikube_profile", os.getenv("MINIKUBE_PROFILE") if "MINIKUBE_PROFILE" in os.environ else "minikube"
    )
    # Direct-subprocess kube calls invoke the `kubectl` binary natively, so they can
    # only carry --context/--kubeconfig when the suite runs --native. A dual-cluster
    # run (context flags present) via the docker-compose runner path cannot route them.
    if self.context.kubectl_context_args and not current().context.native:
        raise ValueError(
            "KUBECTL_CMD carries --context/--kubeconfig but the suite is not --native; "
            "dual-cluster runs require --native so port-forward/minikube calls reach the right cluster"
        )

    self.context.test_namespace = define("test_namespace", os.getenv("TEST_NAMESPACE") if "TEST_NAMESPACE" in os.environ else "test")
    self.context.operator_version = define("operator_version", (
        os.getenv("OPERATOR_VERSION")
        if "OPERATOR_VERSION" in os.environ
        else open(os.path.join(pathlib.Path(__file__).parent.absolute(), "../../release")).read(1024).strip(" \r\n\t")
    ))
    # release_version is the version baked into the binaries at build time
    # (dev/go_build_universal.sh ldflags -X pkg/version.Version from the `release`
    # file), which is what `--fips-info` reports. It is intentionally NOT the
    # OPERATOR_VERSION env / image tag (which can be "dev"): the tag is a
    # deploy-time label, the baked version is a build-time fact. Read the same
    # `release` file the build reads so the two always track on a release bump.
    self.context.release_version = define("release_version", open(os.path.join(pathlib.Path(__file__).parent.absolute(), "../../release")).read(1024).strip(" \r\n\t"))
    self.context.operator_namespace = define("operator_namespace", os.getenv("OPERATOR_NAMESPACE") if "OPERATOR_NAMESPACE" in os.environ else self.context.test_namespace)
    self.context.operator_install = define("operator_install", os.getenv("OPERATOR_INSTALL") if "OPERATOR_INSTALL" in os.environ else "yes")
    self.context.minio_namespace = define("minio_namespace", os.getenv("MINIO_NAMESPACE") if "MINIO_NAMESPACE" in os.environ else "minio")
    self.context.operator_docker_repo = define("operator_docker_repo", (
        os.getenv("OPERATOR_DOCKER_REPO") if "OPERATOR_DOCKER_REPO" in os.environ else "altinity/clickhouse-operator"
    ))
    self.context.metrics_exporter_docker_repo = define("metrics_exporter_docker_repo", (
        os.getenv("METRICS_EXPORTER_DOCKER_REPO")
        if "METRICS_EXPORTER_DOCKER_REPO" in os.environ
        else "altinity/metrics-exporter"
    ))
    self.context.clickhouse_operator_install_manifest = define("clickhouse_operator_install_manifest", (
        os.getenv("CLICKHOUSE_OPERATOR_INSTALL_MANIFEST")
        if "CLICKHOUSE_OPERATOR_INSTALL_MANIFEST" in os.environ
        else "../../deploy/operator/clickhouse-operator-install-template.yaml"
    ))
    self.context.image_pull_policy = define("image_pull_policy", os.getenv("IMAGE_PULL_POLICY") if "IMAGE_PULL_POLICY" in os.environ else "Always")

    # self.context.clickhouse_template = "manifests/chit/tpl-clickhouse-stable.yaml"
    # self.context.clickhouse_template = "manifests/chit/tpl-clickhouse-23.3.yaml"
    # self.context.clickhouse_template = "manifests/chit/tpl-clickhouse-23.8.yaml"
    self.context.clickhouse_template = define("clickhouse_template",  os.getenv("CLICKHOUSE_TEMPLATE") if "CLICKHOUSE_TEMPLATE" in os.environ else "manifests/chit/tpl-clickhouse-stable.yaml")

    self.context.clickhouse_version = define("clickhouse_version", get_ch_version(test_file=self.context.clickhouse_template))

    self.context.prometheus_namespace = define("prometheus_namespace", "prometheus")
    self.context.prometheus_operator_version = define("prometheus_operator_version", "0.68")
    self.context.prometheus_scrape_interval = define("prometheus_scrape_interval", 10)

    self.context.keeper_type = define("keeper_type", os.getenv("KEEPER_TYPE") if "KEEPER_TYPE" in os.environ else "zookeeper") # zookeeper | clickhouse_keeper

    self.context.minio_version = define("minio_version", "latest")


@TestStep(Given)
def create_shell_namespace_clickhouse_template(self):
    """Create shell, namespace and install ClickHouse template."""
    with Given("I create shell"):
        shell = get_shell()
        self.context.shell = shell

    with And("I create test namespace"):
        create_test_namespace()
        # Register namespace cleanup with TestFlows' context.cleanup hook so it
        # runs in the scenario's __exit__ (inside a `with Finally("I clean up")`
        # frame TestFlows owns) regardless of how the test body returns —
        # success, AssertionError, retry, anything. This replaces the leaky
        # trailing `with Finally(...): delete_test_namespace()` pattern, which
        # is unreachable on mid-test exception and was the root cause of
        # leaked namespaces piling up across retried tests (010036, 010023, …).
        # delete_test_namespace is idempotent (kubectl ok_to_fail=True), so it
        # safely co-exists with the trailing-Finally blocks already in many
        # tests until they're cleaned up.
        current().context.cleanup(delete_test_namespace)

    with And(f"Install ClickHouse template {current().context.clickhouse_template}"):
        kubectl.apply(
            util.get_full_path(current().context.clickhouse_template, lookup_in_host=False),
        )


@TestStep(Then)
def check_metrics_monitoring(
        self,
        operator_namespace,
        operator_pod,
        expect_pattern="",
        expect_metric="",
        expect_labels="",
        container="metrics-exporter",
        port="8888",
        max_retries=7
):
    with Then(f"metrics-exporter /metrics endpoint result should contain {expect_pattern}{expect_metric}"):
        expected_pattern_found = False
        for i in range(1, max_retries):
            out = util.get_metrics(operator_pod, operator_namespace, container=container, port=port)
            if expect_metric != "":
                lines = [m for m in out.splitlines() if m.startswith(expect_metric) and expect_labels in m]
                if len(lines) > 0:
                    metric = lines[0]
                    print(f"have: {metric}")
                    assert expect_labels in metric, error(metric)
                    return

            if expect_pattern != "":
                rx = re.compile(expect_pattern, re.MULTILINE)
                matches = rx.findall(out)

                if matches:
                    expected_pattern_found = True
                    break

                with Then("Not ready. Wait for " + str(i * 5) + " seconds"):
                    time.sleep(i * 5)

        assert expected_pattern_found, error(out)
