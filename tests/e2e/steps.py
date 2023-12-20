from testflows.core import *
from testflows.connect import Shell
from testflows.core.name import basename
import e2e.util as util
import uuid
import os
import yaml
import inspect
import pathlib
from testflows.core import current

import e2e.kubectl as kubectl


@TestStep(Given)
def get_shell(self, timeout=600):
    """Create shell terminal."""
    try:
        shell = Shell()
        shell.timeout = timeout
        yield shell
    finally:
        with Finally("I close shell"):
            shell.close()


@TestStep(Given)
def create_test_namespace(self, force=False):
    """Create unique test namespace for test."""

    if (self.cflags & PARALLEL) and not force:
        self.context.test_namespace = self.name[self.name.find('test_0'):self.name.find('. ')].replace("_", "-") + "-" + str(uuid.uuid1())
        self.context.operator_namespace = self.context.test_namespace
        util.create_namespace(self.context.test_namespace)
        util.install_operator_if_not_exist()
        return self.context.test_namespace
    else:
        self.context.operator_namespace = self.context.test_namespace
        util.create_namespace(self.context.test_namespace)
        util.install_operator_if_not_exist()
        return self.context.test_namespace


@TestStep(Finally)
def delete_test_namespace(self):
    shell = get_shell()
    self.context.shell = shell
    util.delete_namespace(namespace=self.context.test_namespace, delete_chi=True)
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

    self.context.test_namespace = define("test_namespace", os.getenv("TEST_NAMESPACE") if "TEST_NAMESPACE" in os.environ else "test")
    self.context.operator_version = define("operator_version", (
        os.getenv("OPERATOR_VERSION")
        if "OPERATOR_VERSION" in os.environ
        else open(os.path.join(pathlib.Path(__file__).parent.absolute(), "../../release")).read(1024).strip(" \r\n\t")
    ))
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
    # self.context.clickhouse_template = "manifests/chit/tpl-clickhouse-19.17.yaml"
    # self.context.clickhouse_template = "manifests/chit/tpl-clickhouse-20.3.yaml"
    # self.context.clickhouse_template = "manifests/chit/tpl-clickhouse-20.8.yaml"
    # self.context.clickhouse_template = "manifests/chit/tpl-clickhouse-21.3.yaml"
    # self.context.clickhouse_template = "manifests/chit/tpl-clickhouse-21.8.yaml"
    # self.context.clickhouse_template = "manifests/chit/tpl-clickhouse-22.3.yaml"
    self.context.clickhouse_template = define("clickhouse_template", "manifests/chit/tpl-clickhouse-22.8.yaml")
    self.context.clickhouse_template_old = define("clickhouse_template_old", "manifests/chit/tpl-clickhouse-22.3.yaml")#todo

    self.context.clickhouse_version = define("clickhouse_version", get_ch_version(test_file=self.context.clickhouse_template))
    self.context.clickhouse_version_old = define("clickhouse_version_old", get_ch_version(test_file=self.context.clickhouse_template_old))

    self.context.prometheus_namespace = define("prometheus_namespace", "prometheus")
    self.context.prometheus_operator_version = define("prometheus_operator_version", "0.57")
    self.context.prometheus_scrape_interval = define("prometheus_scrape_interval", 10)

    self.context.minio_version = define("minio_version", "latest")


@TestStep(Given)
def create_shell_namespace_clickhouse_template(self):
    """Create shell, namespace and install ClickHouse template."""
    with Given("I create shell"):
        shell = get_shell()
        self.context.shell = shell

    with And("I create test namespace"):
        create_test_namespace()

    with And(f"Install ClickHouse template {current().context.clickhouse_template}"):
        kubectl.apply(
            util.get_full_path(current().context.clickhouse_template, lookup_in_host=False),
        )
