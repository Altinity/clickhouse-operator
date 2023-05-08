import os
import yaml
import inspect
import pathlib
from testflows.core import current


def get_ch_version(test_file):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return yaml.safe_load(open(os.path.join(current_dir, test_file), "r"))["spec"]["templates"]["podTemplates"][0][
        "spec"
    ]["containers"][0]["image"]


def get_docker_compose_path():
    caller_dir = os.path.dirname(os.path.abspath(inspect.currentframe().f_back.f_globals["__file__"]))
    docker_compose_project_dir = os.path.join(caller_dir, "../docker-compose")
    docker_compose_file_path = os.path.join(docker_compose_project_dir, "docker-compose.yml")
    return docker_compose_file_path, docker_compose_project_dir


# apply | replace
kubectl_mode = os.getenv("KUBECTL_MODE") if "KUBECTL_MODE" in os.environ else "apply"

kubectl_cmd = (
    "kubectl"
    if current().context.native
    else f"docker-compose -f {get_docker_compose_path()[0]} exec -T runner kubectl"
)

kubectl_cmd = os.getenv("KUBECTL_CMD") if "KUBECTL_CMD" in os.environ else kubectl_cmd

test_namespace = os.getenv("TEST_NAMESPACE") if "TEST_NAMESPACE" in os.environ else "test"
operator_version = (
    os.getenv("OPERATOR_VERSION")
    if "OPERATOR_VERSION" in os.environ
    else open(os.path.join(pathlib.Path(__file__).parent.absolute(), "../../release")).read(1024).strip(" \r\n\t")
)
operator_namespace = os.getenv("OPERATOR_NAMESPACE") if "OPERATOR_NAMESPACE" in os.environ else test_namespace
operator_install = os.getenv("OPERATOR_INSTALL") if "OPERATOR_INSTALL" in os.environ else "yes"
minio_namespace = os.getenv("MINIO_NAMESPACE") if "MINIO_NAMESPACE" in os.environ else "minio"
operator_docker_repo = (
    os.getenv("OPERATOR_DOCKER_REPO") if "OPERATOR_DOCKER_REPO" in os.environ else "altinity/clickhouse-operator"
)
metrics_exporter_docker_repo = (
    os.getenv("METRICS_EXPORTER_DOCKER_REPO")
    if "METRICS_EXPORTER_DOCKER_REPO" in os.environ
    else "altinity/metrics-exporter"
)
clickhouse_operator_install_manifest = (
    os.getenv("CLICKHOUSE_OPERATOR_INSTALL_MANIFEST")
    if "CLICKHOUSE_OPERATOR_INSTALL_MANIFEST" in os.environ
    else "../../deploy/operator/clickhouse-operator-install-template.yaml"
)
image_pull_policy = os.getenv("IMAGE_PULL_POLICY") if "IMAGE_PULL_POLICY" in os.environ else "Always"
clickhouse_template = (
    os.getenv("CLICKHOUSE_TEMPLATE")
    if "CLICKHOUSE_TEMPLATE" in os.environ
    else
    # "manifests/chit/tpl-clickhouse-stable.yaml"
    # "manifests/chit/tpl-clickhouse-19.17.yaml"
    # "manifests/chit/tpl-clickhouse-20.3.yaml"
    # "manifests/chit/tpl-clickhouse-20.8.yaml"
    # "manifests/chit/tpl-clickhouse-21.3.yaml"
    "manifests/chit/tpl-clickhouse-21.8.yaml"
    # "manifests/chit/tpl-clickhouse-22.3.yaml"
    # "manifests/chit/tpl-clickhouse-22.8.yaml"
)
clickhouse_template_old = "manifests/chit/tpl-clickhouse-22.3.yaml"

clickhouse_version = get_ch_version(clickhouse_template)
clickhouse_version_old = get_ch_version(clickhouse_template_old)

prometheus_namespace = "prometheus"
prometheus_operator_version = "0.62"
prometheus_scrape_interval = 10

minio_version = "latest"
