import os
import yaml
import pathlib


def get_ch_version(test_file):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return yaml.safe_load(
        open(os.path.join(current_dir, test_file), "r")
    )["spec"]["templates"]["podTemplates"][0]["spec"]["containers"][0]["image"]


# kubectl_cmd="minikube kubectl --"
kubectl_cmd = "kubectl"
test_namespace = "test"

# Default value
operator_version = os.getenv('OPERATOR_VERSION') if 'OPERATOR_VERSION' in os.environ else \
    open(os.path.join(pathlib.Path(__file__).parent.absolute(), "../release")).read(1024)
# operator_version = "0.11.0"
operator_namespace = os.getenv('OPERATOR_NAMESPACE') if 'OPERATOR_NAMESPACE' in os.environ else \
    'kube-system'
operator_docker_repo = os.getenv('OPERATOR_DOCKER_REPO') if 'OPERATOR_DOCKER_REPO' in os.environ else \
    "altinity/clickhouse-operator"
metrics_exporter_docker_repo = "altinity/metrics-exporter"

clickhouse_template = "templates/tpl-clickhouse-stable.yaml"
# clickhouse_template = "templates/tpl-clickhouse-19.11.yaml"
# clickhouse_template = "templates/tpl-clickhouse-20.1.yaml"
# clickhouse_template = "templates/tpl-clickhouse-20.3.yaml"
# clickhouse_template = "templates/tpl-clickhouse-20.4.yaml"

clickhouse_version = get_ch_version(clickhouse_template)

prometheus_namespace = "prometheus"
prometheus_operator_version = "0.42"
