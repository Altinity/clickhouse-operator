import os
import yaml
import pathlib

def get_ch_version(test_file):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return yaml.safe_load(open(os.path.join(current_dir, test_file),"r"))["spec"]["templates"]["podTemplates"][0]["spec"]["containers"][0]["image"]

# kubectlcmd="minikube kubectl --"
kubectlcmd="kubectl"
test_namespace = "test"

operator_version = open(os.path.join(pathlib.Path(__file__).parent.absolute(), "../release")).read(1024)
# operator_version = "0.11.0"
operator_namespace = os.getenv('OPERATOR_NAMESPACE') if 'OPERATOR_NAMESPACE' in os.environ else 'kube-system'

clickhouse_template = "templates/tpl-clickhouse-stable.yaml"
# clickhouse_template = "templates/tpl-clickhouse-19.11.yaml"
# clickhouse_template = "templates/tpl-clickhouse-20.1.yaml"
# clickhouse_template = "templates/tpl-clickhouse-20.3.yaml"
# clickhouse_template = "templates/tpl-clickhouse-20.4.yaml"

clickhouse_version = get_ch_version(clickhouse_template)

prometheus_namespace = "prometheus"
prometheus_operator_version = "0.41"