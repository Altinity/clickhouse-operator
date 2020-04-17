import os
import kubectl
import pathlib

version = open(os.path.join(pathlib.Path(__file__).parent.absolute(),"../release")).read(1024)
# version = "0.9.7" 
test_namespace = "test"
clickhouse_template = "templates/tpl-clickhouse-stable.yaml"
# clickhouse_template = "templates/tpl-clickhouse-19.11.yaml"
# clickhouse_template = "templates/tpl-clickhouse-20.1.yaml"
# clickhouse_template = "templates/tpl-clickhouse-20.3.yaml"

clickhouse_version = kubectl.get_ch_version(clickhouse_template)
