import kubectl

version = "0.9.4"
test_namespace = "test"
clickhouse_template = "templates/tpl-clickhouse-stable.yaml"
# clickhouse_template = "templates/tpl-clickhouse-19.11.yaml"
# clickhouse_template = "templates/tpl-clickhouse-20.1.yaml"

clickhouse_version = kubectl.get_ch_version(clickhouse_template)
