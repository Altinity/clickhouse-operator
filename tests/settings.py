import kubectl

version = '0.9.2'
test_namespace = "test"
# clickhouse_template = "templates/tpl-clickhouse-stable.yaml"
# clickhouse_template = "templates/tpl-clickhouse-19.6.2.11.yaml"
clickhouse_template = "templates/tpl-clickhouse-20.1.4.14.yaml"

clickhouse_version = kubectl.get_ch_version(clickhouse_template)
