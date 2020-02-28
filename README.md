# ClickHouse Operator

ClickHouse Operator creates, configures and manages ClickHouse clusters running on Kubernetes.

**IMPORTANT: If you upgrade operator from 0.6.0 or earlier to 0.7.0 or later releases please make sure your ClickHouseInstallation name is shorter than 15 symbols. Otherwise DO NOT UPGRADE an operator. Please contact support@altinity.com if you have upgrade issues**

[![issues](https://img.shields.io/github/issues/altinity/clickhouse-operator.svg)](https://github.com/altinity/clickhouse-operator/issues)
[![tags](https://img.shields.io/github/tag/altinity/clickhouse-operator.svg)](https://github.com/altinity/clickhouse-operator/tags)
[![Go Report Card](https://goreportcard.com/badge/github.com/altinity/clickhouse-operator)](https://goreportcard.com/report/github.com/altinity/clickhouse-operator)

## Features

The ClickHouse Operator for Kubernetes currently provides the following:

- Creates ClickHouse clusters based on Custom Resource [specification][chi_max_yaml] provided
- Customized storage provisioning (VolumeClaim templates)
- Customized pod templates
- Customized service templates for endpoints
- ClickHouse configuration and settings (including Zookeeper integration)
- Flexible templating
- ClickHouse cluster scaling including automatic schema propagation
- ClickHouse version upgrades
- Exporting ClickHouse metrics to Prometheus

## Requirements

 * Kubernetes 1.12.6+
 
## Documentation

[Quick Start Guide][quick_start_guide]

**Advanced setups**
 * [Detailed Operator Installation Instructions][detailed_installation_instructions]
   * [Operator Configuration][operator_configuration]
 * [Setup ClickHouse cluster with replication][replication_setup]
   * [Setting up Zookeeper][zookeeper_setup]
 * [Persistent Storage Configuration][storage_configuration]
 * [ClickHouse Installation Custom Resource specification][crd_explained]
 
**Maintenance tasks**
 * [Add replication to an existing ClickHouse cluster][update_cluster_add_replication]
 * [Schema maintenance][schema_migration]
 * [Update ClickHouse version][update_clickhouse_version]
 * [Update Operator version][update_operator]

**Monitoring**
 * [Setup Monitoring][monitoring_setup]
 * [Prometheus & clickhouse-operator integration][prometheus_setup]
 * [Grafana & Prometheus integration][grafana_setup]

**How to contribute**
 * [How to contribute/submit a patch][contributing_manual]
 
---
**All docs**
 * [All available docs list][all_docs_list]
---
 
## License

Copyright (c) 2019-2020, Altinity Ltd and/or its affiliates. All rights reserved.

`clickhouse-operator` is licensed under the Apache License 2.0.

See [LICENSE](./LICENSE) for more details.
 
[chi_max_yaml]: ./docs/chi-examples/99-clickhouseinstallation-max.yaml
[intro]: ./docs/introduction.md
[quick_start_guide]: ./docs/quick_start.md
[detailed_installation_instructions]: ./docs/operator_installation_details.md
[replication_setup]: ./docs/replication_setup.md
[crd_explained]: ./docs/custom_resource_explained.md
[zookeeper_setup]: ./docs/zookeeper_setup.md
[monitoring_setup]: ./docs/monitoring_setup.md
[prometheus_setup]: ./docs/prometheus_setup.md
[grafana_setup]: ./docs/grafana_setup.md
[storage_configuration]: ./docs/storage.md
[update_cluster_add_replication]: ./docs/chi_update_add_replication.md
[update_clickhouse_version]: ./docs/chi_update_clickhouse_version.md
[update_operator]: ./docs/operator_upgrade.md
[schema_migration]: ./docs/schema_migration.md
[operator_configuration]: ./docs/operator_configuration.md
[all_docs_list]: ./docs/README.md
[contributing_manual]: ./CONTRIBUTING.md
