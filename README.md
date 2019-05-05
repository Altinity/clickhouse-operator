# ClickHouse Operator

The ClickHouse Operator creates, configures and manages ClickHouse clusters running on Kubernetes.

[![issues](https://img.shields.io/github/issues/altinity/clickhouse-operator.svg)](https://github.com/altinity/clickhouse-operator/issues)
[![tags](https://img.shields.io/github/tag/altinity/clickhouse-operator.svg)](https://github.com/altinity/clickhouse-operator/tags)
[![Go Report Card](https://goreportcard.com/badge/github.com/altinity/clickhouse-operator)](https://goreportcard.com/report/github.com/altinity/clickhouse-operator)

**Warning! 
ClickHouse Operator is in beta. You can use it at your own risk. There may be backwards incompatible API changes up until the first major release.**

## Features

The ClickHouse Operator for Kubernetes currently provides the following:

- Creates ClickHouse cluster based on Custom Resource [specification][crd_spec] provided
- Storage customization (VolumeClaim templates)
- Pod template customization (Volume and Container templates)
- ClickHouse configuration customization (including Zookeeper integration)
- ClickHouse cluster scaling including automatic schema propagation
- ClickHouse cluster version upgrades
- Exporting ClickHouse metrics to Prometheus

## Requirements

 * Kubernetes 1.11.9+
 
## Documentation

[Quick Start Guide][quick_start]

**Advanced setups**
 * [Detailed Operator Installation Instructions][detailed]
   * [Operator Configuration][operator_configuration]
 * [Setup ClickHouse cluster with replication][replication_setup]
   * [Setting up Zookeeper][zookeeper_setup]
 * [Persistent Storage Configuration][storage]
 * [ClickHouse Installation Custom Resource specification][crd_explained]
 
**Maintenance tasks**
 * [Adding replication to an existing ClickHouse cluster][update_cluster_add_replication]
 * Adding shards and replicas
   * [Automatic schema creation][schema_migration]
 * [Update ClickHouse version][update_clickhouse_version]
 * [Update Operator version][update_operator]

**Monitoring**
 * [Prometheus & clickhouse-operator integration][prometheus_setup]
 * [Grafana & Prometheus integration][grafana_setup]

**All docs**
 * [All available docs list][all_docs_list]
## License

Copyright (c) 2019, Altinity Ltd and/or its affiliates. All rights reserved.

`clickhouse-operator` is licensed under the Apache License 2.0.

See [LICENSE](./LICENSE) for more details.
 
[crd_spec]: ./docs/examples/clickhouseinstallation-object.yaml
[intro]: ./docs/introduction.md
[quick_start]: ./docs/quick-start.md
[detailed]: ./docs/operator_installation_details.md
[replication_setup]: ./docs/replication_setup.md
[crd_explained]: ./docs/custom_resource_explained.md
[zookeeper_setup]: ./docs/zookeeper_setup.md
[prometheus_setup]: ./docs/prometheus_setup.md
[grafana_setup]: ./docs/grafana_setup.md
[storage]: ./docs/storage.md
[update_cluster_add_replication]: ./docs/chi_update_add_replication.md
[update_clickhouse_version]: ./docs/chi_update_clickhouse_version.md
[update_operator]: ./docs/operator_upgrade.md
[schema_migration]: ./docs/schema_migration.md
[operator_configuration]: ./docs/operator_configuration.md
[all_docs_list]: ./docs/README.md
