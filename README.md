# ClickHouse Operator

The ClickHouse Operator creates, configures and manages ClickHouse clusters running on Kubernetes.

[![issues](https://img.shields.io/github/issues/altinity/clickhouse-operator.svg)](https://github.com/altinity/clickhouse-operator/issues)
[![tags](https://img.shields.io/github/tag/altinity/clickhouse-operator.svg)](https://github.com/altinity/clickhouse-operator/tags)
[![Go Report Card](https://goreportcard.com/badge/github.com/altinity/clickhouse-operator)](https://goreportcard.com/report/github.com/altinity/clickhouse-operator)

**Warning! ClickHouse Operator is currently in early minor beta state. 
Its functionality is very limited, as well as its API is in active development, so please keep in mind, that you are fully responsible for your data and the operation of your database clusters. 
There may be backwards incompatible changes up until the first major release.**

## Features

The ClickHouse Operator for Kubernetes currently provides the following:

- Creates cluster of the ClickHouse database based on Custom Resource [specification][crd_spec] provided
- Supports Storage customization (VolumeClaim templates)
- Supports Pod template customization (Volume and Container templates)
- Supports ClickHouse configuration customization (including Zookeeper integration)
- Supports ClickHouse metrics export to Prometheus

## Requirements

 * Kubernetes 1.9.0 +
 
## Documentation

**Start**
1. [Introduction][intro] 
1. [Quick Start Guides][quick_start]
1. [More detailed operator installation instructions][detailed]
1. [ClickHouse Installation Custom Resource explained][crd_explained]

**Replication cluster**
1. [How to setup ClickHouse cluster with replication][replication_setup]
1. [Zookeeper setup][zookeeper_setup]

**Monitoring**
1. [Prometheus & clickhouse-operator integration][prometheus_setup]
1. [Grafana & Prometheus integration][grafana_setup]

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
