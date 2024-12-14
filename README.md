# Altinity Kubernetes Operator for ClickHouse®

Altinity Kubernetes Operator for ClickHouse creates, configures and manages ClickHouse clusters running on Kubernetes.

[![Build Master](https://github.com/Altinity/clickhouse-operator/actions/workflows/build_master.yaml/badge.svg)](https://github.com/Altinity/clickhouse-operator/actions/workflows/build_master.yaml)
[![GitHub release](https://img.shields.io/github/v/release/altinity/clickhouse-operator?include_prereleases)](https://img.shields.io/github/v/release/altinity/clickhouse-operator?include_prereleases)
[![tags](https://img.shields.io/github/tag/altinity/clickhouse-operator.svg)](https://github.com/altinity/clickhouse-operator/tags)
[![Docker Pulls](https://img.shields.io/docker/pulls/altinity/clickhouse-operator.svg)](https://hub.docker.com/r/altinity/clickhouse-operator)
[![Go version](https://img.shields.io/github/go-mod/go-version/altinity/clickhouse-operator)](https://img.shields.io/github/go-mod/go-version/altinity/clickhouse-operator)
[![Go Report Card](https://goreportcard.com/badge/github.com/altinity/clickhouse-operator)](https://goreportcard.com/report/github.com/altinity/clickhouse-operator)
[![issues](https://img.shields.io/github/issues/altinity/clickhouse-operator.svg)](https://github.com/altinity/clickhouse-operator/issues)
<a href="https://altinity.com/slack">
  <img src="https://img.shields.io/static/v1?logo=slack&logoColor=959DA5&label=Slack&labelColor=333a41&message=join%20conversation&color=3AC358" alt="AltinityDB Slack" />
</a>

## Features

- Creates ClickHouse clusters defined as custom resources
- Customized storage provisioning (VolumeClaim templates)
- Customized pod templates
- Customized service templates for endpoints
- ClickHouse configuration management
- ClickHouse users management
- ClickHouse cluster scaling including automatic schema propagation
- ClickHouse version upgrades
- Exporting ClickHouse metrics to Prometheus

## Requirements

 * Kubernetes 1.19+
 * ClickHouse 21.11+. For older ClickHouse versions use operator 0.23.7 or earlier.
 
## Documentation

[Quick Start Guide][quick_start_guide]

**Advanced configuration**
 * [Detailed Operator Installation Instructions][detailed_installation_instructions]
   * [Operator Configuration][operator_configuration]
 * [Setup ClickHouse cluster with replication][replication_setup]
   * [Setting up Zookeeper][zookeeper_setup]
 * [Persistent Storage Configuration][storage_configuration]
 * [Security Hardening][security_hardening]
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
 * [How to easy development process with devspace.sh][devspace_manual]
 
---
 * [Documentation index][all_docs_list]
---
 
## License

Copyright (c) 2019-2023, Altinity Inc and/or its affiliates. All rights reserved.

Altinity Kubernetes Operator for ClickHouse is licensed under the Apache License 2.0.

See [LICENSE](./LICENSE) for more details.

## Commercial Support

Altinity is the primary maintainer of the operator. It is the basis of Altinity.Cloud and
is also used in self-managed installations. Altinity offers a range of 
services related to ClickHouse and analytic applications on Kubernetes. 

- [Official website](https://altinity.com/) - Get a high level overview of Altinity and our offerings.
- [Altinity.Cloud](https://altinity.com/cloud-database/) - Run ClickHouse in our cloud or yours.
- [Altinity Support](https://altinity.com/support/) - Get Enterprise-class support for ClickHouse.
- [Slack](https://altinity.com/slack) - Talk directly with ClickHouse users and Altinity devs.
- [Contact us](https://hubs.la/Q020sH3Z0) - Contact Altinity with your questions or issues.
- [Free consultation](https://hubs.la/Q020sHkv0) - Get a free consultation with a ClickHouse expert today.
 
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
[contributing_manual]: ./CONTRIBUTING.md
[devspace_manual]: ./docs/devspace.md
[all_docs_list]: ./docs/README.md
[security_hardening]: ./docs/security_hardening.md
