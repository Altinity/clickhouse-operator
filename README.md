# ClickHouse Operator

The ClickHouse Operator creates, configures and manages ClickHouse clusters running on Kubernetes.

[![issues](https://img.shields.io/github/issues/altinity/clickhouse-operator.svg)](https://github.com/altinity/clickhouse-operator/issues)
[![tags](https://img.shields.io/github/tag/altinity/clickhouse-operator.svg)](https://github.com/altinity/clickhouse-operator/tags)
[![Go Report Card](https://goreportcard.com/badge/github.com/altinity/clickhouse-operator)](https://goreportcard.com/report/github.com/altinity/clickhouse-operator)

**Warning! ClickHouse Operator is currently in early minor beta state. Its functionality is very limited, as well as its API in active development, so please keep in mind, that you are fully responsible for your data and the operation of your database clusters. There may be backwards incompatible changes up until the first major release.**

## Features

The ClickHouse Operator for Kubernetes currently provides the following:

- Creates cluster of the ClickHouse database based on Custom Resource [specification][1] provided
- Supports Storage customization (VolumeClaim templates)
- Supports Pod template customization (Volume and Container templates)
- Supports ClickHouse configuration customization (including Zookeeper integration)

## Requirements

 * Kubernetes 1.9.0 +
 
## Installation

```console
$ kubectl apply -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/manifests/operator/clickhouse-operator-install.yaml
serviceaccount/clickhouse-operator created
clusterrolebinding.rbac.authorization.k8s.io/clickhouse-operator created
deployment.apps/clickhouse-operator created

```

```console
$ kubectl get pods -n kube-system
NAME                                        READY   STATUS    RESTARTS   AGE
clickhouse-operator-ddc6fd499-fhxqs         1/1     Running   0          5m22s
```

## Documentation

1. [Introduction][2] 
1. [Quick Start Guides][3]
1. [More detailed operator installation instructions][4]
1. [How to setup ClickHouse cluster with replication][5]
1. [Zookeeper setup][6]
1. [ClickHouse Installation Custom Resource explained][7]

## License

Copyright (c) 2019, Altinity Ltd and/or its affiliates. All rights reserved.

`clickhouse-operator` is licensed under the Apache License 2.0.

See [LICENSE](LICENSE) for more details.
 
[1]: docs/examples/clickhouseinstallation-object.yaml
[2]: docs/introduction.md
[3]: docs/quick-start.md
[4]: docs/operator_installation_details.md
[5]: docs/replication_setup.md
[6]: docs/zookeeper_setup.md
[7]: docs/custom_resource_explained.md
