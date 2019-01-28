# ClickHouse Operator

The ClickHouse Operator creates, configures and manages ClickHouse clusters running on Kubernetes.

[![issues](https://img.shields.io/github/issues/altinity/clickhouse-operator.svg)](https://github.com/altinity/clickhouse-operator/issues)
[![tags](https://img.shields.io/github/tag/altinity/clickhouse-operator.svg)](https://github.com/altinity/clickhouse-operator/tags)
[![Go Report Card](https://goreportcard.com/badge/github.com/altinity/clickhouse-operator)](https://goreportcard.com/report/github.com/altinity/clickhouse-operator)

**Warning! ClickHouse Operator is currently in early minor beta state. Its functionality is very limited, as well as its API in active development, so please keep in mind, that you are fully responsible for your data and the operation of your database clusters. There may be backwards incompatible changes up until the first major release.**

## Features

The ClickHouse Operator for Kubernetes currently provides the following:

- Creates cluster of the ClickHouse database based on Custom Resource [specification][1] provided
- Supports Zookeeper integration
- Supports storage customization via PVC templates

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

## Getting started

See the [introduction][2] and [quick-start][3] for usage examples.

## License

Copyright (c) 2018, Altinity Ltd and/or its affiliates. All rights reserved.

`clickhouse-operator` is licensed under the Apache License 2.0.

See [LICENSE](LICENSE) for more details.
 
 [1]: https://github.com/Altinity/clickhouse-operator/blob/master/docs/examples/clickhouseinstallation-object.yaml
 [2]: https://github.com/Altinity/clickhouse-operator/blob/master/docs/introduction.md
 [3]: https://github.com/Altinity/clickhouse-operator/blob/master/docs/quick-start.md
