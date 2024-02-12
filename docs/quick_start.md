# Quick Start Guides

# Table of Contents

* [Prerequisites](#prerequisites)
* [ClickHouse Operator Installation](#clickhouse-operator-installation)
* [Building ClickHouse Operator from Sources](#building-clickhouse-operator-from-sources)
* [Examples](#examples)
  * [Trivial Example](#trivial-example)
  * [Connect to ClickHouse Database](#connect-to-clickhouse-database)
  * [Simple Persistent Volume Example](#simple-persistent-volume-example)
  * [Custom Deployment with Pod and VolumeClaim Templates](#custom-deployment-with-pod-and-volumeclaim-templates)
  * [Custom Deployment with Specific ClickHouse Configuration](#custom-deployment-with-specific-clickhouse-configuration)

# Prerequisites

1. Kubernetes cluster that observes the following version considerations:
    1. `clickhouse-operator` versions **before** `0.16.0` are compatible with [Kubenetes after `1.16` and prior `1.22`](https://kubernetes.io/releases/).
    1. `clickhouse-operator` versions `0.16.0` **and after** are compatible [Kubernetes version `1.16` and after](https://kubernetes.io/releases/).
1. Properly configured `kubectl`
1. `curl`

# ClickHouse Operator Installation

Apply `clickhouse-operator` installation manifest. The simplest way - directly from `github`.

## **In case you are OK to install operator into `kube-system` namespace**

just run:
```bash
kubectl apply -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/operator/clickhouse-operator-install-bundle.yaml
```
## **If you want to install operator on kubernetes version prior to `1.17` in `kube-system` namespace**

just run:
```bash
kubectl apply -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/operator/clickhouse-operator-install-bundle-v1beta1.yaml
```

## **In case you would like to customize installation parameters**,

such as namespace where to install operator or operator's image, use the special installer script.
```bash
curl -s https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/operator-web-installer/clickhouse-operator-install.sh | OPERATOR_NAMESPACE=test-clickhouse-operator bash
```
Take into account explicitly specified namespace
```bash
OPERATOR_NAMESPACE=test-clickhouse-operator
```
This namespace would be created and used to install `clickhouse-operator` into.
Install script would download some `.yaml` and `.xml` files and install `clickhouse-operator` into specified namespace.
After installation **clickhouse-operator** will watch custom resources like a `kind: ClickhouseInstallation` only in `test-clickhouse-operator` namespace.

If no `OPERATOR_NAMESPACE` specified, as:
```bash
cd ~
curl -s https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/operator-web-installer/clickhouse-operator-install.sh | bash
```
installer will install **clickhouse-operator** into `kube-system` namespace and will watch custom resources like a `kind: ClickhouseInstallation` in all available namespaces.


## **In case you can not run scripts from the Internet in your protected environment**,

you can download manually [this template file][clickhouse-operator-install-template.yaml]
and edit it according to your choice. After that apply it with `kubectl`. Or you can use this snippet instead:
```bash
#!/bin/bash

# Namespace to install operator into
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-test-clickhouse-operator}"
# Namespace to install metrics-exporter into
METRICS_EXPORTER_NAMESPACE="${OPERATOR_NAMESPACE}"
# Operator's docker image
OPERATOR_IMAGE="${OPERATOR_IMAGE:-altinity/clickhouse-operator:latest}"
# Metrics exporter's docker image
METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE:-altinity/metrics-exporter:latest}"

# Setup clickhouse-operator into specified namespace
kubectl apply --namespace="${OPERATOR_NAMESPACE}" -f <( \
    curl -s https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/operator/clickhouse-operator-install-template.yaml | \
        OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
        OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
        METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}" \
        METRICS_EXPORTER_NAMESPACE="${METRICS_EXPORTER_NAMESPACE}" \
        envsubst \
)
```

## Operator installation process
```text
Setup ClickHouse Operator into test-clickhouse-operator namespace
namespace/test-clickhouse-operator created
customresourcedefinition.apiextensions.k8s.io/clickhouseinstallations.clickhouse.altinity.com configured
serviceaccount/clickhouse-operator created
clusterrolebinding.rbac.authorization.k8s.io/clickhouse-operator configured
service/clickhouse-operator-metrics created
configmap/etc-clickhouse-operator-files created
configmap/etc-clickhouse-operator-confd-files created
configmap/etc-clickhouse-operator-configd-files created
configmap/etc-clickhouse-operator-templatesd-files created
configmap/etc-clickhouse-operator-usersd-files created
deployment.apps/clickhouse-operator created
```

Check `clickhouse-operator` is running:
```bash
kubectl get pods -n test-clickhouse-operator
```
```text
NAME                                 READY   STATUS    RESTARTS   AGE
clickhouse-operator-5ddc6d858f-drppt 1/1     Running   0          1m
```

## Building ClickHouse Operator from Sources

Complete instructions on how to build ClickHouse operator from sources as well as how to build a docker image and use it inside `kubernetes` described [here][build_from_sources].

# Examples

There are several ready-to-use [ClickHouseInstallation examples][chi-examples]. Below are a few to start with.

## Create Custom Namespace
It is a good practice to have all components run in dedicated namespaces. Let's run examples in `test` namespace
```bash
kubectl create namespace test-clickhouse-operator
```
```text
namespace/test created
```

## Trivial example

This is the trivial [1 shard 1 replica][01-simple-layout-01-1shard-1repl.yaml] example.

**WARNING**: Do not use it for anything other than 'Hello, world!'. It does not have persistent storage!
 
```bash
kubectl apply -n test-clickhouse-operator -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/docs/chi-examples/01-simple-layout-01-1shard-1repl.yaml
```
```text
clickhouseinstallation.clickhouse.altinity.com/simple-01 created
```

Installation specification is straightforward and defines 1-replica cluster:
```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "simple-01"
spec:
  configuration:
    users:
      # printf 'test_password' | sha256sum
      test_user/password_sha256_hex: 10a6e6cc8311a3e2bcc09bf6c199adecd5dd59408c343e926b129c4914f3cb01
      # to allow access outside from kubernetes
      test_user/networks/ip:
        - 0.0.0.0/0
    clusters:
      - name: "simple"
```

Once cluster is created, there are two checks to be made.

```bash
kubectl get pods -n test-clickhouse-operator
```
```text
NAME                    READY   STATUS    RESTARTS   AGE
chi-b3d29f-a242-0-0-0   1/1     Running   0          10m
```

Ensure you see the 'Running' status. Also check services created by an operator:

```bash
kubectl get service -n test-clickhouse-operator
```
```text
NAME                    TYPE           CLUSTER-IP       EXTERNAL-IP                          PORT(S)                         AGE
chi-b3d29f-a242-0-0     ClusterIP      None             <none>                               8123/TCP,9000/TCP,9009/TCP      11m
clickhouse-example-01   LoadBalancer   100.64.167.170   abc-123.us-east-1.elb.amazonaws.com   8123:30954/TCP,9000:32697/TCP   11m
```

ClickHouse is up and running!

## Connect to ClickHouse Database

There are several ways to connect to ClickHouse on Kubenetes.

1. In case the previous command `kubectl get service -n test-clickhouse-operator` reported **EXTERNAL-IP** (abc-123.us-east-1.elb.amazonaws.com in our case) we can directly access ClickHouse with:
```bash
clickhouse-client -h abc-123.us-east-1.elb.amazonaws.com -u test_user --password test_password 
```
```text
ClickHouse client version 18.14.12.
Connecting to abc-123.us-east-1.elb.amazonaws.com:9000.
Connected to ClickHouse server version 19.4.3 revision 54416.
``` 
2. In case there is no **EXTERNAL-IP** available, we can access ClickHouse from inside Kubernetes cluster
```bash
kubectl -n test-clickhouse-operator exec -it chi-b3d29f-a242-0-0-0 -- clickhouse-client
```
```text
ClickHouse client version 19.4.3.11.
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 19.4.3 revision 54416.
```
3. If we have a clickhouse client installed locally we can also use port forwarding
```bash
kubectl -n test-clickhouse-operator port-forward chi-b3d29f-a242-0-0-0 9000:9000 &
clickhouse-client
```
```text
ClickHouse client version 19.4.3.11.
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 19.4.3 revision 54416.
```

## Simple Persistent Volume Example

In cases where Dynamic Volume Provisioning is available - ex.: running on AWS - we are able to use PersistentVolumeClaims
Manifest is [available in examples][03-persistent-volume-01-default-volume.yaml]

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "pv-simple"
spec:
  defaults:
    templates:
      dataVolumeClaimTemplate: data-volume-template
      logVolumeClaimTemplate: log-volume-template
  configuration:
    clusters:
      - name: "simple"
        layout:
          shardsCount: 1
          replicasCount: 1
  templates:
    volumeClaimTemplates:
      - name: data-volume-template
        spec:
          accessModes:
            - ReadWriteOnce
          resources:
            requests:
              storage: 1Gi
      - name: log-volume-template
        spec:
          accessModes:
            - ReadWriteOnce
          resources:
            requests:
              storage: 100Mi
```

## Custom Deployment with Pod and VolumeClaim Templates

Let's install more complex example with:
1. Deployment specified
1. Pod template
1. VolumeClaim template

Manifest is [available in examples][03-persistent-volume-02-pod-template.yaml]

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "pv-log"
spec:
  configuration:
    clusters:
      - name: "deployment-pv"
        # Templates are specified for this cluster explicitly
        templates:
          podTemplate: pod-template-with-volumes
        layout:
          shardsCount: 2
          replicasCount: 2

  templates:
    podTemplates:
      - name: pod-template-with-volumes
        spec:
          containers:
            - name: clickhouse
              image: clickhouse/clickhouse-server:23.8
              volumeMounts:
                - name: data-storage-vc-template
                  mountPath: /var/lib/clickhouse
                - name: log-storage-vc-template
                  mountPath: /var/log/clickhouse-server

    volumeClaimTemplates:
      - name: data-storage-vc-template
        spec:
          accessModes:
            - ReadWriteOnce
          resources:
            requests:
              storage: 3Gi
      - name: log-storage-vc-template
        spec:
          accessModes:
            - ReadWriteOnce
          resources:
            requests:
              storage: 2Gi
```

## Custom Deployment with Specific ClickHouse Configuration

You can tell the operator to configure your ClickHouse, as shown in the example below ([link to the manifest][05-settings-01-overview.yaml]):

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: clickhouse-credentials
type: Opaque
stringData:
  testpwduser1: password
  testpwduser2: 65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5
  testpwduser3: 8bd66e4932b4968ec111da24d7e42d399a05cb90bf96f587c3fa191c56c401f8
---
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "settings-01"
spec:
  configuration:
    users:
      # test user has 'password' specified, while admin user has 'password_sha256_hex' specified
      test/password: qwerty
      test/networks/ip:
        - "127.0.0.1/32"
        - "192.168.74.1/24"
      test/profile: test_profile
      test/quota: test_quota
      test/allow_databases/database:
        - "dbname1"
        - "dbname2"
        - "dbname3"
      # reference to namespace/name/field in the secret with plain password
      testpwduser1/k8s_secret_password: dev/clickhouse-credentials/testpwduser1
      # reference to the same namespace as operator is running in/name/field in the secret with sha256 password
      testpwduser2/k8s_secret_password_sha256_hex: clickhouse-credentials/testpwduser2
      testpwduser3/k8s_secret_password_double_sha1_hex: clickhouse-credentials/testpwduser3
      # admin use has 'password_sha256_hex' so actual password value is not published
      admin/password_sha256_hex: 8bd66e4932b4968ec111da24d7e42d399a05cb90bf96f587c3fa191c56c401f8
      admin/networks/ip: "127.0.0.1/32"
      admin/profile: default
      admin/quota: default
      # readonly user has 'password' field specified, not 'password_sha256_hex' as admin user above
      readonly/password: readonly_password
      readonly/profile: readonly
      readonly/quota: default
    profiles:
      test_profile/max_memory_usage: 1000000000
      test_profile/readonly: 1
      readonly/readonly: 1
    quotas:
      test_quota/interval/duration: 3600
    settings:
      compression/case/method: zstd
      disable_internal_dns_cache: 1
    files:
      dict1.xml: |
        <yandex>
            <!-- ref to file /etc/clickhouse-data/config.d/source1.csv -->
        </yandex>
      source1.csv: |
        a1,b1,c1,d1
        a2,b2,c2,d2
    clusters:
      - name: "standard"
        layout:
          shardsCount: 1
          replicasCount: 1
```

[build_from_sources]: ./operator_build_from_sources.md
[clickhouse-operator-install-template.yaml]: ../deploy/operator/clickhouse-operator-install-template.yaml
[chi-examples]: ./chi-examples/
[01-simple-layout-01-1shard-1repl.yaml]: ./chi-examples/01-simple-layout-01-1shard-1repl.yaml
[03-persistent-volume-01-default-volume.yaml]: ./chi-examples/03-persistent-volume-01-default-volume.yaml
[03-persistent-volume-02-pod-template.yaml]: ./chi-examples/03-persistent-volume-02-pod-template.yaml
[05-settings-01-overview.yaml]: ./chi-examples/05-settings-01-overview.yaml
