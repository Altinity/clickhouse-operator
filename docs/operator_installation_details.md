# Install ClickHouse Operator

# Prerequisites

1. Kubernetes instance with the following version considerations:
    1. `clickhouse-operator` versions **before** `0.16.0` is compatible with [Kubenetes after `1.16` and prior `1.22`](https://kubernetes.io/releases/).
    1. `clickhouse-operator` versions `0.16.0` **and after** is compatible [Kubernetes version `1.16` and after](https://kubernetes.io/releases/).
1. Properly configured `kubectl`
1. `curl`

Verify the Docker manifest is available based on the version table, replacing `{OPERATOR_VERSION}` with the specific version.  For example, for version `0.16.0`, the URL would be `https://github.com/Altinity/clickhouse-operator/raw/0.16.0/deploy/operator/clickhouse-operator-install-bundle.yaml`.

| `clickhouse-operator` version | Kubernetes version | Kubernetes manifest URL |
|---|---|---|
| Current | Kubernetes 1.16+ | https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/operator/clickhouse-operator-install-bundle.yaml |
| Current | Kubernetes before 1.16 | **(Beta)** https://github.com/Altinity/clickhouse-operator/raw/master/deploy/operator/clickhouse-operator-install-bundle-v1beta1.yaml |
| `0.16.0` and greater | Kubernetes 1.16+ | https://github.com/Altinity/clickhouse-operator/raw/{OPERATOR_VERSION}/deploy/operator/clickhouse-operator-install-bundle.yaml |
| Before `0.16.0` | Kubernetes after 1.16 and before 1.22 | kubectl apply -f  https://github.com/Altinity/clickhouse-operator/raw/{OPERATOR_VERSION}/deploy/operator/clickhouse-operator-install.yaml |

[clickhouse-operator-install-bundle.yaml][clickhouse-operator-install-bundle.yaml] file availability.
In is located in `deploy/operator` folder inside `clickhouse-operator` sources.

## Install via kubectl

Operator installation process is quite straightforward and consists of one main step - deploy **ClickHouse operator**.
We'll apply operator manifest directly from github repo
```bash
kubectl apply -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/operator/clickhouse-operator-install-bundle.yaml
```

The following results are expected:
```text
customresourcedefinition.apiextensions.k8s.io/clickhouseinstallations.clickhouse.altinity.com created
serviceaccount/clickhouse-operator created
clusterrolebinding.rbac.authorization.k8s.io/clickhouse-operator created
deployment.apps/clickhouse-operator configured
```

## Verify operator is up and running

Operator is deployed in **kube-system** namespace.

```bash
kubectl get pods --namespace kube-system
```

Expected results:
```text
NAME                                   READY   STATUS    RESTARTS   AGE
...
clickhouse-operator-5c46dfc7bd-7cz5l   1/1     Running   0          43m
...
```


## Install via helm

since 0.20.1 version official clickhouse-operator helm chart, also available

installation
```bash
helm repo add clickhouse-operator https://docs.altinity.com/clickhouse-operator/
helm install clickhouse-operator clickhouse-operator/altinity-clickhouse-operator
```
upgrade
```bash
helm repo upgrade clickhouse-operator
helm upgrade clickhouse-operator clickhouse-operator/altinity-clickhouse-operator
```

Look https://github.com/Altinity/clickhouse-operator/tree/master/deploy/helm/clickhouse-operator/ for details 

## Resources Description

Let's walk over all resources created along with ClickHouse operator, which are:
1. Custom Resource Definition
1. Service account
1. Cluster Role Binding
1. Deployment


### Custom Resource Definition
```text
customresourcedefinition.apiextensions.k8s.io/clickhouseinstallations.clickhouse.altinity.com created
```
New [Custom Resource Definition][customresourcedefinitions] named **ClickHouseInstallation** is created.
k8s API is extended with new kind `ClickHouseInstallation` and we'll be able to manage k8s resource of `kind: ClickHouseInstallation`

### Service Account
```text
serviceaccount/clickhouse-operator created
```
New [Service Account][configure-service-account] named **clickhouse-operator** is created.
A service account provides an identity used to contact the `apiserver` by the processes that run in a Pod. 
Processes in containers inside pods can contact the `apiserver`, and when they do, they are authenticated as a particular `Service Account` - `clickhouse-operator` in this case.

### Cluster Role Binding
```text
clusterrolebinding.rbac.authorization.k8s.io/clickhouse-operator created
```
New [CluserRoleBinding][rolebinding-and-clusterrolebinding] named **clickhouse-operator** is created.
A role binding grants the permissions defined in a role to a set of users. 
It holds a reference to the role being granted to the list of subjects (users, groups, or service accounts).
In this case Role
```yaml
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
``` 
is being granted to
```yaml
subjects:
  - kind: ServiceAccount
    name: clickhouse-operator
    namespace: kube-system
```
`clickhouse-operator` Service Account created earlier.
Permissions are granted cluster-wide with a `ClusterRoleBinding`.

### Deployment
```text
deployment.apps/clickhouse-operator configured
```
New [Deployment][deployment] named **clickhouse-operator** is created. 
ClickHouse operator app would be run by this deployment in `kube-system` namespace.

## Verify Resources

Check Custom Resource Definition
```bash
kubectl get customresourcedefinitions
```
Expected result
```text
NAME                                              CREATED AT
...
clickhouseinstallations.clickhouse.altinity.com   2019-01-25T10:17:57Z
...
```

Check Service Account
```bash
kubectl get serviceaccounts -n kube-system
```
Expected result
```text
NAME                                 SECRETS   AGE
...
clickhouse-operator                  1         27h
...
```

Check Cluster Role Binding
```bash
kubectl get clusterrolebinding
```
Expected result
```text
NAME                                                   AGE
...
clickhouse-operator                                    31m
...

```
Check deployment
```bash
kubectl get deployments --namespace kube-system
```
Expected result
```text
NAME                   READY   UP-TO-DATE   AVAILABLE   AGE
...
clickhouse-operator    1/1     1            1           31m
...

```

[clickhouse-operator-install-bundle.yaml]: ../deploy/operator/clickhouse-operator-install-bundle.yaml
[customresourcedefinitions]: https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/#customresourcedefinitions
[configure-service-account]: https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/
[rolebinding-and-clusterrolebinding]: https://kubernetes.io/docs/reference/access-authn-authz/rbac/#rolebinding-and-clusterrolebinding
[deployment]: https://kubernetes.io/docs/concepts/workloads/controllers/deployment/
