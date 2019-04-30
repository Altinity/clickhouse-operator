## How to upgrade ClickHouse Operator

**Note:** Before you upgrade check releases notes if there are any backward incompatible changes between your version and the latest version.

ClickHouse operator is deployed as Deployment Kubernetes resource (see: [Operator Installation Guide](https://github.com/Altinity/clickhouse-operator/blob/master/docs/operator_installation_details.md)
for more details). Supplied [clickhouse-operator-install.yaml](https://github.com/Altinity/clickhouse-operator/blob/master/manifests/operator/clickhouse-operator-install.yaml) contains the following deployment spec:
```
kind: Deployment
apiVersion: apps/v1
metadata:
  name: clickhouse-operator
  namespace: kube-system
  labels:
    app: clickhouse-operator
spec:
  replicas: 1
  selector:
    matchLabels:
      app: clickhouse-operator
  template:
    metadata:
      labels:
        app: clickhouse-operator
    spec:
      serviceAccountName: clickhouse-operator
      containers:
      - image: altinity/clickhouse-operator:latest
        name: clickhouse-operator
```
The latest available version is installed by default. If version changes, there are two ways to upgrade the operator:
* Specify the new version in operator spec file and reapply
* Upgrade deployment to the required version using Kubernetes API.

The second approach is probably more convenient, since operator is typically installed using predefined spec file.

Deployments can be listed using the following command:
```
$ kubectl describe deployment clickhouse-operator -n kube-system
Name:                   clickhouse-operator
<...>
Pod Template:
  Labels:           app=clickhouse-operator
  Service Account:  clickhouse-operator
  Containers:
   clickhouse-operator:
    Image:        altinity/clickhouse-operator:latest
<...>
```
  
If we want to update to the new version, we can run following command:
  
```
kubectl set image deployment.v1.apps/clickhouse-operator clickhouse-operator=altinity/clickhouse-operator:0.2.1 -n kube-system
deployment.apps/clickhouse-operator image updated
```
  
And then check rollout status with:
```
$ kubectl rollout status deployment.v1.apps/clickhouse-operator -n kube-system
deployment "clickhouse-operator" successfully rolled out
```

and
```
$ kubectl describe deployment clickhouse-operator -n kube-system
Name:                   clickhouse-operator
<...>
Pod Template:
  Labels:           app=clickhouse-operator
  Service Account:  clickhouse-operator
  Containers:
   clickhouse-operator:
    Image:        altinity/clickhouse-operator:0.2.1
<...>
```

If something goes wrong rollout status can be different, for example:
```
$ kubectl rollout status deployment.v1.apps/clickhouse-operator -n kube-system
Waiting for deployment "clickhouse-operator" rollout to finish: 1 old replicas are pending termination...
```

In this case deployment can be undone:
```
kubectl rollout undo deployment.v1.apps/clickhouse-operator -n kube-system
deployment.apps/clickhouse-operator
```
