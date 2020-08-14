## How to upgrade ClickHouse Operator

**Note:** Before you upgrade check releases notes if there are any backward incompatible changes between your version and the latest version.

ClickHouse operator is deployed as Deployment Kubernetes resource (see: [Operator Installation Guide][operator_installation_details.md] for more details). 
Supplied [clickhouse-operator-install.yaml][clickhouse-operator-install.yaml] contains the following deployment spec:
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
The latest available version is installed by default. If version changes, there are three ways to upgrade the operator:
* Delete existing deployment and re-deploy it using spec file above
* Specify the new version in operator spec file and reapply
* Upgrade deployment to the required version using Kubernetes API.

The last approach is probably more convenient, since operator is typically installed using predefined spec file.

Deployments can be listed using the following command:
```
$ kubectl describe deployment clickhouse-operator -n kube-system
Name:                   clickhouse-operator
Namespace:              kube-system
CreationTimestamp:      Sat, 01 Jun 2019 23:44:46 +0300
Labels:                 app=clickhouse-operator
                        version=0.3.0
<...>
Pod Template:
  Labels:           app=clickhouse-operator
  Service Account:  clickhouse-operator
  Containers:
   clickhouse-operator:
    Image:        altinity/clickhouse-operator:latest
<...>
```
Version is labeled and can be also displayed with the command:
```
$ kubectl get deployment clickhouse-operator -L version -n kube-system
NAME                  DESIRED   CURRENT   UP-TO-DATE   AVAILABLE   AGE       VERSION
clickhouse-operator   1         1         1            1           19h       0.3.0
```

If we want to update to the new version, we can run following command:
  
```
kubectl set image deployment.v1.apps/clickhouse-operator clickhouse-operator=altinity/clickhouse-operator:0.3.0 -n kube-system
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
    Image:        altinity/clickhouse-operator:0.3.0
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

[operator_installation_details.md]: ./operator_installation_details.md
[clickhouse-operator-install.yaml]: ../deploy/operator/clickhouse-operator-install.yaml
