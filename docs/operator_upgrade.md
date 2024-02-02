## How to upgrade ClickHouse Operator

**Note:** Before you upgrade check releases notes if there are any backward incompatible changes between your version and the latest version.

ClickHouse operator is deployed as Deployment Kubernetes resource (see: [Operator Installation Guide][operator_installation_details.md] for more details).
Supplied [clickhouse-operator-install-bundle.yaml][clickhouse-operator-install-bundle.yaml] contains the following deployment spec:
```yaml
kind: Deployment
apiVersion: apps/v1
metadata:
  name: clickhouse-operator
  namespace: kube-system
  labels:
    clickhouse.altinity.com/chop: 0.17.0
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
      annotations:
        prometheus.io/port: '8888'
        prometheus.io/scrape: 'true'
        clickhouse-operator-metrics/port: '9999'
        clickhouse-operator-metrics/scrape: 'true'        
    spec:
      serviceAccountName: clickhouse-operator
      volumes:
        - name: etc-clickhouse-operator-folder
          configMap:
            name: etc-clickhouse-operator-files
        - name: etc-clickhouse-operator-confd-folder
          configMap:
            name: etc-clickhouse-operator-confd-files
        - name: etc-clickhouse-operator-configd-folder
          configMap:
            name: etc-clickhouse-operator-configd-files
        - name: etc-clickhouse-operator-templatesd-folder
          configMap:
            name: etc-clickhouse-operator-templatesd-files
        - name: etc-clickhouse-operator-usersd-folder
          configMap:
            name: etc-clickhouse-operator-usersd-files
      containers:
        - name: clickhouse-operator
          image: altinity/clickhouse-operator:0.17.2
          imagePullPolicy: Always
          volumeMounts:
            - name: etc-clickhouse-operator-folder
              mountPath: /etc/clickhouse-operator
            - name: etc-clickhouse-operator-confd-folder
              mountPath: /etc/clickhouse-operator/conf.d
            - name: etc-clickhouse-operator-configd-folder
              mountPath: /etc/clickhouse-operator/config.d
            - name: etc-clickhouse-operator-templatesd-folder
              mountPath: /etc/clickhouse-operator/templates.d
            - name: etc-clickhouse-operator-usersd-folder
              mountPath: /etc/clickhouse-operator/users.d
          env:
            # Pod-specific
            - name: OPERATOR_POD_NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
            - name: OPERATOR_POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: OPERATOR_POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
            - name: OPERATOR_POD_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
            - name: OPERATOR_POD_SERVICE_ACCOUNT
              valueFrom:
                fieldRef:
                  fieldPath: spec.serviceAccountName

            # Container-specific
            - name: OPERATOR_CONTAINER_CPU_REQUEST
              valueFrom:
                resourceFieldRef:
                  containerName: clickhouse-operator
                  resource: requests.cpu
            - name: OPERATOR_CONTAINER_CPU_LIMIT
              valueFrom:
                resourceFieldRef:
                  containerName: clickhouse-operator
                  resource: limits.cpu
            - name: OPERATOR_CONTAINER_MEM_REQUEST
              valueFrom:
                resourceFieldRef:
                  containerName: clickhouse-operator
                  resource: requests.memory
            - name: OPERATOR_CONTAINER_MEM_LIMIT
              valueFrom:
                resourceFieldRef:
                  containerName: clickhouse-operator
                  resource: limits.memory

        - name: metrics-exporter
          image: altinity/metrics-exporter:0.17.0
          imagePullPolicy: Always
          volumeMounts:
            - name: etc-clickhouse-operator-folder
              mountPath: /etc/clickhouse-operator
            - name: etc-clickhouse-operator-confd-folder
              mountPath: /etc/clickhouse-operator/conf.d
            - name: etc-clickhouse-operator-configd-folder
              mountPath: /etc/clickhouse-operator/config.d
            - name: etc-clickhouse-operator-templatesd-folder
              mountPath: /etc/clickhouse-operator/templates.d
            - name: etc-clickhouse-operator-usersd-folder
              mountPath: /etc/clickhouse-operator/users.d
          ports:
            - containerPort: 8888
              name: metrics
```

The latest available version is installed by default. If version changes, there are three ways to upgrade the operator:

* Delete existing deployment
```bash
kubectl delete deploy -n kube-system clickhouse-operator 
```
* Upgrade Custom Resource Definitions to latest version
```bash
kubectl apply -f https://github.com/Altinity/clickhouse-operator/raw/master/deploy/operator/parts/crd.yaml
```

Current deployments can be listed using the following command:
```
$ kubectl describe --all-namespaces deployment -l app=clickhouse-operator
Name:                   clickhouse-operator
Namespace:              kube-system
CreationTimestamp:      Sat, 01 Jun 2019 23:44:46 +0300
Labels:                 app=clickhouse-operator
                        version=0.13.0
<...>
Pod Template:
  Labels:           app=clickhouse-operator
  Service Account:  clickhouse-operator
  Containers:
   clickhouse-operator:
    Image:        altinity/clickhouse-operator:0.17.0
   metrics-exporter:
    Image:        altinity/metrics-exporter:0.17.0
<...>
```

Version is labeled and can be also displayed with the command:
```
$ kubectl get deployment --all-namespaces -L clickhouse.altinity.com/chop -l app=clickhouse-operator 
NAMESPACE   NAME                  UP-TO-DATE   AVAILABLE   AGE       VERSION
kube-system clickhouse-operator   1            1           19h       0.17.0
```

If you want to update to the latest version, we can run following command:
  
```
$ kubectl apply -n kube-system -f https://github.com/Altinity/clickhouse-operator/blob/master/deploy/operator/clickhouse-operator-install-bundle.yaml

```
  
And then check upgrade status with:
```
$ kubectl get deployment --all-namespaces -L clickhouse.altinity.com/chop -l app=clickhouse-operator
NAMESPACE    NAME                  READY   UP-TO-DATE   AVAILABLE   AGE   VERSION
kube-system  clickhouse-operator   1/1     1            1           125d  0.18.2

$ kubectl describe --all-namespaces deployment -l app=clickhouse-operator
Name:                   clickhouse-operator
<...>
Pod Template:
  Labels:           app=clickhouse-operator
  Service Account:  clickhouse-operator
  Containers:
   clickhouse-operator:
    Image:        altinity/clickhouse-operator:0.18.2
   metrics-exporter:
    Image:        altinity/metrics-exporter:0.18.2
<...>
```

[operator_installation_details.md]: ./operator_installation_details.md
[clickhouse-operator-install-bundle.yaml]: ../deploy/operator/clickhouse-operator-install-bundle.yaml
