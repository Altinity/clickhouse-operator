# Howto quick start Clickhouse cluster with zookeeper on Windows 10 + Minikube
## Requirements
Windows 10 - 4Gb free RAM, 20Gb free Disk space 

## Open `Windows PowerShell (adminitrator)` console (press `Win+X`) and run following command to install chocolatelly
```
Set-ExecutionPolicy Bypass -Scope Process -Force; iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))
```

## Install virtualbox, minikube, kubectl
```bash
choco install -y virtualbox minikube cygwin kubernetes-cli curl
```

## Open `bash console (as administrator)` press `WIN+R` type `bash` and press `CTRL+SHIFT+ENTER`
### Start kubernetes in minikube
```bash
minikube start --memory=4g --disk-size=40g
```

### Check kubectl worked
```bash
kubectl get nodes
```
expected output
```bash
NAME       STATUS   ROLES    AGE   VERSION
minikube   Ready    master   64m   v1.17.0
```

### Prepare environment variables
```bash
export BRANCH=${BRANCH:-master} 
export OPERATOR_IMAGE=${OPERATOR_IMAGE:-altinity/clickhouse-operator:latest}
export METRICS_EXPORTER_IMAGE=${METRICS_EXPORTER_IMAGE:-altinity/metrics-exporter:latest}
export OPERATOR_NAMESPACE=${OPERATOR_NAMESPACE:-clickhouse}
export ZK_NAMESPACE=${ZK_NAMESPACE:-zoo1ns}
``` 

### Install clickhouse operator
```bash
curl -sL https://raw.githubusercontent.com/Altinity/clickhouse-operator/${BRANCH}/deploy/operator-installer/clickhouse-operator-install.sh | bash 
```

### Create Zookeeper installation
```bash
kubectl create namespace ${ZK_NAMESPACE}
curl -sL https://raw.githubusercontent.com/Altinity/clickhouse-operator/${BRANCH}/deploy/zookeeper/quick-start-volume-emptyDir/zookeeper-1-node.yaml | kubectl apply --namespace=${ZK_NAMESPACE} -f -
```

### Create ClickHouse installation 2 shards + 2 replicas in each shard with Persistent Volumes
```bash
kubectl apply --namespace=${OPERATOR_NAMESPACE} -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/${BRANCH}/docs/chi-examples/04-replication-zookeeper-05-simple-PV.yaml 
```

## Wait around 30 seconds and check all installed objects
```bash
kubectl get pods --all-namespaces | grep -E "clickhouse-operator|zoo"
```
expected output
```bash
clickhouse    clickhouse-operator-XXXXXXX-XXXXXXXX   2/2     Running   0    4m13s
zoo1ns        zookeeper-0                            1/1     Running   0    3m59s
```

## Watch progress how to clickhouse-operator deploy ClickHouse installation 
```bash
kubectl logs -f --namespace ${OPERATOR_NAMESPACE}  $(kubectl get pods --namespace=${OPERATOR_NAMESPACE} -o wide | grep -E "clickhouse-operator" | cut -d " " -f 1) -c clickhouse-operator
```

## Check installed pods
```bash
kubectl get pods --all-namespaces | grep -E "chi"
```
expected output
```bash
clickhouse    chi-repl-05-replicated-0-0-0           1/1     Running   0          31m
clickhouse    chi-repl-05-replicated-0-1-0           1/1     Running   0          29m
clickhouse    chi-repl-05-replicated-1-0-0           1/1     Running   0          29m
clickhouse    chi-repl-05-replicated-1-1-0           1/1     Running   0          28m
```

## Connect to ClickHouse database
### Start minikube tunnel for assign external-ip for LoadBalancer services
```bash
cmd /c start "minikube tunnel" minikube tunnel $(kubectl get services --namespace=${OPERATOR_NAMESPACE} | grep LoadBalancer | cut -d " " -f 1)
```
expected output in separate input
```bash
Status:
        machine: minikube
        pid: 2836
        route: 10.96.0.0/12 -> 192.168.99.116
        minikube: Running
        services: [clickhouse-repl-05]
    errors:
                minikube: no errors
                router: no errors
                loadbalancer emulator: no errors
```

###  and run following command
```bash
curl "http://$(kubectl get svc --namespace=${OPERATOR_NAMESPACE} | grep LoadBalancer | awk '{print $4}'):8123/?query=SELECT+Version()"
```
expected output
```
19.6.2.11
```

## Clear all installed objects
```bash
minikube delete
```