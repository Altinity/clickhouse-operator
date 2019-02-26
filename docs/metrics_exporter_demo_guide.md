## 1. Accessing k8s cluster on AWS

### Updating ~/.kube/config with new cluster section
```console
apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: # AWS key here
    server: https://api-altinity-k8s-local-9dpc7i-2098180516.us-east-1.elb.amazonaws.com
  name: altinity.k8s.local
contexts:
- context:
    cluster: altinity.k8s.local
    user: altinity.k8s.local
  name: altinity.k8s.local
current-context: altinity.k8s.local
kind: Config
preferences: {}
users:
- name: altinity.k8s.local
  user:
    client-certificate-data: # AWS key here
    client-key-data: # AWS key here
    password: # password here
    username: admin
- name: altinity.k8s.local-basic-auth
  user:
    password: # password here
    username: admin
```
### Setting new context for kubectl
```console
$ kubectl config set-context altinity.k8s.local
```
### Forwarding Graphana dashboard to localhost
```console
$ kubectl --namespace monitoring port-forward svc/grafana 3000
```
Now you can access Grafana's dashboard at http://localhost:3000 
Username/password: admin/admin

### Forwarding Prometheus dashboard to localhost
```console
$ kubectl --namespace monitoring port-forward svc/prometheus-k8s 9090
```
Now you can access Grafana's dashboard at http://localhost:9090
