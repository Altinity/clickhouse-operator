# Howto easy setup development environment with devspace.sh

## Install requirement components
### docker
- https://docs.docker.com/get-docker/
### kubernetes cluster
- Install minikube https://kubernetes.io/docs/tasks/tools/install-minikube/
- Run typical minikube configuration
```bash
    minikube config set vm-driver docker
    minikube config set kubernetes-version 1.18.2
    minikube start
    minikube addons enable ingress
    minikube addons enable ingress-dns
    minikube addons enable metrics-server
```
### kubectl
- https://kubernetes.io/docs/tasks/tools/install-kubectl/
### devspace.sh
- https://devspace.sh/cli/docs/getting-started/installation


## Typical development workflow JetBrains Goland
- open project in JetBrains Goland
- run devspace
```bash
devspace dev --OPERATOR_NAMESPACE=kube-system,DEVSPACE_DEBUG=delve
```
- open once `deploy/devspace/*.run.xml` (Run -> Edit configurations)
- change source files *.go, set breakpoints
- devspace will rebuild docker images automatically and apply kubernetes manifest which install adopted to debug under delve binaries
- run Debug (SHIFT/CMD+F9) 
