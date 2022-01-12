# Howto easy setup development environment with devspace.sh

## Install requirement components
### golang
- https://go.dev/
### yq
- https://github.com/mikefarah/yq/#install
### docker
- https://docs.docker.com/get-docker/
### docker multi-arch support
```bash
sudo apt-get install -y qemu binfmt-support qemu-user-static
docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
```
### kubernetes cluster
- Install minikube https://kubernetes.io/docs/tasks/tools/install-minikube/
- Run typical minikube configuration
```bash
    minikube config set driver docker
    minikube config set kubernetes-version 1.23.1
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
- switch docker into minikube
```bash
eval $(minikube docker-env)
```
- run devspace
```bash
devspace dev --var=OPERATOR_NAMESPACE=kube-system --var=DEVSPACE_DEBUG=delve
```
- open once `deploy/devspace/*.run.xml` (Run -> Edit configurations)
- update go modules if needed
```bash
go mod tidy
go mod vendor
```
- change source files *.go/*.yaml etc., set breakpoints, look to .dockerignore to
- devspace will rebuild docker images automatically and apply kubernetes manifest which install adopted to debug under delve binaries
- run Debug (SHIFT/CMD+F9) 
