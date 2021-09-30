#!/bin/bash
set -xe

# Ensure we have some semi-random machine-id
if [ ! -f  /etc/machine-id ]; then
    dd if=/dev/urandom status=none bs=16 count=1 | md5sum | cut -d' ' -f1 > /etc/machine-id
fi

# Network fixups adapted from kind: https://github.com/kubernetes-sigs/kind/blob/master/images/base/files/usr/local/bin/entrypoint#L176
docker_embedded_dns_ip='127.0.0.11'
# first we need to detect an IP to use for reaching the docker host
docker_host_ip="$(ip -4 route show default | cut -d' ' -f3)"

# patch docker's iptables rules to switch out the DNS IP
iptables-save \
  | sed \
    `# switch docker DNS DNAT rules to our chosen IP` \
    -e "s/-d ${docker_embedded_dns_ip}/-d ${docker_host_ip}/g" \
    `# we need to also apply these rules to non-local traffic (from pods)` \
    -e 's/-A OUTPUT \(.*\) -j DOCKER_OUTPUT/\0\n-A PREROUTING \1 -j DOCKER_OUTPUT/' \
    `# switch docker DNS SNAT rules rules to our chosen IP` \
    -e "s/--to-source :53/--to-source ${docker_host_ip}:53/g"\
  | iptables-restore

# now we can ensure that DNS is configured to use our IP
cp /etc/resolv.conf /etc/resolv.conf.original
sed -e "s/${docker_embedded_dns_ip}/${docker_host_ip}/g" /etc/resolv.conf.original >/etc/resolv.conf

dockerd --host=unix:///var/run/docker.sock &>/var/log/somefile &


set +e
retries=0
while true; do
    docker info &>/dev/null && break
    retries=$((retries+1))
    if [[ $retries -ge 300 ]]
    then
        echo "Can't start docker daemon, timeout exceeded." >&2
        exit 1
    fi
    sleep 1
done
set -e

for img in /var/lib/docker/*.dockerimage; do
  docker load < "${img}"
done

chown -R master /home/master/.kube
chmod -R u+wrx /home/master/.kube

chown -R master /home/master/.minikube
chmod -R u+wrx /home/master/.minikube

su master -c "minikube start --kubernetes-version=1.22.2 --base-image='gcr.io/k8s-minikube/kicbase:v0.0.27'"
for img in /var/lib/docker/*.dockerimage; do
  echo minikube image load "${img}"
  minikube image load "${img}"
done

sed -e 's/imagePullPolicy: Always/imagePullPolicy: IfNotPresent/' < /home/master/clickhouse-operator/deploy/operator/clickhouse-operator-install-bundle.yaml | kubectl apply -f -
# need for metric alerts tests
export NO_WAIT=1
bash -xe /home/master/clickhouse-operator/deploy/prometheus/create-prometheus.sh
bash -xe /home/master/clickhouse-operator/deploy/minio/install-minio-operator.sh
bash -xe /home/master/clickhouse-operator/deploy/minio/install-minio-tenant.sh

tail -f /dev/null
