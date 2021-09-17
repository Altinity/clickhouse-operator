#!/bin/bash
set -e

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

docker load < /var/lib/docker/kicbase.dockerimage
docker load < /var/lib/docker/ch_image.dockerimage
docker load < /var/lib/docker/m_expo.dockerimage
docker load < /var/lib/docker/cho.dockerimage
docker load < /var/lib/docker/s_prov.dockerimage
docker load < /var/lib/docker/cho_old.dockerimage
docker load < /var/lib/docker/m_expo_old.dockerimage
docker load < /var/lib/docker/zk.dockerimage
docker load < /var/lib/docker/ch_old.dockerimage

chown -R master /home/master
chmod -R u+wrx /home/master

su master -c "minikube start --kubernetes-version=1.21.2 --base-image='gcr.io/k8s-minikube/kicbase:v0.0.26'"

minikube image load /var/lib/docker/kicbase.dockerimage
minikube image load /var/lib/docker/ch_image.dockerimage
minikube image load /var/lib/docker/m_expo.dockerimage
minikube image load /var/lib/docker/cho.dockerimage
minikube image load /var/lib/docker/s_prov.dockerimage
minikube image load /var/lib/docker/cho_old.dockerimage
minikube image load /var/lib/docker/m_expo_old.dockerimage
minikube image load /var/lib/docker/zk.dockerimage
minikube image load /var/lib/docker/ch_old.dockerimage

kubectl apply -f /home/master/clickhouse-operator/deploy/operator/clickhouse-operator-install.yaml

export CLICKHOUSE_TESTS_SERVER_BIN_PATH=/clickhouse
export CLICKHOUSE_TESTS_CLIENT_BIN_PATH=/clickhouse
export CLICKHOUSE_TESTS_BASE_CONFIG_DIR=/clickhouse-config
export CLICKHOUSE_ODBC_BRIDGE_BINARY_PATH=/clickhouse-odbc-bridge

tail -f /dev/null
