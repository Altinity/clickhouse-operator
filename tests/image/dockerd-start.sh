#!/bin/bash
set -xe

# Ensure we have some semi-random machine-id
if [ ! -f  /etc/machine-id ]; then
    dd if=/dev/urandom status=none bs=16 count=1 | md5sum | cut -d' ' -f1 > /etc/machine-id
fi

update-alternatives --set iptables /usr/sbin/iptables-legacy
update-alternatives --set ip6tables /usr/sbin/ip6tables-legacy

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

touch /var/log/docker.log
chmod 0777 /var/log/docker.log
# sudo -u master dockerd-rootless-setuptool.sh install --skip-iptables
# whereis dockerd-rootless.sh
# sudo -u master bash -xce "PATH=\"/usr/bin:$PATH\" XDG_RUNTIME_DIR=/home/master/.docker/run DOCKER_HOST=unix:///home/master/.docker/run/docker.sock dockerd-rootless.sh"
dockerd --host=unix:///var/run/docker.sock &>/var/log/docker.log &

set +e
retries=0
while true; do
    docker info && break
    retries=$((retries+1))
    if [[ $retries -ge 300 ]]
    then
        echo "Can't start docker daemon, timeout exceeded." >&2
        exit 1
    fi
    tail -n 10 /var/log/docker.log
    sleep 1
done
set -e

for img in /var/lib/docker/*.dockerimage; do
  txt=${img/dockerimage/txt}
  if [[ "$(docker images -q "$(cat ${txt})" 2> /dev/null)" == "" ]]; then
    docker load < "${img}"
  fi
done

chown -R master /home/master/.kube
chmod -R u+wrx /home/master/.kube

chown -R master /home/master/.minikube
chmod -R u+wrx /home/master/.minikube

sudo -u master bash -c "minikube start --kubernetes-version=1.25.3 --base-image='gcr.io/k8s-minikube/kicbase:v0.0.35' --feature-gates=StatefulSetAutoDeletePVC=true --memory=max --cpus=max"
for img in /var/lib/docker/*.dockerimage; do
  txt=${img/dockerimage/txt}
  if [[ $( sudo -u master minikube image ls | grep -c "$(cat ${txt})" ) == "0" ]]; then
    echo minikube image load "${img}"
    sudo -u master minikube image load "${img}"
  fi
done

sed -e 's/imagePullPolicy: Always/imagePullPolicy: IfNotPresent/' < /home/master/clickhouse-operator/deploy/operator/clickhouse-operator-install-bundle.yaml | sudo -u master kubectl apply -f -
# need for metric alerts tests
export NO_WAIT=1
sudo -u master bash -xe /home/master/clickhouse-operator/deploy/prometheus/create-prometheus.sh
sudo -u master bash -xe /home/master/clickhouse-operator/deploy/minio/install-minio-operator.sh
sudo -u master bash -xe /home/master/clickhouse-operator/deploy/minio/install-minio-tenant.sh

tail -f /dev/null
