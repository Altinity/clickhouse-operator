# -*- mode: ruby -*-
# vi: set ft=ruby :

def total_cpus
  require 'etc'
  Etc.nprocessors
end

def get_provider
    provider='virtualbox'
    for arg in ARGV
        if ['hyperv','docker'].include? arg
            provider=arg
        end
    end
    return provider
end


Vagrant.configure(2) do |config|
  config.vm.box = "generic/ubuntu2004"
  config.vm.box_check_update = false

  if get_provider == "hyperv"
    config.vm.synced_folder ".", "/vagrant", type: "smb", smb_username: ENV['USERNAME'], smb_password: ENV['PASSWORD'], mount_options: ["vers=3.0"]
  else
    config.vm.synced_folder ".", "/vagrant"
  end

  if Vagrant.has_plugin?("vagrant-vbguest")
    config.vbguest.auto_update = true
  end

  if Vagrant.has_plugin?("vagrant-timezone")
    config.timezone.value = "UTC"
  end

  config.vm.provider "virtualbox" do |vb|
    vb.gui = false
    vb.cpus = total_cpus
    vb.memory = "6144"
    vb.default_nic_type = "virtio"
    vb.customize ["modifyvm", :id, "--uartmode1", "file", File::NULL ]
    vb.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
    vb.customize ["modifyvm", :id, "--natdnsproxy1", "on"]
    vb.customize ["modifyvm", :id, "--ioapic", "on"]
    vb.customize ["guestproperty", "set", :id, "/VirtualBox/GuestAdd/VBoxService/--timesync-set-threshold", 10000]
  end

  config.vm.provider "hyperv" do |hv|
    # hv.gui = false
    # hv.default_nic_type = "virtio"
    hv.cpus = total_cpus
    hv.maxmemory = "6144"
    hv.memory = "6144"
    hv.enable_virtualization_extensions = true
    hv.linked_clone = true
    hv.vm_integration_services = {
        time_synchronization: true,
    }
  end

  config.vm.define :clickhouse_operator do |clickhouse_operator|
    clickhouse_operator.vm.network "private_network", ip: "172.16.2.99", nic_type: "virtio"
    # port forwarding works only when pair with kubectl port-forward
    # grafana
    clickhouse_operator.vm.network "forwarded_port", guest_ip: "172.16.2.99", guest: 3000, host_ip: "127.0.0.1", host: 3000
    # mertics-exporter
    clickhouse_operator.vm.network "forwarded_port", guest_ip: "172.16.2.99", guest: 8888, host_ip: "127.0.0.1", host: 8888
    # prometheus
    clickhouse_operator.vm.network "forwarded_port", guest_ip: "172.16.2.99", guest: 9090, host_ip: "127.0.0.1", host: 9090
    # alertmanager
    clickhouse_operator.vm.network "forwarded_port", guest_ip: "172.16.2.99", guest: 9093, host_ip: "127.0.0.1", host: 9093

    # devspace UI
    clickhouse_operator.vm.network "forwarded_port", guest_ip: "172.16.2.99", guest: 8090, host_ip: "127.0.0.1", host: 8090

    # delve for devspace
    clickhouse_operator.vm.network "forwarded_port", guest_ip: "172.16.2.99", guest: 40001, host_ip: "127.0.0.1", host: 40001
    clickhouse_operator.vm.network "forwarded_port", guest_ip: "172.16.2.99", guest: 40002, host_ip: "127.0.0.1", host: 40002

    clickhouse_operator.vm.host_name = "local-altinity-clickhouse-operator"
    # vagrant plugin install vagrant-disksize
    clickhouse_operator.disksize.size = '50GB'
  end

  config.vm.provision "shell", inline: <<-SHELL
    set -xeuo pipefail
    export DEBIAN_FRONTEND=noninteractive
    # make linux fast again
    if [[ "0" == $(grep "mitigations" /etc/default/grub | wc -l) ]]; then
        echo 'GRUB_CMDLINE_LINUX="noibrs noibpb nopti nospectre_v2 nospectre_v1 l1tf=off nospec_store_bypass_disable no_stf_barrier mds=off tsx=on tsx_async_abort=off mitigations=off"' >> /etc/default/grub
        echo 'GRUB_CMDLINE_LINUX_DEFAULT="quiet splash noibrs noibpb nopti nospectre_v2 nospectre_v1 l1tf=off nospec_store_bypass_disable no_stf_barrier mds=off tsx=on tsx_async_abort=off mitigations=off"' >> /etc/default/grub
        grub-mkconfig
    fi
    systemctl enable systemd-timesyncd
    systemctl start systemd-timesyncd

    apt-get update
    apt-get install --no-install-recommends -y apt-transport-https ca-certificates software-properties-common curl
    apt-get install --no-install-recommends -y htop ethtool mc curl wget jq socat git make gcc g++

    # yq
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys CC86BB64
    add-apt-repository ppa:rmescandon/yq
    apt-get install --no-install-recommends -y yq

    # clickhouse
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E0C56BD4
    add-apt-repository "deb http://repo.clickhouse.tech/deb/stable/ main/"
    apt-get install --no-install-recommends -y clickhouse-client

    # golang
    export GOLANG_VERSION=1.17
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys F6BC817356A3D45E
    add-apt-repository ppa:longsleep/golang-backports
    apt-get install --no-install-recommends -y golang-${GOLANG_VERSION}-go
    ln -nvsf /usr/lib/go-${GOLANG_VERSION}/bin/go /bin/go
    ln -nvsf /usr/lib/go-${GOLANG_VERSION}/bin/gofmt /bin/gofmt

    # docker
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
    add-apt-repository "deb https://download.docker.com/linux/ubuntu $(lsb_release -cs) test"
    apt-get install --no-install-recommends -y docker-ce pigz

    # docker compose
    apt-get install -y --no-install-recommends python3-distutils
    curl -sL https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py
    python3 /tmp/get-pip.py

    pip3 install -U setuptools
    pip3 install -U docker-compose

    # k9s CLI
    K9S_VERSION=$(curl -sL https://github.com/derailed/k9s/releases/latest -H "Accept: application/json" | jq -r .tag_name)
    wget -c --progress=bar:force:noscroll -O /usr/local/bin/k9s_${K9S_VERSION}_Linux_x86_64.tar.gz https://github.com/derailed/k9s/releases/download/${K9S_VERSION}/k9s_Linux_x86_64.tar.gz
    curl -sL https://github.com/derailed/k9s/releases/download/${K9S_VERSION}/checksums.txt | grep Linux_x86_64.tar.gz > /usr/local/bin/k9s.sha256
    sed -i -e "s/k9s_Linux_x86_64\.tar\.gz/\\/usr\\/local\\/bin\\/k9s_${K9S_VERSION}_Linux_x86_64\\.tar\\.gz/g" /usr/local/bin/k9s.sha256
    sha256sum -c /usr/local/bin/k9s.sha256
    tar --verbose -zxvf /usr/local/bin/k9s_${K9S_VERSION}_Linux_x86_64.tar.gz -C /usr/local/bin k9s

    # audit2rbac
    AUDIT2RBAC_VERSION=0.8.0
    curl -sL https://github.com/liggitt/audit2rbac/releases/download/v${AUDIT2RBAC_VERSION}/audit2rbac-linux-amd64.tar.gz | tar -zxvf - -C /usr/local/bin

    # minikube
    # MINIKUBE_VERSION=1.18.1
    # MINIKUBE_VERSION=1.19.0
    # MINIKUBE_VERSION=1.20.0
    MINIKUBE_VERSION=1.23.2
    wget -c --progress=bar:force:noscroll -O /usr/local/bin/minikube https://github.com/kubernetes/minikube/releases/download/v${MINIKUBE_VERSION}/minikube-linux-amd64
    chmod +x /usr/local/bin/minikube
    # required for k8s 1.18+
    apt-get install -y conntrack

#    K8S_VERSION=${K8S_VERSION:-1.14.10}
#    export VALIDATE_YAML=false # only for 1.14
#    K8S_VERSION=${K8S_VERSION:-1.15.12}
#    K8S_VERSION=${K8S_VERSION:-1.16.15}
#    K8S_VERSION=${K8S_VERSION:-1.17.17}
#    K8S_VERSION=${K8S_VERSION:-1.18.20}
#    K8S_VERSION=${K8S_VERSION:-1.19.14}
#    performance issue 1.20.x, 1.21.x
#    https://github.com/kubernetes/kubeadm/issues/2395
#    K8S_VERSION=${K8S_VERSION:-1.20.14}
#    K8S_VERSION=${K8S_VERSION:-1.21.8}
#    K8S_VERSION=${K8S_VERSION:-1.22.5}
    K8S_VERSION=${K8S_VERSION:-1.23.1}
    export VALIDATE_YAML=true

    killall kubectl || true
    wget -c --progress=bar:force:noscroll -O /usr/local/bin/kubectl https://storage.googleapis.com/kubernetes-release/release/v${K8S_VERSION}/bin/linux/amd64/kubectl
    chmod +x /usr/local/bin/kubectl

    usermod -a -G docker vagrant
    mkdir -p /home/vagrant/.minikube
    ln -svf /home/vagrant/.minikube /root/.minikube

    mkdir -p /root/.minikube/files/etc/ssl/certs

cat <<EOF >/root/.minikube/files/etc/ssl/certs/audit-policy.yaml
    # Log all requests at the Metadata level.
    apiVersion: audit.k8s.io/v1
    kind: Policy
    rules:
    - level: Metadata
EOF

    mkdir -p /home/vagrant/.kube
    ln -svf /home/vagrant/.kube /root/.kube

    chown vagrant:vagrant -R /home/vagrant/

    sudo -H -u vagrant minikube delete
    sudo -H -u vagrant minikube config set memory 5G
    sudo -H -u vagrant minikube config set driver docker
    sudo -H -u vagrant minikube config set kubernetes-version ${K8S_VERSION}
    sudo -H -u vagrant minikube start --extra-config=apiserver.audit-policy-file=/etc/ssl/certs/audit-policy.yaml --extra-config=apiserver.audit-log-path=-
    sudo -H -u vagrant minikube addons enable ingress
    sudo -H -u vagrant minikube addons enable ingress-dns
    sudo -H -u vagrant minikube addons enable metrics-server

    #krew
    (
        curl -fsSLO "https://github.com/kubernetes-sigs/krew/releases/latest/download/krew.tar.gz" &&
        tar zxvf krew.tar.gz &&
        KREW=./krew-"$(uname | tr '[:upper:]' '[:lower:]')_amd64" &&
        sudo -H -u vagrant "$KREW" install krew
    )
    sudo -H -u vagrant bash -c 'echo export PATH="\${KREW_ROOT:-\$HOME/.krew}/bin:\$PATH" | tee \$HOME/.bashrc'
    echo export PATH="/home/vagrant/.krew/bin:$PATH" | tee $HOME/.bashrc
    source $HOME/.bashrc
    export KREW_ROOT=/home/vagrant/.krew
    kubectl krew install tap
    kubectl krew install sniff
    kubectl krew install flame
    kubectl krew install minio
    # look to https://kubernetes.io/docs/tasks/debug-application-cluster/debug-running-pod/#ephemeral-container
    # kubectl krew install debug
    chown -R vagrant:vagrant /home/vagrant/.krew

    export NO_WAIT=1

    cd /vagrant/

    git_branch=$(git rev-parse --abbrev-ref HEAD)
    export OPERATOR_RELEASE=$(cat release)
    export BRANCH=${BRANCH:-$git_branch}
    export OPERATOR_NAMESPACE=${OPERATOR_NAMESPACE:-kube-system}
    export OPERATOR_IMAGE=altinity/clickhouse-operator:${OPERATOR_RELEASE}
    export METRICS_EXPORTER_IMAGE=altinity/metrics-exporter:${OPERATOR_RELEASE}

    # devspace
    DEVSPACE_VERSION=$(curl -sL https://github.com/devspace-cloud/devspace/releases/latest -H "Accept: application/json" | jq -r .tag_name)
    wget -c --progress=bar:force:noscroll -O /usr/local/bin/${DEVSPACE_VERSION}-devspace-linux-amd64 "https://github.com/devspace-cloud/devspace/releases/download/${DEVSPACE_VERSION}/devspace-linux-amd64"
    wget -c --progress=bar:force:noscroll -O /usr/local/bin/${DEVSPACE_VERSION}-devspace-linux-amd64.sha256 "https://github.com/devspace-cloud/devspace/releases/download/${DEVSPACE_VERSION}/devspace-linux-amd64.sha256"
    sed -i -E "s/\\/Users.+devspace\\-linux\\-amd64/\\/usr\\/local\\/bin\\/${DEVSPACE_VERSION}-devspace-linux-amd64/g" /usr/local/bin/${DEVSPACE_VERSION}-devspace-linux-amd64.sha256
    sha256sum -c /usr/local/bin/${DEVSPACE_VERSION}-devspace-linux-amd64.sha256
    cp -fv /usr/local/bin/${DEVSPACE_VERSION}-devspace-linux-amd64 /usr/local/bin/devspace
    chmod +x /usr/local/bin/devspace

    # docker build
    export COMPANY_REPO=${COMPANY_REPO:-altinity}
    go mod download -x
    go mod tidy
    go mod vendor
    time docker buildx build -f dockerfile/operator/Dockerfile --platform=linux/amd64 --output=type=image,name=$COMPANY_REPO/clickhouse-operator:$OPERATOR_RELEASE .
    time docker buildx build -f dockerfile/metrics-exporter/Dockerfile --platform=linux/amd64 --output=type=image,name=$COMPANY_REPO/metrics-exporter:$OPERATOR_RELEASE .


    # install clickhouse-operator
    if ! kubectl get deployment clickhouse-operator -n "${OPERATOR_NAMESPACE}" 1>/dev/null 2>/dev/null; then
        cd /vagrant/deploy/operator/
        bash -x ./clickhouse-operator-install.sh
        cd /vagrant
    fi

    # install prometheus-operator + prometheus instance + ServiceMonitor for clickhouse
    export PROMETHEUS_NAMESPACE=${PROMETHEUS_NAMESPACE:-prometheus}
    cd /vagrant/deploy/prometheus/
    kubectl delete ns ${PROMETHEUS_NAMESPACE} || true
    bash -xe ./create-prometheus.sh
    cd /vagrant/

    export MINIO_NAMESPACE=${MINIO_NAMESPACE:-minio}
    cd /vagrant/deploy/minio/
    kubectl delete ns ${MINIO_NAMESPACE} || true
    bash -xe ./install-minio-operator.sh
    bash -xe ./install-minio-tenant.sh
    # kubectl create ns ${MINIO_NAMESPACE}
    # kubectl minio init --namespace ${MINIO_NAMESPACE}
    # kubectl minio tenant create --name minio --namespace ${MINIO_NAMESPACE} --servers 1 --volumes 4 --capacity 10Gi --storage-class standard

    cd /vagrant/

    # install grafana-operator + grafana instance + GrafanaDashboard, GrafanaDatasource for clickhouse
    export GRAFANA_NAMESPACE=${GRAFANA_NAMESPACE:-grafana}
    cd /vagrant/deploy/grafana/grafana-with-grafana-operator/
    kubectl delete ns ${GRAFANA_NAMESPACE} || true
    bash -xe ./install-grafana-operator.sh
    bash -xe ./install-grafana-with-operator.sh
    cd /vagrant

    echo "Wait when clickhouse operator installation finished"
    while [[ $(kubectl get pods --all-namespaces -l app=clickhouse-operator | wc -l) != "2" ]]; do
        printf "."
        sleep 1
    done
    echo "...DONE"

    # open http://localhost:9090/targets and check clickhouse-monitor is exists
    kubectl --namespace="${PROMETHEUS_NAMESPACE}" port-forward --address 0.0.0.0 service/prometheus 9090 </dev/null &>/dev/null &

    # open http://localhost:9093/alerts and check which alerts is exists
    kubectl --namespace="${PROMETHEUS_NAMESPACE}" port-forward --address 0.0.0.0 service/alertmanager 9093 </dev/null &>/dev/null &

    # open http://localhost:3000/ and check prometheus datasource exists and grafana dashboard exists
    kubectl --namespace="${GRAFANA_NAMESPACE}" port-forward --address 0.0.0.0 service/grafana-service 3000 </dev/null &>/dev/null &

    # open http://localhost:8888/chi and check exists clickhouse installations
    kubectl --namespace="${OPERATOR_NAMESPACE}" port-forward --address 0.0.0.0 service/clickhouse-operator-metrics 8888 </dev/null &>/dev/null &

    for image in $(cat ./tests/configs/test-017-multi-version.yaml | yq eval -e ".spec.templates.podTemplates[].spec.containers[].image"); do
        docker pull ${image}
    done

    pip3 install -r /vagrant/tests/requirements.txt

    python3 /vagrant/tests/regression.py --native

    # audit2rbac
    kubectl logs kube-apiserver-minikube -n kube-system | grep audit.k8s.io/v1 > /tmp/audit2rbac.log
    audit2rbac -f /tmp/audit2rbac.log --serviceaccount kube-system:clickhouse-operator > /tmp/audit2rbac.yaml
    # cp -fv /tmp/audit2rbac.yaml /vagrant/deploy/dev/clickhouse-operator-install-yaml-template-02-section-rbac-restricted.yaml

  SHELL
end
