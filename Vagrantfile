# -*- mode: ruby -*-
# vi: set ft=ruby :

def total_cpus
  require 'etc'
  Etc.nprocessors
end


Vagrant.configure(2) do |config|
  config.vm.box = "ubuntu/bionic64"
  config.vm.box_check_update = false
  config.vm.synced_folder ".", "/vagrant", type: "nfs"


  if Vagrant.has_plugin?("vagrant-vbguest")
    config.vbguest.auto_update = false
  end

  config.vm.define :clickhouse_operator do |clickhouse_operator|
    clickhouse_operator.vm.network "private_network", ip: "172.16.2.99", nic_type: "virtio"
    # port forwarding works only when pair with kubectl port-forward
    clickhouse_operator.vm.network "forwarded_port", guest_ip: "127.0.0.1", guest: 3000, host_ip: "127.0.0.1", host: 3000
    clickhouse_operator.vm.network "forwarded_port", guest_ip: "127.0.0.1", guest: 8888, host_ip: "127.0.0.1", host: 8888
    clickhouse_operator.vm.network "forwarded_port", guest_ip: "127.0.0.1", guest: 9090, host_ip: "127.0.0.1", host: 9090

    clickhouse_operator.vm.host_name = "local-altinity-clickhouse-operator"
    # vagrant plugin install vagrant-disksize
    clickhouse_operator.disksize.size = '50GB'
  end

  config.vm.provider "virtualbox" do |vb|
    vb.gui = false
    vb.cpus = total_cpus
    vb.memory = "4096"
    vb.default_nic_type = "virtio"
    vb.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
    vb.customize ["modifyvm", :id, "--natdnsproxy1", "on"]
    vb.customize ["modifyvm", :id, "--ioapic", "on"]
  end

  config.vm.provision "shell", inline: <<-SHELL
    set -xeuo pipefail
    export DEBIAN_FRONTEND=noninteractive

    apt-get update
    apt-get install --no-install-recommends -y apt-transport-https ca-certificates software-properties-common curl
    apt-get install --no-install-recommends -y htop ethtool mc curl wget jq socat git

    # yq
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys CC86BB64
    add-apt-repository ppa:rmescandon/yq
    apt-get install --no-install-recommends -y yq

    # clickhouse
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E0C56BD4
    add-apt-repository "deb http://repo.clickhouse.tech/deb/stable/ main/"
    apt-get install --no-install-recommends -y clickhouse-client

    # docker
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 8D81803C0EBFCD88
    add-apt-repository "deb https://download.docker.com/linux/ubuntu bionic edge"
    apt-get install --no-install-recommends -y docker-ce

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


    # minikube
    wget -c --progress=bar:force:noscroll -O /usr/local/bin/minikube https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
    chmod +x /usr/local/bin/minikube
    # required for k8s 1.18+
    apt-get install -y conntrack

    K8S_VERSION=${K8S_VERSION:-1.17.3}
    minikube config set vm-driver none
    minikube config set kubernetes-version ${K8S_VERSION}
    minikube start
    minikube addons enable ingress
    minikube addons enable ingress-dns
    minikube addons enable metrics-server
    ln -svf $(find /var/lib/minikube/binaries/ -type f -name kubectl) /bin/kubectl

    cd /vagrant/

    git_branch=$(git rev-parse --abbrev-ref HEAD)
    export OPERATOR_RELEASE=$(cat release)
    export BRANCH=${BRANCH:-$git_branch}
    export OPERATOR_NAMESPACE=${OPERATOR_NAMESPACE:-kube-system}
    export OPERATOR_IMAGE=altinity/clickhouse-operator:${OPERATOR_RELEASE}
    export METRICS_EXPORTER_IMAGE=altinity/metrics-exporter:${OPERATOR_RELEASE}

    if ! kubectl get deployment clickhouse-operator -n "${OPERATOR_NAMESPACE}" 1>/dev/null 2>/dev/null; then
        cd /vagrant/deploy/operator/
        bash -x ./clickhouse-operator-install.sh
        cd /vagrant
    fi

    export PROMETHEUS_NAMESPACE=${PROMETHEUS_NAMESPACE:-prometheus}
    cd /vagrant/deploy/prometheus/
    kubectl delete ns ${PROMETHEUS_NAMESPACE}
    bash -xe ./create-prometheus.sh
    cd /vagrant/

    export GRAFANA_NAMESPACE=${GRAFANA_NAMESPACE:-grafana}
    cd /vagrant/deploy/grafana/grafana-with-grafana-operator/
    kubectl delete ns ${GRAFANA_NAMESPACE}
    bash -xe ./install-grafana-operator.sh
    bash -xe ./install-grafana-with-operator.sh
    cd /vagrant

    echo "Wait when clickhouse operator installation finished"
    while [[ $(kubectl get pods --all-namespaces -l app=clickhouse-operator | wc -l) != "2" ]]; do
        printf "."
        sleep 1
    done
    echo "...DONE"

    # kubectl --namespace=${PROMETHEUS_NAMESPACE} port-forward service/prometheus 9090
    # open http://localhost:9090/targets and check clickhouse-monitor is exists
    # kubectl --namespace="${GRAFANA_NAMESPACE}" port-forward service/grafana-service 3000
    # open http://localhost:3000/ and check prometheus datasource exists and grafana dashboard exists
    # kubectl --namespace="${OPERATOR_NAMESPACE}" port-forward service/clickhouse-operator-metrics 8888
    # open http://localhost:3000/chi and check exists

    for image in $(cat ./tests/configs/test-017-multi-version.yaml | yq r - "spec.templates.podTemplates[*].spec.containers[*].image"); do
        docker pull ${image}
    done

    pip3 install -r /vagrant/tests/requirements.txt

    # Run tests without circleci
    # python3 /vagrant/tests/test.py --only=operator/*
    # python3 /vagrant/tests/test_examples.py
    # python3 /vagrant/tests/test_metrics_exporter.py

    # Circle CI
    # TODO wait when resolve https://github.com/CircleCI-Public/circleci-cli/issues/394
    # TODO wait when resolve https://github.com/CircleCI-Public/circleci-cli/issues/402
    curl -fLSs https://raw.githubusercontent.com/CircleCI-Public/circleci-cli/master/install.sh | VERSION=0.1.6893 bash -x

    # circleci optimizations for speedup run
    mkdir -p /circleci
    mkdir -v -m 0777 -p /circleci/home/circleci/.minikube /circleci/var/lib/docker /circleci/root/.cache

    # Run tests local with circleci
    time circleci local execute -v ${PWD}:/workdir -v /circleci/home/circleci/.minikube:/home/circleci/.minikube -v /circleci/var/lib/docker:/var/lib/docker -v /circleci/root/.cache:/root/.cache -e COMPANY_REPO=${COMPANY_REPO:-altinity} --job=integration_tests


  SHELL
end
