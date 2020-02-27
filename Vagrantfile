# -*- mode: ruby -*-
# vi: set ft=ruby :
Vagrant.configure(2) do |config|
  config.vm.box = "ubuntu/bionic64"
  config.vm.box_check_update = false

  if Vagrant.has_plugin?("vagrant-vbguest")
    config.vbguest.auto_update = false
  end

  config.vm.define :clickhouse_operator do |clickhouse_operator|
    clickhouse_operator.vm.network "private_network", ip: "172.16.2.99", nic_type: "virtio"
    clickhouse_operator.vm.host_name = "local-altinity-clickhouse-operator"
  end

  config.vm.provider "virtualbox" do |vb|
    vb.gui = false
    vb.memory = "4096"
  end

  config.vm.provision "shell", inline: <<-SHELL
    set -xeuo pipefail

    apt-get update
    apt-get install -y apt-transport-https ca-certificates software-properties-common curl
    # clickhouse
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E0C56BD4
    add-apt-repository "deb http://repo.yandex.ru/clickhouse/deb/stable/ main/"
    # docker
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 8D81803C0EBFCD88
    add-apt-repository "deb https://download.docker.com/linux/ubuntu bionic edge"


    apt-get install --no-install-recommends -y docker-ce
    apt-get install --no-install-recommends -y clickhouse-client
    apt-get install --no-install-recommends -y python3-pip
    apt-get install --no-install-recommends -y htop ethtool mc curl wget jq

    python3 -m pip install -U pip
    rm -rf /usr/bin/pip3
    pip3 install -U setuptools
    pip3 install -U docker-compose
    pip3 install -U -r /vagrant/tests/requirements.txt

    # k9s CLI
    K9S_VERSION=$(curl -sL https://github.com/derailed/k9s/releases/latest -H "Accept: application/json" | jq -r .tag_name)
    curl -L -o /usr/local/bin/k9s_Linux_x86_64.tar.gz https://github.com/derailed/k9s/releases/download/${K9S_VERSION}/k9s_Linux_x86_64.tar.gz
    curl -sL https://github.com/derailed/k9s/releases/download/${K9S_VERSION}/checksums.txt | grep Linux_x86_64.tar.gz > /usr/local/bin/k9s.sha256
    sed -i -e "s/k9s_Linux_x86_64\.tar\.gz/\\/usr\\/local\\/bin\\/k9s_Linux_x86_64\\.tar\\.gz/g" /usr/local/bin/k9s.sha256
    sha256sum -c /usr/local/bin/k9s.sha256
    tar --verbose -zxvf /usr/local/bin/k9s_Linux_x86_64.tar.gz -C /usr/local/bin k9s

    # minikube
    curl -sL -o /usr/local/bin/minikube https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
    chmod +x /usr/local/bin/minikube

    minikube config set vm-driver none
    minikube start --vm-driver=none
    minikube addons enable ingress
    minikube addons enable ingress-dns
    minikube addons enable metrics-server
    ln -svf $(find /var/lib/minikube/binaries/ -type f -name kubectl) /bin/kubectl

    export BRANCH=${BRANCH:-master}
    export OPERATOR_NAMESPACE=${OPERATOR_NAMESPACE:-kube-system}
    export OPERATOR_IMAGE=${OPERATOR_IMAGE:-altinity/clickhouse-operator:latest}
    export METRICS_EXPORTER_IMAGE=${METRICS_EXPORTER_IMAGE:-altinity/metrics-exporter:latest}

    cd /vagrant/

    if ! kubectl get deployment clickhouse-operator -n "${OPERATOR_NAMESPACE}" 1>/dev/null 2>/dev/null; then
        bash -x ./deploy/operator-web-installer/clickhouse-operator-install.sh
    fi

    export PROMETHEUS_NAMESPACE=${PROMETHEUS_NAMESPACE:-prometheus}
    cd /vagrant/deploy/prometheus/
    bash -x ./create-prometheus.sh
    cd /vagrant/

    export GRAFANA_NAMESPACE=${GRAFANA_NAMESPACE:-grafana}
    cd /vagrant/deploy/grafana/
    bash -x ./install-grafana-operator.sh
    bash -x ./install-grafana-with-operator.sh
    cd /vagrant

    while [[ $(kubectl get pods --all-namespaces -l app=clickhouse-operator | wc -l) != "2" ]]; do
        echo .
        sleep 1
    done

    # kubectl --namespace=${PROMETHEUS_NAMESPACE} port-forward service/prometheus 9090
    # open http://localhost:9090/targets and check clickhouse-monitor is exists
    # kubectl --namespace="${GRAFANA_NAMESPACE}" port-forward service/grafana-service 3000
    # open http://localhost:3000/ and check prometheus datasource exists and grafana dashboard exists
    # kubectl --namespace="${OPERATOR_NAMESPACE}" port-forward service/clickhouse-operator-metrics 8888
    # open http://localhost:3000/chi and check exists

    python3 /vagrant/tests/test.py
    python3 /vagrant/tests/test_examples.py
    python3 /vagrant/tests/test_metrics_exporter.py
  SHELL
end