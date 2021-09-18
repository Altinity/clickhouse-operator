# docker build -t registry.gitlab.com/altinity-qa/clickhouse/cicd/clickhouse-operator .
FROM ubuntu:20.04

RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
    apt-transport-https \
    software-properties-common \
    vim \
    gettext-base \
    ethtool \
    jq \
    socat \
    gpg \
    ca-certificates \
    bash \
    btrfs-progs \
    e2fsprogs \
    iptables \
    xfsprogs \
    tar \
    pigz \
    wget \
    git \
    perl \
    iproute2 \
    cgroupfs-mount \
    python3-pip \
    tzdata \
    psmisc \
    libreadline-dev \
    libicu-dev \
    bsdutils \
    curl \
    liblua5.1-dev \
    luajit \
    libssl-dev \
    libcurl4-openssl-dev \
    gdb \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && apt-get clean


RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && apt-get update \
    && apt-get install --yes docker-ce docker-ce-cli containerd.io

RUN curl -fsSLo /usr/share/keyrings/kubernetes-archive-keyring.gpg https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/kubernetes-archive-keyring.gpg] https://apt.kubernetes.io/ kubernetes-xenial main" | tee /etc/apt/sources.list.d/kubernetes.list \
    && curl -LO https://storage.googleapis.com/minikube/releases/v1.23.0/minikube-linux-amd64 \
    && install minikube-linux-amd64 /usr/local/bin/minikube \
    && apt-get update \
    && apt-get install -y kubectl

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY requirements.txt /home/master/requirements.txt
RUN pip3 install -U -r /home/master/requirements.txt

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys CC86BB64 \
    && add-apt-repository ppa:rmescandon/yq \
    && apt install --no-install-recommends -y yq

RUN set -x \
  && addgroup --system dockremap \
    && adduser --system dockremap \
  && adduser dockremap dockremap \
  && echo 'dockremap:165536:65536' >> /etc/subuid \
    && echo 'dockremap:165536:65536' >> /etc/subgid

VOLUME /var/lib/docker
EXPOSE 2375


ENV MINIKUBE_VERSION 1.22.0
ENV MINIKUBE_HOME /home/master/.minikube

ENV K8S_VERSION=${K8S_VERSION:-1.21.2}

RUN wget -c --progress=bar:force:noscroll -O /usr/local/bin/kubectl https://storage.googleapis.com/kubernetes-release/release/v${K8S_VERSION}/bin/linux/amd64/kubectl \
   && chmod +x /usr/local/bin/kubectl

RUN adduser --disabled-password --gecos "" master && usermod -a -G docker master

RUN ln -svf /home/master/.minikube /root/.minikube && mkdir -p /home/master/.minikube/cache/preloaded-tarball && mkdir -p /home/master/.kube

ENV KUBECONFIG /home/master/.kube/config

COPY ./modprobe.sh /usr/local/bin/modprobe
COPY ./dockerd-start.sh /usr/local/bin/
COPY cache/*.dockerimage /var/lib/docker/
COPY cache/preloaded-images-k8s-v12-v1.21.2-docker-overlay2-amd64.tar.lz4 /home/master/.minikube/cache/preloaded-tarball/

RUN chmod +x /usr/local/bin/dockerd-start.sh && chown -R master /home/master/.minikube \
    && chmod -R u+wrx /home/master/.minikube

ENTRYPOINT ["dockerd-start.sh"]
