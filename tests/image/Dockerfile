# docker build -t registry.gitlab.com/altinity-qa/clickhouse/cicd/clickhouse-operator .
FROM ubuntu:20.04

RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
    apt-transport-https \
    software-properties-common \
    htop \
    vim \
    gettext-base \
    ethtool \
    jq \
    socat \
    make \
    gcc \
    g++ \
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


RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
RUN echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt-get update
RUN apt-get install --yes docker-ce docker-ce-cli containerd.io

RUN curl -fsSLo /usr/share/keyrings/kubernetes-archive-keyring.gpg https://packages.cloud.google.com/apt/doc/apt-key.gpg
RUN echo "deb [signed-by=/usr/share/keyrings/kubernetes-archive-keyring.gpg] https://apt.kubernetes.io/ kubernetes-xenial main" | tee /etc/apt/sources.list.d/kubernetes.list
RUN curl -LO https://storage.googleapis.com/minikube/releases/v1.23.0/minikube-linux-amd64
RUN install minikube-linux-amd64 /usr/local/bin/minikube
RUN apt-get update
RUN apt-get install -y kubectl

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN pip3 install urllib3 testflows==1.7.4 PyYAML docker-compose==1.29.1 docker==5.0.0 dicttoxml tzlocal python-dateutil setuptools

ENV GOLANG_VERSION 1.16
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys F6BC817356A3D45E
RUN add-apt-repository ppa:longsleep/golang-backports
RUN apt install --no-install-recommends -y golang-${GOLANG_VERSION}-go
RUN ln -nvsf /usr/lib/go-${GOLANG_VERSION}/bin/go /bin/go
RUN ln -nvsf /usr/lib/go-${GOLANG_VERSION}/bin/gofmt /bin/gofmt

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys CC86BB64
RUN add-apt-repository ppa:rmescandon/yq
RUN apt install --no-install-recommends -y yq

ENV DOCKER_CHANNEL stable
ENV DOCKER_VERSION 20.10.6

RUN set -eux; \
  \
# this "case" statement is generated via "update.sh"
  \
  if ! wget -nv -O docker.tgz "https://download.docker.com/linux/static/${DOCKER_CHANNEL}/x86_64/docker-${DOCKER_VERSION}.tgz"; then \
    echo >&2 "error: failed to download 'docker-${DOCKER_VERSION}' from '${DOCKER_CHANNEL}' for '${x86_64}'"; \
    exit 1; \
  fi; \
  \
  tar --extract \
    --file docker.tgz \
    --strip-components 1 \
    --directory /usr/local/bin/ \
  ; \
  rm docker.tgz; \
  \
  dockerd --version; \
  docker --version

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
RUN wget -c --progress=bar:force:noscroll -O /usr/local/bin/minikube https://github.com/kubernetes/minikube/releases/download/v${MINIKUBE_VERSION}/minikube-linux-amd64
RUN chmod +x /usr/local/bin/minikube


ENV K8S_VERSION=${K8S_VERSION:-1.21.2}

RUN killall kubectl || true
RUN wget -c --progress=bar:force:noscroll -O /usr/local/bin/kubectl https://storage.googleapis.com/kubernetes-release/release/v${K8S_VERSION}/bin/linux/amd64/kubectl \
   && chmod +x /usr/local/bin/kubectl

RUN adduser --disabled-password --gecos "" master && usermod -a -G docker master

RUN ln -svf /home/master/.minikube /root/.minikube && mkdir -p /home/master/.minikube/cache/preloaded-tarball

ENV KUBECONFIG /home/master/.kube/config

COPY ./modprobe.sh /usr/local/bin/modprobe
COPY ./dockerd-start.sh /usr/local/bin/
COPY cache/ch_image.dockerimage /var/lib/docker/
COPY cache/cho.dockerimage /var/lib/docker/
COPY cache/kicbase.dockerimage /var/lib/docker/
COPY cache/m_expo.dockerimage /var/lib/docker/
COPY cache/s_prov.dockerimage /var/lib/docker/
COPY cache/m_expo_old.dockerimage /var/lib/docker/
COPY cache/cho_old.dockerimage /var/lib/docker/
COPY cache/zk.dockerimage /var/lib/docker/
COPY cache/ch_old.dockerimage /var/lib/docker/
COPY cache/preloaded-images-k8s-v12-v1.21.2-docker-overlay2-amd64.tar.lz4 /home/master/.minikube/cache/preloaded-tarball/

RUN chmod +x /usr/local/bin/dockerd-start.sh && chown -R master /home/master/.minikube \
    && chmod -R u+wrx /home/master/.minikube

ENTRYPOINT ["dockerd-start.sh"]
