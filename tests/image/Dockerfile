# look `build_docker.sh`
FROM ubuntu:22.04

RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
    apt-transport-https \
    software-properties-common \
    vim \
    iputils-ping \
    gettext-base \
    ethtool \
    jq \
    sed \
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
    gdb  \
    sudo \
    uidmap \
    slirp4netns \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && apt-get clean

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys CC86BB64 \
    && add-apt-repository ppa:rmescandon/yq \
    && apt install --no-install-recommends -y yq

RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) test" | tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && apt-get update \
    && apt-get install --no-install-recommends --yes docker-ce docker-ce-cli containerd.io docker-ce-rootless-extras

ENV MINIKUBE_VERSION 1.27.1

RUN curl -fsSLo /usr/share/keyrings/kubernetes-archive-keyring.gpg https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/kubernetes-archive-keyring.gpg] https://apt.kubernetes.io/ kubernetes-xenial main" | tee /etc/apt/sources.list.d/kubernetes.list \
    && curl -LO https://storage.googleapis.com/minikube/releases/v${MINIKUBE_VERSION}/minikube-linux-amd64 \
    && install minikube-linux-amd64 /usr/local/bin/minikube

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY requirements.txt /home/master/requirements.txt
RUN pip3 install -U -r /home/master/requirements.txt

RUN set -x \
    && addgroup --system dockremap \
    && adduser --system dockremap \
    && adduser dockremap dockremap \
    && echo 'dockremap:165536:65536' >> /etc/subuid \
    && echo 'dockremap:165536:65536' >> /etc/subgid

VOLUME /var/lib/docker
EXPOSE 2375

ENV MINIKUBE_HOME /home/master/.minikube

ENV K8S_VERSION=${K8S_VERSION:-1.25.3}

RUN wget -c --progress=bar:force:noscroll -O /usr/local/bin/kubectl https://storage.googleapis.com/kubernetes-release/release/v${K8S_VERSION}/bin/linux/amd64/kubectl \
   && chmod +x /usr/local/bin/kubectl

# need for debug internal minikube
RUN K9S_VERSION=$(curl -sL https://github.com/derailed/k9s/releases/latest -H "Accept: application/json" | jq -r .tag_name)  \
    && wget -c --progress=bar:force:noscroll -O /usr/local/bin/k9s_${K9S_VERSION}_Linux_x86_64.tar.gz https://github.com/derailed/k9s/releases/download/${K9S_VERSION}/k9s_Linux_x86_64.tar.gz  \
    && curl -sL https://github.com/derailed/k9s/releases/download/${K9S_VERSION}/checksums.txt | grep Linux_x86_64.tar.gz > /usr/local/bin/k9s.sha256  \
    && sed -i -e "s/k9s_Linux_x86_64\.tar\.gz/\\/usr\\/local\\/bin\\/k9s_${K9S_VERSION}_Linux_x86_64\\.tar\\.gz/g" /usr/local/bin/k9s.sha256  \
    && sha256sum -c /usr/local/bin/k9s.sha256  \
    && tar --verbose -zxvf /usr/local/bin/k9s_${K9S_VERSION}_Linux_x86_64.tar.gz -C /usr/local/bin k9s

RUN adduser --disabled-password --gecos "" master && usermod -a -G docker master

RUN ln -svf /home/master/.minikube /root/.minikube && mkdir -p /home/master/.minikube/cache/preloaded-tarball && mkdir -p /home/master/.kube

ENV KUBECONFIG /home/master/.kube/config

COPY ./modprobe.sh /usr/local/bin/modprobe
COPY cache/*.dockerimage /var/lib/docker/
COPY cache/*.txt /var/lib/docker/

COPY cache/preloaded-images-k8s-v18-v${K8S_VERSION}-docker-overlay2-amd64.tar.lz4 /home/master/.minikube/cache/preloaded-tarball/
RUN mkdir -p  /home/master/.minikube/cache/kic/  \
    &&  ln -nsfv /var/lib/docker/kicbase.dockerimage /home/master/.minikube/cache/kic/kicbase_v0.0.35@sha256_e6f9b2700841634f3b94907f9cfa6785ca4409ef8e428a0322c1781e809d311b.tar  \
    && mkdir -p /home/master/.minikube/cache/kic  \
    && chown -R master:docker /home/master/  \
    && chmod -R u+wrx /home/master/.minikube

RUN update-alternatives --set iptables /usr/sbin/iptables-legacy  \
    && update-alternatives --set ip6tables /usr/sbin/ip6tables-legacy

# RUN sudo -u master bash -xe dockerd-rootless-setuptool.sh install --skip-iptables

COPY ./dockerd-start.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/dockerd-start.sh

ENTRYPOINT ["dockerd-start.sh"]
