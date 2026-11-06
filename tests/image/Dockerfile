# look `build_docker.sh`
FROM ubuntu:26.04

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
    tzdata \
    psmisc \
    libreadline-dev \
    libicu-dev \
    bsdutils \
    curl \
    liblua5.1-0-dev \
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

ARG YQ_VERSION=4.54.2
RUN wget -q -O /usr/local/bin/yq https://github.com/mikefarah/yq/releases/download/v${YQ_VERSION}/yq_linux_amd64 \
    && chmod +x /usr/local/bin/yq

RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo ${VERSION_CODENAME}) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && apt-get update \
    && apt-get install --no-install-recommends --yes docker-ce docker-ce-cli containerd.io docker-ce-rootless-extras \
    && rm -rf /var/lib/apt/lists/*

ENV MINIKUBE_VERSION=1.39.0

RUN curl -fsSLO https://storage.googleapis.com/minikube/releases/v${MINIKUBE_VERSION}/minikube-linux-amd64 \
    && install minikube-linux-amd64 /usr/local/bin/minikube \
    && rm minikube-linux-amd64

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/python VIRTUAL_ENV=/opt/venv PATH=/opt/venv/bin:$PATH
COPY requirements.txt /home/master/requirements.txt
RUN uv venv --python 3.13 /opt/venv \
    && uv pip install -U -r /home/master/requirements.txt

RUN set -x \
    && addgroup --system dockremap \
    && adduser --system dockremap \
    && adduser dockremap dockremap \
    && echo 'dockremap:165536:65536' >> /etc/subuid \
    && echo 'dockremap:165536:65536' >> /etc/subgid

VOLUME /var/lib/docker
EXPOSE 2375

ENV MINIKUBE_HOME=/home/master/.minikube

ARG K8S_VERSION=1.36.5
ENV K8S_VERSION=${K8S_VERSION}

RUN wget -c --progress=bar:force:noscroll -O /usr/local/bin/kubectl https://dl.k8s.io/release/v${K8S_VERSION}/bin/linux/amd64/kubectl \
   && chmod +x /usr/local/bin/kubectl

# need for debug internal minikube
ARG K9S_VERSION=v0.51.0
RUN cd /tmp \
    && wget -q https://github.com/derailed/k9s/releases/download/${K9S_VERSION}/k9s_Linux_amd64.tar.gz https://github.com/derailed/k9s/releases/download/${K9S_VERSION}/checksums.sha256 \
    && grep ' k9s_Linux_amd64.tar.gz$' checksums.sha256 | sha256sum -c - \
    && tar -zxf k9s_Linux_amd64.tar.gz -C /usr/local/bin k9s \
    && rm -f k9s_Linux_amd64.tar.gz checksums.sha256

RUN adduser --disabled-password --gecos "" master && usermod -a -G docker master

RUN ln -svf /home/master/.minikube /root/.minikube && mkdir -p /home/master/.minikube/cache/preloaded-tarball && mkdir -p /home/master/.kube

ENV KUBECONFIG=/home/master/.kube/config

COPY ./modprobe.sh /usr/local/bin/modprobe
COPY cache/*.dockerimage /var/lib/docker/
COPY cache/*.txt /var/lib/docker/

COPY cache/preloaded-images-k8s-v18-v${K8S_VERSION}-docker-overlay2-amd64.tar.lz4 /home/master/.minikube/cache/preloaded-tarball/
RUN mkdir -p  /home/master/.minikube/cache/kic/  \
    &&  ln -nsfv /var/lib/docker/kicbase.dockerimage /home/master/.minikube/cache/kic/kicbase_v0.0.51@sha256_4a1c825b61479e6c898851ea66f13c620aaeab6002746e95067fc2c4b38a0b24.tar  \
    && mkdir -p /home/master/.minikube/cache/kic  \
    && chown -R master:docker /home/master/  \
    && chmod -R u+wrx /home/master/.minikube

RUN update-alternatives --set iptables /usr/sbin/iptables-legacy  \
    && update-alternatives --set ip6tables /usr/sbin/ip6tables-legacy

# RUN sudo -u master bash -xe dockerd-rootless-setuptool.sh install --skip-iptables

COPY ./dockerd-start.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/dockerd-start.sh

ENTRYPOINT ["dockerd-start.sh"]
