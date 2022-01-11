ARG TARGETOS
ARG TARGETARCH
ARG BUILD_TYPE="prod"

# ===================
# ===== Builder =====
# ===================

FROM --platform=${BUILDPLATFORM} golang:1.17 AS builder

# Install required packages
RUN apt-get update && apt-get install -y apt-utils && apt-get install -y gettext-base curl
RUN curl -o /usr/bin/yq -sL https://github.com/mikefarah/yq/releases/download/v4.16.2/yq_${TARGETOS}_${TARGETARCH} && \
    chmod +x /usr/bin/yq

# Reconstruct source tree inside docker
WORKDIR /clickhouse-operator
ADD . .

# ./vendor is excluded in .dockerignore, reconstruct it with 'mod' tool
RUN GO111MODULE=on go mod tidy
RUN GO111MODULE=on go mod vendor

# Build operator binary with explicitly specified output
RUN OPERATOR_BIN=/tmp/clickhouse-operator GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" ./dev/go_build_operator.sh

RUN CGO_ENABLED=0 GO111MODULE=on GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" go install -ldflags "-s -w -extldflags '-static'" github.com/go-delve/delve/cmd/dlv@latest

# ======================
# ===== Image Base =====
# ======================

FROM registry.access.redhat.com/ubi8/ubi-minimal AS image-base
RUN microdnf update && microdnf clean all

MAINTAINER "Altinity <support@altinity.com>"

LABEL name="ClickHouse operator" \
      maintainer="support@altinity.com" \
      vendor="Altinity" \
      version="${VERSION}" \
      release="${RELEASE}" \
      summary="ClickHouse operator" \
      description="ClickHouse operator operates ClickHouse clusters in kubernetes"

ADD LICENSE /licenses/

WORKDIR /

# Add config files from local source dir into image
ADD config/config.yaml   /etc/clickhouse-operator/
ADD config/conf.d/*      /etc/clickhouse-operator/conf.d/
ADD config/config.d/*    /etc/clickhouse-operator/config.d/
ADD config/templates.d/* /etc/clickhouse-operator/templates.d/
ADD config/users.d/*     /etc/clickhouse-operator/users.d/

# Copy clickhouse-operator binary into operator image from builder
COPY --from=builder /tmp/clickhouse-operator .
COPY --from=builder /go/bin/dlv .

# =======================
# ===== Image Debug =====
# =======================
FROM image-base AS image-debug
RUN echo "Building DEBUG image"

ENTRYPOINT ["/dlv"]
CMD ["--listen=:2345", "--headless=true", "--api-version=2", "--accept-multiclient", "exec", "/clickhouse-operator", "--", "-logtostderr=true", "-v=5"]

# ======================
# ===== Image Prod =====
# ======================
FROM image AS image-prod
RUN echo "Building PROD image"

USER nobody

# Run /clickhouse-operator -alsologtostderr=true -v=1
# We can specify additional options, such as:
#   --config=/path/to/confi g
#   --kube-config=/path/to/kubeconf
ENTRYPOINT ["/clickhouse-operator"]
CMD ["-logtostderr=true", "-v=1"]
#CMD ["-alsologtostderr=true", "-v=1"]

######################
### Finalize image ###
######################

# Final image is based on what build type specified
FROM image-${BUILD_TYPE} AS final
ARG BUILD_TYPE
RUN echo "Finalizing build AS ${BUILD_TYPE}"
