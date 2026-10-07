FROM scratch

ARG BUNDLE_PACKAGE=altinity-clickhouse-operator
ARG BUNDLE_CHANNELS=stable
ARG BUNDLE_DEFAULT_CHANNEL=stable
ARG OPENSHIFT_VERSIONS

LABEL operators.operatorframework.io.bundle.mediatype.v1="registry+v1" \
      operators.operatorframework.io.bundle.manifests.v1="manifests/" \
      operators.operatorframework.io.bundle.metadata.v1="metadata/" \
      operators.operatorframework.io.bundle.package.v1="${BUNDLE_PACKAGE}" \
      operators.operatorframework.io.bundle.channels.v1="${BUNDLE_CHANNELS}" \
      operators.operatorframework.io.bundle.channel.default.v1="${BUNDLE_DEFAULT_CHANNEL}" \
      com.redhat.openshift.versions="${OPENSHIFT_VERSIONS}"

COPY deploy/certified-operator/generated/manifests /manifests/
COPY deploy/certified-operator/generated/metadata /metadata/
