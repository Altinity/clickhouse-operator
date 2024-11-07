#!/usr/bin/env bash

function usage() {
  cat << EOT
 Script splits clickhouse-operator-install-bundle.yaml to separate files and adjusts them to conform the helm standards
 NOTE script requires some pre-installed tools:
 - yq ( https://mikefarah.gitbook.io/yq/ ) > v4.14.x
 - jq ( https://github.com/stedolan/jq )
 - helm-docs ( https://github.com/norwoodj/helm-docs ) > v1.14.x
 - perl ( https://learn.perl.org/installing/ )

 Usage: ./generate_helm_chart.sh
EOT
}

function check_required_tools() {
  for cmd in yq jq helm-docs perl; do
    if ! command -v "${cmd}" &> /dev/null; then
      echo "======================================"
      usage
      echo "======================================"
      echo "The following tool is missing: ${cmd}"
      echo "Please install it."
      echo "Abort."
      exit 1
    fi
  done
}

set -o errexit
set -o nounset
set -o pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
readonly manifest_file_path="${script_dir}/../deploy/operator/clickhouse-operator-install-bundle.yaml"
readonly release_file_path="${script_dir}/../release"
readonly dashboards_path="${script_dir}/../grafana-dashboard"
readonly chart_path="${script_dir}/../deploy/helm/clickhouse-operator"
readonly values_yaml="${chart_path}/values.yaml"

function main() {
  echo "Generate HELM chart"
  # process manifests from install bundle
  local tmpdir
  tmpdir=$(mktemp -d)
  # shellcheck disable=SC2016
  readonly manifest_file_realpath="$(get_abs_filename "${manifest_file_path}")"
  (cd "${tmpdir}" && yq e --no-doc -s '$index' "${manifest_file_realpath}")
  for f in "${tmpdir}"/*.yml; do
    process "${f}"
  done

  # update version in Chart.yaml
  readonly detected_version=$(cat "${release_file_path}")
  readonly chart_yaml="${chart_path}/Chart.yaml"
  yq e '.version = "'"${detected_version}"'"' -i "${chart_yaml}"
  yq e '.appVersion = "'"${detected_version}"'"' -i "${chart_yaml}"

  # process grafana dashboards
  readonly prom_ds='{"current":{"selected":false,"text":"","value":""},"hide":0,"includeAll":false,"multi":false,"name":"ds_prometheus","options":[],"query":"prometheus","queryValue":"","refresh":1,"regex":"","skipUrlSync":false,"type":"datasource"}'
  readonly files_dir="${chart_path}/files"
  rm -f "${files_dir}/*"
  for dashboard in "${dashboards_path}"/*.json; do
    local dashboard_name
    dashboard_name=$(basename "${dashboard}")
    #echo "${dashboard_name}"
    jq '(.templating.list) |= ['"${prom_ds}"'] + .' "${dashboard}" >"${files_dir}/${dashboard_name}"
    perl -pi -e 's/"datasource": "\${DS_PROMETHEUS}"/"datasource": {"type":"prometheus","uid":"\${ds_prometheus}"}/g' "${files_dir}/${dashboard_name}"
    perl -pi -e 's/"datasource": "\$db"/"datasource": {"type":"vertamedia-clickhouse-datasource","uid":"\${db}"}/g' "${files_dir}/${dashboard_name}"
  done

  if [[ $(command -v helm-docs) ]]; then
    helm-docs --skip-version-footer --chart-search-root="${chart_path}" --log-level=warning
  else
    echo "WARNING"
    echo "helm-docs is not available, skip docs generation"
  fi
}

function process() {
  local file="${1}"
  local kind=$(yq e '.kind' "${file}")
  local name=$(yq e '.metadata.name' "${file}")

  local processed_file="${kind}-${name}.yaml"
  local copied_file="${processed_file}"
# crds folder use for `helm install` or `helm upgrade --install` to install CRD before deployment
  if [[ "${kind}" == "CustomResourceDefinition" ]]; then
# additional copy to templates for CRD, to fix `helm upgrade` wrong behavior
#    local templates_dir="${chart_path}/templates/generated"
#    copied_file="${templates_dir}/${processed_file}"
#    mkdir -p "$(dirname "${copied_file}")"
#    cp -f "${file}" "${copied_file}"

    local crds_dir="${chart_path}/crds"
    processed_file="${crds_dir}/${processed_file}"
  else
    local templates_dir="${chart_path}/templates/generated"
    processed_file="${templates_dir}/${processed_file}"
  fi
  #echo $(basename "${processed_file}")
  mkdir -p "$(dirname "${processed_file}")"
  mv -f "${file}" "${processed_file}"

  case ${kind} in
#  CustomResourceDefinition)
#    update_crd_resource "${copied_file}"
#    generate_crd_resource "${processed_file}"
#    ;;
  Service)
    update_service_resource "${processed_file}"
    ;;
  Deployment)
    update_deployment_resource "${processed_file}"
    ;;
  ConfigMap)
    update_configmap_resource "${processed_file}"
    ;;
  ClusterRoleBinding)
    update_clusterrolebinding_resource "${processed_file}"
    ;;
  ClusterRole)
    update_clusterrole_resource "${processed_file}"
    ;;
  ServiceAccount)
    update_serviceaccount_resource "${processed_file}"
    ;;
  Secret)
    update_secret_resource "${processed_file}"
    ;;
  CustomResourceDefinition) ;;

  *)
    echo "Do not know how to process resource kind: ${kind}"
    echo "Abort."
    exit 1
    ;;
  esac
}

function generate_crd_resource() {
  local file="${1}"
  version=$(yq e '.version' "${chart_path}/Chart.yaml") chart_name=$(yq e '.name' "${chart_path}/Chart.yaml") yq e -i '.metadata.labels."app.kubernetes.io/managed-by" = "Helm" | .metadata.labels."helm.sh/chart" = env(chart_name) + "-" + env(version) | .metadata.labels."app.kubernetes.io/name" = env(chart_name) | .metadata.labels."app.kubernetes.io/version"= env(version)' "${file}"
}


function update_crd_resource() {
  local file="${1}"
  yq e -i '.metadata.labels |= "{{ include \"altinity-clickhouse-operator.labels\" . | nindent 4 }}"' "${file}"

  perl -pi -e "s/'{/{/g" "${file}"
  perl -pi -e "s/}'/}/g" "${file}"
}

function update_service_resource() {
  readonly file="${1}"
  readonly name=$(yq e '.metadata.name' "${file}")

  if [ "${name}" != 'clickhouse-operator-metrics' ]; then
    echo "do not know how to process ${name} service"
    exit 1
  fi

  yq e -i '.metadata.name |= "{{ printf \"%s-metrics\" (include \"altinity-clickhouse-operator.fullname\" .) }}"' "${file}"
  yq e -i '.metadata.namespace |= "{{ .Release.Namespace }}"' "${file}"
  yq e -i '.metadata.labels |= "{{ include \"altinity-clickhouse-operator.labels\" . | nindent 4 }}"' "${file}"
  yq e -i '.spec.selector |= "{{ include \"altinity-clickhouse-operator.selectorLabels\" . | nindent 4 }}"' "${file}"

  perl -pi -e "s/'//g" "${file}"
}

function update_deployment_resource() {
  readonly file="${1}"
  readonly name=$(yq e '.metadata.name' "${file}")

  if [ "${name}" != 'clickhouse-operator' ]; then
    echo "do not know how to process ${name} deployment"
    exit 1
  fi

  yq e -i '.metadata.name |= "{{ include \"altinity-clickhouse-operator.fullname\" . }}"' "${file}"
  yq e -i '.metadata.namespace |= "{{ .Release.Namespace }}"' "${file}"
  yq e -i '.metadata.labels |= "{{ include \"altinity-clickhouse-operator.labels\" . | nindent 4 }}"' "${file}"
  yq e -i '.spec.selector.matchLabels |= "{{ include \"altinity-clickhouse-operator.selectorLabels\" . | nindent 6 }}"' "${file}"

  readonly annotations=$(yq e '.spec.template.metadata.annotations' "${file}")
  a_data="${annotations}" yq e -i '.podAnnotations |= env(a_data)' "${values_yaml}"
  yq e -i '.spec.template.metadata.annotations = {}' "${file}"

  yq e -i '.spec.template.metadata.labels |= "{{ include \"altinity-clickhouse-operator.labels\" . | nindent 8 }}"' "${file}"
  yq e -i '.spec.template.metadata.annotations += {"{{ toYaml .Values.podAnnotations | nindent 8 }}": null}' "${file}"
  yq e -i '.spec.template.spec.imagePullSecrets |= "{{ toYaml .Values.imagePullSecrets | nindent 8 }}"' "${file}"
  yq e -i '.spec.template.spec.serviceAccountName |= "{{ include \"altinity-clickhouse-operator.serviceAccountName\" . }}"' "${file}"
  yq e -i '.spec.template.spec.nodeSelector |= "{{ toYaml .Values.nodeSelector | nindent 8 }}"' "${file}"
  yq e -i '.spec.template.spec.affinity |= "{{ toYaml .Values.affinity | nindent 8 }}"' "${file}"
  yq e -i '.spec.template.spec.tolerations |= "{{ toYaml .Values.tolerations | nindent 8 }}"' "${file}"
  yq e -i '.spec.template.spec.securityContext |= "{{ toYaml .Values.podSecurityContext | nindent 8 }}"' "${file}"
  yq e -i '.spec.template.spec.topologySpreadConstraints |= "{{ toYaml .Values.topologySpreadConstraints | nindent 8 }}"' "${file}"

  for cm in $(yq e '.spec.template.spec.volumes[].configMap.name' "${file}"); do
    local prefix='{{ include \"altinity-clickhouse-operator.fullname\" . }}'
    local newCm="${cm/etc-clickhouse-operator/$prefix}"
    newCm="${newCm/etc-keeper-operator/${prefix}-keeper}"
    yq e -i '(.spec.template.spec.volumes[].configMap.name | select(. == "'"${cm}"'") | .) |= "'"${newCm}"'"' "${file}"
    local cmName="${cm/etc-clickhouse-operator-/}"
    cmName="${cmName/etc-keeper-operator-/keeper-}"
    yq e -i '.spec.template.metadata.annotations += {"checksum/'"${cmName}"'": "{{ include (print $.Template.BasePath \"/generated/ConfigMap-'"${cm}"'.yaml\") . | sha256sum }}"}' "${file}"
  done

  yq e -i '.spec.template.spec.containers[0].name |= "{{ .Chart.Name }}"' "${file}"
  yq e -i '.spec.template.spec.containers[0].image |= "{{ .Values.operator.image.repository }}:{{ include \"altinity-clickhouse-operator.operator.tag\" . }}"' "${file}"
  yq e -i '.spec.template.spec.containers[0].imagePullPolicy |= "{{ .Values.operator.image.pullPolicy }}"' "${file}"
  yq e -i '.spec.template.spec.containers[0].resources |= "{{ toYaml .Values.operator.resources | nindent 12 }}"' "${file}"
  yq e -i '.spec.template.spec.containers[0].securityContext |= "{{ toYaml .Values.operator.containerSecurityContext | nindent 12 }}"' "${file}"
  yq e -i '(.spec.template.spec.containers[0].env[] | select(.valueFrom.resourceFieldRef.containerName == "clickhouse-operator") | .valueFrom.resourceFieldRef.containerName) = "{{ .Chart.Name }}"' "${file}"
  yq e -i '.spec.template.spec.containers[0].env += ["{{ with .Values.operator.env }}{{ toYaml . | nindent 12 }}{{ end }}"]' "${file}"

  yq e -i '.spec.template.spec.containers[1].image |= "{{ .Values.metrics.image.repository }}:{{ include \"altinity-clickhouse-operator.metrics.tag\" . }}"' "${file}"
  yq e -i '.spec.template.spec.containers[1].imagePullPolicy |= "{{ .Values.metrics.image.pullPolicy }}"' "${file}"
  yq e -i '.spec.template.spec.containers[1].resources |= "{{ toYaml .Values.metrics.resources | nindent 12 }}"' "${file}"
  yq e -i '.spec.template.spec.containers[1].securityContext |= "{{ toYaml .Values.metrics.containerSecurityContext | nindent 12 }}"' "${file}"
  yq e -i '(.spec.template.spec.containers[1].env[] | select(.valueFrom.resourceFieldRef.containerName == "clickhouse-operator") | .valueFrom.resourceFieldRef.containerName) = "{{ .Chart.Name }}"' "${file}"
  yq e -i '.spec.template.spec.containers[1].env += ["{{ with .Values.metrics.env }}{{ toYaml . | nindent 12 }}{{ end }}"]' "${file}"

  perl -pi -e "s/'{{ toYaml .Values.podAnnotations \| nindent 8 }}': null/{{ toYaml .Values.podAnnotations \| nindent 8 }}/g" "${file}"
  perl -pi -e "s/- '{{ with .Values.operator.env }}{{ toYaml . \| nindent 12 }}{{ end }}'/{{ with .Values.operator.env }}{{ toYaml . \| nindent 12 }}{{ end }}/g" "${file}"
  perl -pi -e "s/- '{{ with .Values.metrics.env }}{{ toYaml . \| nindent 12 }}{{ end }}'/{{ with .Values.metrics.env }}{{ toYaml . \| nindent 12 }}{{ end }}/g" "${file}"
  perl -pi -e 's/(\s+\- name: metrics-exporter)/{{ if .Values.metrics.enabled }}\n$1/g' "${file}"
  perl -pi -e "s/(\s+imagePullSecrets: '\{\{ toYaml \.Values\.imagePullSecrets \| nindent 8 \}\}')/{{ end }}\n\$1/g" "${file}"
  perl -pi -e "s/'//g" "${file}"
}

function update_configmap_resource() {
  readonly file="${1}"
  readonly name=$(yq e '.metadata.name' "${file}")
  local data
  data=$(yq e '.data' "${file}")

  if [ "${name}" = "etc-clickhouse-operator-files" ]; then
    local search='name: "clickhouse-operator"'
    local replace="name: '{{ include \"altinity-clickhouse-operator.fullname\" . }}'"
    data=${data/"${search}"/"${replace}"}

    search='config.yaml: |'
    replace='config.yaml:'
    data=${data/"${search}"/"${replace}"}
  fi

  local name_suffix="${name/etc-clickhouse-operator-/}"
  local name_suffix="${name_suffix/etc-keeper-operator-/keeper-}"
  local camel_cased_name
  camel_cased_name=$(to_camel_case "${name_suffix}")

  yq e -i '.metadata.name |= "{{ printf \"%s-'"${name_suffix}"'\" (include \"altinity-clickhouse-operator.fullname\" .) }}"' "${file}"
  yq e -i '.metadata.namespace |= "{{ .Release.Namespace }}"' "${file}"
  yq e -i '.metadata.labels |= "{{ include \"altinity-clickhouse-operator.labels\" . | nindent 4 }}"' "${file}"
  yq e -i '.data |= "{{ include \"altinity-clickhouse-operator.configmap-data\" (list . .Values.configs.'"${camel_cased_name}"') | nindent 2 }}"' "${file}"

  if [ -z "${data}" ]; then
    yq e -i '.configs.'"${camel_cased_name}"' |= null' "${values_yaml}"
  else
    data_arg="${data}" yq e -i '.configs.'"${camel_cased_name}"' |= env(data_arg)' "${values_yaml}"
  fi

  perl -pi -e "s/'//g" "${file}"
}

function update_clusterrolebinding_resource() {
  readonly file="${1}"
  readonly name=$(yq e '.metadata.name' "${file}")

  if [ "${name}" != 'clickhouse-operator-kube-system' ]; then
    echo "do not know how to process ${name} cluster role binding"
    exit 1
  fi

  yq e -i '.metadata.name |= "{{ include \"altinity-clickhouse-operator.fullname\" . }}"' "${file}"
  yq e -i '.metadata.namespace |= "{{ .Release.Namespace }}"' "${file}"
  yq e -i '.metadata.labels |= "{{ include \"altinity-clickhouse-operator.labels\" . | nindent 4 }}"' "${file}"
  yq e -i '.roleRef.name |= "{{ include \"altinity-clickhouse-operator.fullname\" . }}"' "${file}"
  yq e -i '(.subjects[] | select(.kind == "ServiceAccount")) |= with(. ; .name = "{{ include \"altinity-clickhouse-operator.serviceAccountName\" . }}" | .namespace = "{{ .Release.Namespace }}")' "${file}"

  printf '%s\n%s\n' '{{- if .Values.rbac.create -}}' "$(cat "${file}")" >"${file}"
  printf '%s\n%s\n' "$(cat "${file}")" '{{- end }}' >"${file}"

  perl -pi -e "s/'//g" "${file}"
}

function update_clusterrole_resource() {
  readonly file="${1}"
  readonly name=$(yq e '.metadata.name' "${file}")

  if [ "${name}" != 'clickhouse-operator-kube-system' ]; then
    echo "do not know how to process ${name} cluster role"
    exit 1
  fi

  yq e -i '.metadata.name |= "{{ include \"altinity-clickhouse-operator.fullname\" . }}"' "${file}"
  yq e -i '.metadata.namespace |= "{{ .Release.Namespace }}"' "${file}"
  yq e -i '.metadata.labels |= "{{ include \"altinity-clickhouse-operator.labels\" . | nindent 4 }}"' "${file}"

  yq e -i '(.rules[] | select(.resourceNames | contains(["clickhouse-operator"])) | .resourceNames) = ["{{ include \"altinity-clickhouse-operator.fullname\" . }}"]' "${file}"

  printf '%s\n%s\n' '{{- if .Values.rbac.create -}}' "$(cat "${file}")" >"${file}"
  printf '%s\n%s\n' "$(cat "${file}")" '{{- end }}' >"${file}"

  perl -pi -e "s/'//g" "${file}"
}

function update_serviceaccount_resource() {
  readonly file="${1}"
  readonly name=$(yq e '.metadata.name' "${file}")

  if [ "${name}" != 'clickhouse-operator' ]; then
    echo "do not know how to process ${name} service account"
    exit 1
  fi

  yq e -i '.metadata.name |= "{{ include \"altinity-clickhouse-operator.serviceAccountName\" . }}"' "${file}"
  yq e -i '.metadata.namespace |= "{{ .Release.Namespace }}"' "${file}"
  yq e -i '.metadata.labels |= "{{ include \"altinity-clickhouse-operator.labels\" . | nindent 4 }}"' "${file}"
  yq e -i '.metadata.annotations |= "{{ toYaml .Values.serviceAccount.annotations | nindent 4 }}"' "${file}"

  printf '%s\n%s\n' '{{- if .Values.serviceAccount.create -}}' "$(cat "${file}")" >"${file}"
  printf '%s\n%s\n' "$(cat "${file}")" '{{- end -}}' >"${file}"

  perl -pi -e "s/'//g" "${file}"
}

function update_secret_resource() {
  readonly file="${1}"
  readonly name=$(yq e '.metadata.name' "${file}")

  if [ "${name}" != 'clickhouse-operator' ]; then
    echo "do not know how to process ${name} secret"
    exit 1
  fi

  yq e -i '.metadata.name |= "{{ include \"altinity-clickhouse-operator.fullname\" . }}"' "${file}"
  yq e -i '.metadata.namespace |= "{{ .Release.Namespace }}"' "${file}"
  yq e -i '.metadata.labels |= "{{ include \"altinity-clickhouse-operator.labels\" . | nindent 4 }}"' "${file}"

  yq e -i '.data.username |= "{{ .Values.secret.username | b64enc }}"' "${file}"
  yq e -i '.data.password |= "{{ .Values.secret.password | b64enc }}"' "${file}"

  readonly username=$(yq e '.stringData.username' "${file}")
  yq e -i '.secret.username |= "'"${username}"'"' "${values_yaml}"

  readonly password=$(yq e '.stringData.password' "${file}")
  yq e -i '.secret.password |= "'"${password}"'"' "${values_yaml}"

  yq e -i 'del(.stringData)' "${file}"

  printf '%s\n%s\n' '{{- if .Values.secret.create -}}' "$(cat "${file}")" >"${file}"
  printf '%s\n%s\n' "$(cat "${file}")" '{{- end -}}' >"${file}"

  perl -pi -e "s/'//g" "${file}"
}

function to_camel_case() {
  readonly str="${1}"
  echo "${str}" | awk -F - '{printf "%s", $1; for(i=2; i<=NF; i++) printf "%s", toupper(substr($i,1,1)) substr($i,2); print"";}'
}

function get_abs_filename() {
  readonly filename="${1}"
  echo "$(cd "$(dirname "${filename}")" && pwd)/$(basename "${filename}")"
}

check_required_tools
main
