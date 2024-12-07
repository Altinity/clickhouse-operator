export MINIO_BACKUP_BUCKET=${MINIO_BACKUP_BUCKET:-clickhouse-backup}

export MINIO_NAMESPACE="${MINIO_NAMESPACE:-minio}"
# look to https://github.com/minio/operator/blob/master/examples/kustomization/base/tenant.yaml
export MINIO_VERSION="${MINIO_VERSION:-RELEASE.2024-10-02T17-50-41Z}"
export MINIO_CLIENT_VERSION="${MINIO_CLIENT_VERSION:-RELEASE.2024-10-29T15-34-59Z}"

export MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minio-access-key}"
export MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minio-secret-key}"
export MINIO_CONSOLE_ACCESS_KEY="${MINIO_CONSOLE_ACCESS_KEY:-minio_console}"
export MINIO_CONSOLE_SECRET_KEY="${MINIO_CONSOLE_SECRET_KEY:-minio_console}"



###########################
##                       ##
##   Functions Section   ##
##                       ##
###########################

function wait_minio_to_start() {
    # Fetch Minio's deployment_name and namespace from params
    local namespace=$1
    local pod_name=$2

    echo -n "Waiting Minio pod '${namespace}/${pod_name}' to start"
    # Check minio tenatna have all pods ready
    while [[ $(kubectl --namespace="${namespace}" get pods | grep "${pod_name}" | grep -c "Running") == "0" ]]; do
        printf "."
        sleep 1
    done
    echo "...DONE"

}


function wait_minio_bucket() {
    # Fetch Minio's job_name and namespace from params
    local namespace=$1
    local job_name=$2

    echo -n "Waiting Minio Job '${namespace}/${job_name}' to complete"
    # Check minio tenatna have all pods ready
    while [[ $(kubectl --namespace="${namespace}" get jobs | grep "${job_name}" | grep -c "1/1") == "0" ]]; do
        printf "."
        sleep 1
    done
    echo "...DONE"
}


CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

kubectl apply -n "${MINIO_NAMESPACE}" -f <(
  envsubst < "$CUR_DIR/minio-tenant-template.yaml"
)

wait_minio_to_start "$MINIO_NAMESPACE" minio-pool-0-0

kubectl apply -n "${MINIO_NAMESPACE}" -f <(
  envsubst < "$CUR_DIR/minio-tenant-create-bucket-template.yaml"
)

wait_minio_bucket "$MINIO_NAMESPACE" minio-create-bucket
