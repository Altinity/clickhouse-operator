#!/bin/bash
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
bash "${CUR_DIR}/install-minio-operator.sh"
bash "${CUR_DIR}/install-minio-tenant.sh"
