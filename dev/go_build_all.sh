#!/bin/bash

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
echo "Build manifests"
source "${CUR_DIR}/build_manifests.sh"

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
echo "Build metrics-exporter"
source "${CUR_DIR}/go_build_metrics_exporter.sh"

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
echo "Build operator"
source "${CUR_DIR}/go_build_operator.sh"

# helm builder is crushing after commit 6bcda277b752343afd814357666cc99a826fdabe
#CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
#echo "Build helm charts"
source "${CUR_DIR}/generate_helm_chart.sh"

#CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
#echo "Run source checker"
#source "${CUR_DIR}/run_gocard.sh"
#
#CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
#echo "Run security checker"
#source "${CUR_DIR}/run_gosec.sh"
