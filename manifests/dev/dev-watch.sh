#!/bin/bash

. ./dev-config.sh

watch -n1 "kubectl -n ${DEV_NAMESPACE} get all,configmap,endpoints"
