#!/bin/bash

ZK_NAMESPACE="${ZK_NAMESPACE:-zoons}"

kubectl -n "${ZK_NAMESPACE}" get all,pv,pvc -o wide
