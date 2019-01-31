#!/bin/bash

kubectl create namespace zoons
for f in 01-service-client-access.yaml  02-headless-service.yaml  03-pod-disruption-budget.yaml  04-storageclass-zookeeper.yaml  05-stateful-set.yaml; do
    kubectl apply -f $f -n zoons
done

