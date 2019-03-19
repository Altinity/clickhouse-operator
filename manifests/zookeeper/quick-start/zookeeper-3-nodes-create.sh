#!/bin/bash

kubectl create namespace zoo3ns
kubectl --namespace=zoo3ns apply -f zookeeper-3-nodes.yaml

