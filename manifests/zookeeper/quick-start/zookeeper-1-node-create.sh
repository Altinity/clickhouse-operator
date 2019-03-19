#!/bin/bash

kubectl create namespace zoo1ns
kubectl --namespace=zoo1ns apply -f zookeeper-1-node.yaml

