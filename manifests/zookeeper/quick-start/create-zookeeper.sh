#!/bin/bash

kubectl create namespace zoons
kubectl apply -f zookeeper.yaml -n zoons

