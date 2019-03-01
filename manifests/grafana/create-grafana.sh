#!/bin/bash

kubectl create namespace grafana
kubectl apply -n grafana -f grafana.yaml
