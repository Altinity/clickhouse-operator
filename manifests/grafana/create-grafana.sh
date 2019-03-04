#!/bin/bash

# Let's setup all grafana-related stuff into dedicated namespace called "grafana"
kubectl create namespace grafana

# Setup grafana into dedicated namespace
kubectl apply --namespace=grafana -f grafana.yaml
