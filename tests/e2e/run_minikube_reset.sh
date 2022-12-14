#!/bin/bash

NODES="${NODES:-"1"}"
echo "Launching cluster with $NODES nodes"
sleep 5

minikube delete && docker system prune -f && minikube start --nodes="${NODES}" && k9s -c ns

