#!/bin/bash
minikube delete && docker system prune -f && minikube start && k9s -c ns

