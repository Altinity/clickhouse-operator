#!/bin/bash

# Get all nodes with output header
# Skip master node
# Skip output header
# Extract node names
# Label each node
kubectl get nodes | \
grep -v master | \
tail -n +2 | \
awk '{print $1}' | \
while read -r LINE; do
    # Label each node
    NODE="${LINE}"
    #kubectl label nodes <node-name> <label-key>=<label-value>
    #kubectl label nodes --overwrite=true "${NODE}" clickhouse=allow
    kubectl label nodes "${NODE}" clickhouse=allow
done
