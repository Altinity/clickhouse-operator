#!/bin/bash

LABELS=(\
    "clickhouse=allow" \
    "CHOP=test" \
)

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
    for LABEL in ${LABELS[@]}; do
        #kubectl label nodes [--overwrite=true] <node-name> <label-key>=<label-value>
        kubectl label nodes "${NODE}" "${LABEL}"
    done
done
