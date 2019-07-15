#!/bin/bash

kubectl get nodes | \
grep -v master | \
awk '{print $1}' | \
tail -n +2 | \
while read -r line; do
	NODE=$line
	#kubectl label nodes <node-name> <label-key>=<label-value>
	#kubectl label nodes --overwrite=true $NODE clickhouse=allow
	kubectl label nodes $NODE clickhouse=allow
done

