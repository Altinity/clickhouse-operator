Altinity Operator 0.23.x used an experimental implementation of Keeper resource that is not compatible with 0.24.0 and above. Direct upgrade will result in the loss of Keeper data, so dependent ClickHouse cluster will turn read-only.

Here are some difference for CHK named 'test'.

|    | 0.23.x | 0.24+ |
| --- | ------ | ----- |
| Pod name | test-0 | chk-test-simple-0-0-0 |
| Service name | test | keeper-test |
| PVC name | both-paths-test-0 | default-chk-test-0-0-0 |
| Volume mounts | <pre>- mountPath: /var/lib/clickhouse\_keeper<br>  name: working-dir<br>- mountPath: /var/lib/clickhouse\_keeper/coordination/logs<br>  name: both-paths<br>  subPath: logs<br>- mountPath: /var/lib/clickhouse\_keeper/coordination/snapshots<br>  name: both-paths<br>  subPath: snapshots</pre> | <pre>- mountPath: /var/lib/clickhouse\-keeper<br>  name: default </code> |

There are no backwards compatibility guarantees for experimental features. Migration is possible using a manual procedure if needed.

The biggest problem is volume. In order to remap volume, following steps need to be done:

1. Find Persistent Volume (PV) in old CHK installation
2. Patch it setting persistentVolumeReclaimPolicy to ‘Retain’

`kubectl patch pv $PV -p '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'`

3. Delete old CHK installation
4. Delete old PVC, since it is not deleted automatically
5. Patch PV one more time, removing claimRef. That will make volume available for remounting.

`kubectl patch pv $PV -p '{"spec":{"claimRef": null}}'`

6. Upgrade operator to 0.24.x
7. Deploy new CHK with following changes:
  * Add ‘volumeName’ to volumeClaimTemplate referencing the old volume
  * Add settings to mount logs and raft coordination to folders matching old operator:
   
```
      keeper_server/log_storage_path: /var/lib/clickhouse-keeper/logs
      keeper_server/snapshot_storage_path: /var/lib/clickhouse-keeper/snapshots
```

Also, optionally serviceTemplate can be added matching old name in order to avoid changes in CHI.

Please refer to [this example](https://github.com/Altinity/clickhouse-operator/blob/0.24.0/tests/e2e/manifests/chk/test-051-chk-chop-upgrade-3.yaml) and a tested [sequence of steps](https://github.com/Altinity/clickhouse-operator/blob/9d0fc9c9bb3532e0313b0405b02d147c958d3dff/tests/e2e/test_operator.py#L4868)
