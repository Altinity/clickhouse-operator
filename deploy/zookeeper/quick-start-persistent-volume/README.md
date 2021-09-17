Please note that zookeeper-1-node.yaml and zookeeper-3-node.yaml are configured with following requests:

* cpu: 1
* memory: 4Gi
* storage: 50Gi, with default storage class

This is ok for most of cases, but you may need to change it for heavily loaded apps.

Use zookeeper-1-node-1GB-for-tests-only.yaml for testing, it only requests:

* cpu: 0.5
* memory: 1Gi
* storage: 1Gi, with default storage class