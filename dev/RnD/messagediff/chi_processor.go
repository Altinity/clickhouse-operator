package main

import (
	"fmt"
	. "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"gopkg.in/d4l3k/messagediff.v1"
)

func processor(diff *messagediff.Diff) {
	clusterIndex := -1
	shardIndex := -1
	replicaIndex := -1
	structField := ""
	for pPath := range diff.Added {
		for i := range *pPath {
			pathNode := (*pPath)[i]
			switch pathNode.(type) {
			case messagediff.StructField:
				structField = string(pathNode.(messagediff.StructField))
				fmt.Printf("field: pathNode[%d]=%v\n", i, structField)
			case messagediff.SliceIndex:
				idx := int(pathNode.(messagediff.SliceIndex))
				fmt.Printf("idx: pathNode[%d]=%v\n", i, idx)
				switch structField {
				case "Hosts":
					replicaIndex = idx
				case "Shards":
					shardIndex = idx
				case "Clusters":
					clusterIndex = idx
				}

			case messagediff.MapKey:
				// Should not be used
				key := pathNode.(messagediff.MapKey).Key
				fmt.Printf("key: pathNode[%d]=%v\n", i, key)
			}
		}
		value := diff.Added[pPath]
		fmt.Printf("value of %d %d %d = %v\n", clusterIndex, shardIndex, replicaIndex, value)
	}
}

func ReplicaAdd(replica ChiReplica) {
	fmt.Printf("ReplicaAdd() %d %v\n", replica.Port, replica.Deployment)
	Reconfigure()
}

func ReplicaRemove() {
	Reconfigure()
}

func ReplicaModify() {
	Reconfigure()
}

func ShardAdd(shard ChiShard) {
	fmt.Printf("ShardAdd() %v %v %v", shard.Weight, shard.InternalReplication, shard.Hosts)
	Reconfigure()
}

func ShardRemove() {
	Reconfigure()
}

func ShardModify() {
	Reconfigure()
}

func ClusterAdd(cluster ChiCluster) {
	fmt.Printf("ClusterAdd() %v %v", cluster.Name, cluster.Layout.Shards)
	Reconfigure()
}

func ClusterRemove() {
	Reconfigure()
}

func ClusterModify() {
	Reconfigure()
}

func Reconfigure() {
	fmt.Println("Reconfigure CHI")
}
