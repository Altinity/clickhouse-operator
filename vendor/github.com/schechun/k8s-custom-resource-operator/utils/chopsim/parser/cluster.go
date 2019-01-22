package parser

import (
	"strconv"
	"time"
)

func (c *ChiCluster) listInstances() chInstanceDataList {
	switch c.Layout {
	case clusterLayoutStandard:
		instanceCount := c.Rules.ShardsCount * c.Rules.ReplicasCount
		instances := make(chInstanceDataList, instanceCount)
		for i := 0; i < instanceCount; i++ {
			instances[i] = &chInstanceData{
				id:         c.createInstanceID(),
				deployment: &c.Deployment,
			}
		}
		return instances
	case clusterLayoutAdvanced:
		instances := make(chInstanceDataList, 0, c.countShardsWithReplicas())
		for _, shard := range c.Shards {
			if shard.ReplicasCount == shardsReplicasCountInline {
				for _, replica := range shard.Replicas {
					instances = append(instances, &chInstanceData{
						id:         c.createInstanceID(),
						deployment: &replica.Deployment,
					})
				}
				continue
			}
			replicasCount, err := strconv.ParseInt(shard.ReplicasCount, 10, 32)
			if err != nil {
				panic("Unable to parse ReplicasCount value")
			}
			for i := 0; i < int(replicasCount); i++ {
				instances = append(instances, &chInstanceData{
					id:         c.createInstanceID(),
					deployment: &shard.Deployment,
				})
			}
		}
		return instances
	}
	return nil
}

func (c *ChiCluster) createInstanceID() string {
	return strconv.Itoa(int(time.Now().UnixNano()))
}

func (c *ChiCluster) countShardsWithReplicas() int {
	var instanceCount int
	for _, shard := range c.Shards {
		if shard.ReplicasCount == shardsReplicasCountInline {
			instanceCount = instanceCount + len(shard.Replicas)
			continue
		}
		replicasCount, err := strconv.ParseInt(shard.ReplicasCount, 10, 32)
		if err != nil {
			replicasCount = 0
		}
		instanceCount = instanceCount + int(replicasCount)
	}
	return instanceCount
}
