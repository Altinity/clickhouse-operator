// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1

import (
	"math"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
)

// func getMaxNumberOfPodsPerNode
// What is the max number of Pods allowed per Node
// TODO need to support multi-cluster
func getMaxNumberOfPodsPerNode(cr ICustomResource) int {
	maxNumberOfPodsPerNode := 0
	cr.WalkPodTemplates(func(template *PodTemplate) {
		for i := range template.PodDistribution {
			podDistribution := &template.PodDistribution[i]
			if podDistribution.Type == deployment.PodDistributionMaxNumberPerNode {
				maxNumberOfPodsPerNode = podDistribution.Number
			}
		}
	})
	return maxNumberOfPodsPerNode
}

func calcCRAndClusterScopeCycleSizes(cr ICustomResource, maxNumberOfPodsPerNode int) (crScopeCycleSize int, clusterScopeCycleSize int) {
	//          1perNode   2perNode  3perNode  4perNode  5perNode
	// sh1r1    n1   a     n1  a     n1 a      n1  a     n1  a
	// sh1r2    n2   a     n2  a     n2 a      n2  a     n2  a
	// sh1r3    n3   a     n3  a     n3 a      n3  a     n3  a
	// sh2r1    n4   a     n4  a     n4 a      n4  a     n1  b
	// sh2r2    n5   a     n5  a     n5 a      n1  b     n2  b
	// sh2r3    n6   a     n6  a     n1 b      n2  b     n3  b
	// sh3r1    n7   a     n7  a     n2 b      n3  b     n1  c
	// sh3r2    n8   a     n8  a     n3 b      n4  b     n2  c
	// sh3r3    n9   a     n1  b     n4 b      n1  c     n3  c
	// sh4r1    n10  a     n2  b     n5 b      n2  c     n1  d
	// sh4r2    n11  a     n3  b     n1 c      n3  c     n2  d
	// sh4r3    n12  a     n4  b     n2 c      n4  c     n3  d
	// sh5r1    n13  a     n5  b     n3 c      n1  d     n1  e
	// sh5r2    n14  a     n6  b     n4 c      n2  d     n2  e
	// sh5r3    n15  a     n7  b     n5 c      n3  d     n3  e
	// 1perNode = ceil(15 / 1 'cycles num') = 15 'cycle len'
	// 2perNode = ceil(15 / 2 'cycles num') = 8  'cycle len'
	// 3perNode = ceil(15 / 3 'cycles num') = 5  'cycle len'
	// 4perNode = ceil(15 / 4 'cycles num') = 4  'cycle len'
	// 5perNode = ceil(15 / 5 'cycles num') = 3  'cycle len'

	// Number of requested cycles equals to max number of ClickHouses per node, but can't be less than 1
	requestedClusterScopeCyclesNum := maxNumberOfPodsPerNode
	if requestedClusterScopeCyclesNum <= 0 {
		requestedClusterScopeCyclesNum = 1
	}

	crScopeCycleSize = 0 // Unlimited
	clusterScopeCycleSize = 0
	if requestedClusterScopeCyclesNum == 1 {
		// One cycle only requested
		clusterScopeCycleSize = 0 // Unlimited
	} else {
		// Multiple cycles requested
		clusterScopeCycleSize = int(math.Ceil(float64(cr.HostsCount()) / float64(requestedClusterScopeCyclesNum)))
	}

	return crScopeCycleSize, clusterScopeCycleSize
}

// fillSelfCalculatedAddressInfo calculates and fills address info
func fillSelfCalculatedAddressInfo(cr ICustomResource) {
	// What is the max number of Pods allowed per Node
	maxNumberOfPodsPerNode := getMaxNumberOfPodsPerNode(cr)
	chiScopeCycleSize, clusterScopeCycleSize := calcCRAndClusterScopeCycleSizes(cr, maxNumberOfPodsPerNode)

	cr.WalkHostsFullPathAndScope(
		chiScopeCycleSize,
		clusterScopeCycleSize,
		func(
			cr ICustomResource,
			cluster ICluster,
			shard IShard,
			replica IReplica,
			host IHost,
			address *types.HostScopeAddress,
		) error {
			cluster.GetRuntime().GetAddress().SetNamespace(cr.GetNamespace())
			cluster.GetRuntime().GetAddress().SetCRName(cr.GetName())
			cluster.GetRuntime().GetAddress().SetClusterName(cluster.GetName())
			cluster.GetRuntime().GetAddress().SetClusterIndex(address.ClusterIndex)

			shard.GetRuntime().GetAddress().SetNamespace(cr.GetNamespace())
			shard.GetRuntime().GetAddress().SetCRName(cr.GetName())
			shard.GetRuntime().GetAddress().SetClusterName(cluster.GetName())
			shard.GetRuntime().GetAddress().SetClusterIndex(address.ClusterIndex)
			shard.GetRuntime().GetAddress().SetShardName(shard.GetName())
			shard.GetRuntime().GetAddress().SetShardIndex(address.ShardIndex)

			replica.GetRuntime().GetAddress().SetNamespace(cr.GetNamespace())
			replica.GetRuntime().GetAddress().SetCRName(cr.GetName())
			replica.GetRuntime().GetAddress().SetClusterName(cluster.GetName())
			replica.GetRuntime().GetAddress().SetClusterIndex(address.ClusterIndex)
			replica.GetRuntime().GetAddress().SetReplicaName(replica.GetName())
			replica.GetRuntime().GetAddress().SetReplicaIndex(address.ReplicaIndex)

			host.GetRuntime().GetAddress().SetNamespace(cr.GetNamespace())
			// Skip StatefulSet as impossible to self-calculate
			// host.Address.StatefulSet = CreateStatefulSetName(host)
			host.GetRuntime().GetAddress().SetCRName(cr.GetName())
			host.GetRuntime().GetAddress().SetClusterName(cluster.GetName())
			host.GetRuntime().GetAddress().SetClusterIndex(address.ClusterIndex)
			host.GetRuntime().GetAddress().SetShardName(shard.GetName())
			host.GetRuntime().GetAddress().SetShardIndex(address.ShardIndex)
			host.GetRuntime().GetAddress().SetReplicaName(replica.GetName())
			host.GetRuntime().GetAddress().SetReplicaIndex(address.ReplicaIndex)
			host.GetRuntime().GetAddress().SetHostName(host.GetName())
			host.GetRuntime().GetAddress().SetCRScopeIndex(address.CRScopeAddress.Index)
			host.GetRuntime().GetAddress().SetCRScopeCycleSize(address.CRScopeAddress.CycleSpec.Size)
			host.GetRuntime().GetAddress().SetCRScopeCycleIndex(address.CRScopeAddress.CycleAddress.CycleIndex)
			host.GetRuntime().GetAddress().SetCRScopeCycleOffset(address.CRScopeAddress.CycleAddress.Index)
			host.GetRuntime().GetAddress().SetClusterScopeIndex(address.ClusterScopeAddress.Index)
			host.GetRuntime().GetAddress().SetClusterScopeCycleSize(address.ClusterScopeAddress.CycleSpec.Size)
			host.GetRuntime().GetAddress().SetClusterScopeCycleIndex(address.ClusterScopeAddress.CycleAddress.CycleIndex)
			host.GetRuntime().GetAddress().SetClusterScopeCycleOffset(address.ClusterScopeAddress.CycleAddress.Index)
			host.GetRuntime().GetAddress().SetShardScopeIndex(address.ReplicaIndex)
			host.GetRuntime().GetAddress().SetReplicaScopeIndex(address.ShardIndex)

			return nil
		},
	)
}

func fillCRPointer(cr ICustomResource) {
	cr.WalkHostsFullPath(
		func(
			cr ICustomResource,
			cluster ICluster,
			shard IShard,
			replica IReplica,
			host IHost,
			address *types.HostScopeAddress,
		) error {
			cluster.GetRuntime().SetCR(cr)
			shard.GetRuntime().SetCR(cr)
			replica.GetRuntime().SetCR(cr)
			host.GetRuntime().SetCR(cr)
			return nil
		},
	)
}

func FillCR(cr ICustomResource) {
	fillSelfCalculatedAddressInfo(cr)
	fillCRPointer(cr)
}
