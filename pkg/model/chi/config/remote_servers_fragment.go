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

package config

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	commonConfig "github.com/altinity/clickhouse-operator/pkg/model/common/config"
)

type remoteServersShard struct {
	Index               int
	InternalReplication string
	Weight              *int
	ReplicasXML         []string
}

type remoteServersTopology struct {
	ClusterName string
	SecretXML   string
	Shards      []remoteServersShard
}

func (c *Generator) buildRemoteServersTopology(selector *commonConfig.HostSelector) []remoteServersTopology {
	if selector == nil {
		selector = defaultSelectorIncludeAll()
	}

	result := make([]remoteServersTopology, 0)

	c.cr.WalkClusters(func(cluster chi.ICluster) error {
		if c.clusterHostsNum(cluster, selector) < 1 {
			return nil
		}

		t := remoteServersTopology{
			ClusterName: cluster.GetName(),
			SecretXML:   c.renderClusterSecret(cluster.GetSecret()),
		}

		cluster.WalkShards(func(index int, shard chi.IShard) error {
			if c.shardHostsNum(shard, selector) < 1 {
				return nil
			}

			r := make([]string, 0)
			shard.WalkHosts(func(host *chi.Host) error {
				if !selector.Include(host) {
					return nil
				}
				r = append(r, c.renderReplicaXML(host))
				return nil
			})
			if len(r) == 0 {
				return nil
			}

			sh := remoteServersShard{
				Index:               index,
				InternalReplication: shard.GetInternalReplication().String(),
				ReplicasXML:         r,
			}
			if shard.HasWeight() {
				w := shard.GetWeight()
				sh.Weight = &w
			}
			t.Shards = append(t.Shards, sh)
			return nil
		})

		if len(t.Shards) > 0 {
			result = append(result, t)
		}
		return nil
	})

	result = append(result, c.buildAutoTopologies(selector)...)
	return result
}

func (c *Generator) buildAutoTopologies(selector *commonConfig.HostSelector) []remoteServersTopology {
	if c.chiHostsNum(selector) < 1 {
		return nil
	}

	res := make([]remoteServersTopology, 0)

	allReplicated := remoteServersTopology{ClusterName: OneShardAllReplicasClusterName}
	replicas := make([]string, 0)
	c.cr.WalkHosts(func(host *chi.Host) error {
		if selector.Include(host) {
			replicas = append(replicas, c.renderReplicaXML(host))
		}
		return nil
	})
	if len(replicas) > 0 {
		allReplicated.Shards = append(allReplicated.Shards, remoteServersShard{
			Index:               0,
			InternalReplication: "true",
			ReplicasXML:         replicas,
		})
		res = append(res, allReplicated)
	}

	allSharded := remoteServersTopology{ClusterName: AllShardsOneReplicaClusterName}
	if firstCluster := c.cr.FindCluster(0); firstCluster != nil {
		allSharded.SecretXML = c.renderClusterSecret(firstCluster.GetSecret())
	}
	index := 0
	c.cr.WalkHosts(func(host *chi.Host) error {
		if selector.Include(host) {
			allSharded.Shards = append(allSharded.Shards, remoteServersShard{
				Index:               index,
				InternalReplication: "false",
				ReplicasXML:         []string{c.renderReplicaXML(host)},
			})
			index++
		}
		return nil
	})
	if len(allSharded.Shards) > 0 {
		res = append(res, allSharded)
	}

	allClusters := remoteServersTopology{ClusterName: AllClustersClusterName}
	index = 0
	c.cr.WalkClusters(func(cluster chi.ICluster) error {
		cluster.WalkShards(func(_ int, shard chi.IShard) error {
			if c.shardHostsNum(shard, selector) < 1 {
				return nil
			}
			r := make([]string, 0)
			shard.WalkHosts(func(host *chi.Host) error {
				if selector.Include(host) {
					r = append(r, c.renderReplicaXML(host))
				}
				return nil
			})
			if len(r) == 0 {
				return nil
			}
			allClusters.Shards = append(allClusters.Shards, remoteServersShard{
				Index:               index,
				InternalReplication: shard.GetInternalReplication().String(),
				ReplicasXML:         r,
			})
			index++
			return nil
		})
		return nil
	})
	if len(allClusters.Shards) > 0 {
		res = append(res, allClusters)
	}

	return res
}

func (c *Generator) renderClusterSecret(secret *chi.ClusterSecret) string {
	if secret == nil {
		return ""
	}
	switch secret.Source() {
	case chi.ClusterSecretSourcePlaintext:
		return fmt.Sprintf("        <secret>%s</secret>\n", secret.Value)
	case chi.ClusterSecretSourceSecretRef, chi.ClusterSecretSourceAuto:
		return fmt.Sprintf("        <secret from_env=\"%s\" />\n", InternodeClusterSecretEnvName)
	default:
		return ""
	}
}

func (c *Generator) renderReplicaXML(host *chi.Host) string {
	b := &bytes.Buffer{}
	c.getRemoteServersReplica(host, b)
	return b.String()
}

func (c *Generator) renderShardXML(shard remoteServersShard) string {
	b := &bytes.Buffer{}
	b.WriteString("            <shard>\n")
	b.WriteString(fmt.Sprintf("                <internal_replication>%s</internal_replication>\n", shard.InternalReplication))
	if shard.Weight != nil {
		b.WriteString(fmt.Sprintf("                <weight>%d</weight>\n", *shard.Weight))
	}
	for _, replicaXML := range shard.ReplicasXML {
		b.WriteString(replicaXML)
	}
	b.WriteString("            </shard>\n")
	return b.String()
}

func (c *Generator) streamRemoteServersFragments(topology remoteServersTopology, thresholdBytes, maxFragments int) ([]RemoteServersFragment, error) {
	if len(topology.Shards) == 0 {
		return nil, nil
	}

	header := fmt.Sprintf("<%s>\n    <remote_servers>\n        <%s>\n", xmlTagYandex, topology.ClusterName)
	if topology.SecretXML != "" {
		header += topology.SecretXML
	}
	footer := fmt.Sprintf("        </%s>\n    </remote_servers>\n</%s>\n", topology.ClusterName, xmlTagYandex)
	// Overhead includes static XML wrappers around the fragment payload.
	overhead := len(header) + len(footer)

	fragments := make([]RemoteServersFragment, 0)
	for i := 0; i < len(topology.Shards); {
		payload := &strings.Builder{}
		// payloadBytes counts dynamic shard/replica XML only (without wrappers).
		payloadBytes := 0
		start := i

		for ; i < len(topology.Shards); i++ {
			shardXML := c.renderShardXML(topology.Shards[i])
			candidatePayload := payloadBytes + len(shardXML)
			candidateTotal := candidatePayload + overhead
			if candidateTotal > thresholdBytes && payloadBytes > 0 {
				break
			}
			if candidateTotal > thresholdBytes {
				return nil, fmt.Errorf("remote_servers shard is too large: cluster=%s shard=%d size=%d threshold=%d", topology.ClusterName, topology.Shards[i].Index, candidateTotal, thresholdBytes)
			}
			payload.WriteString(shardXML)
			payloadBytes = candidatePayload
		}

		if payloadBytes == 0 {
			return nil, fmt.Errorf("unable to build remote_servers fragment for cluster=%s", topology.ClusterName)
		}

		end := i - 1
		xml := header + payload.String() + footer
		fragments = append(fragments, RemoteServersFragment{
			Cluster:      topology.ClusterName,
			ShardStart:   topology.Shards[start].Index,
			ShardEnd:     topology.Shards[end].Index,
			Index:        topology.Shards[start].Index,
			XML:          xml,
			PayloadBytes: payloadBytes,
			// TotalBytes includes payload + static wrappers/metadata.
			TotalBytes: len(xml),
		})

		if len(fragments) > maxFragments {
			return nil, fmt.Errorf("remote_servers fragments limit exceeded: cluster=%s max=%d", topology.ClusterName, maxFragments)
		}
	}

	return fragments, nil
}

func sortRemoteServersFragments(fragments []RemoteServersFragment) {
	sort.Slice(fragments, func(i, j int) bool {
		if fragments[i].Cluster != fragments[j].Cluster {
			return fragments[i].Cluster < fragments[j].Cluster
		}
		return fragments[i].ShardStart < fragments[j].ShardStart
	})
}
