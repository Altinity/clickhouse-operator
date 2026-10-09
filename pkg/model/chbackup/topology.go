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

// Package chbackup builds the Kubernetes Job/CronJob resources that drive
// clickhouse-backup for the ClickHouseBackup, ClickHouseBackupSchedule and
// ClickHouseRestore custom resources.
package chbackup

import (
	"fmt"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// ShardHosts holds the per-host StatefulSet Service names of a single shard,
// indexed by replica.
type ShardHosts struct {
	Shard int
	Hosts []string
}

// ClusterTopology holds the resolved per-shard host Service names of one cluster.
type ClusterTopology struct {
	Cluster string
	Shards  []ShardHosts
}

// hostServiceName returns the per-host StatefulSet Service name produced by the
// operator for the default naming scheme: "chi-{chi}-{cluster}-{shard}-{replica}".
// The short (namespace-local) name is used on purpose: the backup/restore Job runs
// in the same namespace as the CHI, so DNS resolves it without an FQDN and we avoid
// depending on spec.namespaceDomainPattern.
func hostServiceName(chiName, cluster string, shard, replica int) string {
	return fmt.Sprintf("chi-%s-%s-%d-%d", chiName, cluster, shard, replica)
}

func layoutCounts(c *api.Cluster) (shards, replicas int) {
	shards, replicas = 1, 1
	if l := c.GetLayout(); l != nil {
		if l.ShardsCount > 0 {
			shards = l.ShardsCount
		}
		if l.ReplicasCount > 0 {
			replicas = l.ReplicasCount
		}
	}
	return shards, replicas
}

// Topology resolves the shard/replica host Service names of every cluster of the CHI
// from the cluster layout counts. It is computed from the live CHI at reconcile time.
//
// Known limitation: it assumes the default host naming scheme and layout expressed via
// shardsCount/replicasCount. Clusters that use explicit shard/replica lists or custom
// host names are a documented follow-up.
func Topology(chi *api.ClickHouseInstallation) []ClusterTopology {
	var out []ClusterTopology
	if chi == nil || chi.Spec.Configuration == nil {
		return out
	}
	for _, c := range chi.Spec.Configuration.Clusters {
		if c == nil {
			continue
		}
		shards, replicas := layoutCounts(c)
		ct := ClusterTopology{Cluster: c.Name}
		for s := 0; s < shards; s++ {
			sh := ShardHosts{Shard: s}
			for r := 0; r < replicas; r++ {
				sh.Hosts = append(sh.Hosts, hostServiceName(chi.Name, c.Name, s, r))
			}
			ct.Shards = append(ct.Shards, sh)
		}
		out = append(out, ct)
	}
	return out
}

// AllServices returns the Service names of every host across all clusters/shards/replicas.
func AllServices(top []ClusterTopology) []string {
	var out []string
	for _, ct := range top {
		for _, sh := range ct.Shards {
			out = append(out, sh.Hosts...)
		}
	}
	return out
}

// FirstPerShardServices returns the Service name of the first replica of every shard.
// This is the correct selection for Replicated* table engines, whose data is identical
// across replicas of a shard.
func FirstPerShardServices(top []ClusterTopology) []string {
	var out []string
	for _, ct := range top {
		for _, sh := range ct.Shards {
			if len(sh.Hosts) > 0 {
				out = append(out, sh.Hosts[0])
			}
		}
	}
	return out
}

// Counts returns the total number of shards and the maximum replica count across all
// clusters. Used for restore topology validation.
func Counts(top []ClusterTopology) (shards, replicas int) {
	for _, ct := range top {
		shards += len(ct.Shards)
		for _, sh := range ct.Shards {
			if len(sh.Hosts) > replicas {
				replicas = len(sh.Hosts)
			}
		}
	}
	return shards, replicas
}

// BackupServices returns the host Service names a backup should target for the given
// replica selection.
func BackupServices(top []ClusterTopology, selection api.ReplicaSelection) []string {
	if selection == api.ReplicaSelectionAllReplicas {
		return AllServices(top)
	}
	return FirstPerShardServices(top)
}
