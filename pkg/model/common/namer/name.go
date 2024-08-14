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

package namer

import (
	"fmt"
	"strconv"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// createShardName returns a name of a shard
func createShardName(shard api.IShard, index int) string {
	return strconv.Itoa(index)
}

// createReplicaName returns a name of a replica.
// Here replica is a CHOp-internal replica - i.e. a vertical slice of hosts field.
// In case you are looking for replica name in terms of a hostname to address particular host as in remote_servers.xml
// you need to take a look on CreateInstanceHostname function
func createReplicaName(replica api.IReplica, index int) string {
	return strconv.Itoa(index)
}

// createHostName returns a name of a host
func createHostName(host *api.Host, shard api.IShard, shardIndex int, replica api.IReplica, replicaIndex int) string {
	return fmt.Sprintf("%s-%s", shard.GetName(), replica.GetName())
}

// createHostTemplateName returns a name of a HostTemplate
func createHostTemplateName(host *api.Host) string {
	return "HostTemplate" + host.Name
}

// createPodHostnameRegexp creates pod hostname regexp.
// For example, `template` can be defined in operator config:
// HostRegexpTemplate: chi-{chi}-[^.]+\\d+-\\d+\\.{namespace}.svc.cluster.local$"
func (n *Namer) createPodHostnameRegexp(chi api.ICustomResource, template string) string {
	return n.macro.Scope(chi).Line(template)
}

// createClusterAutoSecretName creates Secret name where auto-generated secret is kept
func createClusterAutoSecretName(cluster api.ICluster) string {
	if cluster.GetName() == "" {
		return fmt.Sprintf(
			"%s-auto-secret",
			cluster.GetRuntime().GetCR().GetName(),
		)
	}

	return fmt.Sprintf(
		"%s-%s-auto-secret",
		cluster.GetRuntime().GetCR().GetName(),
		cluster.GetName(),
	)
}
