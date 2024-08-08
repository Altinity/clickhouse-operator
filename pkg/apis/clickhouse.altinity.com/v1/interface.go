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
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ICustomResource interface {
	meta.Object

	IsNonZero() bool

	GetSpecA() any
	GetSpec() ICRSpec
	GetRuntime() ICustomResourceRuntime
	GetRootServiceTemplate() (*ServiceTemplate, bool)
	GetReconciling() *Reconciling

	WalkClusters(f func(cluster ICluster) error) []error
	WalkHosts(func(host *Host) error) []error
	WalkPodTemplates(f func(template *PodTemplate))
	WalkVolumeClaimTemplates(f func(template *VolumeClaimTemplate))
	WalkHostsFullPath(f WalkHostsAddressFn) []error
	WalkHostsFullPathAndScope(crScopeCycleSize int, clusterScopeCycleSize int, f WalkHostsAddressFn) (res []error)

	FindCluster(needle interface{}) ICluster
	FindShard(needleCluster interface{}, needleShard interface{}) IShard
	FindHost(needleCluster interface{}, needleShard interface{}, needleHost interface{}) *Host

	GetHostTemplate(name string) (*HostTemplate, bool)
	GetPodTemplate(name string) (*PodTemplate, bool)
	GetVolumeClaimTemplate(name string) (*VolumeClaimTemplate, bool)
	GetServiceTemplate(name string) (*ServiceTemplate, bool)

	HasAncestor() bool
	GetAncestor() ICustomResource

	IsStopped() bool
	IsTroubleshoot() bool
	IsRollingUpdate() bool

	HostsCount() int
	IEnsureStatus() IStatus
	GetStatus() IStatus
}

type ICRSpec interface {
	GetNamespaceDomainPattern() *types.String
	GetDefaults() *Defaults
	GetConfiguration() IConfiguration
}

type IConfiguration interface {
	GetProfiles() *Settings
	GetQuotas() *Settings
	GetSettings() *Settings
	GetFiles() *Settings
}

type ICustomResourceRuntime interface {
	GetAttributes() *ComparableAttributes
	LockCommonConfig()
	UnlockCommonConfig()
}

type IStatus interface {
	SetAction(string)
	PushAction(string)
	SetError(string)
	PushError(string)
	GetHostsCount() int
	GetHostsCompletedCount() int
	GetHostsAddedCount() int
	GetHostsWithTablesCreated() []string
	PushHostTablesCreated(host string)

	HasNormalizedCRCompleted() bool

	HostUnchanged()
	HostUpdated()
	HostAdded()
	HostFailed()
	HostCompleted()
}

type ICluster interface {
	GetName() string
	GetZookeeper() *ZookeeperConfig
	GetSchemaPolicy() *SchemaPolicy
	GetInsecure() *types.StringBool
	GetSecure() *types.StringBool
	GetSecret() *ClusterSecret
	GetPDBMaxUnavailable() *types.Int32

	WalkShards(f func(index int, shard IShard) error) []error
	WalkHosts(func(host *Host) error) []error

	HostsCount() int

	FindShard(needle interface{}) IShard
	FindHost(needleShard interface{}, needleHost interface{}) *Host

	GetRuntime() IClusterRuntime
	GetServiceTemplate() (*ServiceTemplate, bool)
}

type IClusterRuntime interface {
	GetAddress() IClusterAddress
	GetCR() ICustomResource
	SetCR(cr ICustomResource)
}

type IClusterAddress interface {
	GetNamespace() string
	SetNamespace(string)

	GetCRName() string
	SetCRName(string)

	GetClusterName() string
	SetClusterName(string)

	GetClusterIndex() int
	SetClusterIndex(int)
}

type IShard interface {
	GetName() string
	GetRuntime() IShardRuntime
	GetServiceTemplate() (*ServiceTemplate, bool)
	GetInternalReplication() *types.StringBool
	HasWeight() bool
	GetWeight() int
	HasSettings() bool
	GetSettings() *Settings
	HasFiles() bool
	GetFiles() *Settings
	HasTemplates() bool
	GetTemplates() *TemplatesList

	WalkHosts(func(host *Host) error) []error

	FindHost(needleHost interface{}) *Host
	FirstHost() *Host

	HostsCount() int
}

type IShardRuntime interface {
	GetAddress() IShardAddress
	SetCR(cr ICustomResource)
}

type IShardAddress interface {
	IClusterAddress

	GetShardName() string
	SetShardName(string)

	GetShardIndex() int
	SetShardIndex(int)
}

type IReplica interface {
	GetName() string
	GetRuntime() IReplicaRuntime
	HasSettings() bool
	GetSettings() *Settings
	HasFiles() bool
	GetFiles() *Settings
	HasTemplates() bool
	GetTemplates() *TemplatesList
}

type IReplicaRuntime interface {
	GetAddress() IReplicaAddress
	SetCR(cr ICustomResource)
}

type IReplicaAddress interface {
	IClusterAddress

	GetReplicaName() string
	SetReplicaName(string)

	GetReplicaIndex() int
	SetReplicaIndex(int)
}

type IHost interface {
	GetName() string
	GetRuntime() IHostRuntime
}

type IHostRuntime interface {
	GetAddress() IHostAddress
	GetCR() ICustomResource
	SetCR(cr ICustomResource)
}

type IHostAddress interface {
	IReplicaAddress
	IShardAddress

	GetStatefulSet() string
	GetFQDN() string

	GetHostName() string
	SetHostName(string)

	GetCRScopeIndex() int
	SetCRScopeIndex(int)
	GetCRScopeCycleSize() int
	SetCRScopeCycleSize(int)
	GetCRScopeCycleIndex() int
	SetCRScopeCycleIndex(int)
	GetCRScopeCycleOffset() int
	SetCRScopeCycleOffset(int)
	GetClusterScopeIndex() int
	SetClusterScopeIndex(int)
	GetClusterScopeCycleSize() int
	SetClusterScopeCycleSize(int)
	GetClusterScopeCycleIndex() int
	SetClusterScopeCycleIndex(int)
	GetClusterScopeCycleOffset() int
	SetClusterScopeCycleOffset(int)
	GetShardScopeIndex() int
	SetShardScopeIndex(int)
	GetReplicaScopeIndex() int
	SetReplicaScopeIndex(int)
}

// WalkHostsAddressFn specifies function to walk over hosts
type WalkHostsAddressFn func(
	cr ICustomResource,
	cluster ICluster,
	shard IShard,
	replica IReplica,
	host IHost,
	address *types.HostScopeAddress,
) error
