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

// Package chbackup holds the model of the operator-managed backup: the topology, names, schedules, retention and
// storage path check the operator drives the clickhouse-backup sidecars by for the ClickHouseBackup,
// ClickHouseBackupSchedule and ClickHouseRestore custom resources.
package chbackup

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	core "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

var (
	// ErrClusterNotFound is returned when the requested cluster is not part of the installation.
	ErrClusterNotFound = errors.New("cluster not found")
	// ErrBackupDisabled is returned when no targeted cluster has operator-managed backup enabled.
	ErrBackupDisabled = errors.New("backup is not enabled on the target")
	// ErrNoBackupStorage is returned when the targeted clusters turn backup on without the settings of a storage,
	// which leaves their backup off.
	ErrNoBackupStorage = errors.New("backup is enabled without a storage on the target")
	// ErrNoTargetHosts is returned when the targeted clusters have no hosts.
	ErrNoTargetHosts = errors.New("no target hosts")
)

// topologyShard lists the host Service names of one shard, in replica order.
type topologyShard struct {
	// name is the value of the {shard} macro on the hosts of the shard
	name  string
	hosts []string
	// macros are, of every host, the macros the operator gives it that differ between the replicas of the shard, see
	// PathHost
	macros []map[string]string
}

// topologyCluster is one backup-enabled cluster of an installation.
type topologyCluster struct {
	name string
	// configSecret is the Secret of the clickhouse-backup configuration the sidecars of the cluster keep backups by
	configSecret string
	shards       []topologyShard
	// sidecar tells how the sidecars of the cluster take the settings that tell where they keep their
	// backups, which every host of the cluster has to take alike for the operator to know them
	sidecar sidecarSettings
	// preferredReplica is the index of the replica of each shard a backup of one replica per shard runs on
	preferredReplica int
	// naming is the naming template of the backups of the cluster, "" for the default naming
	naming string
	// endpoints are how the operator reaches the ClickHouse of every host of the cluster, by host, without the address
	endpoints map[string]HostEndpoint
}

// HostEndpoint is how the operator reaches the ClickHouse of a host, as it does for schema propagation.
type HostEndpoint struct {
	// Address is the name of the host in the DNS domain of the namespace of the installation
	Address string
	// HTTPPort and HTTPSPort are the ports ClickHouse serves HTTP and HTTPS on, 0 for none
	HTTPPort  int
	HTTPSPort int
	// TLS is the TLS configuration of the cluster for connections to ClickHouse
	TLS *api.ClusterSecurityClickHouseTLS
}

// Topology is the set of backup-enabled clusters, and their hosts, an operation targets. Every run
// targets one of them; it also knows every other backup-enabled cluster of the installation, whose
// storage paths its own must not collide with.
type Topology struct {
	// installation is the value of the {installation} macro on every host
	installation string
	// namespace is the value of the {namespace} macro on every host
	namespace string
	// domain is the DNS domain of the namespace of the installation, which the host names resolve in from any
	// namespace, as the operator reaches them
	domain string
	// clusters are the clusters the operation targets, in spec order
	clusters []topologyCluster
	// enabled are all backup-enabled clusters of the installation, in spec order
	enabled []topologyCluster
	// disabled names the clusters of the installation without backup
	disabled []string
	// withoutStorage names the clusters of the installation that turn backup on without a storage,
	// which leaves their backup off
	withoutStorage []string
}

// NewTopology resolves the hosts of every backup-enabled cluster of a normalized installation,
// restricted to the named cluster when cluster is not empty.
//
// The installation must be normalized - templates applied, layouts expanded into explicit
// shards and hosts, cluster backup settings inherited - as the operator stores it once a
// reconcile completes. Host names come from the operator's own namer, so generateName patterns
// in service templates and shortened names resolve exactly as the host Services were created.
func NewTopology(normalized *api.ClickHouseInstallation, cluster string) (*Topology, error) {
	if normalized == nil {
		return nil, ErrNoTargetHosts
	}

	chi, err := prepareNormalized(normalized)
	if err != nil {
		return nil, err
	}
	namer := managers.NewNameManager(managers.NameManagerTypeClickHouse)
	containers := managers.NewContainerManager(managers.ContainerManagerTypeClickHouse)

	topology := &Topology{installation: chi.GetName(), namespace: chi.GetNamespace(), domain: namespaceDomain(chi)}
	for _, c := range chi.GetSpecT().Configuration.GetClusters() {
		if c.GetBackup().LacksStorage() {
			topology.withoutStorage = append(topology.withoutStorage, c.GetName())
			continue
		}
		if !c.GetBackup().IsEnabled() {
			topology.disabled = append(topology.disabled, c.GetName())
			continue
		}
		topology.enabled = append(topology.enabled, newTopologyCluster(c, namer, containers))
	}
	return topology.restrict(cluster)
}

// ForCluster returns the topology restricted to one of the clusters it targets, or why that cluster
// cannot be targeted.
func (t *Topology) ForCluster(cluster string) (*Topology, error) {
	if !slices.Contains(t.ClusterNames(), cluster) {
		if err := t.backupOff(cluster); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}
	return t.restrict(cluster)
}

// backupOff tells why backup is off on the named cluster of the installation, nil when it is not a cluster
// of the installation whose backup is off.
func (t *Topology) backupOff(cluster string) error {
	switch {
	case slices.Contains(t.withoutStorage, cluster):
		return fmt.Errorf("%w: %s", ErrNoBackupStorage, cluster)
	case slices.Contains(t.disabled, cluster):
		return fmt.Errorf("%w: %s", ErrBackupDisabled, cluster)
	}
	return nil
}

// restrict returns a copy of the topology that targets the named cluster, or every backup-enabled
// cluster for "".
func (t *Topology) restrict(cluster string) (*Topology, error) {
	restricted := &Topology{
		installation:   t.installation,
		namespace:      t.namespace,
		domain:         t.domain,
		enabled:        t.enabled,
		disabled:       t.disabled,
		withoutStorage: t.withoutStorage,
	}
	for _, c := range t.enabled {
		if (cluster == "") || (c.name == cluster) {
			restricted.clusters = append(restricted.clusters, c)
		}
	}
	if cluster != "" {
		if err := t.backupOff(cluster); err != nil {
			return nil, err
		}
	}
	switch {
	case (cluster != "") && (len(restricted.clusters) == 0):
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	case (len(restricted.clusters) == 0) && (len(t.withoutStorage) > 0):
		return nil, fmt.Errorf("%w: %s", ErrNoBackupStorage, strings.Join(t.withoutStorage, ", "))
	case len(restricted.clusters) == 0:
		return nil, ErrBackupDisabled
	case len(restricted.allReplicas()) == 0:
		return nil, ErrNoTargetHosts
	}
	return restricted, nil
}

// ClusterNames returns the names of the clusters the topology targets, in spec order.
func (t *Topology) ClusterNames() []string {
	if t == nil {
		return nil
	}
	var names []string
	for _, c := range t.clusters {
		names = append(names, c.name)
	}
	return names
}

// EnabledClusterNames returns the names of every backup-enabled cluster of the installation, in spec
// order, whatever clusters the topology targets.
func (t *Topology) EnabledClusterNames() []string {
	if t == nil {
		return nil
	}
	var names []string
	for _, c := range t.enabled {
		names = append(names, c.name)
	}
	return names
}

// ClustersWithoutStorage returns the names of the clusters of the installation that turn backup on without a
// storage, in spec order, whatever clusters the topology targets. An operation of every cluster leaves them out.
func (t *Topology) ClustersWithoutStorage() []string {
	if t == nil {
		return nil
	}
	return slices.Clone(t.withoutStorage)
}

// BackupRequestedClusters returns the names of the clusters of a normalized installation that turn backup on, in
// spec order, none for none: the backup-enabled ones, and those whose backup a missing storage leaves off.
func BackupRequestedClusters(normalized *api.ClickHouseInstallation) []string {
	return clusterNamesOf(normalized, (*api.ClusterBackup).IsRequested)
}

// BackupClustersWithoutStorage returns the names of the clusters of a normalized installation that turn backup
// on without a storage, which leaves their backup off, in spec order, none for none.
func BackupClustersWithoutStorage(normalized *api.ClickHouseInstallation) []string {
	return clusterNamesOf(normalized, (*api.ClusterBackup).LacksStorage)
}

// clusterNamesOf returns the names of the clusters of a normalized installation whose backup the filter takes, in
// spec order.
func clusterNamesOf(normalized *api.ClickHouseInstallation, filter func(*api.ClusterBackup) bool) []string {
	if normalized == nil {
		return nil
	}
	var names []string
	for _, cluster := range normalized.GetSpecT().GetConfiguration().GetClusters() {
		if filter(cluster.GetBackup()) {
			names = append(names, cluster.GetName())
		}
	}
	return names
}

// prepareNormalized returns an addressable copy of a normalized installation. Template indexes
// and host runtime addresses do not survive serialization, an index comes back as an empty shell
// that ignores additions, so an installation read back from storage lacks what the namer resolves
// host Service names from; they are rebuilt here.
//
// The copy is made through the serialized form the operator stores rather than DeepCopy: an
// installation normalized in memory links its clusters, shards and hosts back to itself, which
// DeepCopy follows without end.
func prepareNormalized(normalized *api.ClickHouseInstallation) (*api.ClickHouseInstallation, error) {
	data, err := json.Marshal(normalized)
	if err != nil {
		return nil, err
	}
	chi := &api.ClickHouseInstallation{}
	if err := json.Unmarshal(data, chi); err != nil {
		return nil, err
	}

	if templates := chi.GetSpecT().GetTemplates(); templates != nil {
		templates.ServiceTemplatesIndex = api.NewServiceTemplatesIndex()
		indexTemplates(templates.ServiceTemplates, templates.ServiceTemplatesIndex.Set)
		templates.PodTemplatesIndex = api.NewPodTemplatesIndex()
		indexTemplates(templates.PodTemplates, templates.PodTemplatesIndex.Set)
	}
	api.FillCR(chi)
	return chi, nil
}

// indexTemplates sets every template of a list into an index under its name.
func indexTemplates[T any, PT interface {
	*T
	GetName() string
}](templates []T, set func(name string, template *T)) {
	for i := range templates {
		set(PT(&templates[i]).GetName(), &templates[i])
	}
}

// newTopologyCluster resolves the hosts of a cluster of a normalized installation, whose own settings,
// the server settings at the installation level, are given.
func newTopologyCluster(
	c *api.Cluster,
	namer interfaces.INameManager,
	containers interfaces.IContainerManager,
) topologyCluster {
	cluster := topologyCluster{
		name:             c.GetName(),
		configSecret:     c.GetBackup().GetStorageConfigSecretName(),
		preferredReplica: c.GetBackup().GetPreferredReplica(),
		naming:           c.GetBackup().GetNaming(),
		endpoints:        map[string]HostEndpoint{},
	}
	if c.GetLayout() == nil {
		return cluster
	}
	var first string
	for _, s := range c.GetLayout().Shards {
		shard := topologyShard{name: s.GetName()}
		for _, host := range s.Hosts {
			service := namer.Name(interfaces.NameStatefulSetService, host)
			shard.hosts = append(shard.hosts, service)
			shard.macros = append(shard.macros, map[string]string{
				MacroReplica:         namer.Name(interfaces.NamePodHostname, host),
				MacroAllShardedShard: strconv.Itoa(host.Runtime.Address.CHIScopeIndex),
			})
			endpoint := HostEndpoint{TLS: c.GetSecurity().GetClickHouse().GetTLS()}
			if host.HTTPPort.HasValue() {
				endpoint.HTTPPort = host.HTTPPort.IntValue()
			}
			if host.HTTPSPort.HasValue() {
				endpoint.HTTPSPort = host.HTTPSPort.IntValue()
			}
			cluster.endpoints[service] = endpoint
			sidecar := newSidecarSettings(host, containers)
			switch {
			case first == "":
				first, cluster.sidecar = service, sidecar
			case cluster.sidecar.unverifiable != "":
			case sidecar.unverifiable != "":
				cluster.sidecar = sidecar
			case !sidecar.equal(cluster.sidecar):
				// Kept as a guard: every host builds its sidecar on one container, so only what the operator sets per
				// host could differ
				cluster.sidecar = cluster.sidecar.cannotVerify(fmt.Sprintf("hosts %s and %s run their %s sidecars with different settings",
					first, service, config.ClickHouseBackupContainerName))
			}
		}
		if len(shard.hosts) == 0 {
			continue
		}
		cluster.shards = append(cluster.shards, shard)
	}
	return cluster
}

// cluster returns the name of the cluster an operation built from the topology targets: the first one, as a
// an operation targets one.
func (t *Topology) cluster() string {
	if (t == nil) || (len(t.clusters) == 0) {
		return ""
	}
	return t.clusters[0].name
}

// configSecret returns the configuration Secret of the cluster an operation built from the topology targets.
func (t *Topology) configSecret() string {
	if (t == nil) || (len(t.clusters) == 0) {
		return ""
	}
	return t.clusters[0].configSecret
}

// firstPerShard returns the first host of every shard. Replicas of a shard hold the same
// replicated data, so one host per shard is enough to back it up or restore it.
func (t *Topology) firstPerShard() []string {
	if t == nil {
		return nil
	}
	var hosts []string
	for _, cluster := range t.clusters {
		for _, shard := range cluster.shards {
			hosts = append(hosts, shard.hosts[0])
		}
	}
	return hosts
}

// preferredPerShard returns the preferred replica of every shard, backup.preferredReplica, or its first one when the
// shard has fewer replicas: the replicas a backup of one replica per shard runs on.
func (t *Topology) preferredPerShard() []string {
	if t == nil {
		return nil
	}
	var hosts []string
	for _, cluster := range t.clusters {
		for _, shard := range cluster.shards {
			if cluster.preferredReplica < len(shard.hosts) {
				hosts = append(hosts, shard.hosts[cluster.preferredReplica])
			} else {
				hosts = append(hosts, shard.hosts[0])
			}
		}
	}
	return hosts
}

// backupHosts returns the hosts a backup with the given replica selection makes its parts on: every replica, or the
// preferred replica of every shard, see preferredPerShard.
func (t *Topology) backupHosts(selection api.ReplicaSelection) []string {
	if selection == api.ReplicaSelectionAllReplicas {
		return t.allReplicas()
	}
	return t.preferredPerShard()
}

// allReplicas returns every host of every shard.
func (t *Topology) allReplicas() []string {
	if t == nil {
		return nil
	}
	var hosts []string
	for _, cluster := range t.clusters {
		for _, shard := range cluster.shards {
			hosts = append(hosts, shard.hosts...)
		}
	}
	return hosts
}

// hosts returns the hosts a backup with the given replica selection targets.
func (t *Topology) hosts(selection api.ReplicaSelection) []string {
	if selection == api.ReplicaSelectionAllReplicas {
		return t.allReplicas()
	}
	return t.firstPerShard()
}

// Naming returns the naming template of the backups of the cluster an operation built from the topology targets, "" for the
// default naming.
func (t *Topology) Naming() string {
	if (t == nil) || (len(t.clusters) == 0) {
		return ""
	}
	return t.clusters[0].naming
}

// ClusterNamings returns the naming template of every cluster the topology targets, by cluster, "" for the default
// naming.
func (t *Topology) ClusterNamings() map[string]string {
	namings := map[string]string{}
	if t == nil {
		return namings
	}
	for _, cluster := range t.clusters {
		namings[cluster.name] = cluster.naming
	}
	return namings
}

// Installation returns the name of the installation of the topology, the value of {chi} in a naming template.
func (t *Topology) Installation() string {
	if t == nil {
		return ""
	}
	return t.installation
}

// Namespace returns the namespace of the installation of the topology, the value of {namespace} in a naming template.
func (t *Topology) Namespace() string {
	if t == nil {
		return ""
	}
	return t.namespace
}

// Cluster returns the name of the cluster an operation built from the topology targets.
func (t *Topology) Cluster() string {
	return t.cluster()
}

// namespaceDomainDefault is the DNS domain of a namespace, as the operator reaches the hosts of an installation that
// sets no namespaceDomainPattern: absolute, which spares the search suffixes of the resolver.
const namespaceDomainDefault = "%s.svc.cluster.local."

// namespaceDomain returns the DNS domain of the namespace of the installation, as the operator names the hosts of it
// it connects to: by its namespaceDomainPattern when it sets one.
func namespaceDomain(chi *api.ClickHouseInstallation) string {
	pattern := namespaceDomainDefault
	if p := chi.GetSpecT().GetNamespaceDomainPattern(); p.HasValue() {
		pattern = p.Value()
	}
	return fmt.Sprintf(pattern, chi.GetNamespace())
}

// Address returns the name the operator reaches the host by from its own namespace: the host name in the DNS domain of
// the namespace of the installation.
func (t *Topology) Address(host string) string {
	if (t == nil) || (t.domain == "") {
		return host
	}
	return host + "." + t.domain
}

// ShardOf returns the shard of the host, as <cluster>/<shard>, "" for a host of no backup-enabled cluster of the
// installation: a backup is made on a shard once it is made on any replica of it.
func (t *Topology) ShardOf(host string) string {
	if t == nil {
		return ""
	}
	for _, c := range t.enabled {
		for _, shard := range c.shards {
			if slices.Contains(shard.hosts, host) {
				return c.name + "/" + shard.name
			}
		}
	}
	return ""
}

// Endpoint returns how the operator reaches the ClickHouse of the host, a backup-enabled host of the installation.
func (t *Topology) Endpoint(host string) HostEndpoint {
	endpoint := HostEndpoint{}
	if t != nil {
		for _, c := range t.enabled {
			if found, ok := c.endpoints[host]; ok {
				endpoint = found
				break
			}
		}
	}
	endpoint.Address = t.Address(host)
	return endpoint
}

// ConfigSecret returns the configuration Secret of the cluster an operation built from the topology targets.
func (t *Topology) ConfigSecret() string {
	return t.configSecret()
}

// FirstPerShard returns the first host of every shard.
func (t *Topology) FirstPerShard() []string {
	return t.firstPerShard()
}

// AllReplicas returns every host of every shard.
func (t *Topology) AllReplicas() []string {
	return t.allReplicas()
}

// BackupHosts returns the hosts a backup with the given replica selection makes its parts on, see backupHosts.
func (t *Topology) BackupHosts(selection api.ReplicaSelection) []string {
	return t.backupHosts(selection)
}

// Hosts returns the hosts a backup with the given replica selection targets.
func (t *Topology) Hosts(selection api.ReplicaSelection) []string {
	return t.hosts(selection)
}

// RunNaming returns how the backups of the cluster an operation built from the topology targets are named on its
// hosts.
func (t *Topology) RunNaming() Naming {
	naming := Naming{Template: t.Naming(), Cluster: t.cluster(), Installation: t.installation, Namespace: t.namespace,
		ShardOf: map[string]int{}}
	if (t == nil) || (len(t.clusters) == 0) {
		return naming
	}
	// The shards of system.clusters are those of the layout, in its order, numbered from 1
	for i, shard := range t.clusters[0].shards {
		naming.Shards = append(naming.Shards, shard.name)
		for _, host := range shard.hosts {
			naming.ShardOf[host] = i
		}
	}
	return naming
}

// SettingResolver resolves an env var of the sidecar of a cluster as kubelet does for the sidecar: its value, or the
// key of the Secret or the ConfigMap it refers to, false for an optional source that lacks it.
type SettingResolver func(source core.EnvVar) (string, bool, error)

// PathCheck returns what the storage path check reads of every backup-enabled cluster of the installation, the
// settings of their sidecars resolved, for an operation on the cluster the topology targets.
func (t *Topology) PathCheck(operation string, selection api.ReplicaSelection, resolve SettingResolver) (*PathCheck, error) {
	check := &PathCheck{
		Installation:          t.installation,
		Namespace:             t.namespace,
		Operation:             operation,
		ReplicaSelection:      selection,
		DefaultPath:           ptr.To(chop.Config().Backup.StoragePath),
		DefaultObjectDiskPath: ptr.To(chop.Config().Backup.StorageObjectDiskPath),
		NamingTemplate:        t.Naming(),
	}
	for index, cluster := range t.enabled {
		if cluster.name == t.cluster() {
			check.Self = index
		}
		pathCluster := PathCluster{Name: cluster.name, Unverifiable: cluster.sidecar.unverifiable, Settings: map[string]*string{}}
		for i, shard := range cluster.shards {
			pathCluster.Shards = append(pathCluster.Shards, shard.name)
			for j, host := range shard.hosts {
				pathCluster.Hosts = append(pathCluster.Hosts, PathHost{Name: host, Shard: i, Macros: shard.macros[j]})
			}
		}
		if pathCluster.Unverifiable == "" {
			for _, key := range PathCheckKeys {
				for _, source := range cluster.sidecar.sources[key] {
					value, set, err := resolve(source)
					if err != nil {
						return nil, err
					}
					if set {
						pathCluster.Settings[key] = ptr.To(value)
					}
				}
			}
		}
		check.Clusters = append(check.Clusters, pathCluster)
	}
	return check, nil
}
