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

// Package chbackup builds the Kubernetes Job/CronJob resources that drive the operator-managed
// clickhouse-backup sidecars for the ClickHouseBackup, ClickHouseBackupSchedule and
// ClickHouseRestore custom resources.
package chbackup

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

// settingMaxTableSizeToDrop is the server setting that bounds the size of a table ClickHouse drops, and
// clickHouseDefaultMaxTableSizeToDrop its default, in bytes.
const (
	settingMaxTableSizeToDrop                  = "max_table_size_to_drop"
	clickHouseDefaultMaxTableSizeToDrop uint64 = 50_000_000_000
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
}

// topologyCluster is one backup-enabled cluster of an installation.
type topologyCluster struct {
	name string
	// storage is the backup storage the sidecars of the cluster keep backups in
	storage string
	// pod holds what a Job inherits from the cluster's first host: its sidecar and its pod template.
	pod    podSettings
	shards []topologyShard
	// sidecar tells how the sidecars of the cluster take the settings that tell where they keep their
	// backups, which every host of the cluster has to take alike for a Job to know them
	sidecar sidecarSettings
	// replicated tells whether the hosts of the cluster have ZooKeeper, which ON CLUSTER DDL needs
	replicated bool
	// maxTableSizeToDrop is the smallest max_table_size_to_drop of the hosts of the cluster, 0 when none
	// of them limits the size of a table it drops
	maxTableSizeToDrop uint64
}

// podSettings are the settings a Job inherits from a host it drives: the image of its clickhouse-backup
// sidecar, as it pulls it, and the identity, security context and registry access admission already
// accepts for the installation's own pods in that namespace.
type podSettings struct {
	// image and imagePullPolicy are the sidecar's: the Job needs bash 4.4 or newer and curl from the image
	image           string
	imagePullPolicy core.PullPolicy
	securityContext *core.PodSecurityContext
	// containerSecurityContext is the sidecar's security context, whose identity the Job takes where
	// the pod level leaves it open
	containerSecurityContext *core.SecurityContext
	imagePullSecrets         []core.LocalObjectReference
	serviceAccountName       string
}

// Topology is the set of backup-enabled clusters, and their hosts, an operation targets. Every Job
// targets one of them; it also knows every other backup-enabled cluster of the installation, whose
// storage paths its own must not collide with.
type Topology struct {
	// installation is the value of the {installation} macro on every host
	installation string
	// namespace is the value of the {namespace} macro on every host
	namespace string
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

	topology := &Topology{installation: chi.GetName(), namespace: chi.GetNamespace()}
	for _, c := range chi.GetSpecT().Configuration.GetClusters() {
		if c.GetBackup().LacksStorage() {
			topology.withoutStorage = append(topology.withoutStorage, c.GetName())
			continue
		}
		if !c.GetBackup().IsEnabled() {
			topology.disabled = append(topology.disabled, c.GetName())
			continue
		}
		topology.enabled = append(topology.enabled, newTopologyCluster(c, chi.GetSpecT().GetConfiguration().GetSettings(), namer, containers))
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
	installationSettings *api.Settings,
	namer interfaces.INameManager,
	containers interfaces.IContainerManager,
) topologyCluster {
	cluster := topologyCluster{
		name:       c.GetName(),
		storage:    c.GetBackup().GetStorageName(),
		replicated: !c.GetZookeeper().IsEmpty(),
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
			cluster.maxTableSizeToDrop = smallerLimit(cluster.maxTableSizeToDrop, hostMaxTableSizeToDrop(installationSettings, host))
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
		if len(cluster.shards) == 0 {
			cluster.pod = newPodSettings(s.Hosts[0], containers)
		}
		cluster.shards = append(cluster.shards, shard)
	}
	return cluster
}

// hostMaxTableSizeToDrop returns the max_table_size_to_drop a host runs with, 0 for no limit. The
// installation-level settings and the host's own ones go to configuration files ClickHouse merges in an
// order of their own, so a host set at both levels is taken by the smaller limit, which refuses no less
// than either. A value that is no number of bytes counts as ClickHouse's default.
func hostMaxTableSizeToDrop(installationSettings *api.Settings, host *api.Host) uint64 {
	set := false
	limit := uint64(0)
	for _, settings := range []*api.Settings{installationSettings, host.Settings} {
		setting := settings.Get(settingMaxTableSizeToDrop)
		if !setting.HasValue() {
			continue
		}
		value, err := strconv.ParseUint(strings.TrimSpace(setting.String()), 10, 64)
		if err != nil {
			value = clickHouseDefaultMaxTableSizeToDrop
		}
		if !set {
			set, limit = true, value
			continue
		}
		limit = smallerLimit(limit, value)
	}
	if !set {
		return clickHouseDefaultMaxTableSizeToDrop
	}
	return limit
}

// smallerLimit returns the smaller of two limits, 0 standing for none.
func smallerLimit(x, y uint64) uint64 {
	if (x == 0) || ((y > 0) && (y < x)) {
		return y
	}
	return x
}

// newPodSettings returns what a Job inherits from a host: the image, its pull policy and the identity of the
// host's clickhouse-backup sidecar, and what its pod template sets at the pod level
func newPodSettings(host *api.Host, containers interfaces.IContainerManager) podSettings {
	sidecar, _ := containers.NewBackupContainer(host)
	settings := podSettings{
		image:                    sidecar.Image,
		imagePullPolicy:          sidecar.ImagePullPolicy,
		containerSecurityContext: sidecar.SecurityContext,
	}
	if template, ok := host.GetPodTemplate(); ok {
		settings.securityContext = template.Spec.SecurityContext
		settings.imagePullSecrets = template.Spec.ImagePullSecrets
		settings.serviceAccountName = template.Spec.ServiceAccountName
	}
	return settings
}

// cluster returns the name of the cluster a Job built from the topology targets: the first one, as a
// Job targets one.
func (t *Topology) cluster() string {
	if (t == nil) || (len(t.clusters) == 0) {
		return ""
	}
	return t.clusters[0].name
}

// storage returns the backup storage of the cluster a Job built from the topology targets.
func (t *Topology) storage() string {
	if (t == nil) || (len(t.clusters) == 0) {
		return ""
	}
	return t.clusters[0].storage
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

// pod returns what the operation Job inherits from a pod template, taken from the first target
// cluster: a Job targets one.
func (t *Topology) pod() podSettings {
	if (t == nil) || (len(t.clusters) == 0) {
		return podSettings{}
	}
	return t.clusters[0].pod
}

// replicated reports whether the cluster an operation Job targets has ZooKeeper, which ON CLUSTER DDL
// needs.
func (t *Topology) replicated() bool {
	return (t != nil) && (len(t.clusters) > 0) && t.clusters[0].replicated
}

// maxTableSizeToDrop returns the max_table_size_to_drop of the cluster an operation Job targets, 0 for no
// limit.
func (t *Topology) maxTableSizeToDrop() uint64 {
	if (t == nil) || (len(t.clusters) == 0) {
		return 0
	}
	return t.clusters[0].maxTableSizeToDrop
}
