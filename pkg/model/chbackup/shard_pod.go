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

package chbackup

import (
	"errors"
	"fmt"
	"net"
	"path"
	"slices"
	"strings"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sLabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	clickhouse_altinity_com "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/backupconfig"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	chiCreator "github.com/altinity/clickhouse-operator/pkg/model/chi/creator"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Labels and annotations of the backup Pod of a shard. The Pod carries none of the labels of the
// installation's own pods: the Services of the installation select their pods by those, and would send
// ClickHouse clients to the backup Pod.
const (
	// LabelBackupShard names the shard of the cluster the backup Pod works on.
	LabelBackupShard = clickhouse_altinity_com.APIGroupName + "/backup-shard"
	// LabelBackupComponent tells what a Pod or Service built here is.
	LabelBackupComponent = clickhouse_altinity_com.APIGroupName + "/backup-component"
	// BackupComponentShardPod is the LabelBackupComponent of the backup Pod of a shard.
	BackupComponentShardPod = "shard-pod"
	// BackupComponentShardService is the LabelBackupComponent of the headless Service of the backup Pods of a
	// cluster.
	BackupComponentShardService = "shard-service"
	// LabelApp is the key of the app label the operator puts on the pods it makes. The backup Pod carries a value of
	// its own, ShardPodAppValue, which no selector of the installation's objects matches, while one cache of pods
	// holds the pods of both kinds.
	LabelApp = clickhouse_altinity_com.APIGroupName + "/app"
	// ShardPodAppValue is the LabelApp of the backup Pod of a shard.
	ShardPodAppValue = "chop-backup"
	// InstallationPodAppValue is the LabelApp of the pods of an installation.
	InstallationPodAppValue = "chop"

	// AnnotationShardPodReplica names the pod of the replica the backup Pod works next to.
	AnnotationShardPodReplica = clickhouse_altinity_com.APIGroupName + "/backup-replica-pod"
	// AnnotationShardPodReplicaUID is the UID of that pod: a replica pod created anew, on whatever node, is
	// another pod, and the backup Pod is built anew next to it.
	AnnotationShardPodReplicaUID = clickhouse_altinity_com.APIGroupName + "/backup-replica-pod-uid"
	// AnnotationShardPodSpecHash is the fingerprint of the Pod as the operator built it. A Pod spec cannot be
	// changed, so a Pod whose fingerprint differs from the one the operator builds now is replaced.
	AnnotationShardPodSpecHash = clickhouse_altinity_com.APIGroupName + "/backup-pod-spec-hash"
)

const (
	shardPodNameSuffix = "bk"
	// shardServiceNameSuffix ends the name of the headless Service of the backup Pods of a cluster.
	shardServiceNameSuffix = "backup"
	// shardPodNameMaxLength keeps the name a valid host name, which a Pod name becomes, and the name of the Service
	// a valid DNS label, which its subdomain becomes.
	shardPodNameMaxLength = 63
	// shardPodTerminationGracePeriod is how long the Pod has to stop: the server of clickhouse-backup ends what it
	// does on SIGTERM.
	shardPodTerminationGracePeriod = int64(30)
	// hostnameLabel is the well-known node label whose value is the node name, which the Pod is pinned to.
	hostnameLabel = "kubernetes.io/hostname"
	// shardPodReadinessPeriodSeconds paces the readiness probe of the REST API of the backup Pod.
	shardPodReadinessPeriodSeconds = int32(5)
)

// Reasons the backup Pod of a shard cannot be built, which the operator reports as they are.
var (
	// ErrShardPodNoPersistentData tells that the data of ClickHouse is not on a volume a second pod can mount: an
	// emptyDir, a generic ephemeral volume, or the filesystem of the container.
	ErrShardPodNoPersistentData = errors.New("the data of ClickHouse is not on a persistent volume")
	// ErrShardPodVolumeUnshareable tells that a volume of the replica pod can be mounted by that pod alone.
	ErrShardPodVolumeUnshareable = errors.New("a volume of the replica pod cannot be mounted by a second pod")
	// ErrShardPodNotScheduled tells that the replica pod has no node yet.
	ErrShardPodNotScheduled = errors.New("the replica pod is not scheduled to a node")
	// ErrShardPodNoClickHouse tells that the replica pod runs no ClickHouse container.
	ErrShardPodNoClickHouse = errors.New("the replica pod has no ClickHouse container")
	// ErrShardPodBackupDisabled tells that the cluster of the host has no backup.
	ErrShardPodBackupDisabled = errors.New("backup is not enabled on the cluster")
)

// ShardPodSource is what the backup Pod of a shard is built from.
type ShardPodSource struct {
	// Host is the host of the replica the Pod works next to, of the normalized installation.
	Host *api.Host
	// ReplicaPod is the running pod of that host. The Pod copies its volumes, identity and placement from it, never
	// from the spec: what the pod runs is what clickhouse-backup has to see.
	ReplicaPod *core.Pod
	// Claims are the PersistentVolumeClaims of the replica pod, by name. A claim missing here is taken as it
	// stands in the pod: its access modes are not checked.
	Claims map[string]*core.PersistentVolumeClaim
	// Owner is the reference to the installation the Pod is garbage-collected with.
	Owner meta.OwnerReference
}

// ShardPodName returns the name of the backup Pod of a shard of a cluster of an installation, which is its host name
// as well.
func ShardPodName(chiName, cluster, shard string) string {
	return boundedName(joinName("chi", chiName, cluster, shard), shardPodNameSuffix, shardPodNameMaxLength)
}

// ShardPodServiceName returns the name of the headless Service of the backup Pods of a cluster of an installation,
// <chi>-<cluster>-backup: the subdomain of every one of them.
func ShardPodServiceName(chiName, cluster string) string {
	return boundedName(joinName(chiName, cluster), shardServiceNameSuffix, shardPodNameMaxLength)
}

// ShardPodHost returns the DNS name the backup Pod of a shard is reached by, <pod>.<service>.<namespace>.svc. It
// resolves once the Pod has an address, ready or not: the Service publishes not-ready addresses.
func ShardPodHost(namespace, chiName, cluster, shard string) string {
	return strings.Join([]string{ShardPodName(chiName, cluster, shard), ShardPodServiceName(chiName, cluster), namespace, "svc"}, ".")
}

// ShardPodHosts returns the DNS names of the backup Pods of the shards of a cluster of a normalized installation, one
// per shard in the order of the shards, none for a cluster the installation does not have.
func ShardPodHosts(chi *api.ClickHouseInstallation, cluster string) []string {
	if chi == nil {
		return nil
	}
	var hosts []string
	for _, c := range chi.GetSpecT().GetConfiguration().GetClusters() {
		if (c.GetName() != cluster) || (c.GetLayout() == nil) {
			continue
		}
		for _, shard := range c.GetLayout().Shards {
			if len(shard.Hosts) > 0 {
				hosts = append(hosts, ShardPodHost(chi.GetNamespace(), chi.GetName(), cluster, shard.GetName()))
			}
		}
	}
	return hosts
}

// ShardPodLabels returns the labels of the backup Pod of a shard.
func ShardPodLabels(chiName, cluster, shard string) map[string]string {
	return map[string]string{
		LabelBackupCHI:       LabelValue(chiName),
		LabelBackupCluster:   LabelValue(cluster),
		LabelBackupShard:     LabelValue(shard),
		LabelBackupComponent: BackupComponentShardPod,
		LabelApp:             ShardPodAppValue,
	}
}

// ShardPodSelector returns the labels that select the backup Pods of the shards of a cluster of an installation.
func ShardPodSelector(chiName, cluster string) map[string]string {
	return map[string]string{
		LabelBackupCHI:       LabelValue(chiName),
		LabelBackupCluster:   LabelValue(cluster),
		LabelBackupComponent: BackupComponentShardPod,
	}
}

// ShardPodServiceLabels returns the labels of the headless Service of the backup Pods of a cluster.
func ShardPodServiceLabels(chiName, cluster string) map[string]string {
	return map[string]string{
		LabelBackupCHI:       LabelValue(chiName),
		LabelBackupCluster:   LabelValue(cluster),
		LabelBackupComponent: BackupComponentShardService,
	}
}

// ShardServiceSelector selects the headless Services of the backup Pods, the only Services the backup controllers
// watch.
func ShardServiceSelector() k8sLabels.Selector {
	requirement, _ := k8sLabels.NewRequirement(LabelBackupComponent, selection.Equals, []string{BackupComponentShardService})
	return k8sLabels.NewSelector().Add(*requirement)
}

// NewShardPodService builds the headless Service of the backup Pods of a cluster: each Pod sets its host name and
// this subdomain, which gives it the DNS name ShardPodHost - as long as it is ready, so that a name that resolves
// is a REST API that answers.
func NewShardPodService(namespace, chiName, cluster string, owner meta.OwnerReference) *core.Service {
	return &core.Service{
		ObjectMeta: meta.ObjectMeta{
			Name:            ShardPodServiceName(chiName, cluster),
			Namespace:       namespace,
			Labels:          ShardPodServiceLabels(chiName, cluster),
			OwnerReferences: []meta.OwnerReference{owner},
		},
		Spec: core.ServiceSpec{
			ClusterIP: core.ClusterIPNone,
			Type:      core.ServiceTypeClusterIP,
			Selector:  ShardPodSelector(chiName, cluster),
			// The name of a backup Pod resolves, forward and reverse, before it is ready: clickhouse-backup serves its
			// REST API, which makes the Pod ready, only once it logged in to ClickHouse, and ClickHouse lets the backup
			// user in by the reverse DNS name of the Pod (see backupPodHostRegexp of the CHI normalizer). The
			// orchestrator waits for the REST API to answer, not for the name.
			PublishNotReadyAddresses: true,
			Ports: []core.ServicePort{{
				Name:       config.ClickHouseBackupAPIPortName,
				Protocol:   core.ProtocolTCP,
				Port:       config.ClickHouseBackupAPIPortNumber,
				TargetPort: intstr.FromInt32(config.ClickHouseBackupAPIPortNumber),
			}},
		},
	}
}

// NewShardPod builds the backup Pod of a shard: the server of clickhouse-backup, serving its REST API on the port of
// the sidecar with the configuration file Secret the sidecar would mount, on the node of the replica pod, with every
// volume of the ClickHouse container of that pod mounted at the same path. clickhouse-backup works on the files of
// the server directly - FREEZE leaves its copy in shadow/ of the data disk, a restore leaves the parts to attach in
// detached/ - so it has to see what the server sees.
func NewShardPod(src ShardPodSource) (*core.Pod, error) {
	replica := src.ReplicaPod
	if replica.Spec.NodeName == "" {
		return nil, ErrShardPodNotScheduled
	}
	clickHouse, ok := chiCreator.PodSpecContainerGetClickHouse(&replica.Spec)
	if !ok {
		return nil, ErrShardPodNoClickHouse
	}

	containers := managers.NewContainerManager(managers.ContainerManagerTypeClickHouse)
	container, ok := containers.NewBackupServerContainer(src.Host)
	if !ok {
		return nil, ErrShardPodBackupDisabled
	}
	volumes, mounts, err := shardPodDataVolumes(replica, clickHouse, src.Claims)
	if err != nil {
		return nil, err
	}

	cluster := src.Host.GetCluster().GetName()
	chiName := src.Host.GetCR().GetName()
	shard := src.Host.GetShard().GetName()
	name := ShardPodName(chiName, cluster, shard)
	configureShardPodContainer(&container, src.Host, clickHouse, mounts)

	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			Name:            name,
			Namespace:       replica.Namespace,
			Labels:          ShardPodLabels(chiName, cluster, shard),
			OwnerReferences: []meta.OwnerReference{src.Owner},
			Annotations: map[string]string{
				AnnotationShardPodReplica:    replica.Name,
				AnnotationShardPodReplicaUID: string(replica.UID),
			},
		},
		Spec: core.PodSpec{
			Containers: []core.Container{container},
			Volumes:    append(volumes, shardPodConfigVolume(src.Host, chiCreator.IsBackupSidecarCommand(&container))),
			// The DNS name of the Pod is <hostname>.<subdomain>.<namespace>.svc, see ShardPodHost
			Hostname:  name,
			Subdomain: ShardPodServiceName(chiName, cluster),
			// Pinned to the node of the replica pod by its hostname: a ReadWriteOnce volume is mounted by pods of one
			// node only, and FREEZE leaves its copy on the disk of that node
			NodeSelector:                  map[string]string{hostnameLabel: replica.Spec.NodeName},
			Tolerations:                   slices.Clone(replica.Spec.Tolerations),
			ImagePullSecrets:              slices.Clone(replica.Spec.ImagePullSecrets),
			SecurityContext:               replica.Spec.SecurityContext.DeepCopy(),
			ServiceAccountName:            replica.Spec.ServiceAccountName,
			SchedulerName:                 replica.Spec.SchedulerName,
			RuntimeClassName:              replica.Spec.RuntimeClassName,
			HostAliases:                   shardPodHostAliases(replica.Spec.HostAliases),
			DNSPolicy:                     replica.Spec.DNSPolicy,
			DNSConfig:                     replica.Spec.DNSConfig.DeepCopy(),
			RestartPolicy:                 core.RestartPolicyAlways,
			TerminationGracePeriodSeconds: ptr.To(shardPodTerminationGracePeriod),
			// A Service named, say, sftp would set SFTP_PORT=tcp://..., which clickhouse-backup fails to parse
			EnableServiceLinks: ptr.To(false),
			// clickhouse-backup talks to ClickHouse and to the storage, never to the Kubernetes API
			AutomountServiceAccountToken: ptr.To(false),
		},
	}
	pod.Annotations[AnnotationShardPodSpecHash] = shardPodSpecHash(pod)
	return pod, nil
}

// shardPodHostAliases returns the host aliases of the replica pod the backup Pod takes: all but those to a loopback
// address. The StatefulSet maps the host name of the replica - the name of its host Service, CLICKHOUSE_HOST of the
// backup Pod - to 127.0.0.1, which in the backup Pod would send clickhouse-backup to itself rather than to ClickHouse.
func shardPodHostAliases(aliases []core.HostAlias) []core.HostAlias {
	var kept []core.HostAlias
	for _, alias := range aliases {
		if ip := net.ParseIP(alias.IP); (ip != nil) && ip.IsLoopback() {
			continue
		}
		kept = append(kept, *alias.DeepCopy())
	}
	return kept
}

// configureShardPodContainer turns the container NewBackupServerContainer builds into the container of the backup
// Pod: it connects to ClickHouse through the host Service of its replica rather than over localhost, is ready once
// its REST API accepts connections, and mounts the volumes of the ClickHouse container besides its own.
func configureShardPodContainer(container *core.Container, host *api.Host, clickHouse *core.Container, mounts []core.VolumeMount) {
	service := managers.NewNameManager(managers.NameManagerTypeClickHouse).Name(interfaces.NameStatefulSetService, host)
	container.Env = util.OverwriteEnvVars(container.Env, core.EnvVar{Name: config.ClickHouseBackupEnvClickHouseHost, Value: service})
	if container.ReadinessProbe == nil {
		container.ReadinessProbe = &core.Probe{
			ProbeHandler: core.ProbeHandler{TCPSocket: &core.TCPSocketAction{
				Port: intstr.FromInt32(config.ClickHouseBackupAPIPortNumber),
			}},
			PeriodSeconds: shardPodReadinessPeriodSeconds,
		}
	}
	if container.SecurityContext == nil {
		container.SecurityContext = clickHouse.SecurityContext.DeepCopy()
	}
	for _, mount := range mounts {
		if !slices.ContainsFunc(container.VolumeMounts, func(m core.VolumeMount) bool { return m.MountPath == mount.MountPath }) {
			container.VolumeMounts = append(container.VolumeMounts, mount)
		}
	}
}

// shardPodDataVolumes returns the volumes of the replica pod the ClickHouse container mounts and the mounts of them,
// as the backup Pod takes them: storage as it is, configuration read-only. A volume that lives in the replica pod
// alone - emptyDir, generic ephemeral - is left out, unless it holds data of ClickHouse, which refuses the Pod.
func shardPodDataVolumes(replica *core.Pod, clickHouse *core.Container, claims map[string]*core.PersistentVolumeClaim) ([]core.Volume, []core.VolumeMount, error) {
	byName := map[string]*core.Volume{}
	for i := range replica.Spec.Volumes {
		byName[replica.Spec.Volumes[i].Name] = &replica.Spec.Volumes[i]
	}

	dataMounted := false
	var volumes []core.Volume
	var mounts []core.VolumeMount
	for _, mount := range clickHouse.VolumeMounts {
		if isPathWithin(mount.MountPath, config.DirPathClickHouseBackupConfig) || isPathWithin(config.DirPathClickHouseBackupConfig, mount.MountPath) {
			continue
		}
		volume, ok := byName[mount.Name]
		if !ok {
			continue
		}
		holdsData := isPathWithin(config.DirPathDataStorage, mount.MountPath) || isPathWithin(mount.MountPath, config.DirPathDataStorage)
		switch {
		case (volume.EmptyDir != nil) || (volume.Ephemeral != nil):
			if holdsData {
				return nil, nil, fmt.Errorf("%w: volume %s at %s is a %s - give the hosts a volumeClaimTemplate for their data",
					ErrShardPodNoPersistentData, volume.Name, mount.MountPath, volumeKind(volume))
			}
			continue
		case volume.PersistentVolumeClaim != nil:
			if claim, ok := claims[volume.PersistentVolumeClaim.ClaimName]; ok && slices.Contains(claim.Spec.AccessModes, core.ReadWriteOncePod) {
				return nil, nil, fmt.Errorf("%w: claim %s is ReadWriteOncePod", ErrShardPodVolumeUnshareable, claim.Name)
			}
		case (volume.ConfigMap != nil) || (volume.Secret != nil) || (volume.Projected != nil) || (volume.DownwardAPI != nil):
			mount.ReadOnly = true
		}
		if holdsData {
			dataMounted = true
		}
		mount.SubPathExpr = ""
		if !slices.ContainsFunc(volumes, func(v core.Volume) bool { return v.Name == volume.Name }) {
			volumes = append(volumes, *volume.DeepCopy())
		}
		mounts = append(mounts, *mount.DeepCopy())
	}
	if !dataMounted {
		return nil, nil, fmt.Errorf("%w: no volume is mounted at %s - give the hosts a volumeClaimTemplate for their data",
			ErrShardPodNoPersistentData, config.DirPathDataStorage)
	}
	return volumes, mounts, nil
}

// volumeKind names the kind of a pod-local volume in a message.
func volumeKind(volume *core.Volume) string {
	if volume.Ephemeral != nil {
		return "generic ephemeral volume"
	}
	return "emptyDir"
}

// shardPodConfigVolume returns the volume of the configuration file Secret of the cluster of the host, as the sidecar
// mounts it: the file and the fingerprint of the credentials of the REST API. It is optional for the operator's
// command, which waits for the file: the Pod may start before the Secret exists.
func shardPodConfigVolume(host *api.Host, optional bool) core.Volume {
	namer := managers.NewNameManager(managers.NameManagerTypeClickHouse)
	return core.Volume{
		Name: config.ClickHouseBackupConfigVolumeName,
		VolumeSource: core.VolumeSource{Secret: &core.SecretVolumeSource{
			SecretName: namer.Name(interfaces.NameBackupConfigSecret, host.GetCR(), host.GetCluster().GetName()),
			Items: []core.KeyToPath{
				{Key: backupconfig.FileName, Path: backupconfig.FileName},
				{Key: backupconfig.APIFingerprintFileName, Path: backupconfig.APIFingerprintFileName},
			},
			Optional: ptr.To(optional),
		}},
	}
}

// shardPodSpecHash fingerprints what the operator sets on the Pod: its spec, and the replica pod it follows.
func shardPodSpecHash(pod *core.Pod) string {
	return util.HashIntoString([]byte(util.Dump(struct {
		Spec    core.PodSpec
		Replica string
		UID     string
	}{pod.Spec, pod.Annotations[AnnotationShardPodReplica], pod.Annotations[AnnotationShardPodReplicaUID]})))
}

// IsShardPodOutdated reports whether a backup Pod differs from the one the operator builds now, and has to be
// replaced: a Pod spec cannot be changed.
func IsShardPodOutdated(existing, desired *core.Pod) bool {
	return existing.Annotations[AnnotationShardPodSpecHash] != desired.Annotations[AnnotationShardPodSpecHash]
}

// IsPodReady reports whether a pod runs and passes its readiness checks, nil-safe.
func IsPodReady(pod *core.Pod) bool {
	if (pod == nil) || (pod.DeletionTimestamp != nil) || (pod.Status.Phase != core.PodRunning) {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == core.PodReady {
			return condition.Status == core.ConditionTrue
		}
	}
	return false
}

// isPathWithin reports whether target is dir itself or lies below it.
func isPathWithin(target, dir string) bool {
	target = path.Clean(target)
	dir = path.Clean(dir)
	return (dir == "/") || (target == dir) || strings.HasPrefix(target, dir+"/")
}

// BackupShard is a shard of a cluster backed up by backup Pods, with the hosts its backup Pod may work next to.
type BackupShard struct {
	Cluster string
	Shard   string
	// Hosts are the hosts of the shard, in replica order
	Hosts []*api.Host
	// Preferred is the index into Hosts of the replica the backup Pod works next to while it is ready
	Preferred int
}

// BackupShards lists the shards of every cluster of a normalized installation backed up by backup Pods,
// backup.mode pod, whose hosts link back to their shard, cluster and installation.
func BackupShards(normalized *api.ClickHouseInstallation) ([]BackupShard, error) {
	if normalized == nil {
		return nil, nil
	}
	chi, err := prepareNormalized(normalized)
	if err != nil {
		return nil, err
	}
	var shards []BackupShard
	for _, cluster := range chi.GetSpecT().GetConfiguration().GetClusters() {
		if !cluster.GetBackup().IsEnabled() || !cluster.GetBackup().IsPodMode() || (cluster.GetLayout() == nil) {
			continue
		}
		for _, shard := range cluster.GetLayout().Shards {
			if len(shard.Hosts) == 0 {
				continue
			}
			preferred := cluster.GetBackup().GetPreferredReplica()
			if preferred >= len(shard.Hosts) {
				preferred = len(shard.Hosts) - 1
			}
			shards = append(shards, BackupShard{
				Cluster:   cluster.GetName(),
				Shard:     shard.GetName(),
				Hosts:     shard.Hosts,
				Preferred: preferred,
			})
		}
	}
	return shards, nil
}

// Candidates returns the hosts of the shard in the order the backup Pod falls back over them: the preferred
// replica, then the ones after it, then the ones before it.
func (s BackupShard) Candidates() []*api.Host {
	var hosts []*api.Host
	for i := range s.Hosts {
		hosts = append(hosts, s.Hosts[(s.Preferred+i)%len(s.Hosts)])
	}
	return hosts
}

// ReplicaPodName returns the name of the pod of a host: the only pod of its StatefulSet.
func ReplicaPodName(host *api.Host) string {
	return managers.NewNameManager(managers.NameManagerTypeClickHouse).Name(interfaces.NamePod, host)
}

// ShardPodOwner returns the reference to the installation that the backup Pods of its shards are garbage-collected
// with.
func ShardPodOwner(chi *api.ClickHouseInstallation) meta.OwnerReference {
	return meta.OwnerReference{
		APIVersion: api.SchemeGroupVersion.String(),
		Kind:       api.ClickHouseInstallationCRDResourceKind,
		Name:       chi.GetName(),
		UID:        chi.GetUID(),
		Controller: ptr.To(true),
	}
}

// PodSelector selects the pods the backup controllers watch: the pods of the installations and the backup Pods.
func PodSelector() k8sLabels.Selector {
	requirement, _ := k8sLabels.NewRequirement(LabelApp, selection.In, []string{InstallationPodAppValue, ShardPodAppValue})
	return k8sLabels.NewSelector().Add(*requirement)
}
