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
	"context"
	"fmt"
	"time"

	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	apiMachinery "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
	"github.com/altinity/clickhouse-operator/pkg/model/common/namer/short"
)

// shardPodControllerKind names the controller of backup Pods in logs and events.
const shardPodControllerKind = "BackupPod"

// Reasons of the events on an installation about the backup Pods of its shards.
const (
	eventReasonShardPodCreated  = "BackupPodCreated"
	eventReasonShardPodReplaced = "BackupPodReplaced"
	eventReasonShardPodRefused  = "BackupPodRefused"
	eventReasonShardPodFallback = "BackupPodFallback"
	// eventReasonOrchestratorRBACFailed tells that the ServiceAccount of the backup orchestrator, or its Role or
	// RoleBinding, could not be made in the namespace of an installation
	eventReasonOrchestratorRBACFailed = "BackupOrchestratorRBACFailed"
	eventActionShardPodReconcile      = "ReconcileBackupPod"
)

// shardPodFallbackGrace is how long the preferred replica of a shard may be not ready before the backup Pod moves
// to another replica: a readiness probe failing for a few periods must not move it back and forth.
const shardPodFallbackGrace = 2 * time.Minute

// ShardPodController keeps the backup Pods of the clusters of an installation backed up in backup.mode pod: one per
// shard, on the node of the pod of the replica it works next to, with that pod's volumes, for as long as a backup,
// verify or restore Job of the cluster is active - and none otherwise, so that a second pod holds the volumes of a
// replica no longer than an operation takes. A Pod spec cannot change: a Pod that no longer matches - the replica
// pod moved to another node, or was created anew - is deleted, and the next reconcile creates it anew. A Pod left
// behind on the former node of its replica would keep a ReadWriteOnce volume attached there, which keeps the replica
// itself from starting. The headless Service of the backup Pods of every such cluster gives each Pod the DNS name
// the orchestrator reaches it by, see model.ShardPodHost.
type ShardPodController struct {
	*reconciler
}

// NewShardPodController builds the controller of the backup Pods of the shards.
func NewShardPodController(c client.Client, apiReader client.Reader, scheme *apiMachinery.Scheme, cr interfaces.IKubeCR) *ShardPodController {
	return &ShardPodController{
		reconciler: newReconciler(c, apiReader, scheme, cr, shardPodControllerKind),
	}
}

// Reconcile brings the backup Pods of the shards of an installation, and their Services, in line with its last
// completed reconcile, the pods of its replicas and the Jobs active on its clusters.
func (r *ShardPodController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if r.skip(ctx, req.Namespace) {
		return ctrl.Result{}, nil
	}
	chi, err := r.getInstallation(ctx, req.Namespace, req.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	existing, err := r.listShardPods(ctx, req.Namespace, req.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	services, err := r.listShardServices(ctx, req.Namespace, req.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	if (chi == nil) || (chi.GetDeletionTimestamp() != nil) {
		// The garbage collector deletes them with the installation; an installation gone before they had an owner
		// leaves them to the operator
		if err := r.deleteShardPods(ctx, existing, nil); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.deleteShardServices(ctx, services, nil)
	}
	if chi.GetAncestorT().IsBackupEnabled() {
		r.ensureOrchestratorServiceAccount(ctx, chi)
	}
	shards, err := model.BackupShards(chi.GetAncestorT())
	if err != nil {
		return ctrl.Result{}, err
	}
	if chi.IsStopped() {
		shards = nil
	}

	if err := r.reconcileShardServices(ctx, chi, shards, services); err != nil {
		return ctrl.Result{}, err
	}
	active, err := r.activeClusters(ctx, chi)
	if err != nil {
		return ctrl.Result{}, err
	}

	wanted := map[string]bool{}
	var requeue time.Duration
	for _, shard := range shards {
		if !active[model.LabelValue(shard.Cluster)] {
			continue
		}
		name := model.ShardPodName(chi.GetName(), shard.Cluster, shard.Shard)
		wanted[name] = true
		after, err := r.reconcileShard(ctx, chi, shard, existing[name])
		if err != nil {
			return ctrl.Result{}, err
		}
		requeue = sooner(requeue, after)
	}
	if err := r.deleteShardPods(ctx, existing, wanted); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// activeClusters returns, by the label value of their names, the clusters of an installation a backup, verify or
// restore Job is active on: neither complete nor failed, nor being deleted. Every such Job, one-off or made by the
// CronJob of a schedule, carries the labels of the installation and the cluster it operates on.
func (r *ShardPodController) activeClusters(ctx context.Context, chi *api.ClickHouseInstallation) (map[string]bool, error) {
	list := &batch.JobList{}
	if err := r.List(ctx, list, client.InNamespace(chi.GetNamespace()),
		client.MatchingLabelsSelector{Selector: model.CHIJobsSelector(chi.GetName(), "")}); err != nil {
		return nil, err
	}
	active := map[string]bool{}
	for i := range list.Items {
		if isJobActive(&list.Items[i]) {
			active[model.ObjectCluster(&list.Items[i])] = true
		}
	}
	return active, nil
}

// isJobActive reports whether a Job may still run a pod: it is neither being deleted, nor complete, nor failed.
func isJobActive(job *batch.Job) bool {
	if (job.DeletionTimestamp != nil) || (job.Status.CompletionTime != nil) {
		return false
	}
	for _, condition := range job.Status.Conditions {
		if (condition.Status == core.ConditionTrue) &&
			((condition.Type == batch.JobComplete) || (condition.Type == batch.JobFailed)) {
			return false
		}
	}
	return true
}

// reconcileShardServices keeps the headless Service of the backup Pods of every cluster backed up in backup.mode
// pod, whether a Job is active on it or not: a Service costs nothing, and is in place before the first Pod. The
// Services of the other clusters are deleted.
func (r *ShardPodController) reconcileShardServices(
	ctx context.Context,
	chi *api.ClickHouseInstallation,
	shards []model.BackupShard,
	existing map[string]*core.Service,
) error {
	wanted := map[string]bool{}
	for _, shard := range shards {
		name := model.ShardPodServiceName(chi.GetName(), shard.Cluster)
		if wanted[name] {
			continue
		}
		wanted[name] = true
		desired := model.NewShardPodService(chi.GetNamespace(), chi.GetName(), shard.Cluster, model.ShardPodOwner(chi))
		current, ok := existing[name]
		if !ok {
			if err := r.Create(ctx, desired); err != nil && !apiErrors.IsAlreadyExists(err) {
				return err
			}
			continue
		}
		if equality.Semantic.DeepEqual(current.Spec.Selector, desired.Spec.Selector) &&
			equality.Semantic.DeepEqual(current.Spec.Ports, desired.Spec.Ports) &&
			(current.Spec.PublishNotReadyAddresses == desired.Spec.PublishNotReadyAddresses) {
			continue
		}
		current.Spec.Selector = desired.Spec.Selector
		current.Spec.Ports = desired.Spec.Ports
		current.Spec.PublishNotReadyAddresses = desired.Spec.PublishNotReadyAddresses
		if err := r.Update(ctx, current); err != nil && !apiErrors.IsConflict(err) {
			return err
		}
	}
	return r.deleteShardServices(ctx, existing, wanted)
}

// listShardServices returns the headless Services of the backup Pods of an installation, by name.
func (r *ShardPodController) listShardServices(ctx context.Context, namespace, chiName string) (map[string]*core.Service, error) {
	list := &core.ServiceList{}
	err := r.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels{
		model.LabelBackupCHI:       model.LabelValue(chiName),
		model.LabelBackupComponent: model.BackupComponentShardService,
	})
	if err != nil {
		return nil, err
	}
	services := map[string]*core.Service{}
	for i := range list.Items {
		services[list.Items[i].Name] = &list.Items[i]
	}
	return services, nil
}

// deleteShardServices deletes the Services not wanted, all of them when wanted is nil.
func (r *ShardPodController) deleteShardServices(ctx context.Context, services map[string]*core.Service, wanted map[string]bool) error {
	for name, service := range services {
		if wanted[name] || (service.DeletionTimestamp != nil) {
			continue
		}
		err := r.Delete(ctx, service, client.Preconditions{UID: &service.UID})
		if err != nil && !apiErrors.IsNotFound(err) && !apiErrors.IsConflict(err) {
			return err
		}
	}
	return nil
}

// ensureOrchestratorServiceAccount makes sure the orchestrator Jobs of a backup-enabled installation can run as the
// operator's ServiceAccount in its namespace. A failure is warned about, and does not stop the backup Pods: the
// controller that builds a Job ensures the ServiceAccount itself, and fails that Job's operation instead.
func (r *ShardPodController) ensureOrchestratorServiceAccount(ctx context.Context, chi *api.ClickHouseInstallation) {
	if _, err := r.EnsureOrchestratorServiceAccount(ctx, chi.GetNamespace()); err != nil {
		r.events.EventWarning(chi, eventActionShardPodReconcile, eventReasonOrchestratorRBACFailed,
			fmt.Sprintf("unable to ensure the ServiceAccount %s of the backup orchestrator in namespace %s: %v",
				OrchestratorServiceAccountName(), chi.GetNamespace(), err))
	}
}

// reconcileShard keeps the backup Pod of one shard, and returns when to look at it again, 0 for when anything
// changes.
func (r *ShardPodController) reconcileShard(
	ctx context.Context,
	chi *api.ClickHouseInstallation,
	shard model.BackupShard,
	current *core.Pod,
) (time.Duration, error) {
	host, replica, wait, err := r.chooseReplica(ctx, shard, current)
	if (err != nil) || (replica == nil) {
		return wait, err
	}
	claims, err := r.replicaClaims(ctx, replica)
	if err != nil {
		return 0, err
	}
	desired, buildErr := model.NewShardPod(model.ShardPodSource{
		Host:       host,
		ReplicaPod: replica,
		Claims:     claims,
		Owner:      model.ShardPodOwner(chi),
	})
	if buildErr != nil {
		r.events.EventWarning(chi, eventActionShardPodReconcile, eventReasonShardPodRefused,
			fmt.Sprintf("no backup Pod for shard %s of cluster %s: %v", shard.Shard, shard.Cluster, buildErr))
		if current != nil {
			return 0, r.deleteShardPod(ctx, current)
		}
		return targetRequeueInterval, nil
	}

	switch {
	case current == nil:
		if err := r.Create(ctx, desired); err != nil {
			if apiErrors.IsAlreadyExists(err) {
				return time.Second, nil
			}
			return 0, err
		}
		r.events.EventInfo(chi, eventActionShardPodReconcile, eventReasonShardPodCreated,
			fmt.Sprintf("backup Pod %s of shard %s of cluster %s created on node %s next to %s", desired.Name, shard.Shard,
				shard.Cluster, replica.Spec.NodeName, replica.Name))
		return wait, nil
	case current.DeletionTimestamp != nil:
		// Deleted, and not gone yet: created anew once it is
		return time.Second * 5, nil
	case model.IsShardPodOutdated(current, desired):
		if (current.Annotations[model.AnnotationShardPodReplicaUID] == string(replica.UID)) && model.IsPodReady(current) {
			// Same replica pod, changed settings: the Pod serves the operation running now, which finishes first -
			// the Pod goes with it. A moved replica does not wait, see ShardPodController
			return targetRequeueInterval, nil
		}
		if err := r.deleteShardPod(ctx, current); err != nil {
			return 0, err
		}
		r.events.EventInfo(chi, eventActionShardPodReconcile, eventReasonShardPodReplaced,
			fmt.Sprintf("backup Pod %s of shard %s of cluster %s is built anew next to %s on node %s", current.Name, shard.Shard,
				shard.Cluster, replica.Name, replica.Spec.NodeName))
		return time.Second * 5, nil
	}
	// A preferred replica not ready a moment ago is looked at again once its grace has passed
	return wait, nil
}

// chooseReplica returns the host and the running pod of the replica the backup Pod of the shard works next to: the
// preferred one while it is ready, else, once it has not been ready for shardPodFallbackGrace, the next ready
// replica. The replica the current Pod follows is kept while it is ready, so that a preferred replica turning
// ready again moves the Pod back only when the Pod is rebuilt anyway. Nil pod with a requeue interval means no
// replica can be chosen now.
func (r *ShardPodController) chooseReplica(ctx context.Context, shard model.BackupShard, current *core.Pod) (*api.Host, *core.Pod, time.Duration, error) {
	type candidate struct {
		host *api.Host
		pod  *core.Pod
	}
	var candidates []candidate
	for _, host := range shard.Candidates() {
		pod := &core.Pod{}
		err := r.Get(ctx, types.NamespacedName{Namespace: host.GetCR().GetNamespace(), Name: model.ReplicaPodName(host)}, pod)
		switch {
		case apiErrors.IsNotFound(err):
			pod = nil
		case err != nil:
			return nil, nil, 0, err
		}
		candidates = append(candidates, candidate{host: host, pod: pod})
	}

	preferred := candidates[0]
	if model.IsPodReady(preferred.pod) {
		return preferred.host, preferred.pod, 0, nil
	}
	if (current != nil) && (current.DeletionTimestamp == nil) {
		for _, c := range candidates[1:] {
			if (c.pod != nil) && (current.Annotations[model.AnnotationShardPodReplicaUID] == string(c.pod.UID)) && model.IsPodReady(c.pod) {
				return c.host, c.pod, 0, nil
			}
		}
	}
	if since := notReadySince(preferred.pod); since < shardPodFallbackGrace {
		if (preferred.pod != nil) && (preferred.pod.Spec.NodeName != "") {
			// Not ready a moment ago: the Pod follows it still, as volumes and node are known
			return preferred.host, preferred.pod, shardPodFallbackGrace - since, nil
		}
		return nil, nil, shardPodFallbackGrace - since, nil
	}
	for _, c := range candidates[1:] {
		if model.IsPodReady(c.pod) {
			log.V(1).M(r.meta(c.pod)).Info("backup Pod of shard %s of cluster %s falls back to replica pod %s", shard.Shard, shard.Cluster, c.pod.Name)
			r.events.EventWarning(c.host.GetCR(), eventActionShardPodReconcile, eventReasonShardPodFallback,
				fmt.Sprintf("the preferred replica of shard %s of cluster %s is not ready: the backup Pod works next to %s",
					shard.Shard, shard.Cluster, c.pod.Name))
			return c.host, c.pod, 0, nil
		}
	}
	return nil, nil, targetRequeueInterval, nil
}

// notReadySince tells how long a pod has not been ready, as long as it may stay so before the backup Pod falls
// back when that is unknown: a pod that does not exist, or never reported readiness, is given no grace.
func notReadySince(pod *core.Pod) time.Duration {
	if pod == nil {
		return shardPodFallbackGrace
	}
	for _, condition := range pod.Status.Conditions {
		if (condition.Type == core.PodReady) && !condition.LastTransitionTime.IsZero() {
			return time.Since(condition.LastTransitionTime.Time)
		}
	}
	return shardPodFallbackGrace
}

// replicaClaims reads the PersistentVolumeClaims the replica pod mounts, past the cache, which holds none: their
// access modes tell whether a second pod may mount them.
func (r *ShardPodController) replicaClaims(ctx context.Context, replica *core.Pod) (map[string]*core.PersistentVolumeClaim, error) {
	claims := map[string]*core.PersistentVolumeClaim{}
	for _, volume := range replica.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		claim := &core.PersistentVolumeClaim{}
		err := r.apiReader.Get(ctx, types.NamespacedName{Namespace: replica.Namespace, Name: volume.PersistentVolumeClaim.ClaimName}, claim)
		switch {
		case apiErrors.IsNotFound(err):
			continue
		case err != nil:
			return nil, err
		}
		claims[claim.Name] = claim
	}
	return claims, nil
}

// listShardPods returns the backup Pods of the shards of an installation, by name.
func (r *ShardPodController) listShardPods(ctx context.Context, namespace, chiName string) (map[string]*core.Pod, error) {
	list := &core.PodList{}
	err := r.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels{
		model.LabelBackupCHI:       model.LabelValue(chiName),
		model.LabelBackupComponent: model.BackupComponentShardPod,
	})
	if err != nil {
		return nil, err
	}
	pods := map[string]*core.Pod{}
	for i := range list.Items {
		pods[list.Items[i].Name] = &list.Items[i]
	}
	return pods, nil
}

// deleteShardPods deletes the backup Pods not wanted, all of them when wanted is nil.
func (r *ShardPodController) deleteShardPods(ctx context.Context, pods map[string]*core.Pod, wanted map[string]bool) error {
	for name, pod := range pods {
		if wanted[name] || (pod.DeletionTimestamp != nil) {
			continue
		}
		if err := r.deleteShardPod(ctx, pod); err != nil {
			return err
		}
	}
	return nil
}

func (r *ShardPodController) deleteShardPod(ctx context.Context, pod *core.Pod) error {
	err := r.Delete(ctx, pod, client.Preconditions{UID: &pod.UID})
	if apiErrors.IsNotFound(err) || apiErrors.IsConflict(err) {
		return nil
	}
	return err
}

// InstallationsForPod maps a pod to the installation whose backup Pods depend on it: a pod of one of its hosts, or
// one of its backup Pods.
func (r *ShardPodController) InstallationsForPod(ctx context.Context, obj client.Object) []reconcile.Request {
	labels := obj.GetLabels()
	if labels[model.LabelBackupComponent] == model.BackupComponentShardPod {
		return ownerInstallation(obj)
	}
	if labels[model.LabelApp] != model.InstallationPodAppValue {
		return nil
	}
	// The label of the installation may be a shortened name: the installations of the namespace are matched by
	// the value they label their pods with
	value, ok := labels[installationLabel]
	if !ok {
		return nil
	}
	return r.installationsLabelled(ctx, obj.GetNamespace(), installationLabelValue, value)
}

// InstallationsForService maps a headless Service of backup Pods to the installation that owns it.
func (r *ShardPodController) InstallationsForService(_ context.Context, obj client.Object) []reconcile.Request {
	if obj.GetLabels()[model.LabelBackupComponent] != model.BackupComponentShardService {
		return nil
	}
	return ownerInstallation(obj)
}

// InstallationsForJob maps a backup, verify or restore Job to the installation it operates on, whose backup Pods
// exist while the Job is active.
func (r *ShardPodController) InstallationsForJob(ctx context.Context, obj client.Object) []reconcile.Request {
	value, ok := obj.GetLabels()[model.LabelBackupCHI]
	if !ok {
		return nil
	}
	// The label may be a shortened name, see model.LabelValue
	return r.installationsLabelled(ctx, obj.GetNamespace(), func(chi *api.ClickHouseInstallation) string {
		return model.LabelValue(chi.GetName())
	}, value)
}

// installationsLabelled returns the requests of the installations of the namespace whose label value is the given one.
func (r *ShardPodController) installationsLabelled(
	ctx context.Context,
	namespace string,
	labelValue func(*api.ClickHouseInstallation) string,
	value string,
) []reconcile.Request {
	list := &api.ClickHouseInstallationList{}
	if err := r.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil
	}
	var requests []reconcile.Request
	for i := range list.Items {
		if labelValue(&list.Items[i]) == value {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: list.Items[i].Name}})
		}
	}
	return requests
}

// ownerInstallation returns the request of the installation that owns an object, none when no installation does.
func ownerInstallation(obj client.Object) []reconcile.Request {
	for _, owner := range obj.GetOwnerReferences() {
		if owner.Kind == api.ClickHouseInstallationCRDResourceKind {
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: owner.Name}}}
		}
	}
	return nil
}

// sooner returns the sooner of two requeue intervals, 0 standing for none.
func sooner(a, b time.Duration) time.Duration {
	switch {
	case a == 0:
		return b
	case (b == 0) || (a < b):
		return a
	}
	return b
}

// installationLabel is the label the pods of an installation name it by.
const installationLabel = "clickhouse.altinity.com/chi"

// installationLabelValue returns the value of installationLabel on the pods of an installation.
func installationLabelValue(chi *api.ClickHouseInstallation) string {
	return short.NameLabel(short.CRName, chi)
}
