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
	"errors"
	"fmt"
	"strings"
	"time"

	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// target is the installation an operation runs on and the hosts it drives.
type target struct {
	chi      *api.ClickHouseInstallation
	topology *model.Topology
}

// pending explains why an operation cannot run on its installation yet.
type pending struct {
	reason  string
	message string
	requeue time.Duration
	// released tells a hold of CronJobs an ended hold left suspended: it announces no suspension anew
	released bool
}

// chiNotFound explains the wait for an installation that does not exist (yet).
func chiNotFound(chiName string) *pending {
	return &pending{
		reason:  api.BackupReasonCHINotFound,
		message: fmt.Sprintf("ClickHouseInstallation %s not found", chiName),
		requeue: pendingRequeueInterval,
	}
}

// waitsForInstallation reports whether the operation waits for the installation itself, which
// resolves without anybody touching it, as opposed to a target that needs a change to run.
func (p *pending) waitsForInstallation() bool {
	return (p.reason == api.BackupReasonCHINotFound) || (p.reason == api.BackupReasonCHINotReady)
}

// refusesOneOff reports whether a one-off operation can never run on the target as it is: the
// installation completed its reconcile without the cluster, or without backup on it, or the schedule a
// backup runs backs up another cluster. A schedule keeps waiting for a change instead.
func (p *pending) refusesOneOff() bool {
	switch p.reason {
	case api.BackupReasonClusterNotFound, api.BackupReasonBackupDisabled, api.BackupReasonNoBackupStorage,
		api.BackupReasonNoTargetHosts:
		return true
	}
	return false
}

// readInstallation reads the installation together with the normalized state of its last completed
// reconcile. requireReady additionally holds the operation until the installation's current
// reconcile has completed. A stopped installation is refused: its hosts are down, so a Job could only
// wait for them. The installation comes along with either hold when it completed a reconcile before,
// for what that one tells.
func (r *reconciler) readInstallation(
	ctx context.Context,
	namespace string,
	chiName string,
	requireReady bool,
) (*api.ClickHouseInstallation, *pending, error) {
	chi, err := r.getInstallation(ctx, namespace, chiName)
	switch {
	case err != nil:
		return nil, nil, err
	case chi == nil:
		return nil, chiNotFound(chiName), nil
	}

	completed := chi.GetAncestorT()
	if (requireReady && (chi.Status.GetStatus() != api.StatusCompleted)) || (completed == nil) {
		notReady := &pending{
			reason:  api.BackupReasonCHINotReady,
			message: fmt.Sprintf("ClickHouseInstallation %s has not completed its reconcile", chiName),
			requeue: pendingRequeueInterval,
		}
		if completed == nil {
			return nil, notReady, nil
		}
		return chi, notReady, nil
	}
	if chi.IsStopped() || completed.IsStopped() {
		return chi, &pending{
			reason:  api.BackupReasonCHIStopped,
			message: fmt.Sprintf("ClickHouseInstallation %s is stopped", chiName),
			requeue: targetRequeueInterval,
		}, nil
	}
	return chi, nil, nil
}

// getInstallation reads the installation together with the normalized state of its last completed
// reconcile, nil when it does not exist. An installation found gone takes the last-outcome series of its
// clusters along: no cluster of it makes a backup any more.
func (r *reconciler) getInstallation(ctx context.Context, namespace, chiName string) (*api.ClickHouseInstallation, error) {
	cr, err := r.cr.Get(ctx, namespace, chiName)
	if (err != nil) && !apiErrors.IsNotFound(err) {
		return nil, err
	}
	chi, _ := cr.(*api.ClickHouseInstallation)
	if (err != nil) || (chi == nil) {
		retainClusters(namespace, chiName, nil)
		return nil, nil
	}
	return chi, nil
}

// resolveTarget reads the installation and resolves the hosts an operation on cluster targets, ""
// standing for every backup-enabled cluster. The hosts come from the normalized state of the last
// completed reconcile, never from the raw spec: templates, explicit layouts and generated names are
// all applied there. requireReady is as for readInstallation.
func (r *reconciler) resolveTarget(
	ctx context.Context,
	namespace string,
	chiName string,
	cluster string,
	requireReady bool,
) (*target, *pending, error) {
	chi, wait, err := r.readInstallation(ctx, namespace, chiName, requireReady)
	if (err != nil) || (wait != nil) {
		return nil, wait, err
	}
	return newTarget(chi, cluster)
}

// newTarget resolves the hosts an operation on cluster of an installation read by readInstallation
// targets, "" standing for every backup-enabled cluster.
func newTarget(chi *api.ClickHouseInstallation, cluster string) (*target, *pending, error) {
	topology, err := model.NewTopology(chi.GetAncestorT(), cluster)
	if err != nil {
		return nil, topologyPending(chi.GetName(), err), ignoreTopologyError(err)
	}
	return &target{chi: chi, topology: topology}, nil, nil
}

// topologyPending explains why a topology cannot be targeted, nil for an error that tells nothing
// about the target.
func topologyPending(chiName string, err error) *pending {
	var reason string
	switch {
	case errors.Is(err, model.ErrClusterNotFound):
		reason = api.BackupReasonClusterNotFound
	case errors.Is(err, model.ErrBackupDisabled):
		reason = api.BackupReasonBackupDisabled
	case errors.Is(err, model.ErrNoBackupStorage):
		reason = api.BackupReasonNoBackupStorage
	case errors.Is(err, model.ErrNoTargetHosts):
		reason = api.BackupReasonNoTargetHosts
	default:
		return nil
	}
	message := fmt.Sprintf("ClickHouseInstallation %s: %v", chiName, err)
	if reason == api.BackupReasonNoBackupStorage {
		message += " - " + api.BackupStorageHint
	}
	return &pending{
		reason:  reason,
		message: message,
		requeue: targetRequeueInterval,
	}
}

// leftOutWithoutStorage tells which clusters an operation of every cluster of the installation leaves out
// as they turn backup on without a storage, "" when it leaves out none, or targets one cluster: a cluster
// whose spec asks for backup would otherwise go without one unnoticed.
func leftOutWithoutStorage(target *target, specCluster string) string {
	clusters := target.topology.ClustersWithoutStorage()
	if (specCluster != "") || (len(clusters) == 0) {
		return ""
	}
	return fmt.Sprintf("leaves out the clusters with backup enabled without a storage: %s - %s",
		strings.Join(clusters, ", "), api.BackupStorageHint)
}

// ignoreTopologyError returns the error of a topology that tells nothing about the target, nil for
// one that topologyPending explains.
func ignoreTopologyError(err error) error {
	if topologyPending("", err) != nil {
		return nil
	}
	return err
}

// targetResolver reads the installation of an operation once per reconcile, when a cluster first needs
// it, and resolves the target of each cluster of the operation from it.
type targetResolver struct {
	read     func() (*api.ClickHouseInstallation, *pending, error)
	resolved bool
	chi      *api.ClickHouseInstallation
	wait     *pending
	err      error
}

// newTargetResolver returns a resolver of the targets an operation on the clusters of an installation
// starts its Jobs on; requireReady is as for readInstallation. Each cluster is resolved by the name the
// operation pinned, never by its spec: an edit of the spec the API server does not refuse must not
// retarget, or fail, the clusters the operation runs on.
func (r *reconciler) newTargetResolver(ctx context.Context, namespace, chiName string, requireReady bool) *targetResolver {
	return &targetResolver{read: func() (*api.ClickHouseInstallation, *pending, error) {
		return r.readInstallation(ctx, namespace, chiName, requireReady)
	}}
}

// forCluster returns the target of one cluster, or why that cluster cannot be targeted.
func (t *targetResolver) forCluster(cluster string) (*target, *pending, error) {
	if !t.resolved {
		t.chi, t.wait, t.err = t.read()
		t.resolved = true
	}
	if (t.err != nil) || (t.wait != nil) {
		return nil, t.wait, t.err
	}
	return newTarget(t.chi, cluster)
}
