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

package chk

import (
	"context"
	"time"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// waitForIPAddresses waits for all pods to get IP address assigned
func (w *worker) waitForIPAddresses(ctx context.Context, cr *apiChk.ClickHouseKeeperInstallation) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. CR polling IP: %s ", cr.GetName())
		return
	}

	if cr.IsStopped() {
		// No need to wait for stopped CHI
		return
	}

	l := w.a.V(1).M(cr)
	l.F().S().Info("wait for IP addresses to be assigned to all pods")

	// Let's limit polling time
	start := time.Now()
	timeout := 1 * time.Minute

	w.c.poll(ctx, cr, func(c *apiChk.ClickHouseKeeperInstallation, e error) bool {
		// TODO fix later
		// status IPs list can be empty
		// Instead of doing in status:
		// 	podIPs := c.getPodsIPs(chi)
		//	cur.EnsureStatus().SetPodIPs(podIPs)
		// and here
		// c.Status.GetPodIPs()
		podIPs := w.c.getPodsIPs(ctx, cr)
		if len(podIPs) >= len(c.Status.GetPods()) {
			l.Info("all IP addresses are in place")
			// Stop polling
			return false
		}
		if time.Since(start) > timeout {
			l.Warning("not all IP addresses are in place but time has elapsed")
			// Stop polling
			return false
		}

		l.Info("still waiting - not all IP addresses are in place yet")

		// Continue polling
		return true
	})
}

// shouldIncludeHost determines whether host to be included into cluster after reconcile
func (w *worker) shouldIncludeHost(host *api.Host) bool {
	switch {
	case host.IsStopped():
		// No need to include stopped host
		return false
	}
	return true
}

// includeHost includes host back into ClickHouse clusters
func (w *worker) includeHost(ctx context.Context, host *api.Host) error {
	if !w.shouldIncludeHost(host) {
		w.a.V(1).
			M(host).F().
			Info("No need to include host into cluster. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return nil
	}

	// After the StatefulSet exists, including steady state. A transient ConfigMap
	// conflict must not abort the pass; the next reconcile republishes. Staged
	// admission calls includeHostIntoRaftCluster itself and keeps the error.
	if err := w.includeHostIntoRaftCluster(ctx, host); err != nil {
		w.a.V(1).
			M(host).F().
			Warning("Include host - unable to publish raft_configuration, continuing. Host: %s Err: %v", host.GetName(), err)
	}
	return nil
}

// includeHostIntoRaftCluster publishes the host into raft_configuration.
// On ConfigMap failure a host that was excluded is excluded again, so the
// started joiner can retry publication without claiming membership succeeded.
// A host that was not excluded stays that way: SetExclude here would drop an
// existing voter from raft_configuration after a conflict.
func (w *worker) includeHostIntoRaftCluster(ctx context.Context, host *api.Host) error {
	w.a.V(1).
		M(host).F().
		Info("going to include host. Host/shard/cluster: %d/%d/%s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)

	wasExcluded := host.GetReconcileAttributes().IsExclude()
	host.GetCR().GetRuntime().LockCommonConfig()
	host.GetReconcileAttributes().UnsetExclude()
	err := w.publishRaftConfiguration(ctx, host)
	if err != nil && wasExcluded {
		host.GetReconcileAttributes().SetExclude()
	}
	host.GetCR().GetRuntime().UnlockCommonConfig()
	return err
}

// publishStagedAdmit publishes only after the joiner's Pod has Started.
// There is no pre-create publication to roll back after failed startup.
func (w *worker) publishStagedAdmit(ctx context.Context, host *api.Host) error {
	if host == nil || !host.GetReconcileAttributes().IsExclude() {
		return nil
	}
	return w.includeHostIntoRaftCluster(ctx, host)
}

// publishRaftConfiguration writes raft_configuration for the host's CR.
// publishRaftFn replaces the ConfigMap write in tests.
func (w *worker) publishRaftConfiguration(ctx context.Context, host *api.Host) error {
	if w.publishRaftFn != nil {
		return w.publishRaftFn(ctx, host)
	}
	return w.reconcileConfigMapCommon(ctx, host.GetCR(), w.options())
}
