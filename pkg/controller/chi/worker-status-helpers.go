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

package chi

import (
	"context"
	"time"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	commonConfig "github.com/altinity/clickhouse-operator/pkg/model/common/config"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// timeToStart specifies time that operator does not accept changes
const timeToStart = 1 * time.Minute

// isJustStarted checks whether worked just started
func (w *worker) isJustStarted() bool {
	return time.Since(w.start) < timeToStart
}

func (w *worker) isPodCrushed(host *api.Host) bool {
	if pod, err := w.c.kube.Pod().Get(host); err == nil {
		return k8s.PodHasCrushedContainers(pod)
	}
	return true
}

func (w *worker) isPodReady(ctx context.Context, host *api.Host) bool {
	if pod, err := w.c.kube.Pod().Get(host); err == nil {
		return !k8s.PodHasNotReadyContainers(pod)
	}
	return false
}

func (w *worker) isPodStarted(ctx context.Context, host *api.Host) bool {
	if pod, err := w.c.kube.Pod().Get(host); err == nil {
		return k8s.PodHasAllContainersStarted(pod)
	}
	return false
}

func (w *worker) isPodRunning(ctx context.Context, host *api.Host) bool {
	if pod, err := w.c.kube.Pod().Get(host); err == nil {
		return k8s.PodPhaseIsRunning(pod)
	}
	return false
}

func (w *worker) isPodOK(ctx context.Context, host *api.Host) bool {
	if pod, err := w.c.kube.Pod().Get(host); err == nil {
		return k8s.IsPodOK(pod)
	}
	return false
}

func (w *worker) isPodRestarted(ctx context.Context, host *api.Host, start map[string]int) bool {
	cur, _ := w.c.kube.Pod().(interfaces.IKubePodEx).GetRestartCounters(host)
	return !util.MapsAreTheSame(start, cur)
}

func (w *worker) doesHostHaveNoRunningQueries(ctx context.Context, host *api.Host) bool {
	n, _ := w.ensureClusterSchemer(host).HostActiveQueriesNum(ctx, host)
	log.V(1).Info("active queries %d host: %s", n, host.GetName())
	return n <= 1
}

func (w *worker) doesHostHaveNoReplicationDelay(ctx context.Context, host *api.Host) bool {
	delay, _ := w.ensureClusterSchemer(host).HostMaxReplicaDelay(ctx, host)
	log.V(1).Info("replication lag %d host: %s", delay, host.GetName())
	return delay <= chop.Config().Reconcile.Host.Wait.Replicas.Delay.IntValue()
}

// isCHIProcessedOnTheSameIP checks whether it is just a restart of the operator on the same IP
func (w *worker) isCHIProcessedOnTheSameIP(chi *api.ClickHouseInstallation) bool {
	ip, _ := chop.GetRuntimeParam(deployment.OPERATOR_POD_IP)
	operatorIpIsTheSame := ip == chi.Status.GetCHOpIP()
	log.V(1).Info("Operator IPs to process CHI: %s. Previous: %s Cur: %s", chi.Name, chi.Status.GetCHOpIP(), ip)

	if !operatorIpIsTheSame {
		// Operator has restarted on the different IP address.
		// We may need to reconcile config files
		log.V(1).Info("Operator IPs are different. Operator was restarted on another IP since previous reconcile of the CHI: %s", chi.Name)
		return false
	}

	log.V(1).Info("Operator IPs are the same as on previous reconcile of the CHI: %s", chi.Name)
	return w.isCleanRestart(chi)
}

// isCleanRestart checks whether it is just a restart of the operator and CHI has no changes since last processed
func (w *worker) isCleanRestart(chi *api.ClickHouseInstallation) bool {
	// Clean restart may be only in case operator has just recently started
	if !w.isJustStarted() {
		log.V(1).Info("Operator is not just started. May not be clean restart")
		return false
	}

	log.V(1).Info("Operator just started. May be clean restart")

	// Migration support
	// Do we have have previously completed CHI?
	// In case no - this means that CHI has either not completed or we are migrating from
	// such a version of the operator, where there is no completed CHI at all
	noCompletedCHI := !chi.HasAncestor()
	// Having status completed and not having completed CHI suggests we are migrating operator version
	statusIsCompleted := chi.Status.GetStatus() == api.StatusCompleted
	if noCompletedCHI && statusIsCompleted {
		// In case of a restart - assume that normalized is already completed
		chi.SetAncestor(chi.GetTarget())
	}

	// Check whether anything has changed in CHI spec
	// In case the generation is the same as already completed - it is clean restart
	generationIsOk := false
	// However, completed CHI still can be missing, for example, in newly requested CHI
	if chi.HasAncestor() {
		generationIsOk = chi.Generation == chi.GetAncestor().GetGeneration()
		log.V(1).Info(
			"CHI %s has ancestor. Generations. Prev: %d Cur: %d Generation is the same: %t",
			chi.Name,
			chi.GetAncestor().GetGeneration(),
			chi.Generation,
			generationIsOk,
		)
	} else {
		log.V(1).Info("CHI %s has NO ancestor, meaning reconcile cycle was never completed.", chi.Name)
	}

	log.V(1).Info("Is CHI %s clean on operator restart: %t", chi.Name, generationIsOk)
	return generationIsOk
}

// areUsableOldAndNew checks whether there are old and new usable
func (w *worker) areUsableOldAndNew(old, new *api.ClickHouseInstallation) bool {
	if old == nil {
		return false
	}
	if new == nil {
		return false
	}
	return true
}

// isAfterFinalizerInstalled checks whether we are just installed finalizer
func (w *worker) isAfterFinalizerInstalled(old, new *api.ClickHouseInstallation) bool {
	if !w.areUsableOldAndNew(old, new) {
		return false
	}

	finalizerIsInstalled := len(old.Finalizers) == 0 && len(new.Finalizers) > 0
	return w.isGenerationTheSame(old, new) && finalizerIsInstalled
}

// isGenerationTheSame checks whether old and new CHI have the same generation
func (w *worker) isGenerationTheSame(old, new *api.ClickHouseInstallation) bool {
	if !w.areUsableOldAndNew(old, new) {
		return false
	}

	return old.GetGeneration() == new.GetGeneration()
}

// getRemoteServersGeneratorOptions build base set of RemoteServersOptions
func (w *worker) getRemoteServersGeneratorOptions() *commonConfig.HostSelector {
	// Base model specifies to exclude:
	// 1. all newly added hosts
	// 2. all explicitly excluded hosts
	return commonConfig.NewHostSelector().ExcludeReconcileAttributes(
		types.NewReconcileAttributes().
			SetStatus(types.ObjectStatusRequested).
			SetExclude(),
	)
}

// options build FilesGeneratorOptionsClickHouse
func (w *worker) options() *config.FilesGeneratorOptions {
	opts := w.getRemoteServersGeneratorOptions()
	w.a.Info("RemoteServersOptions: %s", opts)
	return config.NewFilesGeneratorOptions().SetRemoteServersOptions(opts)
}
