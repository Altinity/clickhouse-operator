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
	"strings"

	core "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
	kubeTypes "k8s.io/apimachinery/pkg/types"
)

// normalizeTimeAbortReasons are Aborted reasons a Ready pod cannot fix.
// The spec has to change. Same set CHI skips in shouldTriggerAutoRecovery,
// limited to reasons the CHK normalizer actually writes.
var normalizeTimeAbortReasons = []string{
	apiChk.StatusReasonFIPSValidationFailed,
	apiChk.StatusReasonFIPSImagePolicyViolation,
	apiChk.StatusReasonNoKeeperListener,
}

// PodBecameReadyPredicate lets a pod update through only when its containers
// flip NotReady to Ready. That is the CHI auto-recovery signal.
func PodBecameReadyPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPod, okOld := e.ObjectOld.(*core.Pod)
			newPod, okNew := e.ObjectNew.(*core.Pod)
			if !okOld || !okNew {
				return false
			}
			return podBecameReady(oldPod, newPod)
		},
	}
}

func podBecameReady(oldPod, newPod *core.Pod) bool {
	if oldPod == nil || newPod == nil {
		return false
	}
	return k8s.PodHasNotReadyContainers(oldPod) && !k8s.PodHasNotReadyContainers(newPod)
}

// RequestsForReadyPod re-enqueues an Aborted CHK when one of its pods becomes
// Ready. reconcile.recovery.onStatus.aborted.onPodReady controls it, same as CHI.
// The generation annotation is removed first: while it matches metadata.generation
// reconcileCR returns before any work.
func (c *Controller) RequestsForReadyPod(ctx context.Context, obj client.Object) []reconcile.Request {
	if c == nil || chop.Get() == nil || !chop.Config().ShouldRecoverAbortedOnPodReady() {
		return nil
	}
	pod, ok := obj.(*core.Pod)
	if !ok || pod == nil {
		return nil
	}
	cr, err := c.GetCR(&pod.ObjectMeta)
	if err != nil || !shouldRecoverAbortedCHK(cr) {
		return nil
	}
	if err := c.clearAbortGeneration(ctx, cr.Namespace, cr.Name); err != nil {
		log.V(1).M(cr).F().Warning(
			"Auto-recovery: pod %s became Ready while CHK %s/%s is Aborted, but the abort generation was not cleared: %v",
			pod.Name, cr.Namespace, cr.Name, err,
		)
		return nil
	}
	log.V(1).M(cr).F().Info(
		"Auto-recovery: pod %s became Ready while CHK %s/%s is Aborted — re-enqueuing reconcile",
		pod.Name, cr.Namespace, cr.Name,
	)
	return []reconcile.Request{{
		NamespacedName: kubeTypes.NamespacedName{Namespace: cr.Namespace, Name: cr.Name},
	}}
}

func shouldRecoverAbortedCHK(cr *apiChk.ClickHouseKeeperInstallation) bool {
	if cr == nil || cr.Status == nil || cr.Status.GetStatus() != api.StatusAborted {
		return false
	}
	if !cr.GetDeletionTimestamp().IsZero() {
		return false
	}
	for _, err := range cr.Status.GetErrors() {
		for _, reason := range normalizeTimeAbortReasons {
			if strings.HasPrefix(err, "["+reason+"]") {
				return false
			}
		}
	}
	return true
}

// clearAbortGeneration drops the latch written by noteAbortGeneration.
// A missing annotation is already unlatched.
func (c *Controller) clearAbortGeneration(ctx context.Context, namespace, name string) error {
	if c == nil || c.Client == nil || c.APIReader == nil {
		return nil
	}
	cur := &apiChk.ClickHouseKeeperInstallation{}
	key := kubeTypes.NamespacedName{Namespace: namespace, Name: name}
	if err := c.APIReader.Get(ctx, key, cur); err != nil {
		return err
	}
	if cur.Annotations == nil || cur.Annotations[abortGenerationAnnotation] == "" {
		return nil
	}
	base := cur.DeepCopy()
	delete(cur.Annotations, abortGenerationAnnotation)
	return c.Client.Patch(ctx, cur, client.MergeFrom(base))
}
