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

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
	"github.com/altinity/clickhouse-operator/pkg/util/runtime"
)

func (w *worker) reconcileCHK(ctx context.Context, old, new *apiChk.ClickHouseKeeperInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	if new.HasAncestor() {
		log.V(2).M(new).F().Info("has ancestor, use it as a base for reconcile. CHK: %s/%s", new.Namespace, new.Name)
		old = new.GetAncestor()
	} else {
		log.V(2).M(new).F().Info("has NO ancestor, use empty CHK as a base for reconcile. CHK: %s/%s", new.Namespace, new.Name)
		old = nil
	}

	log.V(2).M(new).F().Info("Normalized OLD CHK: %s/%s", new.Namespace, new.Name)
	old = w.normalize(old)

	log.V(2).M(new).F().Info("Normalized NEW CHK %s/%s", new.Namespace, new.Name)
	new = w.normalize(new)
	new.SetAncestor(old)

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.newTask(new)

	if old.GetGeneration() != new.GetGeneration() {
		if err := w.reconcile(ctx, new); err != nil {
			// Something went wrong
		} else {
			// Reconcile successful
		}
	}

	return nil
}

func (w *worker) reconcile(ctx context.Context, chk *apiChk.ClickHouseKeeperInstallation) error {
	for _, f := range []reconcileFunc{
		w.reconcileConfigMap,
		w.reconcileStatefulSet,
		w.reconcileClientService,
		w.reconcileHeadlessService,
		w.reconcilePodDisruptionBudget,
	} {
		if err := f(chk); err != nil {
			log.V(1).Error("Error during reconcile. f: %s err: %s", runtime.FunctionName(f), err)
			return err
		}
	}
	return nil
}
