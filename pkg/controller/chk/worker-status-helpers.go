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

	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
)

// isPodCrushed reports whether the host's pod has crashed containers. An absent pod counts as
// crushed - there is nothing to wait for and the StatefulSet must recreate it - but a pod that
// merely could not be READ does not: the operator reads live, so a Forbidden or a spent retry
// budget reaches here, and treating that as a crash sends a healthy Keeper into the force-restart
// branch (which shouldForceRestartHost gates on an unknown running version).
func (w *worker) isPodCrushed(ctx context.Context, host *api.Host) bool {
	pod, err := w.c.kube.Pod().Get(ctx, host)
	switch {
	case err == nil:
		return k8s.PodHasCrushedContainers(pod)
	case apiErrors.IsNotFound(err):
		return true
	}
	return false
}

// areUsableOldAndNew checks whether there are old and new usable
func (w *worker) areUsableOldAndNew(old, new *apiChk.ClickHouseKeeperInstallation) bool {
	if old == nil {
		return false
	}
	if new == nil {
		return false
	}
	return true
}

// isAfterFinalizerInstalled checks whether we are just installed finalizer
func (w *worker) isAfterFinalizerInstalled(old, new *apiChk.ClickHouseKeeperInstallation) bool {
	if !w.areUsableOldAndNew(old, new) {
		return false
	}

	finalizerIsInstalled := len(old.Finalizers) == 0 && len(new.Finalizers) > 0
	return w.isGenerationTheSame(old, new) && finalizerIsInstalled
}

// isGenerationTheSame checks whether old and new CHI have the same generation
func (w *worker) isGenerationTheSame(old, new *apiChk.ClickHouseKeeperInstallation) bool {
	if !w.areUsableOldAndNew(old, new) {
		return false
	}

	return old.GetGeneration() == new.GetGeneration()
}
