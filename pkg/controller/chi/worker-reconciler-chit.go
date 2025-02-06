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
	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
)

func (w *worker) shouldUpdateCHITList() bool {
	update := false
	switch chop.Config().Template.CHI.Policy {
	case api.OperatorConfigCHIPolicyReadOnStart:
		update = w.isJustStarted()
	case api.OperatorConfigCHIPolicyApplyOnNextReconcile:
		update = true
	default:
		update = false
	}
	return update
}

// addChit sync new CHIT - creates all its resources
func (w *worker) addChit(chit *api.ClickHouseInstallationTemplate) error {
	if w.shouldUpdateCHITList() {
		log.V(1).M(chit).F().Info("Add CHIT: %s/%s", chit.GetNamespace(), chit.GetName())
		chop.Config().AddCHITemplate((*api.ClickHouseInstallation)(chit))
	} else {
		log.V(1).M(chit).F().Info("CHIT will not be added: %s/%s", chit.GetNamespace(), chit.GetName())
	}
	return nil
}

// updateChit sync CHIT which was already created earlier
func (w *worker) updateChit(old, new *api.ClickHouseInstallationTemplate) error {
	if old.GetObjectMeta().GetResourceVersion() == new.GetObjectMeta().GetResourceVersion() {
		log.V(2).M(old).F().Info("ResourceVersion did not change: %s", old.GetObjectMeta().GetResourceVersion())
		// No need to react
		return nil
	}

	log.V(1).M(new).F().Info("ResourceVersion change: %s to %s", old.GetObjectMeta().GetResourceVersion(), new.GetObjectMeta().GetResourceVersion())
	if w.shouldUpdateCHITList() {
		log.V(1).M(new).F().Info("Update CHIT: %s/%s", new.GetNamespace(), new.GetName())
		chop.Config().UpdateCHITemplate((*api.ClickHouseInstallation)(new))
	} else {
		log.V(1).M(new).F().Info("CHIT will not be updated: %s/%s", new.GetNamespace(), new.GetName())
	}
	return nil
}

// deleteChit deletes CHIT
func (w *worker) deleteChit(chit *api.ClickHouseInstallationTemplate) error {
	log.V(1).M(chit).F().P()

	if w.shouldUpdateCHITList() {
		log.V(1).M(chit).F().Info("Delete CHIT: %s/%s", chit.GetNamespace(), chit.GetName())
		chop.Config().DeleteCHITemplate((*api.ClickHouseInstallation)(chit))
	} else {
		log.V(1).M(chit).F().Info("CHIT will not be deleted: %s/%s", chit.GetNamespace(), chit.GetName())
	}
	return nil
}
