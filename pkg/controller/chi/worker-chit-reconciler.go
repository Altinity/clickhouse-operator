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
	chiV1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
)

// addChit sync new CHIT - creates all its resources
func (w *worker) addChit(chit *chiV1.ClickHouseInstallationTemplate) error {
	log.V(1).M(chit).F().P()
	chop.Config().AddCHITemplate((*chiV1.ClickHouseInstallation)(chit))
	return nil
}

// updateChit sync CHIT which was already created earlier
func (w *worker) updateChit(old, new *chiV1.ClickHouseInstallationTemplate) error {
	if old.ObjectMeta.ResourceVersion == new.ObjectMeta.ResourceVersion {
		log.V(2).M(old).F().Info("ResourceVersion did not change: %s", old.ObjectMeta.ResourceVersion)
		// No need to react
		return nil
	}

	log.V(1).M(new).F().Info("ResourceVersion change: %s to %s", old.ObjectMeta.ResourceVersion, new.ObjectMeta.ResourceVersion)
	chop.Config().UpdateCHITemplate((*chiV1.ClickHouseInstallation)(new))
	return nil
}

// deleteChit deletes CHIT
func (w *worker) deleteChit(chit *chiV1.ClickHouseInstallationTemplate) error {
	log.V(1).M(chit).F().P()
	chop.Config().DeleteCHITemplate((*chiV1.ClickHouseInstallation)(chit))
	return nil
}
