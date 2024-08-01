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
	"fmt"

	core "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

func (w *worker) reconcileClientService(chk *apiChk.ClickHouseKeeperInstallation) error {
	return w.c.reconcile(
		chk,
		&core.Service{},
		w.task.GetCreator().CreateService(interfaces.ServiceCR, chk),
		"Client Service",
		reconcileUpdaterService,
	)
}

func (w *worker) reconcileHeadlessService(chk *apiChk.ClickHouseKeeperInstallation) error {
	return w.c.reconcile(
		chk,
		&core.Service{},
		w.task.GetCreator().CreateService(interfaces.ServiceCHIHost, chk),
		"Headless Service",
		reconcileUpdaterService,
	)
}

func reconcileUpdaterService(_cur, _new client.Object) error {
	cur, ok1 := _cur.(*core.Service)
	new, ok2 := _new.(*core.Service)
	if !ok1 || !ok2 {
		return fmt.Errorf("unable to cast")
	}
	return updateService(cur, new)
}

func updateService(cur, new *core.Service) error {
	cur.Spec.Ports = new.Spec.Ports
	cur.Spec.Type = new.Spec.Type
	cur.SetAnnotations(new.GetAnnotations())
	return nil
}
