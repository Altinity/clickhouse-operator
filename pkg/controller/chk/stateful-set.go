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
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/creator"
	apps "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *Reconciler) reconcileStatefulSet(chk *apiChk.ClickHouseKeeperInstallation) error {
	return r.reconcile(
		chk,
		&apps.StatefulSet{},
		creator.CreateStatefulSet(chk),
		"StatefulSet",
		func(curObject, newObject client.Object) error {
			cur, ok1 := curObject.(*apps.StatefulSet)
			new, ok2 := newObject.(*apps.StatefulSet)
			if !ok1 || !ok2 {
				return fmt.Errorf("unable to cast")
			}
			markPodRestartedNow(new)
			cur.Spec.Selector = new.Spec.Selector
			cur.Spec.Replicas = new.Spec.Replicas
			cur.Spec.Template = new.Spec.Template
			cur.Spec.UpdateStrategy = new.Spec.UpdateStrategy
			return nil
		},
	)
}
