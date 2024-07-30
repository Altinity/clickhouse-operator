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
	chkConfig "github.com/altinity/clickhouse-operator/pkg/model/chk/config"
)

func (w *worker) reconcileConfigMap(chk *apiChk.ClickHouseKeeperInstallation) error {
	return w.c.reconcile(
		chk,
		&core.ConfigMap{},
		w.task.Creator.CreateConfigMap(interfaces.ConfigMapConfig, chkConfig.NewConfigFilesGeneratorOptionsKeeper().SetSettings(chk.GetSpec().GetConfiguration().GetSettings())),
		"ConfigMap",
		func(_cur, _new client.Object) error {
			cur, ok1 := _cur.(*core.ConfigMap)
			new, ok2 := _new.(*core.ConfigMap)
			if !ok1 || !ok2 {
				return fmt.Errorf("unable to cast")
			}
			cur.Data = new.Data
			cur.BinaryData = new.BinaryData
			return nil
		},
	)
}
