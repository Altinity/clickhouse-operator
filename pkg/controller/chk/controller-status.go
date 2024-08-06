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
	//model "github.com/altinity/clickhouse-operator/pkg/model/chk"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func (c *Controller) reconcileClusterStatus(chk *apiChk.ClickHouseKeeperInstallation) (err error) {
	return nil
	//readyMembers, err := c.getReadyPods(chk)
	if err != nil {
		return err
	}

	for {
		// Fetch the latest ClickHouseKeeper instance again
		cur := &apiChk.ClickHouseKeeperInstallation{}
		if err := c.Client.Get(context.TODO(), util.NamespacedName(chk), cur); err != nil {
			log.V(1).Error("Error: not found %s err: %s", chk.Name, err)
			return err
		}

		if cur.GetStatus() == nil {
			cur.Status = cur.EnsureStatus()
		}
		//cur.Status.Replicas = int32(model.GetReplicasCount(chk))
		//
		//cur.Status.ReadyReplicas = []apiChi.ZookeeperNode{}
		//for _, readyOne := range readyMembers {
		//	cur.Status.ReadyReplicas = append(cur.Status.ReadyReplicas,
		//		apiChi.ZookeeperNode{
		//			Host:   fmt.Sprintf("%s.%s.svc.cluster.local", readyOne, chk.Namespace),
		//			Port:   types.NewInt32(int32(chk.Spec.GetClientPort())),
		//			Secure: types.NewStringBool(false),
		//		})
		//}
		//
		//log.V(2).Info("ReadyReplicas: " + fmt.Sprintf("%v", cur.Status.ReadyReplicas))

		//if len(readyMembers) == model.GetReplicasCount(chk) {
		//	cur.Status.Status = "Completed"
		//} else {
		//	cur.Status.Status = "In progress"
		//}

		cur.Status.NormalizedCR = nil
		cur.Status.NormalizedCRCompleted = chk.DeepCopy()
		cur.Status.NormalizedCRCompleted.ObjectMeta.ResourceVersion = ""
		cur.Status.NormalizedCRCompleted.ObjectMeta.ManagedFields = nil
		cur.Status.NormalizedCRCompleted.Status = nil

		if err := c.Status().Update(context.TODO(), cur); err != nil {
			log.V(1).Error("err: %s", err.Error())
		} else {
			return nil
		}
	}
}
