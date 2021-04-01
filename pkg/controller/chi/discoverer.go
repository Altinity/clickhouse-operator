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

	"github.com/altinity/clickhouse-operator/pkg/util"
	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopmodel "github.com/altinity/clickhouse-operator/pkg/model"
)

func (c *Controller) discovery(ctx context.Context, chi *chop.ClickHouseInstallation) *chopmodel.Registry {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	r := chopmodel.NewRegistry()

	labeler := chopmodel.NewLabeler(c.chop, chi)
	opts := newListOptions(labeler.GetSelectorCHIScope())

	if list, err := c.kubeClient.AppsV1().StatefulSets(chi.Namespace).List(ctx, opts); err != nil {
		log.M(chi).A().Error("FAIL list StatefulSet err:%v", err)
	} else if list != nil {
		for _, obj := range list.Items {
			r.RegisterStatefulSet(obj.ObjectMeta)
		}
	} else {
		log.M(chi).A().Error("FAIL list StatefulSet list is nil")
	}

	if list, err := c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).List(ctx, opts); err != nil {
		log.M(chi).A().Error("FAIL list ConfigMap err:%v", err)
	} else if list != nil {
		for _, obj := range list.Items {
			r.RegisterConfigMap(obj.ObjectMeta)
		}
	} else {
		log.M(chi).A().Error("FAIL list ConfigMap list is nil")
	}

	if list, err := c.kubeClient.CoreV1().Services(chi.Namespace).List(ctx, opts); err != nil {
		log.M(chi).A().Error("FAIL list Service err:%v", err)
	} else if list != nil {
		for _, obj := range list.Items {
			r.RegisterService(obj.ObjectMeta)
		}
	} else {
		log.M(chi).A().Error("FAIL list Service list is nil")
	}

	if list, err := c.kubeClient.CoreV1().PersistentVolumeClaims(chi.Namespace).List(ctx, opts); err != nil {
		log.M(chi).A().Error("FAIL list PVC err:%v", err)
	} else if list != nil {
		for _, obj := range list.Items {
			r.RegisterPVC(obj.ObjectMeta)
		}
	} else {
		log.M(chi).A().Error("FAIL list PVC list is nil")
	}

	if list, err := c.kubeClient.CoreV1().PersistentVolumes().List(ctx, opts); err != nil {
		log.M(chi).A().Error("FAIL list PV err:%v", err)
	} else if list != nil {
		for _, obj := range list.Items {
			r.RegisterPV(obj.ObjectMeta)
		}
	} else {
		log.M(chi).A().Error("FAIL list PV list is nil")
	}

	return r
}
