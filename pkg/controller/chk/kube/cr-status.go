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

package kube

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	common "github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type CRStatus struct {
	kube client.Client
}

func NewCRStatus(kubeClient client.Client) *CRStatus {
	return &CRStatus{
		kube: kubeClient,
	}
}

// updateCHIObjectStatus updates ClickHouseInstallation object's Status
func (c *CRStatus) Update(ctx context.Context, cr apiChi.ICustomResource, opts common.UpdateStatusOptions) (err error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	for retry, attempt := true, 1; retry; attempt++ {
		if attempt > 60 {
			retry = false
		}

		err = c.doUpdateCRStatus(ctx, cr, opts)
		if err == nil {
			return nil
		}

		if retry {
			log.V(2).M(cr).F().Warning("got error, will retry. err: %q", err)
			time.Sleep(1 * time.Second)
		} else {
			log.V(1).M(cr).F().Error("got error, all retries are exhausted. err: %q", err)
		}
	}
	return
}

// doUpdateCRStatus updates ClickHouseInstallation object's Status
func (c *CRStatus) doUpdateCRStatus(ctx context.Context, cr apiChi.ICustomResource, opts common.UpdateStatusOptions) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	chk := cr.(*apiChk.ClickHouseKeeperInstallation)
	namespace, name := util.NamespaceName(chk)
	log.V(3).M(chk).F().Info("Update CHK status")

	cur := &apiChk.ClickHouseKeeperInstallation{}
	err := c.kube.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, cur)
	if err != nil {
		if opts.TolerateAbsence {
			return nil
		}
		log.V(1).M(chk).F().Error("%q", err)
		return err
	}
	if cur == nil {
		if opts.TolerateAbsence {
			return nil
		}
		log.V(1).M(chk).F().Error("NULL returned")
		return fmt.Errorf("ERROR GetCR (%s/%s): NULL returned", namespace, name)
	}

	// Update status of a real object.
	cur.EnsureStatus().CopyFrom(chk.Status, opts.CopyStatusOptions)

	err = c.kube.Status().Update(ctx, cur)
	if err != nil {
		// Error update
		log.V(2).M(chk).F().Info("Got error upon update, may retry. err: %q", err)
		return err
	}

	new := &apiChk.ClickHouseKeeperInstallation{}
	err = c.kube.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, cur)
	if err != nil {
		if opts.TolerateAbsence {
			return nil
		}
		log.V(1).M(chk).F().Error("%q", err)
		return err
	}

	// Propagate updated ResourceVersion into chi
	if chk.GetResourceVersion() != new.GetResourceVersion() {
		log.V(3).M(chk).F().Info("ResourceVersion change: %s to %s", chk.GetResourceVersion(), new.GetResourceVersion())
		chk.SetResourceVersion(new.GetResourceVersion())
		return nil
	}

	// ResourceVersion not changed - no update performed?

	return nil
}
