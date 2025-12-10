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
	"encoding/json"
	"fmt"
	"time"

	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube "k8s.io/client-go/kubernetes"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	commonTypes "github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	chopClientSet "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/creator"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/macro"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type CR struct {
	chopClient chopClientSet.Interface
	kubeClient kube.Interface
	macro      interfaces.IMacro
}

// NewCR is a constructor
func NewCR(chopClient chopClientSet.Interface, kubeClient kube.Interface) *CR {
	return &CR{
		chopClient: chopClient,
		kubeClient: kubeClient,
		macro:      macro.New(),
	}
}

func (c *CR) Get(ctx context.Context, namespace, name string) (api.ICustomResource, error) {
	ctx = k8sCtx(ctx)

	chi, err := c.getCR(ctx, namespace, name)
	if err != nil {
		return nil, err
	}

	// Disregard any errors coming from aux resources, it is non-blocker in this case
	cm, _ := c.getCM(ctx, chi)

	chi = c.buildCR(chi, cm)

	return chi, nil
}

func (c *CR) getCR(ctx context.Context, namespace, name string) (*api.ClickHouseInstallation, error) {
	ctx = k8sCtx(ctx)
	return c.chopClient.ClickhouseV1().ClickHouseInstallations(namespace).Get(ctx, name, controller.NewGetOptions())
}

func (c *CR) getCM(ctx context.Context, chi api.ICustomResource) (*core.ConfigMap, error) {
	ctx = k8sCtx(ctx)
	return NewConfigMap(c.kubeClient).Get(ctx, c.buildCMNamespace(chi), c.buildCMName(chi))
}

// buildCR builds CR out of provided components
func (c *CR) buildCR(chi *api.ClickHouseInstallation, cm *core.ConfigMap) *api.ClickHouseInstallation {
	if cm == nil {
		return chi
	}

	if len(cm.Data[statusNormalized]) > 0 {
		normalized := &api.ClickHouseInstallation{}
		if json.Unmarshal([]byte(cm.Data[statusNormalized]), normalized) != nil {
			return chi
		}
		chi.EnsureStatus().NormalizedCR = normalized
	}

	if len(cm.Data[statusNormalizedCompleted]) > 0 {
		normalizedCompleted := &api.ClickHouseInstallation{}
		if json.Unmarshal([]byte(cm.Data[statusNormalizedCompleted]), normalizedCompleted) != nil {
			return chi
		}
		chi.EnsureStatus().NormalizedCRCompleted = normalizedCompleted
	}

	if len(cm.Data[statusActionPlan]) > 0 {
		chi.EnsureStatus().ActionPlan = nil
	}

	return chi
}

// StatusUpdate updates CR object's Status
func (c *CR) StatusUpdate(ctx context.Context, cr api.ICustomResource, opts commonTypes.UpdateStatusOptions) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. cr: %s ", cr.GetName())
		return nil
	}

	return c.statusUpdateRetry(ctx, cr, opts)
}

const maxRetryAttempts = 50

func (c *CR) statusUpdateRetry(ctx context.Context, cr api.ICustomResource, opts commonTypes.UpdateStatusOptions) (err error) {
	for retry, attempt := true, 1; retry; attempt++ {
		if attempt >= maxRetryAttempts {
			// Make last attempt
			retry = false
		}

		err = c.statusUpdateProcess(ctx, cr, opts)
		if err == nil {
			return nil
		}

		if retry {
			log.V(2).M(cr).F().Warning("got error, will retry attempt %d. err: %q", attempt, err)
			time.Sleep(1 * time.Second)
		} else {
			log.V(1).M(cr).F().Error("got error, attempt %d all retries are exhausted. err: %q", attempt, err)
		}
	}
	return
}

// statusUpdateProcess updates CR object's Status
func (c *CR) statusUpdateProcess(ctx context.Context, icr api.ICustomResource, opts commonTypes.UpdateStatusOptions) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. cr: %s ", icr.GetName())
		return nil
	}

	cr, ok := icr.(*api.ClickHouseInstallation)
	if !ok {
		return nil
	}

	namespace, name := cr.NamespaceName()
	log.V(3).M(cr).F().Info("Update CR status")

	_cur, err := c.Get(ctx, namespace, name)
	if err != nil {
		if apiErrors.IsNotFound(err) {
			return nil
		}
		if opts.TolerateAbsence {
			return nil
		}
		log.V(1).M(cr).F().Error("%q", err)
		return err
	}

	cur, ok := _cur.(*api.ClickHouseInstallation)
	if !ok {
		return nil
	}
	if cur == nil {
		if opts.TolerateAbsence {
			return nil
		}
		log.V(1).M(cr).F().Error("NULL returned")
		return fmt.Errorf("ERROR GetCR (%s/%s): NULL returned", namespace, name)
	}

	// Update status of a real (current) object.
	cur.EnsureStatus().CopyFrom(cr.Status, opts.CopyStatusOptions)

	err = c.statusUpdate(ctx, cur)
	if err != nil {
		// Error update
		log.V(2).M(cr).F().Info("Got error upon update, may retry. err: %q", err)
		return err
	}

	_cur, err = c.Get(ctx, namespace, name)
	if err != nil {
		return nil
	}
	cur, ok = _cur.(*api.ClickHouseInstallation)
	if !ok {
		return nil
	}

	// Propagate updated ResourceVersion upstairs into the CR
	if cr.GetResourceVersion() != cur.GetResourceVersion() {
		log.V(3).M(cr).F().Info("ResourceVersion change: %s to %s", cr.GetResourceVersion(), cur.GetResourceVersion())
		cr.SetResourceVersion(cur.GetResourceVersion())
		return nil
	}

	// ResourceVersion not changed - no update performed?

	return nil
}

func (c *CR) statusUpdate(ctx context.Context, chi *api.ClickHouseInstallation) error {
	chi, cm := c.buildStatusResources(chi)

	// Start with aux resources, disregard any errors
	_ = c.statusUpdateCM(ctx, cm)

	// Finish with primary CR
	return c.statusUpdateCR(ctx, chi)
}

// buildStatusResources builds several status resources out of a single CR - a.k.a split
func (c *CR) buildStatusResources(chi *api.ClickHouseInstallation) (*api.ClickHouseInstallation, *core.ConfigMap) {
	// Build required components
	tagger := managers.NewTagManager(managers.TagManagerTypeClickHouse, chi)
	namespace, name := c.buildNamespaceName(chi)

	// Build ConfigMap
	cm := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
			Namespace:       namespace,
			Name:            name,
			Labels:          c.macro.Scope(chi).Map(tagger.Label(interfaces.LabelConfigMapStorage)),
			Annotations:     c.macro.Scope(chi).Map(tagger.Annotate(interfaces.AnnotateConfigMapStorage)),
			OwnerReferences: creator.NewOwnerReferencer().CreateOwnerReferences(chi),
		},
		Data: c.buildStatusResourceData(chi),
	}

	// Clean data that are coped into resource
	c.cleanStatusResourceData(chi)

	return chi, cm
}

func (c *CR) buildNamespaceName(chi *api.ClickHouseInstallation) (string, string) {
	return c.buildCMNamespace(chi), c.buildCMName(chi)
}

func (c *CR) buildStatusResourceData(chi *api.ClickHouseInstallation) map[string]string {
	data := make(map[string]string)
	if chi.Status.NormalizedCR != nil {
		bytes, _ := json.Marshal(chi.Status.NormalizedCR)
		data[statusNormalized] = string(bytes)
	}
	if chi.Status.NormalizedCRCompleted != nil {
		bytes, _ := json.Marshal(chi.Status.NormalizedCRCompleted)
		data[statusNormalizedCompleted] = string(bytes)
	}
	if chi.Status.ActionPlan != nil {
		data[statusActionPlan] = chi.Status.ActionPlan.String()
	} else {
		log.V(1).Info("ActionPlan is empty!")
	}
	return data
}

func (c *CR) cleanStatusResourceData(chi *api.ClickHouseInstallation) {
	chi.Status.NormalizedCR = nil
	chi.Status.NormalizedCRCompleted = nil
	chi.Status.ActionPlan = nil
}

func (c *CR) statusUpdateCR(ctx context.Context, chi *api.ClickHouseInstallation) error {
	_, err := c.chopClient.ClickhouseV1().ClickHouseInstallations(chi.GetNamespace()).UpdateStatus(ctx, chi, controller.NewUpdateOptions())
	return err
}

func (c *CR) statusUpdateCM(ctx context.Context, cm *core.ConfigMap) error {
	if cm == nil {
		return nil
	}
	cmm := NewConfigMap(c.kubeClient)
	_, err := cmm.Update(ctx, cm)
	if apiErrors.IsNotFound(err) {
		_, err = cmm.Create(ctx, cm)
	}
	return err
}

func (c *CR) buildCMNamespace(obj meta.Object) string {
	return obj.GetNamespace()
}

func (c *CR) buildCMName(obj meta.Object) string {
	return "chi-storage-" + obj.GetName()
}

const (
	statusNormalized          = "status-normalized"
	statusNormalizedCompleted = "status-normalizedCompleted"
	statusActionPlan          = "status-actionPlan"
)
