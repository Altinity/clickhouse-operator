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

	"gopkg.in/yaml.v3"

	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube "k8s.io/client-go/kubernetes"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	commonTypes "github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	chopClientSet "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type CR struct {
	chopClient chopClientSet.Interface
	kubeClient kube.Interface
}

func NewCR(chopClient chopClientSet.Interface, kubeClient kube.Interface) *CR {
	return &CR{
		chopClient: chopClient,
		kubeClient: kubeClient,
	}
}

func (c *CR) Get(ctx context.Context, namespace, name string) (api.ICustomResource, error) {
	chi, err := c.getCR(ctx, namespace, name)
	if err != nil {
		return nil, err
	}

	cm, _ := c.getCM(ctx, chi)

	chi = c.buildCR(chi, cm)

	return chi, nil
}

func (c *CR) getCR(ctx context.Context, namespace, name string) (*api.ClickHouseInstallation, error) {
	return c.chopClient.ClickhouseV1().ClickHouseInstallations(namespace).Get(ctx, name, controller.NewGetOptions())
}

func (c *CR) getCM(ctx context.Context, chi api.ICustomResource) (*core.ConfigMap, error) {
	return NewConfigMap(c.kubeClient).Get(ctx, c.buildCMNamespace(chi), c.buildCMName(chi))
}

func (c *CR) buildCR(chi *api.ClickHouseInstallation, cm *core.ConfigMap) *api.ClickHouseInstallation {
	if cm == nil {
		return chi
	}

	if len(cm.Data[statusNormalized]) > 0 {
		normalized := &api.ClickHouseInstallation{}
		if yaml.Unmarshal([]byte(cm.Data[statusNormalized]), normalized) != nil {
			return chi
		}
		chi.EnsureStatus().NormalizedCR = normalized
	}

	if len(cm.Data[statusNormalizedCompleted]) > 0 {
		normalizedCompleted := &api.ClickHouseInstallation{}
		if yaml.Unmarshal([]byte(cm.Data[statusNormalizedCompleted]), normalizedCompleted) != nil {
			return chi
		}
		chi.EnsureStatus().NormalizedCRCompleted = normalizedCompleted
	}

	return chi
}

// StatusUpdate updates CR object's Status
func (c *CR) StatusUpdate(ctx context.Context, cr api.ICustomResource, opts commonTypes.UpdateStatusOptions) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	return c.statusUpdateRetry(ctx, cr, opts)
}

func (c *CR) statusUpdateRetry(ctx context.Context, cr api.ICustomResource, opts commonTypes.UpdateStatusOptions) (err error) {
	for retry, attempt := true, 1; retry; attempt++ {
		if attempt > 60 {
			retry = false
		}

		err = c.statusUpdateProcess(ctx, cr, opts)
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

// statusUpdateProcess updates CR object's Status
func (c *CR) statusUpdateProcess(ctx context.Context, icr api.ICustomResource, opts commonTypes.UpdateStatusOptions) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	cr := icr.(*api.ClickHouseInstallation)
	namespace, name := cr.NamespaceName()
	log.V(3).M(cr).F().Info("Update CR status")

	_cur, err := c.Get(ctx, namespace, name)
	cur := _cur.(*api.ClickHouseInstallation)
	if err != nil {
		if opts.TolerateAbsence {
			return nil
		}
		log.V(1).M(cr).F().Error("%q", err)
		return err
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
	cur = _cur.(*api.ClickHouseInstallation)

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
	chi, cm := c.buildResources(chi)

	err := c.statusUpdateCR(ctx, chi)
	if err != nil {
		return err
	}

	err = c.statusUpdateCM(ctx, cm)
	if err != nil {
		return err
	}

	return nil
}

func (c *CR) buildResources(chi *api.ClickHouseInstallation) (*api.ClickHouseInstallation, *core.ConfigMap) {
	var normalized, normalizedCompleted []byte
	if chi.Status.NormalizedCR != nil {
		normalized, _ = yaml.Marshal(chi.Status.NormalizedCR)
	}
	if chi.Status.NormalizedCRCompleted != nil {
		normalizedCompleted, _ = yaml.Marshal(chi.Status.NormalizedCRCompleted)
	}
	cm := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
			Namespace: c.buildCMNamespace(chi),
			Name:      c.buildCMName(chi),
		},
		Data: map[string]string{
			statusNormalized:          string(normalized),
			statusNormalizedCompleted: string(normalizedCompleted),
		},
	}
	chi.Status.NormalizedCR = nil
	chi.Status.NormalizedCRCompleted = nil
	return chi, cm
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
)
