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

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	chopInformers "github.com/altinity/clickhouse-operator/pkg/client/informers/externalversions"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/cmd_queue"
	chiLabeler "github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
)

// passwordSecretReconcileKey marks a reconcile that was enqueued because a
// source Secret changed, rather than because the CHI spec changed.
type passwordSecretReconcileKey struct{}

func passwordSecretReconcile(ctx context.Context) bool {
	v, _ := ctx.Value(passwordSecretReconcileKey{}).(bool)
	return v
}

// WatchPasswordSecrets reacts to changes in source password Secret metadata.
// The regular Kubernetes informer factory is restricted to operator-generated
// objects, so source Secrets need a separate, unfiltered metadata informer.
func (c *Controller) WatchPasswordSecrets(
	chopInformerFactory chopInformers.SharedInformerFactory,
	secretInformer cache.SharedIndexInformer,
) {
	c.chiLister = chopInformerFactory.Clickhouse().V1().ClickHouseInstallations().Lister()
	secretInformer.AddEventHandler(passwordSecretEventHandlers(c.onPasswordSecret))
}

// passwordSecretEventHandlers handles metadata only: resource-version changes
// may include non-password updates, but the renderer avoids writing an unchanged
// managed Secret. Data is never fetched or cached by this informer.
func passwordSecretEventHandlers(onSecret func(meta.Object)) cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			secret, ok := obj.(*meta.PartialObjectMetadata)
			if ok && secret != nil {
				onSecret(secret)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldSecret, okOld := oldObj.(*meta.PartialObjectMetadata)
			newSecret, okNew := newObj.(*meta.PartialObjectMetadata)
			if !okOld || !okNew || oldSecret == nil || newSecret == nil {
				return
			}
			if oldSecret.ResourceVersion == newSecret.ResourceVersion {
				return
			}
			onSecret(newSecret)
		},
		DeleteFunc: deleteHandler("passwordSecretInformer.DeleteFunc", func(secret *meta.PartialObjectMetadata) {
			onSecret(secret)
		}),
	}
}

func (c *Controller) onPasswordSecret(secret meta.Object) {
	if secret == nil || c.chiLister == nil {
		return
	}
	namespace, name := secret.GetNamespace(), secret.GetName()
	if !chop.Config().IsNamespaceWatched(namespace) {
		return
	}
	// The managed users Secret is operator-generated; ignore its own updates.
	if chiLabeler.New(nil).IsCHOPGeneratedObject(secret) {
		return
	}
	chis, err := c.chiLister.ClickHouseInstallations(namespace).List(labels.Everything())
	if err != nil {
		log.V(1).F().Error("unable to list ClickHouseInstallations for Secret %s/%s: %v", namespace, name, err)
		return
	}
	for _, cr := range chis {
		if chiReferencesHotReloadSecret(cr, name) {
			c.enqueueObject(cmd_queue.NewReconcileCHI(cmd_queue.ReconcilePasswordSecret, nil, cr))
		}
	}
}

// chiReferencesHotReloadSecret reports whether the CHI, as stored by the user,
// opts a password field into hotReload against this Secret. It does not read
// Secret data. References that exist only on a ClickHouseInstallationTemplate
// are not visible here; those need a manual CHI reconcile.
func chiReferencesHotReloadSecret(cr *api.ClickHouseInstallation, secretName string) bool {
	if cr == nil || secretName == "" {
		return false
	}
	users := cr.GetSpecT().GetConfiguration().GetUsers()
	if users == nil {
		return false
	}
	found := false
	users.Walk(func(name string, setting *api.Setting) {
		ref := setting.GetSecretKeyRef()
		if setting.IsHotReload() && api.IsHotReloadUserAuthField(name) && ref != nil && ref.Name == secretName {
			found = true
		}
	})
	return found
}
