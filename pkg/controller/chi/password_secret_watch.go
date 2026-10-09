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

	core "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/labels"
	kubeInformers "k8s.io/client-go/informers"
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

// WatchPasswordSecrets watches Secrets that are not created by the operator.
// The kube informer factory used for Pods and StatefulSets is limited to
// objects carrying the operator's app label, so a user's password Secret would
// never be delivered there. This factory must not use that label selector.
func (c *Controller) WatchPasswordSecrets(
	chopInformerFactory chopInformers.SharedInformerFactory,
	secretInformerFactory kubeInformers.SharedInformerFactory,
) {
	c.chiLister = chopInformerFactory.Clickhouse().V1().ClickHouseInstallations().Lister()
	secretInformerFactory.Core().V1().Secrets().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			secret, ok := obj.(*core.Secret)
			if !ok {
				return
			}
			c.onPasswordSecret(secret)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldSecret, okOld := oldObj.(*core.Secret)
			newSecret, okNew := newObj.(*core.Secret)
			if !okOld || !okNew {
				return
			}
			if apiequality.Semantic.DeepEqual(oldSecret.Data, newSecret.Data) {
				return
			}
			c.onPasswordSecret(newSecret)
		},
		DeleteFunc: deleteHandler("passwordSecretInformer.DeleteFunc", func(secret *core.Secret) {
			c.onPasswordSecret(secret)
		}),
	})
}

func (c *Controller) onPasswordSecret(secret *core.Secret) {
	if secret == nil || c.chiLister == nil {
		return
	}
	if !chop.Config().IsNamespaceWatched(secret.Namespace) {
		return
	}
	// The managed users Secret is operator-generated. Reconciling it again
	// would rewrite it and enqueue another pass.
	if chiLabeler.New(nil).IsCHOPGeneratedObject(secret) {
		return
	}
	chis, err := c.chiLister.ClickHouseInstallations(secret.Namespace).List(labels.Everything())
	if err != nil {
		log.V(1).F().Error("unable to list ClickHouseInstallations for Secret %s/%s: %v", secret.Namespace, secret.Name, err)
		return
	}
	for _, cr := range chis {
		if chiReferencesHotReloadSecret(cr, secret.Name) {
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
