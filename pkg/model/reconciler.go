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

package model

import (
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/config"
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Reconciler is the base struct to create k8s objects
type Reconciler struct {
	appVersion                string
	chi                       *chiv1.ClickHouseInstallation
	chopConfig                *config.Config
	chConfigGenerator         *ClickHouseConfigGenerator
	chConfigSectionsGenerator *configSections
	labeler                   *Labeler
	funcs                     *ReconcileFuncs
}

type ReconcileFuncs struct {
	ReconcileConfigMap   func(configMap *corev1.ConfigMap) error
	ReconcileService     func(service *corev1.Service) error
	ReconcileStatefulSet func(newStatefulSet *apps.StatefulSet, host *chiv1.ChiHost) error
}

// NewReconciler creates new creator
func NewReconciler(
	chi *chiv1.ClickHouseInstallation,
	chopConfig *config.Config,
	appVersion string,
	funcs *ReconcileFuncs,
) *Reconciler {
	reconciler := &Reconciler{
		chi:               chi,
		chopConfig:        chopConfig,
		appVersion:        appVersion,
		chConfigGenerator: NewClickHouseConfigGenerator(chi),
		labeler:           NewLabeler(appVersion, chi),
		funcs:             funcs,
	}
	reconciler.chConfigSectionsGenerator = NewConfigSections(reconciler.chConfigGenerator, reconciler.chopConfig)

	return reconciler
}

// Reconcile runs reconcile process
func (r *Reconciler) Reconcile() error {
	return r.chi.WalkClusterTillError(
		r.reconcileChi,
		r.reconcileCluster,
		r.reconcileShard,
		r.reconcileHost,
	)
}

// reconcileChi reconciles CHI global objects
func (r *Reconciler) reconcileChi(chi *chiv1.ClickHouseInstallation) error {
	if err := r.reconcileChiService(r.chi); err != nil {
		return err
	}

	if err := r.reconcileChiConfigMaps(); err != nil {
		return err
	}

	return nil
}

// reconcileCluster reconciles Cluster, excluding nested shards
func (r *Reconciler) reconcileCluster(cluster *chiv1.ChiCluster) error {
	// Add Cluster's Service
	if service := r.createServiceCluster(cluster); service != nil {
		return r.funcs.ReconcileService(service)
	} else {
		return nil
	}
}

// reconcileShard reconciles Shard, excluding nested replicas
func (r *Reconciler) reconcileShard(shard *chiv1.ChiShard) error {
	// Add Shard's Service
	if service := r.createServiceShard(shard); service != nil {
		return r.funcs.ReconcileService(service)
	} else {
		return nil
	}
}

// reconcileHost reconciles ClickHouse host
func (r *Reconciler) reconcileHost(host *chiv1.ChiHost) error {
	// Add host's Service
	service := r.createServiceHost(host)
	if err := r.funcs.ReconcileService(service); err != nil {
		return err
	}

	// Add host's ConfigMap
	configMap := r.createConfigMapHost(host)
	if err := r.funcs.ReconcileConfigMap(configMap); err != nil {
		return err
	}

	// Add host's StatefulSet
	statefulSet := r.createStatefulSet(host)
	if err := r.funcs.ReconcileStatefulSet(statefulSet, host); err != nil {
		return err
	}

	return nil
}

// reconcileChiService reconciles global Services belonging to CHI
func (r *Reconciler) reconcileChiService(chi *chiv1.ClickHouseInstallation) error {
	service := r.createServiceChi(chi)
	return r.funcs.ReconcileService(service)
}

// reconcileChiConfigMaps reconciles global ConfigMaps belonging to CHI
func (r *Reconciler) reconcileChiConfigMaps() error {
	r.chConfigSectionsGenerator.CreateConfigsUsers()
	r.chConfigSectionsGenerator.CreateConfigsCommon()

	// ConfigMap common for all resources in CHI
	// contains several sections, mapped as separated chopConfig files,
	// such as remote servers, zookeeper setup, etc
	configMapCommon := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CreateConfigMapCommonName(r.chi),
			Namespace: r.chi.Namespace,
			Labels:    r.labeler.getLabelsChiScope(),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: r.chConfigSectionsGenerator.commonConfigSections,
	}
	if err := r.funcs.ReconcileConfigMap(configMapCommon); err != nil {
		return err
	}

	// ConfigMap common for all users resources in CHI
	configMapUsers := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CreateConfigMapCommonUsersName(r.chi),
			Namespace: r.chi.Namespace,
			Labels:    r.labeler.getLabelsChiScope(),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: r.chConfigSectionsGenerator.commonUsersConfigSections,
	}
	if err := r.funcs.ReconcileConfigMap(configMapUsers); err != nil {
		return err
	}

	return nil
}
