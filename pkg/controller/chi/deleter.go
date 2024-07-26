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
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/kube"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/storage"
	apps "k8s.io/api/apps/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// deleteHost deletes all kubernetes resources related to replica *chop.Host
func (c *Controller) deleteHost(ctx context.Context, host *api.Host) error {
	log.V(1).M(host).S().Info(host.Runtime.Address.ClusterNameString())

	// Each host consists of:
	_ = c.deleteStatefulSet(ctx, host)
	_ = storage.NewStoragePVC(kube.NewPVCClickHouse(c.kubeClient)).DeletePVC(ctx, host)
	_ = c.deleteConfigMap(ctx, host)
	_ = c.deleteServiceHost(ctx, host)

	log.V(1).M(host).E().Info(host.Runtime.Address.ClusterNameString())

	return nil
}

// deleteConfigMapsCHI
func (c *Controller) deleteConfigMapsCHI(ctx context.Context, chi *api.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// Delete common ConfigMap's
	//
	// chi-b3d29f-common-configd   2      61s
	// chi-b3d29f-common-usersd    0      61s
	// service/clickhouse-example-01         LoadBalancer   10.106.183.200   <pending>     8123:31607/TCP,9000:31492/TCP,9009:31357/TCP   33s   clickhouse.altinity.com/chi=example-01

	var err error

	configMapCommon := c.namer.Name(interfaces.NameConfigMapCommon, chi)
	configMapCommonUsersName := c.namer.Name(interfaces.NameConfigMapCommonUsers, chi)

	// Delete ConfigMap
	err = c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).Delete(ctx, configMapCommon, controller.NewDeleteOptions())
	switch {
	case err == nil:
		log.V(1).M(chi).Info("OK delete ConfigMap %s/%s", chi.Namespace, configMapCommon)
	case apiErrors.IsNotFound(err):
		log.V(1).M(chi).Info("NEUTRAL not found ConfigMap %s/%s", chi.Namespace, configMapCommon)
	default:
		log.V(1).M(chi).F().Error("FAIL delete ConfigMap %s/%s err:%v", chi.Namespace, configMapCommon, err)
	}

	err = c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).Delete(ctx, configMapCommonUsersName, controller.NewDeleteOptions())
	switch {
	case err == nil:
		log.V(1).M(chi).Info("OK delete ConfigMap %s/%s", chi.Namespace, configMapCommonUsersName)
	case apiErrors.IsNotFound(err):
		log.V(1).M(chi).Info("NEUTRAL not found ConfigMap %s/%s", chi.Namespace, configMapCommonUsersName)
		err = nil
	default:
		log.V(1).M(chi).F().Error("FAIL delete ConfigMap %s/%s err:%v", chi.Namespace, configMapCommonUsersName, err)
	}

	return err
}

// statefulSetDeletePod delete a pod of a StatefulSet. This requests StatefulSet to relaunch deleted pod
func (c *Controller) statefulSetDeletePod(ctx context.Context, statefulSet *apps.StatefulSet, host *api.Host) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	name := c.namer.Name(interfaces.NamePod, statefulSet)
	log.V(1).M(host).Info("Delete Pod %s/%s", statefulSet.Namespace, name)
	err := c.kubeClient.CoreV1().Pods(statefulSet.Namespace).Delete(ctx, name, controller.NewDeleteOptions())
	if err == nil {
		log.V(1).M(host).Info("OK delete Pod %s/%s", statefulSet.Namespace, name)
	} else if apiErrors.IsNotFound(err) {
		log.V(1).M(host).Info("NEUTRAL not found Pod %s/%s", statefulSet.Namespace, name)
		err = nil
	} else {
		log.V(1).M(host).F().Error("FAIL delete Pod %s/%s err:%v", statefulSet.Namespace, name, err)
	}

	return err
}

func (c *Controller) deleteStatefulSet(ctx context.Context, host *api.Host) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	name := c.namer.Name(interfaces.NameStatefulSet, host)
	namespace := host.Runtime.Address.Namespace
	log.V(1).M(host).F().Info("%s/%s", namespace, name)
	return c.kube.STS().Delete(namespace, name)
}

// deleteConfigMap deletes ConfigMap
func (c *Controller) deleteConfigMap(ctx context.Context, host *api.Host) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	name := c.namer.Name(interfaces.NameConfigMapHost, host)
	namespace := host.Runtime.Address.Namespace
	log.V(1).M(host).F().Info("%s/%s", namespace, name)

	if err := c.kubeClient.CoreV1().ConfigMaps(namespace).Delete(ctx, name, controller.NewDeleteOptions()); err == nil {
		log.V(1).M(host).Info("OK delete ConfigMap %s/%s", namespace, name)
	} else if apiErrors.IsNotFound(err) {
		log.V(1).M(host).Info("NEUTRAL not found ConfigMap %s/%s", namespace, name)
	} else {
		log.V(1).M(host).F().Error("FAIL delete ConfigMap %s/%s err:%v", namespace, name, err)
	}
	return nil
}

// deleteServiceHost deletes Service
func (c *Controller) deleteServiceHost(ctx context.Context, host *api.Host) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	serviceName := c.namer.Name(interfaces.NameStatefulSetService, host)
	namespace := host.Runtime.Address.Namespace
	log.V(1).M(host).F().Info("%s/%s", namespace, serviceName)
	return c.deleteServiceIfExists(ctx, namespace, serviceName)
}

// deleteServiceShard
func (c *Controller) deleteServiceShard(ctx context.Context, shard *api.ChiShard) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	serviceName := c.namer.Name(interfaces.NameShardService, shard)
	namespace := shard.Runtime.Address.Namespace
	log.V(1).M(shard).F().Info("%s/%s", namespace, serviceName)
	return c.deleteServiceIfExists(ctx, namespace, serviceName)
}

// deleteServiceCluster
func (c *Controller) deleteServiceCluster(ctx context.Context, cluster *api.ChiCluster) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	serviceName := c.namer.Name(interfaces.NameClusterService, cluster)
	namespace := cluster.Runtime.Address.Namespace
	log.V(1).M(cluster).F().Info("%s/%s", namespace, serviceName)
	return c.deleteServiceIfExists(ctx, namespace, serviceName)
}

// deleteServiceCHI
func (c *Controller) deleteServiceCHI(ctx context.Context, chi *api.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	serviceName := c.namer.Name(interfaces.NameCRService, chi)
	namespace := chi.Namespace
	log.V(1).M(chi).F().Info("%s/%s", namespace, serviceName)
	return c.deleteServiceIfExists(ctx, namespace, serviceName)
}

// deleteServiceIfExists deletes Service in case it does not exist
func (c *Controller) deleteServiceIfExists(ctx context.Context, namespace, name string) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// Check specified service exists
	_, err := c.kubeClient.CoreV1().Services(namespace).Get(ctx, name, controller.NewGetOptions())

	if err != nil {
		// No such a service, nothing to delete
		log.V(1).M(namespace, name).F().Info("Not Found Service: %s/%s err: %v", namespace, name, err)
		return nil
	}

	// Delete service
	err = c.kubeClient.CoreV1().Services(namespace).Delete(ctx, name, controller.NewDeleteOptions())
	if err == nil {
		log.V(1).M(namespace, name).F().Info("OK delete Service: %s/%s", namespace, name)
	} else {
		log.V(1).M(namespace, name).F().Error("FAIL delete Service: %s/%s err:%v", namespace, name, err)
	}

	return err
}

// deleteSecretCluster
func (c *Controller) deleteSecretCluster(ctx context.Context, cluster *api.ChiCluster) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	secretName := c.namer.Name(interfaces.NameClusterAutoSecret, cluster)
	namespace := cluster.Runtime.Address.Namespace
	log.V(1).M(cluster).F().Info("%s/%s", namespace, secretName)
	return c.deleteSecretIfExists(ctx, namespace, secretName)
}

// deleteSecretIfExists deletes Secret in case it does not exist
func (c *Controller) deleteSecretIfExists(ctx context.Context, namespace, name string) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// Check specified service exists
	_, err := c.kubeClient.CoreV1().Secrets(namespace).Get(ctx, name, controller.NewGetOptions())

	if err != nil {
		// No such a service, nothing to delete
		return nil
	}

	// Delete
	err = c.kubeClient.CoreV1().Secrets(namespace).Delete(ctx, name, controller.NewDeleteOptions())
	if err == nil {
		log.V(1).M(namespace, name).Info("OK delete Secret/%s", namespace, name)
	} else {
		log.V(1).M(namespace, name).F().Error("FAIL delete Secret %s/%s err:%v", namespace, name, err)
	}

	return err
}
