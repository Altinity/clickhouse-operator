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
	"time"

	apps "k8s.io/api/apps/v1"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopmodel "github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// deleteHost deletes all kubernetes resources related to replica *chop.ChiHost
func (c *Controller) deleteHost(ctx context.Context, host *chop.ChiHost) error {
	log.V(1).M(host).S().Info(host.Address.ClusterNameString())

	// Each host consists of:
	_ = c.deleteStatefulSet(ctx, host)
	_ = c.deletePVC(ctx, host)
	_ = c.deleteConfigMap(ctx, host)
	_ = c.deleteServiceHost(ctx, host)

	log.V(1).M(host).E().Info(host.Address.ClusterNameString())

	return nil
}

// deleteConfigMapsCHI
func (c *Controller) deleteConfigMapsCHI(ctx context.Context, chi *chop.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	// Delete common ConfigMap's
	//
	// chi-b3d29f-common-configd   2      61s
	// chi-b3d29f-common-usersd    0      61s
	// service/clickhouse-example-01         LoadBalancer   10.106.183.200   <pending>     8123:31607/TCP,9000:31492/TCP,9009:31357/TCP   33s   clickhouse.altinity.com/chi=example-01

	var err error

	configMapCommon := chopmodel.CreateConfigMapCommonName(chi)
	configMapCommonUsersName := chopmodel.CreateConfigMapCommonUsersName(chi)

	// Delete ConfigMap
	err = c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).Delete(ctx, configMapCommon, newDeleteOptions())
	switch {
	case err == nil:
		log.V(1).M(chi).Info("OK delete ConfigMap %s/%s", chi.Namespace, configMapCommon)
	case apierrors.IsNotFound(err):
		log.V(1).M(chi).Info("NEUTRAL not found ConfigMap %s/%s", chi.Namespace, configMapCommon)
	default:
		log.V(1).M(chi).F().Error("FAIL delete ConfigMap %s/%s err:%v", chi.Namespace, configMapCommon, err)
	}

	err = c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).Delete(ctx, configMapCommonUsersName, newDeleteOptions())
	switch {
	case err == nil:
		log.V(1).M(chi).Info("OK delete ConfigMap %s/%s", chi.Namespace, configMapCommonUsersName)
	case apierrors.IsNotFound(err):
		log.V(1).M(chi).Info("NEUTRAL not found ConfigMap %s/%s", chi.Namespace, configMapCommonUsersName)
		err = nil
	default:
		log.V(1).M(chi).F().Error("FAIL delete ConfigMap %s/%s err:%v", chi.Namespace, configMapCommonUsersName, err)
	}

	return err
}

// statefulSetDeletePod delete a pod of a StatefulSet. This requests StatefulSet to relaunch deleted pod
func (c *Controller) statefulSetDeletePod(ctx context.Context, statefulSet *apps.StatefulSet, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	name := chopmodel.CreatePodName(statefulSet)
	log.V(1).M(host).Info("Delete Pod %s/%s", statefulSet.Namespace, name)
	err := c.kubeClient.CoreV1().Pods(statefulSet.Namespace).Delete(ctx, name, newDeleteOptions())
	if err == nil {
		log.V(1).M(host).Info("OK delete Pod %s/%s", statefulSet.Namespace, name)
	} else if apierrors.IsNotFound(err) {
		log.V(1).M(host).Info("NEUTRAL not found Pod %s/%s", statefulSet.Namespace, name)
		err = nil
	} else {
		log.V(1).M(host).F().Error("FAIL delete ConfigMap %s/%s err:%v", statefulSet.Namespace, name, err)
	}

	return err
}

// deleteStatefulSet gracefully deletes StatefulSet through zeroing Pod's count
func (c *Controller) deleteStatefulSet(ctx context.Context, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	// IMPORTANT
	// StatefulSets do not provide any guarantees on the termination of pods when a StatefulSet is deleted.
	// To achieve ordered and graceful termination of the pods in the StatefulSet,
	// it is possible to scale the StatefulSet down to 0 prior to deletion.

	// Namespaced name
	name := chopmodel.CreateStatefulSetName(host)
	namespace := host.Address.Namespace
	log.V(1).M(host).F().Info("%s/%s", namespace, name)

	if sts, err := c.getStatefulSet(host); err == nil {
		// We need to set cur StatefulSet to a deletable one temporary
		host.StatefulSet = sts
	} else {
		if apierrors.IsNotFound(err) {
			log.V(1).M(host).Info("NEUTRAL not found StatefulSet %s/%s", namespace, name)
		} else {
			log.V(1).M(host).F().Error("FAIL get StatefulSet %s/%s err:%v", namespace, name, err)
		}
		return err
	}

	// Scale StatefulSet down to 0 pods count.
	// This is the proper and graceful way to delete StatefulSet
	var zero int32 = 0
	host.StatefulSet.Spec.Replicas = &zero
	if _, err := c.kubeClient.AppsV1().StatefulSets(namespace).Update(ctx, host.StatefulSet, newUpdateOptions()); err != nil {
		log.V(1).M(host).Error("UNABLE to update StatefulSet %s/%s", namespace, name)
		return err
	}

	// Wait until StatefulSet scales down to 0 pods count.
	_ = c.waitHostReady(ctx, host)

	// And now delete empty StatefulSet
	if err := c.kubeClient.AppsV1().StatefulSets(namespace).Delete(ctx, name, newDeleteOptions()); err == nil {
		log.V(1).M(host).Info("OK delete StatefulSet %s/%s", namespace, name)
		c.waitHostDeleted(host)
	} else if apierrors.IsNotFound(err) {
		log.V(1).M(host).Info("NEUTRAL not found StatefulSet %s/%s", namespace, name)
	} else {
		log.V(1).M(host).F().Error("FAIL delete StatefulSet %s/%s err: %v", namespace, name, err)
	}

	return nil
}

// syncStatefulSet
func (c *Controller) syncStatefulSet(ctx context.Context, host *chop.ChiHost) {
	for {
		if util.IsContextDone(ctx) {
			log.V(2).Info("ctx is done")
			return
		}
		// TODO
		// There should be better way to sync cache
		if sts, err := c.getStatefulSetByHost(host); err == nil {
			log.V(2).Info("cache NOT yet synced sts %s/%s is scheduled for deletion on %s", sts.Namespace, sts.Name, sts.DeletionTimestamp)
			util.WaitContextDoneOrTimeout(ctx, 15*time.Second)
		} else {
			log.V(1).Info("cache synced")
			return
		}
	}
}

// deletePVC deletes PersistentVolumeClaim
func (c *Controller) deletePVC(ctx context.Context, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	log.V(2).M(host).S().P()
	defer log.V(2).M(host).E().P()

	namespace := host.Address.Namespace
	c.walkDiscoveredPVCs(host, func(pvc *v1.PersistentVolumeClaim) {
		if util.IsContextDone(ctx) {
			log.V(2).Info("ctx is done")
			return
		}

		if !chopmodel.HostCanDeletePVC(host, pvc.Name) {
			log.V(1).M(host).Info("PVC %s/%s should not be deleted, leave it intact", namespace, pvc.Name)
			// Move to the next PVC
			return
		}

		// Actually delete PVC
		if err := c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, newDeleteOptions()); err == nil {
			log.V(1).M(host).Info("OK delete PVC %s/%s", namespace, pvc.Name)
		} else if apierrors.IsNotFound(err) {
			log.V(1).M(host).Info("NEUTRAL not found PVC %s/%s", namespace, pvc.Name)
		} else {
			log.M(host).F().Error("FAIL to delete PVC %s/%s err:%v", namespace, pvc.Name, err)
		}
	})

	return nil
}

// deleteConfigMap deletes ConfigMap
func (c *Controller) deleteConfigMap(ctx context.Context, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	name := chopmodel.CreateConfigMapHostName(host)
	namespace := host.Address.Namespace
	log.V(1).M(host).F().Info("%s/%s", namespace, name)

	if err := c.kubeClient.CoreV1().ConfigMaps(namespace).Delete(ctx, name, newDeleteOptions()); err == nil {
		log.V(1).M(host).Info("OK delete ConfigMap %s/%s", namespace, name)
	} else if apierrors.IsNotFound(err) {
		log.V(1).M(host).Info("NEUTRAL not found ConfigMap %s/%s", namespace, name)
	} else {
		log.V(1).M(host).F().Error("FAIL delete ConfigMap %s/%s err:%v", namespace, name, err)
	}

	//name = chopmodel.CreateConfigMapHostMigrationName(host)
	//namespace = host.Address.Namespace
	//log.V(1).M(host).F().Info("%s/%s", namespace, name)
	//
	//if err := c.kubeClient.CoreV1().ConfigMaps(namespace).Delete(ctx, name, newDeleteOptions()); err == nil {
	//	log.V(1).M(host).Info("OK delete ConfigMap %s/%s", namespace, name)
	//} else if apierrors.IsNotFound(err) {
	//	log.V(1).M(host).Info("NEUTRAL not found ConfigMap %s/%s", namespace, name)
	//} else {
	//	log.V(1).M(host).F().Error("FAIL delete ConfigMap %s/%s err:%v", namespace, name, err)
	//}

	return nil
}

// deleteServiceHost deletes Service
func (c *Controller) deleteServiceHost(ctx context.Context, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	serviceName := chopmodel.CreateStatefulSetServiceName(host)
	namespace := host.Address.Namespace
	log.V(1).M(host).F().Info("%s/%s", namespace, serviceName)
	return c.deleteServiceIfExists(ctx, namespace, serviceName)
}

// deleteServiceShard
func (c *Controller) deleteServiceShard(ctx context.Context, shard *chop.ChiShard) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	serviceName := chopmodel.CreateShardServiceName(shard)
	namespace := shard.Address.Namespace
	log.V(1).M(shard).F().Info("%s/%s", namespace, serviceName)
	return c.deleteServiceIfExists(ctx, namespace, serviceName)
}

// deleteServiceCluster
func (c *Controller) deleteServiceCluster(ctx context.Context, cluster *chop.ChiCluster) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	serviceName := chopmodel.CreateClusterServiceName(cluster)
	namespace := cluster.Address.Namespace
	log.V(1).M(cluster).F().Info("%s/%s", namespace, serviceName)
	return c.deleteServiceIfExists(ctx, namespace, serviceName)
}

// deleteServiceCHI
func (c *Controller) deleteServiceCHI(ctx context.Context, chi *chop.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	serviceName := chopmodel.CreateCHIServiceName(chi)
	namespace := chi.Namespace
	log.V(1).M(chi).F().Info("%s/%s", namespace, serviceName)
	return c.deleteServiceIfExists(ctx, namespace, serviceName)
}

// deleteServiceIfExists deletes Service in case it does not exist
func (c *Controller) deleteServiceIfExists(ctx context.Context, namespace, name string) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	// Check specified service exists
	_, err := c.kubeClient.CoreV1().Services(namespace).Get(ctx, name, newGetOptions())

	if err != nil {
		// No such a service, nothing to delete
		return nil
	}

	// Delete service
	err = c.kubeClient.CoreV1().Services(namespace).Delete(ctx, name, newDeleteOptions())
	if err == nil {
		log.V(1).M(namespace, name).Info("OK delete Service %s/%s", namespace, name)
	} else {
		log.V(1).M(namespace, name).F().Error("FAIL delete Service %s/%s err:%v", namespace, name, err)
	}

	return err
}
