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
	"fmt"

	core "k8s.io/api/core/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// updateService
func (w *worker) updateService(
	ctx context.Context,
	chi *api.ClickHouseInstallation,
	curService *core.Service,
	targetService *core.Service,
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	if curService.Spec.Type != targetService.Spec.Type {
		return fmt.Errorf(
			"just recreate the service in case of service type change '%s'=>'%s'",
			curService.Spec.Type, targetService.Spec.Type)
	}

	// Updating a Service is a complicated business

	newService := targetService.DeepCopy()

	// spec.resourceVersion is required in order to update an object
	newService.ResourceVersion = curService.ResourceVersion

	//
	// Migrate ClusterIP to the new service
	//
	// spec.clusterIP field is immutable, need to use already assigned value
	// From https://kubernetes.io/docs/concepts/services-networking/service/#defining-a-service
	// Kubernetes assigns this Service an IP address (sometimes called the “cluster IP”), which is used by the Service proxies
	// See also https://kubernetes.io/docs/concepts/services-networking/service/#virtual-ips-and-service-proxies
	// You can specify your own cluster IP address as part of a Service creation request. To do this, set the .spec.clusterIP
	newService.Spec.ClusterIP = curService.Spec.ClusterIP

	//
	// Migrate existing ports to the new service for NodePort and LoadBalancer services
	//
	// The port on each node on which this service is exposed when type=NodePort or LoadBalancer.
	// Usually assigned by the system. If specified, it will be allocated to the service if unused
	// or else creation of the service will fail.
	// Default is to auto-allocate a port if the ServiceType of this Service requires one.
	// More info: https://kubernetes.io/docs/concepts/services-networking/service/#type-nodeport

	// !!! IMPORTANT !!!
	// No changes in service type is allowed.
	// Already exposed port details can not be changed.

	serviceTypeIsNodePort := (curService.Spec.Type == core.ServiceTypeNodePort) && (newService.Spec.Type == core.ServiceTypeNodePort)
	serviceTypeIsLoadBalancer := (curService.Spec.Type == core.ServiceTypeLoadBalancer) && (newService.Spec.Type == core.ServiceTypeLoadBalancer)
	if serviceTypeIsNodePort || serviceTypeIsLoadBalancer {
		for i := range newService.Spec.Ports {
			newPort := &newService.Spec.Ports[i]
			for j := range curService.Spec.Ports {
				curPort := &curService.Spec.Ports[j]
				if newPort.Port == curPort.Port {
					// Already have this port specified - reuse all internals,
					// due to limitations with auto-assigned values
					*newPort = *curPort
					w.a.M(chi).F().Info("reuse Port %d values", newPort.Port)
					break
				}
			}
		}
	}

	//
	// Migrate HealthCheckNodePort to the new service
	//
	// spec.healthCheckNodePort field is used with ExternalTrafficPolicy=Local only and is immutable within ExternalTrafficPolicy=Local
	// In case ExternalTrafficPolicy is changed it seems to be irrelevant
	// https://kubernetes.io/docs/tasks/access-application-cluster/create-external-load-balancer/#preserving-the-client-source-ip
	curExternalTrafficPolicyTypeLocal := curService.Spec.ExternalTrafficPolicy == core.ServiceExternalTrafficPolicyTypeLocal
	newExternalTrafficPolicyTypeLocal := newService.Spec.ExternalTrafficPolicy == core.ServiceExternalTrafficPolicyTypeLocal
	if curExternalTrafficPolicyTypeLocal && newExternalTrafficPolicyTypeLocal {
		newService.Spec.HealthCheckNodePort = curService.Spec.HealthCheckNodePort
	}

	//
	// Migrate LoadBalancerClass to the new service
	//
	// This field can only be set when creating or updating a Service to type 'LoadBalancer'.
	// Once set, it can not be changed. This field will be wiped when a service is updated to a non 'LoadBalancer' type.
	if curService.Spec.LoadBalancerClass != nil {
		newService.Spec.LoadBalancerClass = curService.Spec.LoadBalancerClass
	}

	//
	// Migrate labels, annotations and finalizers to the new service
	//
	newService.GetObjectMeta().SetLabels(util.MergeStringMapsPreserve(newService.GetObjectMeta().GetLabels(), curService.GetObjectMeta().GetLabels()))
	newService.GetObjectMeta().SetAnnotations(util.MergeStringMapsPreserve(newService.GetObjectMeta().GetAnnotations(), curService.GetObjectMeta().GetAnnotations()))
	newService.GetObjectMeta().SetFinalizers(util.MergeStringArrays(newService.GetObjectMeta().GetFinalizers(), curService.GetObjectMeta().GetFinalizers()))

	//
	// And only now we are ready to actually update the service with new version of the service
	//

	_, err := w.c.kubeClient.CoreV1().Services(newService.GetNamespace()).Update(ctx, newService, controller.NewUpdateOptions())
	if err == nil {
		w.a.V(1).
			WithEvent(chi, common.EventActionUpdate, common.EventReasonUpdateCompleted).
			WithStatusAction(chi).
			M(chi).F().
			Info("Update Service success: %s/%s", newService.GetNamespace(), newService.GetName())
	} else {
		w.a.M(chi).F().Error("Update Service fail: %s/%s failed with error %v", newService.GetNamespace(), newService.GetName())
	}

	return err
}

// createService
func (w *worker) createService(ctx context.Context, chi *api.ClickHouseInstallation, service *core.Service) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	_, err := w.c.kubeClient.CoreV1().Services(service.Namespace).Create(ctx, service, controller.NewCreateOptions())
	if err == nil {
		w.a.V(1).
			WithEvent(chi, common.EventActionCreate, common.EventReasonCreateCompleted).
			WithStatusAction(chi).
			M(chi).F().
			Info("OK Create Service: %s/%s", service.Namespace, service.Name)
	} else {
		w.a.WithEvent(chi, common.EventActionCreate, common.EventReasonCreateFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).F().
			Error("FAILED Create Service: %s/%s err: %v", service.Namespace, service.Name, err)
	}

	return err
}
