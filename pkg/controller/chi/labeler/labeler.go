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

package labeler

import (
	"context"
	"errors"
	"fmt"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	chiLabeler "github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type Labeler struct {
	pod        interfaces.IKubePod
	service    interfaces.IKubeService
	replicaSet interfaces.IKubeReplicaSet
	deployment interfaces.IKubeDeployment
}

func New(kube interfaces.IKube) *Labeler {
	return &Labeler{
		pod:        kube.Pod(),
		service:    kube.Service(),
		replicaSet: kube.ReplicaSet(),
		deployment: kube.Deployment(),
	}
}

func (l *Labeler) LabelMyObjectsTree(ctx context.Context) error {

	// Operator is running in the Pod. We need to label this Pod
	// Pod is owned by ReplicaSet. We need to label this ReplicaSet also.
	// ReplicaSet is owned by Deployment. We need to label this Deployment also.
	// Deployment is not owned by any entity so far.
	//
	// Excerpt from Pod's yaml
	// metadata:
	//  ownerReferences:
	//  - apiVersion: apps/v1
	//    blockOwnerDeletion: true
	//    controller: true
	//    kind: ReplicaSet
	//    name: clickhouse-operator-79bf98f9b8
	//    uid: a276f30c-83ae-11e9-b92d-0208b778ea1a
	//
	// Excerpt from ReplicaSet's yaml
	// metadata:
	//  ownerReferences:
	//  - apiVersion: apps/v1
	//    blockOwnerDeletion: true
	//    controller: true
	//    kind: Deployment
	//    name: clickhouse-operator
	//    uid: a275a8a0-83ae-11e9-b92d-0208b778ea1a

	// What pod does operator run in?
	name, ok1 := chop.GetRuntimeParam(deployment.OPERATOR_POD_NAME)
	namespace, ok2 := chop.GetRuntimeParam(deployment.OPERATOR_POD_NAMESPACE)

	if !ok1 || !ok2 {
		str := fmt.Sprintf("ERROR read env vars: %s/%s ", deployment.OPERATOR_POD_NAME, deployment.OPERATOR_POD_NAMESPACE)
		log.V(1).M(namespace, name).F().Error(str)
		return fmt.Errorf("%w %s", ErrEnvVarNotSpecified, str)
	}

	log.V(1).Info("OPERATOR_POD_NAMESPACE=%s OPERATOR_POD_NAME=%s", namespace, name)
	if len(namespace) == 0 || len(name) == 0 {
		return ErrOperatorPodNotSpecified
	}

	// Put labels on the pod
	pod, err := l.labelPod(ctx, namespace, name)
	if err != nil {
		return fmt.Errorf("%w %s/%s err: %v", ErrUnableToLabelPod, namespace, name, err)
	}
	if pod == nil {
		return fmt.Errorf("%w %s/%s", ErrUnableToLabelPod, namespace, name)
	}

	// Put labels on the ReplicaSet
	replicaSet, err := l.labelReplicaSet(ctx, pod)
	if err != nil {
		return fmt.Errorf("%w %s err: %v", ErrUnableToLabelReplicaSet, util.NamespacedName(pod), err)
	}
	if replicaSet == nil {
		return fmt.Errorf("%w %s", ErrUnableToLabelReplicaSet, util.NamespacedName(pod))
	}

	// Put labels on the Deployment
	err = l.labelDeployment(ctx, replicaSet)
	if err != nil {
		fmt.Errorf("%w %s err: %v", ErrUnableToLabelDeployment, util.NamespacedName(replicaSet), err)
		return err
	}

	return nil
}

func (l *Labeler) labelPod(ctx context.Context, namespace, name string) (*core.Pod, error) {
	pod, err := l.pod.Get(namespace, name)
	if err != nil {
		log.V(1).M(namespace, name).F().Error("ERROR get Pod %s/%s %v", namespace, name, err)
		return nil, err
	}
	if pod == nil {
		str := fmt.Sprintf("ERROR get Pod is nil %s/%s ", namespace, name)
		log.V(1).M(namespace, name).F().Error(str)
		return nil, errors.New(str)
	}

	// Put label on the Pod
	pod.Labels = l.addLabels(pod.Labels)
	pod, err = l.pod.Update(ctx, pod)
	if err != nil {
		log.V(1).M(namespace, name).F().Error("ERROR put label on Pod %s/%s %v", namespace, name, err)
		return nil, err
	}
	if pod == nil {
		str := fmt.Sprintf("ERROR update Pod is nil %s/%s ", namespace, name)
		log.V(1).M(namespace, name).F().Error(str)
		return nil, errors.New(str)
	}

	return pod, nil
}

func (l *Labeler) labelReplicaSet(ctx context.Context, pod *core.Pod) (*apps.ReplicaSet, error) {
	// Find parent ReplicaSet
	replicaSetName := ""
	for i := range pod.OwnerReferences {
		owner := &pod.OwnerReferences[i]
		if owner.Kind == "ReplicaSet" {
			// ReplicaSet found
			replicaSetName = owner.Name
			break
		}
	}

	if replicaSetName == "" {
		// ReplicaSet not found
		str := fmt.Sprintf("ERROR ReplicaSet for Pod %s/%s not found", pod.Namespace, pod.Name)
		log.V(1).M(pod.Namespace, pod.Name).F().Error(str)
		return nil, errors.New(str)
	}

	// ReplicaSet namespaced name found, fetch the ReplicaSet
	replicaSet, err := l.replicaSet.Get(ctx, pod.Namespace, replicaSetName)
	if err != nil {
		log.V(1).M(pod.Namespace, replicaSetName).F().Error("ERROR get ReplicaSet %s/%s %v", pod.Namespace, replicaSetName, err)
		return nil, err
	}
	if replicaSet == nil {
		str := fmt.Sprintf("ERROR get ReplicaSet is nil %s/%s ", pod.Namespace, replicaSetName)
		log.V(1).M(pod.Namespace, replicaSetName).F().Error(str)
		return nil, errors.New(str)
	}

	// Put label on the ReplicaSet
	replicaSet.Labels = l.addLabels(replicaSet.Labels)
	replicaSet, err = l.replicaSet.Update(ctx, replicaSet)
	if err != nil {
		log.V(1).M(pod.Namespace, replicaSetName).F().Error("ERROR put label on ReplicaSet %s/%s %v", pod.Namespace, replicaSetName, err)
		return nil, err
	}
	if replicaSet == nil {
		str := fmt.Sprintf("ERROR update ReplicaSet is nil %s/%s ", pod.Namespace, replicaSetName)
		log.V(1).M(pod.Namespace, replicaSetName).F().Error(str)
		return nil, errors.New(str)
	}

	return replicaSet, nil
}

func (l *Labeler) labelDeployment(ctx context.Context, rs *apps.ReplicaSet) error {
	// Find parent Deployment
	deploymentName := ""
	for i := range rs.OwnerReferences {
		owner := &rs.OwnerReferences[i]
		if owner.Kind == "Deployment" {
			// Deployment found
			deploymentName = owner.Name
			break
		}
	}

	if deploymentName == "" {
		// Deployment not found
		str := fmt.Sprintf("ERROR find Deployment for ReplicaSet %s/%s not found", rs.Namespace, rs.Name)
		log.V(1).M(rs.Namespace, rs.Name).F().Error(str)
		return errors.New(str)
	}

	// Deployment namespaced name found, fetch the Deployment
	deployment, err := l.deployment.Get(rs.Namespace, deploymentName)
	if err != nil {
		log.V(1).M(rs.Namespace, deploymentName).F().Error("ERROR get Deployment %s/%s", rs.Namespace, deploymentName)
		return err
	}
	if deployment == nil {
		str := fmt.Sprintf("ERROR get Deployment is nil %s/%s ", rs.Namespace, deploymentName)
		log.V(1).M(rs.Namespace, deploymentName).F().Error(str)
		return errors.New(str)
	}

	// Put label on the Deployment
	deployment.Labels = l.addLabels(deployment.Labels)
	deployment, err = l.deployment.Update(deployment)
	if err != nil {
		log.V(1).M(rs.Namespace, deploymentName).F().Error("ERROR put label on Deployment %s/%s %v", rs.Namespace, deploymentName, err)
		return err
	}
	if deployment == nil {
		str := fmt.Sprintf("ERROR update Deployment is nil %s/%s ", rs.Namespace, deploymentName)
		log.V(1).M(rs.Namespace, deploymentName).F().Error(str)
		return errors.New(str)
	}

	return nil
}

// addLabels adds app and version labels
func (l *Labeler) addLabels(labels map[string]string) map[string]string {
	return util.MergeStringMapsOverwrite(
		labels,
		chiLabeler.New(nil).GetCHOpSignature(),
	)
}

// appendLabelReadyOnPod appends Label "Ready" to the pod of the specified host
func (l *Labeler) appendLabelReadyOnPod(ctx context.Context, host *api.Host) error {
	pod, err := l.pod.Get(host)
	if err != nil {
		log.M(host).F().Error("FAIL get pod for host %s err:%v", host.Runtime.Address.NamespaceNameString(), err)
		return err
	}

	if chiLabeler.New(host.GetCR()).AppendLabelReady(&pod.ObjectMeta) {
		// Modified, need to update
		_, err = l.pod.Update(ctx, pod)
		if err != nil {
			log.M(host).F().Error("FAIL setting 'ready' label for host %s err:%v", host.Runtime.Address.NamespaceNameString(), err)
			return err
		}
	}

	return nil
}

// deleteLabelReadyOnPod deletes Label "Ready" from the pod of the specified host
func (l *Labeler) deleteLabelReadyOnPod(ctx context.Context, host *api.Host) error {
	if host == nil {
		return nil
	}
	pod, err := l.pod.Get(host)
	if apiErrors.IsNotFound(err) {
		// Pod may be missing in case, say, StatefulSet has 0 pods because CHI is stopped
		// This is not an error, after all
		return nil
	}

	if err != nil {
		log.V(1).M(host).F().Info("FAIL get pod for host '%s' err: %v", host.Runtime.Address.NamespaceNameString(), err)
		return err
	}

	if chiLabeler.New(host.GetCR()).DeleteLabelReady(&pod.ObjectMeta) {
		// Modified, need to update
		_, err = l.pod.Update(ctx, pod)
		return err
	}

	return nil
}

// appendAnnotationReadyOnService appends Annotation "Ready" to the service of the specified host
func (l *Labeler) appendAnnotationReadyOnService(ctx context.Context, host *api.Host) error {
	svc, err := l.service.Get(ctx, host)
	if err != nil {
		log.M(host).F().Error("FAIL get service for host %s err:%v", host.Runtime.Address.NamespaceNameString(), err)
		return err
	}

	if chiLabeler.New(host.GetCR()).AppendAnnotationReady(&svc.ObjectMeta) {
		// Modified, need to update
		_, err = l.service.Update(ctx, svc)
		if err != nil {
			log.M(host).F().Error("FAIL setting 'ready' annotation for host service %s err:%v", host.Runtime.Address.NamespaceNameString(), err)
			return err
		}
	}

	return nil
}

// deleteAnnotationReadyOnService deletes Annotation "Ready" from the service of the specified host
func (l *Labeler) deleteAnnotationReadyOnService(ctx context.Context, host *api.Host) error {
	if host == nil {
		return nil
	}

	svc, err := l.service.Get(ctx, host)
	if apiErrors.IsNotFound(err) {
		// Service may be missing in case, say, StatefulSet has 0 pods because CHI is stopped
		// This is not an error, after all
		return nil
	}
	if err != nil {
		log.V(1).M(host).F().Info("FAIL get service for host '%s' err: %v", host.Runtime.Address.NamespaceNameString(), err)
		return err
	}

	if chiLabeler.New(host.GetCR()).DeleteAnnotationReady(&svc.ObjectMeta) {
		// Modified, need to update
		_, err = l.service.Update(ctx, svc)
		return err
	}

	return nil
}

func (l *Labeler) DeleteReadyMarkOnPodAndService(ctx context.Context, host *api.Host) error {
	if l == nil {
		return nil
	}
	_ = l.deleteLabelReadyOnPod(ctx, host)
	_ = l.deleteAnnotationReadyOnService(ctx, host)

	return nil
}

func (l *Labeler) SetReadyMarkOnPodAndService(ctx context.Context, host *api.Host) error {
	if l == nil {
		return nil
	}
	_ = l.appendLabelReadyOnPod(ctx, host)
	_ = l.appendAnnotationReadyOnService(ctx, host)

	return nil
}
