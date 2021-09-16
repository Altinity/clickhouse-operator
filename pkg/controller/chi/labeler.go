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

	v13 "k8s.io/api/apps/v1"
	v12 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func (c *Controller) labelMyObjectsTree(ctx context.Context) {

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

	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return
	}

	// Label operator's Pod with version label
	podName, ok1 := chop.Get().ConfigManager.GetRuntimeParam(chiv1.OPERATOR_POD_NAME)
	namespace, ok2 := chop.Get().ConfigManager.GetRuntimeParam(chiv1.OPERATOR_POD_NAMESPACE)

	if !ok1 || !ok2 {
		log.V(1).M(namespace, podName).A().Error("ERROR fetch Pod name out of %s/%s", namespace, podName)
		return
	}

	// Pod namespaced name found, find and label the Pod
	pod := c.labelPod(ctx, namespace, podName)
	if pod == nil {
		return
	}

	replicaSet := c.labelReplicaSet(ctx, pod)
	if replicaSet == nil {
		return
	}

	c.labelDeployment(ctx, replicaSet)
}

func (c *Controller) labelPod(ctx context.Context, namespace, name string) *v12.Pod {
	pod, err := c.kubeClient.CoreV1().Pods(namespace).Get(ctx, name, newGetOptions())
	if err != nil {
		log.V(1).M(namespace, name).A().Error("ERROR get Pod %s/%s", namespace, name)
		return nil
	}

	// Put label on the Pod
	c.addLabels(&pod.ObjectMeta)
	if _, err := c.kubeClient.CoreV1().Pods(namespace).Update(ctx, pod, newUpdateOptions()); err != nil {
		log.V(1).M(namespace, name).A().Error("ERROR put label on Pod %s/%s %v", namespace, name, err)
	}
	return pod
}

func (c *Controller) labelReplicaSet(ctx context.Context, pod *v12.Pod) *v13.ReplicaSet {
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
		log.V(1).M(pod.Namespace, pod.Name).A().Error("ERROR ReplicaSet for Pod %s/%s not found", pod.Namespace, pod.Name)
		return nil
	}

	// ReplicaSet namespaced name found, fetch the ReplicaSet
	replicaSet, err := c.kubeClient.AppsV1().ReplicaSets(pod.Namespace).Get(ctx, replicaSetName, newGetOptions())
	if err != nil {
		log.V(1).M(pod.Namespace, replicaSetName).A().Error("ERROR get ReplicaSet %s/%s %v", pod.Namespace, replicaSetName, err)
		return replicaSet
	}

	// Put label on the ReplicaSet
	c.addLabels(&replicaSet.ObjectMeta)
	if _, err := c.kubeClient.AppsV1().ReplicaSets(pod.Namespace).Update(ctx, replicaSet, newUpdateOptions()); err != nil {
		log.V(1).M(pod.Namespace, replicaSetName).A().Error("ERROR put label on ReplicaSet %s/%s %v", pod.Namespace, replicaSetName, err)
	}

	return replicaSet
}

func (c *Controller) labelDeployment(ctx context.Context, rs *v13.ReplicaSet) {
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
		log.V(1).M(rs.Namespace, rs.Name).A().Error("ERROR find Deployment for ReplicaSet %s/%s not found", rs.Namespace, rs.Name)
		return
	}

	// Deployment namespaced name found, fetch the Deployment
	deployment, err := c.kubeClient.AppsV1().Deployments(rs.Namespace).Get(ctx, deploymentName, newGetOptions())
	if err != nil {
		log.V(1).M(rs.Namespace, deploymentName).A().Error("ERROR get Deployment %s/%s", rs.Namespace, deploymentName)
		return
	}

	// Put label on the Deployment
	c.addLabels(&deployment.ObjectMeta)
	if _, err := c.kubeClient.AppsV1().Deployments(rs.Namespace).Update(ctx, deployment, newUpdateOptions()); err != nil {
		log.V(1).M(rs.Namespace, deploymentName).A().Error("ERROR put label on Deployment %s/%s %v", rs.Namespace, deploymentName, err)
	}
}

// addLabels adds app and version labels
func (c *Controller) addLabels(meta *v1.ObjectMeta) {
	util.MergeStringMapsOverwrite(
		meta.Labels,
		// Add the following labels
		map[string]string{
			model.LabelAppName: model.LabelAppValue,
			model.LabelCHOP:    chop.Get().Version,
		},
	)
}
