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
	log "github.com/golang/glog"
	// log "k8s.io/klog"
	"k8s.io/apimachinery/pkg/apis/meta/v1"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model"
)

func (c *Controller) labelMyObjectsTree() {

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

	// Label operator's Pod with version label
	podName, ok1 := c.chop.ConfigManager.GetRuntimeParam(chiv1.OPERATOR_POD_NAME)
	namespace, ok2 := c.chop.ConfigManager.GetRuntimeParam(chiv1.OPERATOR_POD_NAMESPACE)

	if !ok1 || !ok2 {
		log.V(1).Infof("ERROR fetch Pod name out of %s/%s", namespace, podName)
		return
	}

	// Pod namespaced name found, fetch the Pod
	pod, err := c.podLister.Pods(namespace).Get(podName)
	if err != nil {
		log.V(1).Infof("ERROR get Pod %s/%s", namespace, podName)
		return
	}

	// Put label on the Pod
	c.addLabels(&pod.ObjectMeta)
	if _, err := c.kubeClient.CoreV1().Pods(namespace).Update(pod); err != nil {
		log.V(1).Infof("ERROR put label on Pod %s/%s %v", namespace, podName, err)
	}

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
		log.V(1).Infof("ERROR ReplicaSet for Pod %s/%s not found", namespace, podName)
		return
	}

	// ReplicaSet namespaced name found, fetch the ReplicaSet
	replicaSet, err := c.kubeClient.AppsV1().ReplicaSets(namespace).Get(replicaSetName, v1.GetOptions{})
	if err != nil {
		log.V(1).Infof("ERROR get ReplicaSet %s/%s %v", namespace, replicaSetName, err)
		return
	}

	// Put label on the ReplicaSet
	c.addLabels(&replicaSet.ObjectMeta)
	if _, err := c.kubeClient.AppsV1().ReplicaSets(namespace).Update(replicaSet); err != nil {
		log.V(1).Infof("ERROR put label on ReplicaSet %s/%s %v", namespace, replicaSetName, err)
	}

	// Find parent Deployment
	deploymentName := ""
	for i := range replicaSet.OwnerReferences {
		owner := &replicaSet.OwnerReferences[i]
		if owner.Kind == "Deployment" {
			// Deployment found
			deploymentName = owner.Name
			break
		}
	}

	if deploymentName == "" {
		// Deployment not found
		log.V(1).Infof("ERROR Deployment for %s Pod %s ReplicaSet %s not found", namespace, podName, replicaSetName)
		return
	}

	// Deployment namespaced name found, fetch the Deployment
	deployment, err := c.kubeClient.AppsV1().Deployments(namespace).Get(deploymentName, v1.GetOptions{})
	if err != nil {
		log.V(1).Infof("ERROR get Deployment %s/%s", namespace, deploymentName)
		return
	}

	// Put label on the Deployment
	c.addLabels(&deployment.ObjectMeta)
	if _, err := c.kubeClient.AppsV1().Deployments(namespace).Update(deployment); err != nil {
		log.V(1).Infof("ERROR put label on Deployment %s/%s %v", namespace, deploymentName, err)
	}
}

func (c *Controller) addLabels(meta *v1.ObjectMeta) {
	meta.Labels[model.LabelAppName] = model.LabelAppValue
	meta.Labels[model.LabelCHOP] = c.chop.Version
}
