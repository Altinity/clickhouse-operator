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

package chk

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.com/v1alpha1"
)

func getCheckSum(chk *v1alpha1.ClickHouseKeeper) (string, error) {
	specString, err := json.Marshal(chk.Spec)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte(specString))
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func getFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

func getLastAppliedConfiguration(chk *v1alpha1.ClickHouseKeeper) *v1alpha1.ClickHouseKeeper {
	lastApplied := chk.Annotations["kubectl.kubernetes.io/last-applied-configuration"]

	tmp := v1alpha1.ClickHouseKeeper{}

	json.Unmarshal([]byte(lastApplied), &tmp)
	return &tmp
}

func (r *ChkReconciler) getReadyMembers(instance *v1alpha1.ClickHouseKeeper) ([]string, error) {
	foundPods := &corev1.PodList{}
	labelSelector := labels.SelectorFromSet(getPodLabels(instance))
	listOps := &client.ListOptions{
		Namespace:     instance.Namespace,
		LabelSelector: labelSelector,
	}
	if err := r.List(context.TODO(), foundPods, listOps); err != nil {
		return nil, err
	}

	var readyMembers []string
	for _, p := range foundPods.Items {
		ready := true
		for _, c := range p.Status.ContainerStatuses {
			r.Log.Info(fmt.Sprintf("%s: %t", c.Name, c.Ready))
			if !c.Ready {
				ready = false
			}
		}
		if ready {
			readyMembers = append(readyMembers, p.Name)
		}
	}

	return readyMembers, nil
}

func isReplicasChanged(chk *v1alpha1.ClickHouseKeeper) bool {
	lastApplied := getLastAppliedConfiguration(chk)
	if lastApplied.Spec.Replicas != chk.Spec.Replicas {
		return true
	} else {
		return false
	}
}

func restartPods(sts *appsv1.StatefulSet) {
	v, _ := time.Now().UTC().MarshalText()
	sts.Spec.Template.Annotations = map[string]string{"kubectl.kubernetes.io/restartedAt": string(v)}
}

func updateLastReplicas(chk *v1alpha1.ClickHouseKeeper) {
	lastAppliedString := chk.Annotations["kubectl.kubernetes.io/last-applied-configuration"]

	tmp := v1alpha1.ClickHouseKeeper{}
	json.Unmarshal([]byte(lastAppliedString), &tmp)
	tmp.Spec.Replicas = chk.Spec.Replicas

	updatedLastApplied, _ := json.Marshal(tmp)
	chk.Annotations["kubectl.kubernetes.io/last-applied-configuration"] = string(updatedLastApplied)
}
