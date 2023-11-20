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

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.com/v1alpha1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chk"
)

func getNamespacedName(obj meta.Object) types.NamespacedName {
	return types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}
}

func getCheckSum(chk *api.ClickHouseKeeper) (string, error) {
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

func getLastAppliedClickHouseKeeper(chk *api.ClickHouseKeeper) *api.ClickHouseKeeper {
	lastApplied := chk.Annotations["kubectl.kubernetes.io/last-applied-configuration"]

	tmp := api.ClickHouseKeeper{}

	json.Unmarshal([]byte(lastApplied), &tmp)
	return &tmp
}

func (r *ChkReconciler) getReadyPods(chk *api.ClickHouseKeeper) ([]string, error) {
	podList := &core.PodList{}
	labelSelector := labels.SelectorFromSet(model.GetPodLabels(chk))
	listOps := &client.ListOptions{
		Namespace:     chk.Namespace,
		LabelSelector: labelSelector,
	}
	if err := r.List(context.TODO(), podList, listOps); err != nil {
		return nil, err
	}

	var readyPods []string
	for _, pod := range podList.Items {
		ready := true
		for _, containerStatus := range pod.Status.ContainerStatuses {
			r.Log.Info(fmt.Sprintf("%s: %t", containerStatus.Name, containerStatus.Ready))
			if !containerStatus.Ready {
				ready = false
			}
		}
		if ready {
			readyPods = append(readyPods, pod.Name)
		}
	}

	return readyPods, nil
}

func isReplicasChanged(chk *api.ClickHouseKeeper) bool {
	lastApplied := getLastAppliedClickHouseKeeper(chk)
	if lastApplied.Spec.Replicas != chk.Spec.Replicas {
		return true
	} else {
		return false
	}
}

func restartPods(sts *apps.StatefulSet) {
	v, _ := time.Now().UTC().MarshalText()
	sts.Spec.Template.Annotations = map[string]string{"kubectl.kubernetes.io/restartedAt": string(v)}
}

func updateLastReplicas(chk *api.ClickHouseKeeper) {
	lastAppliedString := chk.Annotations["kubectl.kubernetes.io/last-applied-configuration"]

	tmp := api.ClickHouseKeeper{}
	json.Unmarshal([]byte(lastAppliedString), &tmp)
	tmp.Spec.Replicas = chk.Spec.Replicas

	updatedLastApplied, _ := json.Marshal(tmp)
	chk.Annotations["kubectl.kubernetes.io/last-applied-configuration"] = string(updatedLastApplied)
}
