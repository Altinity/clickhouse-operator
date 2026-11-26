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

package kube

import (
	"testing"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func pvcList(names ...string) *core.PersistentVolumeClaimList {
	list := &core.PersistentVolumeClaimList{}
	for _, name := range names {
		list.Items = append(list.Items, core.PersistentVolumeClaim{ObjectMeta: meta.ObjectMeta{Name: name}})
	}
	return list
}

func pvcNames(list *core.PersistentVolumeClaimList) (names []string) {
	for _, pvc := range list.Items {
		names = append(names, pvc.GetName())
	}
	return names
}

// Hosts 0-1 and 0-2 shifted in layout.shards[].replicas carry the same replica labels for a while,
// so the label selector of host 0-1 returns PVCs of both of them.
func TestFilterPVCsOfPod(t *testing.T) {
	list := pvcList(
		"default-chi-test-default-0-1-0",
		"logs-chi-test-default-0-1-0",
		"default-chi-test-default-0-2-0",
		"default-chi-test-default-0-1x-0",
	)
	got := pvcNames(FilterPVCsOfPod(list, "chi-test-default-0-1-0"))
	want := []string{"default-chi-test-default-0-1-0", "logs-chi-test-default-0-1-0"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("FilterPVCsOfPod() = %v, want %v", got, want)
	}
}

func TestFilterPVCsOfPodNil(t *testing.T) {
	if FilterPVCsOfPod(nil, "pod-0") != nil {
		t.Fatalf("FilterPVCsOfPod(nil) is not nil")
	}
}
