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
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestShouldDeleteUnreferencedSecret(t *testing.T) {
	const (
		name = "chi-demo-users"
		kind = "ClickHouseInstallation"
		uid  = types.UID("chi-uid")
	)
	owned := &core.Secret{
		ObjectMeta: meta.ObjectMeta{
			Name: name,
			OwnerReferences: []meta.OwnerReference{{
				Kind: kind,
				Name: "demo",
				UID:  uid,
			}},
		},
	}
	projected := &core.Pod{
		Spec: core.PodSpec{
			Volumes: []core.Volume{{
				Name: name,
				VolumeSource: core.VolumeSource{
					Projected: &core.ProjectedVolumeSource{
						Sources: []core.VolumeProjection{{
							Secret: &core.SecretProjection{
								LocalObjectReference: core.LocalObjectReference{Name: name},
							},
						}},
					},
				},
			}},
		},
	}
	other := &core.Pod{
		Spec: core.PodSpec{
			Volumes: []core.Volume{{
				VolumeSource: core.VolumeSource{
					Secret: &core.SecretVolumeSource{SecretName: "other"},
				},
			}},
		},
	}

	require.False(t, shouldDeleteUnreferencedSecret(owned, "demo", uid, kind, []*core.Pod{projected}))
	require.True(t, shouldDeleteUnreferencedSecret(owned, "demo", uid, kind, []*core.Pod{other}))
	require.True(t, shouldDeleteUnreferencedSecret(owned, "demo", uid, kind, nil))
	require.False(t, shouldDeleteUnreferencedSecret(owned, "demo", "", kind, nil))
	require.False(t, shouldDeleteUnreferencedSecret(owned, "other-chi", uid, kind, nil))
	require.False(t, shouldDeleteUnreferencedSecret(&core.Secret{ObjectMeta: meta.ObjectMeta{Name: name}}, "demo", uid, kind, nil))
}
