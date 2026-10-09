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

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

func TestChiReferencesHotReloadSecret(t *testing.T) {
	cr := &api.ClickHouseInstallation{}
	cr.Spec.Configuration = api.NewConfiguration()
	cr.Spec.Configuration.Users = api.NewSettings()
	cr.Spec.Configuration.Users.Set("alice/password", api.NewSettingSource(&api.SettingSource{
		ValueFrom: &types.DataSource{
			SecretKeyRef: &core.SecretKeySelector{
				LocalObjectReference: core.LocalObjectReference{Name: "clickhouse-passwords"},
				Key:                  "alice_password",
			},
			HotReload: boolPtr(true),
		},
	}))
	cr.Spec.Configuration.Users.Set("bob/password", api.NewSettingSource(&api.SettingSource{
		ValueFrom: &types.DataSource{
			SecretKeyRef: &core.SecretKeySelector{
				LocalObjectReference: core.LocalObjectReference{Name: "other"},
				Key:                  "bob_password",
			},
		},
	}))

	require.True(t, chiReferencesHotReloadSecret(cr, "clickhouse-passwords"))
	require.False(t, chiReferencesHotReloadSecret(cr, "other"))
	require.False(t, chiReferencesHotReloadSecret(cr, "missing"))
}

func boolPtr(v bool) *bool { return &v }
