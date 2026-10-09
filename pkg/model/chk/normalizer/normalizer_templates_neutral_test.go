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

package normalizer

import (
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"

	chk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
)

// Keeper shares the template and settings merge leaves with ClickHouse, but no template is ever
// stacked under a CHK in production - there is no Keeper template kind, and the internal
// template list the reconciler hands the normalizer is always empty. So every value a CHK sets
// has to come out exactly as set.
func TestKeeperTemplatesMergeIsNeutral(t *testing.T) {
	cr := &chk.ClickHouseKeeperInstallation{}
	cr.Name = "chk"
	cr.Namespace = "ns"
	cr.Spec.Templates = &chi.Templates{PodTemplates: []chi.PodTemplate{{
		Name: "default",
		Spec: core.PodSpec{Containers: []core.Container{{Name: "clickhouse-keeper", Image: "keeper-image"}}},
	}}}
	settings := chi.NewSettings()
	settings.Set("keeper_server/tcp_port", chi.NewSettingScalar("2181"))
	cr.Spec.Configuration = &chk.Configuration{Settings: settings}

	got, err := New(nil).CreateTemplated(cr, commonNormalizer.NewOptions[chk.ClickHouseKeeperInstallation]())
	require.NoError(t, err)

	require.Equal(t, "keeper-image", got.Spec.Templates.PodTemplates[0].Spec.Containers[0].Image)
	require.Equal(t, "2181", got.Spec.Configuration.Settings.Get("keeper_server/tcp_port").String())
}
