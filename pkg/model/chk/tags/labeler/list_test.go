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
	"testing"

	"github.com/stretchr/testify/require"

	chk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	commonLabeler "github.com/altinity/clickhouse-operator/pkg/model/common/tags/labeler"
)

// The labeler reads the global operator config (chop.Config()) during construction.
func init() { chop.New(nil, nil, "") }

// TestServiceTierLabelValues guards the Service-tier label-value mapping (issue #1982). The
// client Service tier MUST resolve to a concrete, distinct value: a missing entry in the
// labeler `list` map silently yields an empty value, which makes the client Service unselectable
// by the keeper-ref resolver and leaves clients pointed at the not-ready peer tier.
func TestServiceTierLabelValues(t *testing.T) {
	l := New(chk.NewClickHouseKeeperInstallation("kpr", "ns"))

	require.Equal(t, "host", l.Get(commonLabeler.LabelServiceValueHost))
	require.Equal(t, "host-client", l.Get(commonLabeler.LabelServiceValueHostClient),
		"host-client tier must map to a concrete value; empty means the resolver can't select it")
	require.NotEqual(t, l.Get(commonLabeler.LabelServiceValueHost), l.Get(commonLabeler.LabelServiceValueHostClient),
		"peer and client Service tiers must be distinguishable by label")
}
