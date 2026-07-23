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

package deployment

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNormalizePodDistributionType verifies casing folds to the canonical const
// while unrecognized values pass through (the normalizer maps those to Unspecified).
func TestNormalizePodDistributionType(t *testing.T) {
	require.Equal(t, PodDistributionClickHouseAntiAffinity, NormalizePodDistributionType("clickhouseantiaffinity"))
	require.Equal(t, PodDistributionClickHouseAntiAffinity, NormalizePodDistributionType("ClickHouseAntiAffinity"))
	require.Equal(t, PodDistributionCircularReplication, NormalizePodDistributionType("CIRCULARREPLICATION"))
	require.Equal(t, PodDistributionMaxNumberPerNode, NormalizePodDistributionType("maxnumberpernode"))
	require.Equal(t, "bogus", NormalizePodDistributionType("bogus"))
	require.Equal(t, "", NormalizePodDistributionType(""))
}

// TestNormalizePodDistributionScope verifies scope casing folds to the canonical const.
func TestNormalizePodDistributionScope(t *testing.T) {
	require.Equal(t, PodDistributionScopeShard, NormalizePodDistributionScope("shard"))
	require.Equal(t, PodDistributionScopeCluster, NormalizePodDistributionScope("Cluster"))
	require.Equal(t, PodDistributionScopeClickHouseInstallation, NormalizePodDistributionScope("clickhouseinstallation"))
	require.Equal(t, "bogus", NormalizePodDistributionScope("bogus"))
}

// TestNormalizePortDistributionType verifies port-distribution casing folds to the canonical const.
func TestNormalizePortDistributionType(t *testing.T) {
	require.Equal(t, PortDistributionClusterScopeIndex, NormalizePortDistributionType("clusterscopeindex"))
	require.Equal(t, PortDistributionUnspecified, NormalizePortDistributionType("UNSPECIFIED"))
	require.Equal(t, "bogus", NormalizePortDistributionType("bogus"))
}
