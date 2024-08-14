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

package affinity

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/model/common/macro"
	commonLabeler "github.com/altinity/clickhouse-operator/pkg/model/common/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// newMatchLabels
func (a *Affinity) newMatchLabels(podDistribution *api.PodDistribution, matchLabels map[string]string) map[string]string {
	var scopeLabels map[string]string

	switch podDistribution.Scope {
	case deployment.PodDistributionScopeShard:
		scopeLabels = map[string]string{
			commonLabeler.LabelNamespace:   a.macro.Get(macro.MacrosNamespace),
			commonLabeler.LabelCRName:      a.macro.Get(macro.MacrosCRName),
			commonLabeler.LabelClusterName: a.macro.Get(macro.MacrosClusterName),
			commonLabeler.LabelShardName:   a.macro.Get(macro.MacrosShardName),
		}
	case deployment.PodDistributionScopeReplica:
		scopeLabels = map[string]string{
			commonLabeler.LabelNamespace:   a.macro.Get(macro.MacrosNamespace),
			commonLabeler.LabelCRName:      a.macro.Get(macro.MacrosCRName),
			commonLabeler.LabelClusterName: a.macro.Get(macro.MacrosClusterName),
			commonLabeler.LabelReplicaName: a.macro.Get(macro.MacrosReplicaName),
		}
	case deployment.PodDistributionScopeCluster:
		scopeLabels = map[string]string{
			commonLabeler.LabelNamespace:   a.macro.Get(macro.MacrosNamespace),
			commonLabeler.LabelCRName:      a.macro.Get(macro.MacrosCRName),
			commonLabeler.LabelClusterName: a.macro.Get(macro.MacrosClusterName),
		}
	case deployment.PodDistributionScopeClickHouseInstallation:
		scopeLabels = map[string]string{
			commonLabeler.LabelNamespace: a.macro.Get(macro.MacrosNamespace),
			commonLabeler.LabelCRName:    a.macro.Get(macro.MacrosCRName),
		}
	case deployment.PodDistributionScopeNamespace:
		scopeLabels = map[string]string{
			commonLabeler.LabelNamespace: a.macro.Get(macro.MacrosNamespace),
		}
	case deployment.PodDistributionScopeGlobal:
		scopeLabels = map[string]string{}
	}

	return util.MergeStringMapsOverwrite(matchLabels, scopeLabels)
}

// processLabelSelector
func (a *Affinity) processLabelSelector(labelSelector *meta.LabelSelector, host *api.Host) {
	if labelSelector == nil {
		return
	}

	for k := range labelSelector.MatchLabels {
		labelSelector.MatchLabels[k] = a.macro.Scope(host).Line(labelSelector.MatchLabels[k])
	}
	for j := range labelSelector.MatchExpressions {
		labelSelectorRequirement := &labelSelector.MatchExpressions[j]
		a.processLabelSelectorRequirement(labelSelectorRequirement, host)
	}
}

// processLabelSelectorRequirement
func (a *Affinity) processLabelSelectorRequirement(labelSelectorRequirement *meta.LabelSelectorRequirement, host *api.Host) {
	if labelSelectorRequirement == nil {
		return
	}
	labelSelectorRequirement.Key = a.macro.Scope(host).Line(labelSelectorRequirement.Key)
	// Update values only, keys are not macros-ed
	for i := range labelSelectorRequirement.Values {
		labelSelectorRequirement.Values[i] = a.macro.Scope(host).Line(labelSelectorRequirement.Values[i])
	}
}
