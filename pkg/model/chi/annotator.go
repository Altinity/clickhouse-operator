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
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Annotator is an entity which can annotate CHI artifacts
type Annotator struct {
	chi *api.ClickHouseInstallation
}

// NewAnnotator creates new annotator with context
func NewAnnotator(chi *api.ClickHouseInstallation) *Annotator {
	return &Annotator{
		chi: chi,
	}
}

// GetConfigMapCHICommon
func (a *Annotator) GetConfigMapCHICommon() map[string]string {
	return util.MergeStringMapsOverwrite(
		a.getCHIScope(),
		nil,
	)
}

// GetConfigMapCHICommonUsers
func (a *Annotator) GetConfigMapCHICommonUsers() map[string]string {
	return util.MergeStringMapsOverwrite(
		a.getCHIScope(),
		nil,
	)
}

// GetConfigMapHost
func (a *Annotator) GetConfigMapHost(host *api.ChiHost) map[string]string {
	return util.MergeStringMapsOverwrite(
		a.GetHostScope(host),
		nil,
	)
}

// GetServiceCHI
func (a *Annotator) GetServiceCHI(chi *api.ClickHouseInstallation) map[string]string {
	return util.MergeStringMapsOverwrite(
		a.getCHIScope(),
		nil,
	)
}

// GetServiceCluster
func (a *Annotator) GetServiceCluster(cluster *api.Cluster) map[string]string {
	return util.MergeStringMapsOverwrite(
		a.GetClusterScope(cluster),
		nil,
	)
}

// GetServiceShard
func (a *Annotator) GetServiceShard(shard *api.ChiShard) map[string]string {
	return util.MergeStringMapsOverwrite(
		a.getShardScope(shard),
		nil,
	)
}

// GetServiceHost
func (a *Annotator) GetServiceHost(host *api.ChiHost) map[string]string {
	return util.MergeStringMapsOverwrite(
		a.GetHostScope(host),
		nil,
	)
}

// getCHIScope gets annotations for CHI-scoped object
func (a *Annotator) getCHIScope() map[string]string {
	// Combine generated annotations and CHI-provided annotations
	return a.filterOutPredefined(a.appendCHIProvidedTo(nil))
}

// GetClusterScope gets annotations for Cluster-scoped object
func (a *Annotator) GetClusterScope(cluster *api.Cluster) map[string]string {
	// Combine generated annotations and CHI-provided annotations
	return a.filterOutPredefined(a.appendCHIProvidedTo(nil))
}

// getShardScope gets annotations for Shard-scoped object
func (a *Annotator) getShardScope(shard *api.ChiShard) map[string]string {
	// Combine generated annotations and CHI-provided annotations
	return a.filterOutPredefined(a.appendCHIProvidedTo(nil))
}

// GetHostScope gets annotations for Host-scoped object
func (a *Annotator) GetHostScope(host *api.ChiHost) map[string]string {
	return a.filterOutPredefined(a.appendCHIProvidedTo(nil))
}

// filterOutPredefined filters out predefined values
func (a *Annotator) filterOutPredefined(m map[string]string) map[string]string {
	return util.CopyMapFilter(m, nil, util.AnnotationsTobeSkipped)
}

// appendCHIProvidedTo appends CHI-provided annotations to specified annotations
func (a *Annotator) appendCHIProvidedTo(dst map[string]string) map[string]string {
	source := util.CopyMapFilter(a.chi.Annotations, chop.Config().Annotation.Include, chop.Config().Annotation.Exclude)
	return util.MergeStringMapsOverwrite(dst, source)
}

// GetPV
func (a *Annotator) GetPV(pv *core.PersistentVolume, host *api.ChiHost) map[string]string {
	return util.MergeStringMapsOverwrite(pv.Annotations, a.GetHostScope(host))
}

// GetPVC
func (a *Annotator) GetPVC(
	pvc *core.PersistentVolumeClaim,
	host *api.ChiHost,
	template *api.VolumeClaimTemplate,
) map[string]string {
	annotations := util.MergeStringMapsOverwrite(pvc.Annotations, template.ObjectMeta.Annotations)
	return util.MergeStringMapsOverwrite(annotations, a.GetHostScope(host))
}
