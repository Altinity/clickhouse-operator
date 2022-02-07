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

package model

import (
	v1 "k8s.io/api/core/v1"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Annotator is an entity which can annotate CHI artifacts
type Annotator struct {
	chi *chiv1.ClickHouseInstallation
}

// NewAnnotator creates new annotator with context
func NewAnnotator(chi *chiv1.ClickHouseInstallation) *Annotator {
	return &Annotator{
		chi: chi,
	}
}

// getConfigMapCHICommon
func (a *Annotator) getConfigMapCHICommon() map[string]string {
	return util.MergeStringMapsOverwrite(
		a.getCHIScope(),
		nil,
	)
}

// getConfigMapCHICommonUsers
func (a *Annotator) getConfigMapCHICommonUsers() map[string]string {
	return util.MergeStringMapsOverwrite(
		a.getCHIScope(),
		nil,
	)
}

// getConfigMapHost
func (a *Annotator) getConfigMapHost(host *chiv1.ChiHost) map[string]string {
	return util.MergeStringMapsOverwrite(
		a.getHostScope(host),
		nil,
	)
}

// getServiceCHI
func (a *Annotator) getServiceCHI(chi *chiv1.ClickHouseInstallation) map[string]string {
	return util.MergeStringMapsOverwrite(
		a.getCHIScope(),
		nil,
	)
}

// getServiceCluster
func (a *Annotator) getServiceCluster(cluster *chiv1.ChiCluster) map[string]string {
	return util.MergeStringMapsOverwrite(
		a.getClusterScope(cluster),
		nil,
	)
}

// getServiceShard
func (a *Annotator) getServiceShard(shard *chiv1.ChiShard) map[string]string {
	return util.MergeStringMapsOverwrite(
		a.getShardScope(shard),
		nil,
	)
}

// getServiceHost
func (a *Annotator) getServiceHost(host *chiv1.ChiHost) map[string]string {
	return util.MergeStringMapsOverwrite(
		a.getHostScope(host),
		nil,
	)
}

// getCHIScope gets annotations for CHI-scoped object
func (a *Annotator) getCHIScope() map[string]string {
	// Combine generated annotations and CHI-provided annotations
	return a.filterOutPredefined(a.appendCHIProvidedTo(nil))
}

// getClusterScope gets annotations for Cluster-scoped object
func (a *Annotator) getClusterScope(cluster *chiv1.ChiCluster) map[string]string {
	// Combine generated annotations and CHI-provided annotations
	return a.filterOutPredefined(a.appendCHIProvidedTo(nil))
}

// getShardScope gets annotations for Shard-scoped object
func (a *Annotator) getShardScope(shard *chiv1.ChiShard) map[string]string {
	// Combine generated annotations and CHI-provided annotations
	return a.filterOutPredefined(a.appendCHIProvidedTo(nil))
}

// getHostScope gets annotations for Host-scoped object
func (a *Annotator) getHostScope(host *chiv1.ChiHost) map[string]string {
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

// getPV
func (a *Annotator) getPV(pv *v1.PersistentVolume, host *chiv1.ChiHost) map[string]string {
	return util.MergeStringMapsOverwrite(pv.Annotations, a.getHostScope(host))
}

// getPVC
func (a *Annotator) getPVC(
	pvc *v1.PersistentVolumeClaim,
	host *chiv1.ChiHost,
	template *chiv1.ChiVolumeClaimTemplate,
) map[string]string {
	annotations := util.MergeStringMapsOverwrite(pvc.Annotations, template.ObjectMeta.Annotations)
	return util.MergeStringMapsOverwrite(annotations, a.getHostScope(host))
}
