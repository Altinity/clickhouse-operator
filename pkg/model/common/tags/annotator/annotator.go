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

package annotator

import (
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Annotator is an entity which can annotate CHI artifacts
type Annotator struct {
	*Config
	cr api.ICustomResource
}

// New creates new annotator with context
func New(cr api.ICustomResource, _config ...*Config) *Annotator {
	var config *Config
	if len(_config) == 0 {
		config = NewDefaultConfig()
	} else {
		config = _config[0]
	}
	return &Annotator{
		Config: config,
		cr:     cr,
	}
}

func (a *Annotator) Annotate(what interfaces.AnnotateType, params ...any) map[string]string {
	switch what {

	case interfaces.AnnotateServiceCR:
		return a.GetCRScope()
	case interfaces.AnnotateServiceCluster:
		var cluster api.ICluster
		if len(params) > 0 {
			cluster = params[0].(api.ICluster)
			return a.getClusterScope(cluster)
		}
	case interfaces.AnnotateServiceShard:
		var shard api.IShard
		if len(params) > 0 {
			shard = params[0].(api.IShard)
			return a.getShardScope(shard)
		}
	case interfaces.AnnotateServiceHost:
		var host *api.Host
		if len(params) > 0 {
			host = params[0].(*api.Host)
			return a.GetHostScope(host)
		}

	case interfaces.AnnotateExistingPV:
		var pv *core.PersistentVolume
		var host *api.Host
		if len(params) > 1 {
			pv = params[0].(*core.PersistentVolume)
			host = params[1].(*api.Host)
			// Merge annotations from
			// 1. Existing PV
			// 2. Scope
			return util.MergeStringMapsOverwrite(pv.GetAnnotations(), a.GetHostScope(host))
		}

	case interfaces.AnnotateNewPVC:
		var host *api.Host
		if len(params) > 0 {
			host = params[0].(*api.Host)
			return a.GetHostScope(host)
		}

	case interfaces.AnnotateExistingPVC:
		var pvc *core.PersistentVolumeClaim
		var host *api.Host
		var template *api.VolumeClaimTemplate
		if len(params) > 2 {
			pvc = params[0].(*core.PersistentVolumeClaim)
			host = params[1].(*api.Host)
			template = params[2].(*api.VolumeClaimTemplate)
			// Merge annotations from
			// 1. Template
			// 2. Existing PVC
			// 3. Scope
			annotations := util.MergeStringMapsOverwrite(pvc.GetAnnotations(), template.ObjectMeta.GetAnnotations())
			return util.MergeStringMapsOverwrite(annotations, a.GetHostScope(host))
		}

	case interfaces.AnnotatePDB:
		var cluster api.ICluster
		if len(params) > 0 {
			cluster = params[0].(api.ICluster)
			return a.getClusterScope(cluster)
		}

	case interfaces.AnnotateSTS:
		var host *api.Host
		if len(params) > 0 {
			host = params[0].(*api.Host)
			return a.GetHostScope(host)
		}

	case interfaces.AnnotatePodTemplate:
		var host *api.Host
		if len(params) > 0 {
			host = params[0].(*api.Host)
			return a.GetHostScope(host)
		}
	}
	panic("unknown annotate type")
}

// GetCRScope gets annotations for CR-scoped object
func (a *Annotator) GetCRScope() map[string]string {
	// Combine generated annotations and CR-provided annotations
	return a.filterOutAnnotationsToBeSkipped(a.appendCRProvidedAnnotations(nil))
}

// getClusterScope gets annotations for Cluster-scoped object
func (a *Annotator) getClusterScope(cluster api.ICluster) map[string]string {
	// Combine generated annotations and CR-provided annotations
	return a.filterOutAnnotationsToBeSkipped(a.appendCRProvidedAnnotations(nil))
}

// getShardScope gets annotations for Shard-scoped object
func (a *Annotator) getShardScope(shard api.IShard) map[string]string {
	// Combine generated annotations and CR-provided annotations
	return a.filterOutAnnotationsToBeSkipped(a.appendCRProvidedAnnotations(nil))
}

// GetHostScope gets annotations for Host-scoped object
func (a *Annotator) GetHostScope(host *api.Host) map[string]string {
	// Combine generated annotations and CR-provided annotations
	return a.filterOutAnnotationsToBeSkipped(a.appendCRProvidedAnnotations(nil))
}

// filterOutAnnotationsToBeSkipped filters out annotations that have to be skipped
func (a *Annotator) filterOutAnnotationsToBeSkipped(m map[string]string) map[string]string {
	return util.CopyMapFilter(m, nil, util.AnnotationsToBeSkipped)
}

// appendCRProvidedAnnotations appends CR-provided annotations to specified annotations
func (a *Annotator) appendCRProvidedAnnotations(dst map[string]string) map[string]string {
	source := util.CopyMapFilter(
		// Start with CR-provided annotations
		a.cr.GetAnnotations(),
		// Respect include-exclude policies
		a.Include,
		a.Exclude,
	)
	// Merge on top of provided dst
	return util.MergeStringMapsOverwrite(dst, source)
}
