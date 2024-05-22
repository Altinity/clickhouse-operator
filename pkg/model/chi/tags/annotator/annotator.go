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
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Annotator is an entity which can annotate CHI artifacts
type Annotator struct {
	Config
	cr api.ICustomResource
}

type Config struct {
	Include []string
	Exclude []string
}

// NewAnnotator creates new annotator with context
func NewAnnotator(cr api.ICustomResource, config Config) *Annotator {
	return &Annotator{
		Config: config,
		cr:     cr,
	}
}

func (a *Annotator) Annotate(what AnnotateType, params ...any) map[string]string {
	switch what {

	case AnnotateServiceCR:
		return a.getCRScope()
	case AnnotateServiceCluster:
		var cluster api.ICluster
		if len(params) > 0 {
			cluster = params[0].(api.ICluster)
			return a.getClusterScope(cluster)
		}
	case AnnotateServiceShard:
		var shard api.IShard
		if len(params) > 0 {
			shard = params[0].(api.IShard)
			return a.getShardScope(shard)
		}
	case AnnotateServiceHost:
		var host *api.Host
		if len(params) > 0 {
			host = params[0].(*api.Host)
			return a.getHostScope(host)
		}

	case AnnotateExistingPV:
		var pv *core.PersistentVolume
		var host *api.Host
		if len(params) > 1 {
			pv = params[0].(*core.PersistentVolume)
			host = params[1].(*api.Host)
			// Merge annotations from
			// 1. Existing PV
			// 2. Scope
			return util.MergeStringMapsOverwrite(pv.GetAnnotations(), a.getHostScope(host))
		}

	case AnnotateNewPVC:
		var host *api.Host
		if len(params) > 0 {
			host = params[0].(*api.Host)
			return a.getHostScope(host)
		}

	case AnnotateExistingPVC:
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
			return util.MergeStringMapsOverwrite(annotations, a.getHostScope(host))
		}

	case AnnotatePDB:
		var cluster api.ICluster
		if len(params) > 0 {
			cluster = params[0].(api.ICluster)
			return a.getClusterScope(cluster)
		}

	case AnnotateSTS:
		var host *api.Host
		if len(params) > 0 {
			host = params[0].(*api.Host)
			return a.getHostScope(host)
		}

	case AnnotatePodTemplate:
		var host *api.Host
		if len(params) > 0 {
			host = params[0].(*api.Host)
			return a.getHostScope(host)
		}
	}
	panic("unknown annotate type")
}

// getCRScope gets annotations for CR-scoped object
func (a *Annotator) getCRScope() map[string]string {
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

// getHostScope gets annotations for Host-scoped object
func (a *Annotator) getHostScope(host *api.Host) map[string]string {
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
