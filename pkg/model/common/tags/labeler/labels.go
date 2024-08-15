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
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// labelServiceCR
func (l *Labeler) labelServiceCR() map[string]string {
	return util.MergeStringMapsOverwrite(
		l.GetCRScope(),
		map[string]string{
			l.Get(LabelService): l.Get(LabelServiceValueCR),
		})
}

// labelServiceCluster
func (l *Labeler) labelServiceCluster(params ...any) map[string]string {
	var cluster api.ICluster
	if len(params) > 0 {
		cluster = params[0].(api.ICluster)
		return l._labelServiceCluster(cluster)
	}
	panic("not enough params for labeler")
}

// _labelServiceCluster
func (l *Labeler) _labelServiceCluster(cluster api.ICluster) map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getClusterScope(cluster),
		map[string]string{
			l.Get(LabelService): l.Get(LabelServiceValueCluster),
		})
}

// labelServiceCluster
func (l *Labeler) labelServiceShard(params ...any) map[string]string {
	var shard api.IShard
	if len(params) > 0 {
		shard = params[0].(api.IShard)
		return l._labelServiceShard(shard)
	}
	panic("not enough params for labeler")
}

// _labelServiceShard
func (l *Labeler) _labelServiceShard(shard api.IShard) map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getShardScope(shard),
		map[string]string{
			l.Get(LabelService): l.Get(LabelServiceValueShard),
		})
}

// labelServiceHost
func (l *Labeler) labelServiceHost(params ...any) map[string]string {
	var host *api.Host
	if len(params) > 0 {
		host = params[0].(*api.Host)
		return l._labelServiceHost(host)
	}
	panic("not enough params for labeler")
}

// _labelServiceHost
func (l *Labeler) _labelServiceHost(host *api.Host) map[string]string {
	return util.MergeStringMapsOverwrite(
		l.GetHostScope(host, false),
		map[string]string{
			l.Get(LabelService): l.Get(LabelServiceValueHost),
		})
}

func (l *Labeler) labelExistingPV(params ...any) map[string]string {
	var pv *core.PersistentVolume
	var host *api.Host
	if len(params) > 1 {
		pv = params[0].(*core.PersistentVolume)
		host = params[1].(*api.Host)
		return l._labelExistingPV(pv, host)
	}
	panic("not enough params for labeler")
}

// _labelExistingPV
func (l *Labeler) _labelExistingPV(pv *core.PersistentVolume, host *api.Host) map[string]string {
	return util.MergeStringMapsOverwrite(pv.GetLabels(), l.GetHostScope(host, false))
}

func (l *Labeler) labelNewPVC(params ...any) map[string]string {
	var host *api.Host
	if len(params) > 0 {
		host = params[0].(*api.Host)
		return l._labelNewPVC(host)
	}
	panic("not enough params for labeler")
}

func (l *Labeler) _labelNewPVC(host *api.Host) map[string]string {
	return l.GetHostScope(host, false)
}

func (l *Labeler) labelExistingPVC(params ...any) map[string]string {
	var pvc *core.PersistentVolumeClaim
	var host *api.Host
	var template *api.VolumeClaimTemplate
	if len(params) > 2 {
		pvc = params[0].(*core.PersistentVolumeClaim)
		host = params[1].(*api.Host)
		template = params[2].(*api.VolumeClaimTemplate)
		return l._labelExistingPVC(pvc, host, template)
	}
	panic("not enough params for labeler")
}

// _labelExistingPVC
func (l *Labeler) _labelExistingPVC(
	pvc *core.PersistentVolumeClaim,
	host *api.Host,
	template *api.VolumeClaimTemplate,
) map[string]string {
	// Prepare main labels based on template
	labels := util.MergeStringMapsOverwrite(pvc.GetLabels(), template.ObjectMeta.GetLabels())
	// Append reclaim policy labels
	return util.MergeStringMapsOverwrite(
		labels,
		l.getHostScopeReclaimPolicy(host, template, false),
	)
}

func (l *Labeler) labelPDB(params ...any) map[string]string {
	var cluster api.ICluster
	if len(params) > 0 {
		cluster = params[0].(api.ICluster)
		return l._labelPDB(cluster)
	}
	panic("not enough params for labeler")
}

func (l *Labeler) _labelPDB(cluster api.ICluster) map[string]string {
	return l.getClusterScope(cluster)
}

func (l *Labeler) labelSTS(params ...any) map[string]string {
	var host *api.Host
	if len(params) > 0 {
		host = params[0].(*api.Host)
		return l._labelSTS(host)
	}
	panic("not enough params for labeler")
}

func (l *Labeler) _labelSTS(host *api.Host) map[string]string {
	return l.GetHostScope(host, true)
}

func (l *Labeler) labelPodTemplate(params ...any) map[string]string {
	var host *api.Host
	if len(params) > 0 {
		host = params[0].(*api.Host)
		return l._labelPodTemplate(host)
	}
	panic("not enough params for labeler")
}

func (l *Labeler) _labelPodTemplate(host *api.Host) map[string]string {
	return l.getHostScopeReady(host, true)
}
