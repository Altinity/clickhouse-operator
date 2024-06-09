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

package v1

import (
	"github.com/imdario/mergo"

	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func (chk *ClickHouseKeeperInstallation) GetRuntime() apiChi.ICustomResourceRuntime {
	return chk.ensureRuntime()
}

func (chk *ClickHouseKeeperInstallation) ensureRuntime() *ClickHouseKeeperInstallationRuntime {
	if chk == nil {
		return nil
	}

	// Assume that most of the time, we'll see a non-nil value.
	if chk.runtime != nil {
		return chk.runtime
	}

	// Otherwise, we need to acquire a lock to initialize the field.
	chk.runtimeCreatorMutex.Lock()
	defer chk.runtimeCreatorMutex.Unlock()
	// Note that we have to check this property again to avoid a TOCTOU bug.
	if chk.runtime == nil {
		chk.runtime = newClickHouseKeeperInstallationRuntime()
	}
	return chk.runtime
}

func (chk *ClickHouseKeeperInstallation) IEnsureStatus() apiChi.IStatus {
	return any(chk.EnsureStatus()).(apiChi.IStatus)
}

// WalkClusters walks clusters
func (chk *ClickHouseKeeperInstallation) WalkClusters(f func(i apiChi.ICluster) error) []error {
	if chk == nil {
		return nil
	}
	res := make([]error, 0)

	for clusterIndex := range chk.GetSpec().Configuration.Clusters {
		res = append(res, f(chk.GetSpec().Configuration.Clusters[clusterIndex]))
	}

	return res
}

// WalkHosts walks hosts with a function
func (chi *ClickHouseKeeperInstallation) WalkHosts(f func(host *apiChi.Host) error) []error {
	return nil
}

// EnsureStatus ensures status
func (chk *ClickHouseKeeperInstallation) EnsureStatus() *ChkStatus {
	if chk == nil {
		return nil
	}

	// Assume that most of the time, we'll see a non-nil value.
	if chk.Status != nil {
		return chk.Status
	}

	// Otherwise, we need to acquire a lock to initialize the field.
	chk.statusCreatorMutex.Lock()
	defer chk.statusCreatorMutex.Unlock()
	// Note that we have to check this property again to avoid a TOCTOU bug.
	if chk.Status == nil {
		chk.Status = &ChkStatus{}
	}
	return chk.Status
}

// GetStatus gets Status
func (chk *ClickHouseKeeperInstallation) GetStatus() *ChkStatus {
	if chk == nil {
		return nil
	}
	return chk.Status
}

// HasStatus checks whether CHI has Status
func (chk *ClickHouseKeeperInstallation) HasStatus() bool {
	if chk == nil {
		return false
	}
	return chk.Status != nil
}

// HasAncestor checks whether CHI has an ancestor
func (chk *ClickHouseKeeperInstallation) HasAncestor() bool {
	if !chk.HasStatus() {
		return false
	}
	return chk.Status.HasNormalizedCHKCompleted()
}

// GetAncestor gets ancestor of a CHI
func (chk *ClickHouseKeeperInstallation) GetAncestor() *ClickHouseKeeperInstallation {
	if !chk.HasAncestor() {
		return nil
	}
	return chk.Status.GetNormalizedCHKCompleted()
}

// SetAncestor sets ancestor of a CHI
func (chk *ClickHouseKeeperInstallation) SetAncestor(a *ClickHouseKeeperInstallation) {
	if chk == nil {
		return
	}
	chk.EnsureStatus().NormalizedCHKCompleted = a
}

// HasTarget checks whether CHI has a target
func (chk *ClickHouseKeeperInstallation) HasTarget() bool {
	if !chk.HasStatus() {
		return false
	}
	return chk.Status.HasNormalizedCHK()
}

// GetTarget gets target of a CHI
func (chk *ClickHouseKeeperInstallation) GetTarget() *ClickHouseKeeperInstallation {
	if !chk.HasTarget() {
		return nil
	}
	return chk.Status.GetNormalizedCHK()
}

// SetTarget sets target of a CHI
func (chk *ClickHouseKeeperInstallation) SetTarget(a *ClickHouseKeeperInstallation) {
	if chk == nil {
		return
	}
	chk.EnsureStatus().NormalizedCHK = a
}

// MergeFrom merges from CHI
func (chk *ClickHouseKeeperInstallation) MergeFrom(from *ClickHouseKeeperInstallation, _type apiChi.MergeType) {
	if from == nil {
		return
	}

	// Merge Meta
	switch _type {
	case apiChi.MergeTypeFillEmptyValues:
		_ = mergo.Merge(&chk.TypeMeta, from.TypeMeta)
		_ = mergo.Merge(&chk.ObjectMeta, from.ObjectMeta)
	case apiChi.MergeTypeOverrideByNonEmptyValues:
		_ = mergo.Merge(&chk.TypeMeta, from.TypeMeta, mergo.WithOverride)
		_ = mergo.Merge(&chk.ObjectMeta, from.ObjectMeta, mergo.WithOverride)
	}
	// Exclude skipped annotations
	chk.Annotations = util.CopyMapFilter(
		chk.Annotations,
		nil,
		util.ListSkippedAnnotations(),
	)

	// Do actual merge for Spec
	(&chk.Spec).MergeFrom(&from.Spec, _type)

	chk.EnsureStatus().CopyFrom(from.Status, apiChi.CopyStatusOptions{
		InheritableFields: true,
	})
}

// GetRootServiceTemplate gets ServiceTemplate of a CHI
func (chk *ClickHouseKeeperInstallation) GetRootServiceTemplate() (*apiChi.ServiceTemplate, bool) {
	return nil, false
}

func (chk *ClickHouseKeeperInstallation) GetSpec() *ChkSpec {
	return &chk.Spec
}
