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

package statefulset

import (
	"context"

	"gopkg.in/d4l3k/messagediff.v1"
	apps "k8s.io/api/apps/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type IHostStatefulSetPoller interface {
	WaitHostStatefulSetReady(ctx context.Context, host *api.Host) error
	WaitHostStatefulSetDeleted(host *api.Host)
}

type fallback interface {
	OnStatefulSetCreateFailed(ctx context.Context, host *api.Host) common.ErrorCRUD
	OnStatefulSetUpdateFailed(ctx context.Context, oldStatefulSet *apps.StatefulSet, host *api.Host, sts interfaces.IKubeSTS) common.ErrorCRUD
}

type DefaultFallback struct{}

func NewDefaultFallback() *DefaultFallback {
	return &DefaultFallback{}
}

func (f *DefaultFallback) OnStatefulSetCreateFailed(ctx context.Context, host *api.Host) common.ErrorCRUD {
	return common.ErrCRUDIgnore
}
func (f *DefaultFallback) OnStatefulSetUpdateFailed(ctx context.Context, oldStatefulSet *apps.StatefulSet, host *api.Host, sts interfaces.IKubeSTS) common.ErrorCRUD {
	return common.ErrCRUDIgnore
}

func dumpDiff(old, new *apps.StatefulSet) string {
	diff, equal := messagediff.DeepDiff(old.Spec, new.Spec)

	str := ""
	if equal {
		str += "EQUAL: "
	} else {
		str += "NOT EQUAL: "
	}

	if len(diff.Added) > 0 {
		// Something added
		str += util.MessageDiffItemString("added spec items", "none", "", diff.Added)
	}

	if len(diff.Removed) > 0 {
		// Something removed
		str += util.MessageDiffItemString("removed spec items", "none", "", diff.Removed)
	}

	if len(diff.Modified) > 0 {
		// Something modified
		str += util.MessageDiffItemString("modified spec items", "none", "", diff.Modified)
	}
	return str
}
