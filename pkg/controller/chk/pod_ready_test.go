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

package chk

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
)

func keeperPod(ready bool) *core.Pod {
	return &core.Pod{
		Status: core.PodStatus{
			ContainerStatuses: []core.ContainerStatus{{Name: "clickhouse-keeper", Ready: ready}},
		},
	}
}

func TestPodBecameReady(t *testing.T) {
	require.True(t, podBecameReady(keeperPod(false), keeperPod(true)))
	require.False(t, podBecameReady(keeperPod(true), keeperPod(true)))
	require.False(t, podBecameReady(keeperPod(true), keeperPod(false)))
	require.False(t, podBecameReady(nil, keeperPod(true)))
}

func TestShouldRecoverAbortedCHK(t *testing.T) {
	aborted := &apiChk.ClickHouseKeeperInstallation{}
	aborted.Status = &apiChk.Status{Status: apiChk.StatusAborted}
	require.True(t, shouldRecoverAbortedCHK(aborted))

	completed := &apiChk.ClickHouseKeeperInstallation{}
	completed.Status = &apiChk.Status{Status: apiChk.StatusCompleted}
	require.False(t, shouldRecoverAbortedCHK(completed))

	deleting := aborted.DeepCopy()
	now := meta.NewTime(time.Now())
	deleting.DeletionTimestamp = &now
	require.False(t, shouldRecoverAbortedCHK(deleting))

	fips := aborted.DeepCopy()
	fips.Status.Errors = []string{"[FIPSValidationFailed] cluster must be secure"}
	require.False(t, shouldRecoverAbortedCHK(fips))

	require.False(t, shouldRecoverAbortedCHK(nil))
}
