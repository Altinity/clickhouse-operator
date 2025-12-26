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

package metrics

import (
	"context"
	"sync"

	"github.com/altinity/clickhouse-operator/pkg/util"
)

func CRInitZeroValues(ctx context.Context, src labelsSource) {
	chiInitZeroValues(ctx, src)
}

func CRReconcilesStarted(ctx context.Context, src labelsSource) {
	chiReconcilesStarted(ctx, src)
}
func CRReconcilesCompleted(ctx context.Context, src labelsSource) {
	chiReconcilesCompleted(ctx, src)
}
func CRReconcilesAborted(ctx context.Context, src labelsSource) {
	chiReconcilesAborted(ctx, src)
}
func CRReconcilesTimings(ctx context.Context, src labelsSource, seconds float64) {
	chiReconcilesTimings(ctx, src, seconds)
}

func HostReconcilesStarted(ctx context.Context, src labelsSource) {
	hostReconcilesStarted(ctx, src)
}
func HostReconcilesCompleted(ctx context.Context, src labelsSource) {
	hostReconcilesCompleted(ctx, src)
}
func HostReconcilesRestart(ctx context.Context, src labelsSource) {
	hostReconcilesRestart(ctx, src)
}
func HostReconcilesErrors(ctx context.Context, src labelsSource) {
	hostReconcilesErrors(ctx, src)
}
func HostReconcilesTimings(ctx context.Context, src labelsSource, seconds float64) {
	hostReconcilesTimings(ctx, src, seconds)
}

func PodAdd(ctx context.Context) {
	podAdd(ctx)
}
func PodUpdate(ctx context.Context) {
	podUpdate(ctx)
}
func PodDelete(ctx context.Context) {
	podDelete(ctx)
}

var r = map[string]bool{}
var mx = sync.Mutex{}

func CRRegister(ctx context.Context, src labelsSource) {
	mx.Lock()
	defer mx.Unlock()

	if registered, found := r[createRegistryKey(src)]; found && registered {
		// Already registered
		return
	}

	// Need to register
	r[createRegistryKey(src)] = true
	chiRegister(ctx, src)
}

func CRUnregister(ctx context.Context, src labelsSource) {
	mx.Lock()
	defer mx.Unlock()

	if registered, found := r[createRegistryKey(src)]; !registered || !found {
		// Already unregistered
		return
	}

	// Need to unregister
	r[createRegistryKey(src)] = false
	chiUnregister(ctx, src)
}

func createRegistryKey(src labelsSource) string {
	return util.NamespaceNameString(src)
}
