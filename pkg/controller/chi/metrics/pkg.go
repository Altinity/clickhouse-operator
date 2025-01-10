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
)

func CHIInitZeroValues(ctx context.Context, src labelsSource) {
	chiInitZeroValues(ctx, src)
}

func CHIReconcilesStarted(ctx context.Context, src labelsSource) {
	chiReconcilesStarted(ctx, src)
}
func CHIReconcilesCompleted(ctx context.Context, src labelsSource) {
	chiReconcilesCompleted(ctx, src)
}
func CHIReconcilesAborted(ctx context.Context, src labelsSource) {
	chiReconcilesAborted(ctx, src)
}
func CHIReconcilesTimings(ctx context.Context, src labelsSource, seconds float64) {
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
