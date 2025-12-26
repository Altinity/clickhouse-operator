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

func CRInitZeroValues(ctx context.Context, src labelsSource) {
}

func CRReconcilesStarted(ctx context.Context, src labelsSource) {
}
func CRReconcilesCompleted(ctx context.Context, src labelsSource) {
}
func CRReconcilesAborted(ctx context.Context, src labelsSource) {
}
func CRReconcilesTimings(ctx context.Context, src labelsSource, seconds float64) {
}

func HostReconcilesStarted(ctx context.Context, src labelsSource) {
}
func HostReconcilesCompleted(ctx context.Context, src labelsSource) {
}
func HostReconcilesRestart(ctx context.Context, src labelsSource) {
}
func HostReconcilesErrors(ctx context.Context, src labelsSource) {
}
func HostReconcilesTimings(ctx context.Context, src labelsSource, seconds float64) {
}

func PodAdd(ctx context.Context) {
}
func PodUpdate(ctx context.Context) {
}
func PodDelete(ctx context.Context) {
}

func CRRegister(ctx context.Context, src labelsSource) {
}

func CRUnregister(ctx context.Context, src labelsSource) {
}
