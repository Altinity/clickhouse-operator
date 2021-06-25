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
	"context"
)

type reconcileContextKey string

var ReconciledRegistry = reconcileContextKey("reconciled_registry")
var FailedRegistry = reconcileContextKey("failed_registry")

// NewReconcileContext
func NewReconcileContext(ctx context.Context) context.Context {
	return context.WithValue(context.WithValue(ctx, ReconciledRegistry, NewRegistry()), FailedRegistry, NewRegistry())
}

// GetReconciledRegistry
func GetReconciledRegistry(ctx context.Context) *Registry {
	return ctx.Value(ReconciledRegistry).(*Registry)
}

// GetFailedRegistry
func GetFailedRegistry(ctx context.Context) *Registry {
	return ctx.Value(FailedRegistry).(*Registry)
}
