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

//go:build !acvp_wrapper

package app

// TryACVPDispatch is the stub used in default (non-validation) builds. It
// returns false unconditionally so the operator boots normally. The acvp
// responder package is intentionally NOT imported here: that keeps the
// production binary free of ACVP test-harness code and its dependencies.
// The companion file acvp_dispatch_on.go provides the real implementation
// under -tags acvp_wrapper.
func TryACVPDispatch() bool {
	return false
}
