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

// WatchedCHI specifies watched CLickHouseInstallation
type WatchedCHI struct {
	Namespace string   `json:"namespace"`
	Name      string   `json:"name"`
	Hostnames []string `json:"hostnames"`
}

func (chi *WatchedCHI) isValid() bool {
	return !chi.empty()
}

func (chi *WatchedCHI) empty() bool {
	return (len(chi.Namespace) == 0) && (len(chi.Name) == 0) && (len(chi.Hostnames) == 0)
}

func (chi *WatchedCHI) equal(chi2 *WatchedCHI) bool {
	// Must have the same namespace
	if chi.Namespace != chi2.Namespace {
		return false
	}

	// Must have the same name
	if chi.Name != chi2.Name {
		return false
	}

	// Must have the same number of items
	if len(chi.Hostnames) != len(chi2.Hostnames) {
		return false
	}

	// Must have the same items
	for i := range chi.Hostnames {
		if chi.Hostnames[i] != chi2.Hostnames[i] {
			return false
		}
	}

	// All checks passed
	return true
}

func (chi *WatchedCHI) indexKey() string {
	return chi.Namespace + ":" + chi.Name
}
