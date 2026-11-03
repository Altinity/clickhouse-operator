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

package util

import "strings"

// NormalizeFQDN strips the trailing dot from an FQDN for storage/display.
// DNS connections use the full form with dot; status fields use the clean form.
func NormalizeFQDN(fqdn string) string {
	return strings.TrimSuffix(fqdn, ".")
}

// NormalizeFQDNs returns a copy of fqdns with trailing dots stripped from each entry.
func NormalizeFQDNs(fqdns []string) []string {
	result := make([]string, len(fqdns))
	for i, f := range fqdns {
		result[i] = NormalizeFQDN(f)
	}
	return result
}

// TrimHostListToFQDNs keeps only entries from hosts that appear in fqdns (after FQDN normalization).
// If fqdns is nil, hosts is returned unchanged — callers that refresh status FQDNs later may sync explicitly.
func TrimHostListToFQDNs(hosts []string, fqdns []string) []string {
	if fqdns == nil {
		return hosts
	}
	return IntersectStringArrays(NormalizeFQDNs(hosts), NormalizeFQDNs(fqdns))
}
