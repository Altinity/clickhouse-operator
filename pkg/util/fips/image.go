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

// Package fips holds shared FIPS-policy helpers callable from normalizers,
// reconcilers, and unit tests. Pure stdlib-only; no imports from pkg/apis or
// pkg/model so it can be used at any layer without creating cycles.
package fips

import "strings"

// ImageHasFIPSMarker reports whether the image tag (everything after the
// last ':' in the reference) contains the substring "fips" (case-insensitive).
//
// Intentionally inspects the TAG portion only, not the full reference:
//   - false positive risk: a registry path like `fips-registry.example.com/...`
//     would match a naive `strings.Contains(image, "fips")`.
//   - Altinity's convention is `<repo>:<version>.altinityfips` (e.g.
//     `altinity/clickhouse-server:25.3.8.30001.altinityfips`).
//
// Digest-only references (`<repo>@sha256:...`) have no tag and therefore
// always return false. Callers that pin by digest under Required policy
// must either tag-AND-digest, or rely on the runtime `version()` confirmation.
//
// Empty image returns false.
func ImageHasFIPSMarker(image string) bool {
	tag := imageTag(image)
	return strings.Contains(strings.ToLower(tag), "fips")
}

// VersionHasFIPSMarker reports whether the ClickHouse / Keeper runtime
// version string (the result of `SELECT version()`) contains the substring
// "fips" (case-insensitive). Altinity FIPS builds bake the tag suffix into
// the binary's reported version (e.g. `25.3.8.30001.altinityfips`).
//
// Empty version returns false.
func VersionHasFIPSMarker(version string) bool {
	return strings.Contains(strings.ToLower(version), "fips")
}

// imageTag extracts the tag portion (after the last ':') from an image
// reference. If the reference is digest-pinned (contains '@') or has no
// tag separator, returns the empty string.
//
//	"repo/name:tag"           → "tag"
//	"repo/name@sha256:abcd"   → ""
//	"repo/name"               → ""
//	"registry:5000/name:tag"  → "tag"   (last colon wins)
//	"registry:5000/name"      → ""      (digest-less, no tag)
//
// Note: the heuristic "no '/' after the last ':'" distinguishes a port
// (registry:5000/name) from a tag (name:tag).
func imageTag(image string) string {
	if image == "" {
		return ""
	}
	// Digest-pinned references have no tag.
	if at := strings.LastIndex(image, "@"); at >= 0 {
		image = image[:at]
	}
	colon := strings.LastIndex(image, ":")
	if colon < 0 {
		return ""
	}
	// If there's a '/' after the colon, that colon was a port separator.
	if slash := strings.LastIndex(image, "/"); slash > colon {
		return ""
	}
	return image[colon+1:]
}
