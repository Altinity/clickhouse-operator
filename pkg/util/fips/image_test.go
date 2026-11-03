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

package fips

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestImageHasFIPSMarker — covers Altinity's actual convention plus the
// canonical false-positive (registry path with "fips") and digest-pin edge
// case. The function inspects only the TAG portion after the last ':'.
func TestImageHasFIPSMarker(t *testing.T) {
	cases := []struct {
		image string
		want  bool
	}{
		// Altinity's documented convention — passes.
		{"altinity/clickhouse-server:25.3.8.30001.altinityfips", true},
		{"altinity/clickhouse-keeper:25.3.8.30001.altinityfips", true},
		// Case-insensitive.
		{"altinity/clickhouse-server:25.3.8-FIPS", true},
		{"altinity/clickhouse-server:25.3.8-Fips", true},
		// Hyphenated convention.
		{"private.registry.io/clickhouse:24.1.0-fips-rc1", true},
		// Community / non-FIPS — fails.
		{"clickhouse/clickhouse-server:latest", false},
		{"clickhouse/clickhouse-server:25.3.8.30001", false},
		// False-positive defense: substring in registry/repo path, not tag.
		{"fips-registry.example.com/clickhouse:24.1.0", false},
		{"fipsbase/clickhouse-server:25.3", false},
		// Port + tag — colons in port must not confuse the tag extraction.
		{"registry:5000/clickhouse-server:25.3.8-fips", true},
		{"registry:5000/clickhouse-server:25.3.8", false},
		// Digest-only pin — no tag, returns false (callers must use version()
		// runtime check OR include both a tag AND digest).
		{"clickhouse/clickhouse-server@sha256:0000000000000000000000000000000000000000000000000000000000000000", false},
		// Untagged — implicit :latest is non-FIPS.
		{"clickhouse/clickhouse-server", false},
		// Empty — false.
		{"", false},
	}
	for _, c := range cases {
		t.Run(c.image, func(t *testing.T) {
			require.Equal(t, c.want, ImageHasFIPSMarker(c.image))
		})
	}
}

// TestVersionHasFIPSMarker — runtime check against `SELECT version()` reply.
func TestVersionHasFIPSMarker(t *testing.T) {
	cases := []struct {
		version string
		want    bool
	}{
		{"25.3.8.30001.altinityfips", true},
		{"25.3.8-fips", true},
		{"25.3.8.FIPS", true},
		{"25.3.8.30001", false},
		{"", false},
	}
	for _, c := range cases {
		t.Run(c.version, func(t *testing.T) {
			require.Equal(t, c.want, VersionHasFIPSMarker(c.version))
		})
	}
}
