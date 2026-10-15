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

package normalizer

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// A user keeps one password only, the strongest one given: a double SHA1 hash over a SHA256 hash over a
// plaintext password, which is replaced by its SHA256 hash. ClickHouse refuses a user with more than one.
func TestUserPasswordPriority(t *testing.T) {
	const (
		plaintext      = "plaintext"
		sha256Hex      = "sha256-hex"
		doubleSHA1Hex  = "double-sha1-hex"
		userDoubleSHA1 = "user-double-sha1"
		userSHA256     = "user-sha256"
		userPlaintext  = "user-plaintext"
	)
	field := func(user, field string) string { return user + "/" + field }
	cr := &chi.ClickHouseInstallation{}
	cr.Spec.Configuration = &chi.Configuration{
		Users: users(
			field(userDoubleSHA1, userFieldPassword), plaintext,
			field(userDoubleSHA1, userFieldPasswordSHA256Hex), sha256Hex,
			field(userDoubleSHA1, userFieldPasswordDoubleSHA1Hex), doubleSHA1Hex,
			field(userSHA256, userFieldPassword), plaintext,
			field(userSHA256, userFieldPasswordSHA256Hex), sha256Hex,
			field(userPlaintext, userFieldPassword), plaintext,
		),
		Clusters: []*chi.Cluster{backupCluster("c", nil)},
	}

	got := normalizeBackupCR(t, cr, nil).Spec.Configuration.Users
	hash := sha256.Sum256([]byte(plaintext))
	for user, want := range map[string]map[string]string{
		userDoubleSHA1: {userFieldPasswordDoubleSHA1Hex: doubleSHA1Hex},
		userSHA256:     {userFieldPasswordSHA256Hex: sha256Hex},
		userPlaintext:  {userFieldPasswordSHA256Hex: hex.EncodeToString(hash[:])},
	} {
		for _, password := range []string{userFieldPassword, userFieldPasswordSHA256Hex, userFieldPasswordDoubleSHA1Hex} {
			value, kept := want[password]
			require.Equal(t, kept, got.Has(field(user, password)), "%s: %s", user, password)
			if kept {
				require.Equal(t, value, got.Get(field(user, password)).String(), "%s: %s", user, password)
			}
		}
	}
}
