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

package chop

import (
	"os"
	"path/filepath"
	"testing"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/stretchr/testify/require"
)

func ipcCfg(mode api.IPCMode, tokenPath string) *api.OperatorConfigSecurityIPC {
	return &api.OperatorConfigSecurityIPC{
		Mode:      types.NewString(string(mode)),
		TokenPath: tokenPath,
	}
}

func TestProvisionIPCTokenPlainModeIsNoop(t *testing.T) {
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")

	require.NoError(t, provisionIPCToken(ipcCfg(api.IPCModePlain, tokenPath)))

	_, err := os.Stat(tokenPath)
	require.True(t, os.IsNotExist(err), "Plain mode must not write any token file")
}

func TestProvisionIPCTokenNilIPCIsNoop(t *testing.T) {
	require.NoError(t, provisionIPCToken(nil))
}

func TestProvisionIPCTokenSecureModeWritesToken(t *testing.T) {
	dir := t.TempDir()
	// Path under a subdirectory that doesn't exist yet — exercises MkdirAll.
	tokenPath := filepath.Join(dir, "subdir", "token")

	require.NoError(t, provisionIPCToken(ipcCfg(api.IPCModeSecure, tokenPath)))

	data, err := os.ReadFile(tokenPath)
	require.NoError(t, err)
	require.Equal(t, 64, len(data), "32-byte token must hex-encode to 64 chars")
	for _, b := range data {
		require.True(t, (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f'),
			"token must be lowercase hex; got byte %q", b)
	}
	info, err := os.Stat(tokenPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o400), info.Mode().Perm(), "token file must be 0400")
}

// TestProvisionIPCTokenReuseExisting verifies the operator does NOT overwrite
// an existing non-empty token across operator-container restarts.
func TestProvisionIPCTokenReuseExisting(t *testing.T) {
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(tokenPath, []byte("preexisting-token-value"), 0o400))

	require.NoError(t, provisionIPCToken(ipcCfg(api.IPCModeSecure, tokenPath)))

	data, err := os.ReadFile(tokenPath)
	require.NoError(t, err)
	require.Equal(t, "preexisting-token-value", string(data),
		"existing non-empty token must not be overwritten")
}

// TestProvisionIPCTokenTwoCallsSameToken verifies two consecutive Secure-mode
// provisions on the same path yield the same token (re-use, not re-generate).
func TestProvisionIPCTokenTwoCallsSameToken(t *testing.T) {
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	cfg := ipcCfg(api.IPCModeSecure, tokenPath)

	require.NoError(t, provisionIPCToken(cfg))
	first, err := os.ReadFile(tokenPath)
	require.NoError(t, err)
	require.NotEmpty(t, first)

	require.NoError(t, provisionIPCToken(cfg))
	second, err := os.ReadFile(tokenPath)
	require.NoError(t, err)
	require.Equal(t, first, second, "second call must reuse the existing token")
}
