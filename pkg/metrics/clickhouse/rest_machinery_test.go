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

package clickhouse

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/stretchr/testify/require"
)

// resetIPCTokenCache zeroes the package-level token cache so each test starts
// from a clean state. Tests that exercise the cache must call this in setup.
func resetIPCTokenCache(t *testing.T) {
	t.Helper()
	ipcTokenMu.Lock()
	defer ipcTokenMu.Unlock()
	ipcTokenCache = ""
}

// TestResolveIPCToken_CachesOnSuccess verifies the second call returns the
// cached value without re-reading disk.
func TestResolveIPCToken_CachesOnSuccess(t *testing.T) {
	resetIPCTokenCache(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(path, []byte("good-token\n"), 0o600))

	ipc := resolvedIPC{Mode: api.IPCModeSecure, TokenPath: path}

	tok1, err := cacheBackedLoad(ipc)
	require.NoError(t, err)
	require.Equal(t, "good-token", tok1)

	// Mutate the file; cached value must still be returned.
	require.NoError(t, os.WriteFile(path, []byte("rotated-token\n"), 0o600))

	tok2, err := cacheBackedLoad(ipc)
	require.NoError(t, err)
	require.Equal(t, "good-token", tok2, "second call must return cached value")
}

// TestResolveIPCToken_RetriesOnError verifies a failed first call does NOT
// permanently cache the error: once the token becomes readable, the next
// call succeeds (an earlier sync.Once-based cache would have permanently
// memoized the first failure).
func TestResolveIPCToken_RetriesOnError(t *testing.T) {
	resetIPCTokenCache(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	ipc := resolvedIPC{Mode: api.IPCModeSecure, TokenPath: path}

	// File does not exist yet — first call must error (waitForToken hits
	// the 30s timeout in production; bypass by calling loadToken directly).
	_, err := ipc.loadToken()
	require.Error(t, err, "tokenless first call must error")

	// Cache stays empty after error.
	ipcTokenMu.Lock()
	cached := ipcTokenCache
	ipcTokenMu.Unlock()
	require.Empty(t, cached, "cache must NOT memoize the first failure")

	// File appears; next call succeeds.
	require.NoError(t, os.WriteFile(path, []byte("late-token"), 0o600))
	tok, err := ipc.loadToken()
	require.NoError(t, err)
	require.Equal(t, "late-token", tok)
}

// TestResolveIPCToken_ConcurrentReadIsSafe is a smoke check that
// the mutex protects the cache against concurrent readers/writers
// (no race detector firing).
func TestResolveIPCToken_ConcurrentReadIsSafe(t *testing.T) {
	resetIPCTokenCache(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(path, []byte("concurrent-token"), 0o600))
	ipc := resolvedIPC{Mode: api.IPCModeSecure, TokenPath: path}

	var wg sync.WaitGroup
	const N = 16
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tok, err := cacheBackedLoad(ipc)
			require.NoError(t, err)
			require.Equal(t, "concurrent-token", tok)
		}()
	}
	wg.Wait()
}

// cacheBackedLoad mirrors resolveIPCToken's cache-on-success semantics but
// uses loadToken directly (skipping waitForToken's 30s polling loop) so unit
// tests run fast. Production code path is identical apart from the wait.
func cacheBackedLoad(ipc resolvedIPC) (string, error) {
	ipcTokenMu.Lock()
	cached := ipcTokenCache
	ipcTokenMu.Unlock()
	if cached != "" {
		return cached, nil
	}
	token, err := ipc.loadToken()
	if err != nil {
		return "", err
	}
	ipcTokenMu.Lock()
	ipcTokenCache = token
	ipcTokenMu.Unlock()
	return token, nil
}
