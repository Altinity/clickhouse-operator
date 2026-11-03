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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// ipcTokenBytes is the random-byte length of the operator↔exporter shared
// bearer token. 32 bytes = 256 bits → 64 hex characters in the file.
// Comfortably above the brute-force-resistance threshold; not so large that
// it stresses /etc tmpfs.
const ipcTokenBytes = 32

// ProvisionIPCToken generates a fresh random token and writes it to the
// configured TokenPath when chopconf has IPC Mode=Secure. Plain mode is a
// no-op so existing deployments are unaffected.
//
// Called once at operator startup AFTER chop.New has loaded chopconf and
// BEFORE the metrics-exporter sidecar's StartMetricsREST returns.
func ProvisionIPCToken() error {
	return provisionIPCToken(Config().Security.GetIPC())
}

// provisionIPCToken is the test-accessible inner implementation. Splitting
// the IPC-config read from the file-write logic lets unit tests drive the
// behavior with a synthetic *api.OperatorConfigSecurityIPC rather
// than spinning up a full chop subsystem.
//
// Idempotent across restarts: if the path already holds a non-empty token,
// the existing value is reused (avoids invalidating the token if the operator
// container restarts while the exporter container keeps holding the old
// value).
func provisionIPCToken(ipc *api.OperatorConfigSecurityIPC) error {
	if (ipc == nil) || (ipc.GetMode() != api.IPCModeSecure) {
		return nil
	}

	path := ipc.GetTokenPath()
	if path == "" {
		path = api.IPCDefaultTokenPath
	}

	// Reuse non-empty existing token across operator-container restarts.
	if data, err := os.ReadFile(path); (err == nil) && (len(data) > 0) {
		log.V(1).Info("IPC: reusing existing token at %s", path)
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("IPC: cannot create token dir %s: %w", filepath.Dir(path), err)
	}

	raw := make([]byte, ipcTokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return fmt.Errorf("IPC: crypto/rand failure generating token: %w", err)
	}
	encoded := hex.EncodeToString(raw)

	if err := os.WriteFile(path, []byte(encoded), 0o400); err != nil {
		return fmt.Errorf("IPC: cannot write token to %s: %w", path, err)
	}
	log.Info("IPC: Secure mode — provisioned token at %s (%d bytes)", path, len(encoded))
	return nil
}
