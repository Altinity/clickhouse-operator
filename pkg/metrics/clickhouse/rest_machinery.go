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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// chiRESTPort is the operator-side REST port (matches StartMetricsREST's bind).
// Hardcoded to 8888 because the listener also lives in this package.
const chiRESTPort = 8888

// ipcURL caches the resolved Secure-mode loopback URL across calls (the URL is
// fixed by chopconf at startup, so once-resolved is safe). Token is cached
// only on success; failures fall through so a transient first-call miss can
// recover on the next call.
var (
	ipcURLOnce sync.Once
	ipcURL     string

	ipcTokenMu    sync.Mutex
	ipcTokenCache string
)

// resolveIPCURL builds the operator-REST URL for the local sidecar call. In
// Secure mode it honors ipc.BindHost so a non-default bind (rare) doesn't
// diverge from the listener.
func resolveIPCURL() string {
	ipcURLOnce.Do(func() {
		ipc := resolveIPC()
		host := api.IPCDefaultBindHost
		if (ipc.Mode == api.IPCModeSecure) && (ipc.BindHost != "") {
			host = ipc.BindHost
		}
		ipcURL = "http://" + net.JoinHostPort(host, strconv.Itoa(chiRESTPort)) + "/chi"
	})
	return ipcURL
}

// resolveIPCToken returns the token, loading once from disk on first success
// and reusing the cached value thereafter. On error the cache stays empty so
// the next call retries — a transient first-call failure (e.g. emptyDir mount
// race) should not be permanently fatal.
func resolveIPCToken() (string, error) {
	ipcTokenMu.Lock()
	cached := ipcTokenCache
	ipcTokenMu.Unlock()
	if cached != "" {
		return cached, nil
	}
	token, err := resolveIPC().waitForToken()
	if err != nil {
		return "", err
	}
	ipcTokenMu.Lock()
	ipcTokenCache = token
	ipcTokenMu.Unlock()
	return token, nil
}

func makeRESTCall(restReq *RESTRequest, method string) error {
	payload, err := json.Marshal(restReq)
	if err != nil {
		return err
	}

	httpReq, err := http.NewRequest(method, resolveIPCURL(), bytes.NewBuffer(payload))
	if err != nil {
		return err
	}

	// IPC Secure mode: inject the X-CHOP-Token header. Token + URL both come
	// from resolveIPC()-backed caches built once at first call.
	if resolveIPC().Mode == api.IPCModeSecure {
		token, err := resolveIPCToken()
		if err != nil {
			return fmt.Errorf("IPC Secure mode but token unreadable: %w", err)
		}
		httpReq.Header.Set(api.IPCHeaderToken, token)
	}

	_, err = doRequest(httpReq)

	return err
}

func doRequest(req *http.Request) ([]byte, error) {
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("NON 200 status code: %s", body)
	}

	return body, nil
}
