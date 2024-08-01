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

package config

import (
	"fmt"
	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

func DefaultSettings(storagePath string) *apiChi.Settings {
	settings := apiChi.NewSettings()
	settings.SetScalarsFromMap(
		map[string]string{
			"logger/level":   "information",
			"logger/console": "1",

			"listen_host":     "0.0.0.0",
			"max_connections": "4096",

			"keeper_server/tcp_port":                                     "9181",
			"keeper_server/storage_path":                                 storagePath,
			"keeper_server/log_storage_path":                             fmt.Sprintf("%s/coordination/logs", storagePath),
			"keeper_server/snapshot_storage_path":                        fmt.Sprintf("%s/coordination/snapshots", storagePath),
			"keeper_server/coordination_settings/operation_timeout_ms":   "10000",
			"keeper_server/coordination_settings/min_session_timeout_ms": "10000",
			"keeper_server/coordination_settings/session_timeout_ms":     "100000",
			"keeper_server/coordination_settings/raft_logs_level":        "information",
			"keeper_server/hostname_checks_enabled":                      "true",

			"openSSL/server/certificateFile":     "/etc/clickhouse-keeper/server.crt",
			"openSSL/server/privateKeyFile":      "/etc/clickhouse-keeper/server.key",
			"openSSL/server/dhParamsFile":        "/etc/clickhouse-keeper/dhparam.pem",
			"openSSL/server/verificationMode":    "none",
			"openSSL/server/loadDefaultCAFile":   "true",
			"openSSL/server/cacheSessions":       "true",
			"openSSL/server/disableProtocols":    "sslv2,sslv3",
			"openSSL/server/preferServerCiphers": "true",
		},
	)
	return settings
}
