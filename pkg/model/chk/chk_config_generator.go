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

package chk

import (
	"bytes"
	"fmt"
	"strings"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

	"github.com/altinity/clickhouse-operator/pkg/util"
	"github.com/altinity/clickhouse-operator/pkg/xml"
)

func defaultKeeperSettings(path string) map[string]string {
	return map[string]string{
		"logger/level":   "information",
		"logger/console": "1",

		"listen_host":     "0.0.0.0",
		"max_connections": "4096",

		"keeper_server/tcp_port":                                     "9181",
		"keeper_server/storage_path":                                 path,
		"keeper_server/log_storage_path":                             fmt.Sprintf("%s/coordination/logs", path),
		"keeper_server/snapshot_storage_path":                        fmt.Sprintf("%s/coordination/snapshots", path),
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
	}
}

// generateXMLConfig creates XML using map[string]string definitions
func generateXMLConfig(_settings map[string]string, chk *apiChk.ClickHouseKeeperInstallation) string {
	if len(_settings) == 0 {
		return ""
	}

	settings := apiChi.NewSettings()
	settings.SetScalarsFromMap(_settings)
	settings.Set("keeper_server/server_id", apiChi.NewSettingScalar("KEEPER_ID"))
	settings.Set("keeper_server/raft_configuration/server", apiChi.NewSettingScalar(""))

	b := &bytes.Buffer{}
	// <clickhouse>
	// XML code
	// </clickhouse>
	util.Iline(b, 0, "<clickhouse>")
	xml.GenerateFromSettings(b, settings, "")
	util.Iline(b, 0, "</clickhouse>")

	raft := &bytes.Buffer{}
	raftPort := chk.Spec.GetRaftPort()
	for i := 0; i < int(chk.Spec.GetReplicas()); i++ {
		util.Iline(raft, 12, "<server>")
		util.Iline(raft, 12, "    <id>%d</id>", i)
		util.Iline(raft, 12, "    <hostname>%s-%d.%s-headless.%s.svc.cluster.local</hostname>", chk.Name, i, chk.Name, chk.Namespace)
		util.Iline(raft, 12, "    <port>%s</port>", fmt.Sprintf("%d", raftPort))
		util.Iline(raft, 12, "</server>")
	}

	tmp := b.String()

	// tmp = strings.Replace(tmp, "<server_id></server_id>", "<server_id from_env=\"KEEPER_ID\" />", 1)

	return strings.Replace(tmp, "            <server></server>\n", raft.String(), 1)
}
