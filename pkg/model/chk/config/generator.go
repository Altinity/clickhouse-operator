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
	"bytes"
	"strings"

	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
	"github.com/altinity/clickhouse-operator/pkg/xml"
)

type Generator struct {
	cr    apiChi.ICustomResource
	namer interfaces.INameManager
	opts  *GeneratorOptions
}

func newGenerator(cr apiChi.ICustomResource, namer interfaces.INameManager, opts *GeneratorOptions) *Generator {
	return &Generator{
		cr:    cr,
		namer: namer,
		opts:  opts,
	}
}

func getServerId(host *apiChi.Host) int {
	return host.GetRuntime().GetAddress().GetReplicaIndex() + 1
}

func (c *Generator) getHostConfig(host *apiChi.Host, settings *apiChi.Settings) string {
	if settings.Len() == 0 {
		return ""
	}

	// Init container will replace this macro with real value on bootstrap
	serverId, _ := apiChi.NewSettingScalarFromAny(getServerId(host))
	settings.Set("keeper_server/server_id", serverId)

	// Prepare empty placeholder for RAFT config and will be replaced later in this function:
	// <raft_configuration>
	//     <server></server>
	// </raft_configuration>
	settings.Set("keeper_server/raft_configuration/server", apiChi.NewSettingScalar(""))
	// 3-rd level (keeper_server/raft_configuration/server) by 4 spaces
	raftIndent := 12
	// Prepare RAFT config for injection into main config
	raft := &bytes.Buffer{}
	host.GetShard().WalkHosts(func(_host *apiChi.Host) error {
		util.Iline(raft, raftIndent, "<server>")
		util.Iline(raft, raftIndent, "    <id>%d</id>", getServerId(_host))
		util.Iline(raft, raftIndent, "    <hostname>%s</hostname>", c.namer.Name(interfaces.NameInstanceHostname, _host))
		util.Iline(raft, raftIndent, "    <port>%d</port>", _host.RaftPort.Value())
		util.Iline(raft, raftIndent, "</server>")
		return nil
	})

	// <clickhouse>
	// 		CONFIG
	// </clickhouse>
	config := &bytes.Buffer{}
	util.Iline(config, 0, "<clickhouse>")
	xml.GenerateFromSettings(config, settings, "")
	util.Iline(config, 0, "</clickhouse>")

	// tmp = strings.Replace(tmp, "<server_id></server_id>", "<server_id from_env=\"KEEPER_ID\" />", 1)

	// Inject RAFT config
	// Replace empty placeholder for RAFT config with actual RAFT config value
	// <raft_configuration>
	//     <server></server>
	// </raft_configuration>
	// TODO use raftIndent value here instead of spaces
	return strings.Replace(config.String(), "            <server></server>\n", raft.String(), 1)
}
