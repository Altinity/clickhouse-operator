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
	"github.com/altinity/clickhouse-operator/pkg/util"
	"github.com/altinity/clickhouse-operator/pkg/xml"
)

type GeneratorKeeper struct {
	cr   apiChi.ICustomResource
	opts *GeneratorOptions
}

func newConfigGeneratorKeeper(cr apiChi.ICustomResource, opts *GeneratorOptions) *GeneratorKeeper {
	return &GeneratorKeeper{
		cr:   cr,
		opts: opts,
	}
}

func (c *GeneratorKeeper) getConfig(settings *apiChi.Settings) string {
	if settings.Len() == 0 {
		return ""
	}

	// Init container will replace this macro with real value on bootstrap
	settings.Set("keeper_server/server_id", apiChi.NewSettingScalar("KEEPER_ID"))

	// Prepare empty placeholder for RAFT config and will be replaced later in this function:
	// <raft_configuration>
	//     <server></server>
	// </raft_configuration>
	settings.Set("keeper_server/raft_configuration/server", apiChi.NewSettingScalar(""))
	// 3-rd level (keeper_server/raft_configuration/server) by 4 spaces
	raftIndent := 12
	// Prepare RAFT config for injection into main config
	raft := &bytes.Buffer{}
	for i := 0; i < c.opts.ReplicasCount; i++ {
		util.Iline(raft, raftIndent, "<server>")
		util.Iline(raft, raftIndent, "    <id>%d</id>", i)
		util.Iline(raft, raftIndent, "    <hostname>%s-%d.%s-headless.%s.svc.cluster.local</hostname>", c.cr.GetName(), i, c.cr.GetName(), c.cr.GetNamespace())
		util.Iline(raft, raftIndent, "    <port>%d</port>", c.opts.RaftPort)
		util.Iline(raft, raftIndent, "</server>")
	}

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
