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
	"flag"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopclientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
)

type CHOp struct {
	Version       string
	ConfigManager *ConfigManager
}

func NewCHOp(
	version string,
	chopClient *chopclientset.Clientset,
	initConfigFilePath string,
) *CHOp {
	return &CHOp{
		Version:       version,
		ConfigManager: NewConfigManager(chopClient, initConfigFilePath),
	}
}

func (c *CHOp) Init() error {
	return c.ConfigManager.Init()
}

func (c *CHOp) Config() *v1.OperatorConfig {
	return c.ConfigManager.Config()
}

func (c *CHOp) SetupLog() {
	updated := false
	if c.Config().Logtostderr != "" {
		c.logUpdate("logtostderr", c.Config().Logtostderr)
		updated = true
		_ = flag.Set("logtostderr", c.Config().Logtostderr)
	}
	if c.Config().Alsologtostderr != "" {
		c.logUpdate("alsologtostderr", c.Config().Alsologtostderr)
		updated = true
		_ = flag.Set("alsologtostderr", c.Config().Alsologtostderr)
	}
	if c.Config().Stderrthreshold != "" {
		c.logUpdate("stderrthreshold", c.Config().Stderrthreshold)
		updated = true
		_ = flag.Set("stderrthreshold", c.Config().Stderrthreshold)
	}
	if c.Config().V != "" {
		c.logUpdate("v", c.Config().V)
		updated = true
		_ = flag.Set("v", c.Config().V)
	}

	if updated {
		log.V(1).Info("Additional log options applied")
	}
}

func (c *CHOp) logUpdate(name, value string) {
	log.V(1).Info("Log option '%s' change value from '%s' to '%s'", name, flag.Lookup(name).Value, value)
}
