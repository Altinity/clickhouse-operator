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
	"fmt"

	kube "k8s.io/client-go/kubernetes"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopclientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
)

// CHOp defines ClickHouse Operator
type CHOp struct {
	// Version specifies version of the operator
	Version string
	// Commit specifies git commit of the operator
	Commit string
	// Date specified date when operator was built
	Date string
	// ConfigManager specifies configuration manager in charge of operator's configuration
	ConfigManager *ConfigManager
}

// NewCHOp creates new CHOp
func NewCHOp(
	version string,
	commit string,
	date string,
	kubeClient *kube.Clientset,
	chopClient *chopclientset.Clientset,
	initConfigFilePath string,
) *CHOp {
	return &CHOp{
		Version:       version,
		Commit:        commit,
		Date:          date,
		ConfigManager: NewConfigManager(kubeClient, chopClient, initConfigFilePath),
	}
}

// Init initializes CHOp
func (c *CHOp) Init() error {
	if c == nil {
		return fmt.Errorf("chop not created")
	}
	return c.ConfigManager.Init()
}

// Config returns operator config
func (c *CHOp) Config() *v1.OperatorConfig {
	if c == nil {
		return nil
	}
	return c.ConfigManager.Config()
}

// SetupLog sets up loggging options
func (c *CHOp) SetupLog() {
	updated := false
	if c.Config().Logger.LogToStderr != "" {
		c.logUpdate("logtostderr", c.Config().Logger.LogToStderr)
		updated = true
		_ = flag.Set("logtostderr", c.Config().Logger.LogToStderr)
	}
	if c.Config().Logger.AlsoLogToStderr != "" {
		c.logUpdate("alsologtostderr", c.Config().Logger.AlsoLogToStderr)
		updated = true
		_ = flag.Set("alsologtostderr", c.Config().Logger.AlsoLogToStderr)
	}
	if c.Config().Logger.StderrThreshold != "" {
		c.logUpdate("stderrthreshold", c.Config().Logger.StderrThreshold)
		updated = true
		_ = flag.Set("stderrthreshold", c.Config().Logger.StderrThreshold)
	}
	if c.Config().Logger.V != "" {
		c.logUpdate("v", c.Config().Logger.V)
		updated = true
		_ = flag.Set("v", c.Config().Logger.V)
	}

	if updated {
		log.V(1).Info("Additional log options applied")
	}
}

// logUpdate
func (c *CHOp) logUpdate(name, value string) {
	log.V(1).Info("Log option '%s' change value from '%s' to '%s'", name, flag.Lookup(name).Value, value)
}
