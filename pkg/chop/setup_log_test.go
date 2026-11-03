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
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// restoreFlags snapshots glog's process-wide flags so a test that drives SetupLog cannot leak its
// values into the rest of the package's tests.
func restoreFlags(t *testing.T, names ...string) {
	t.Helper()
	saved := make(map[string]string, len(names))
	for _, n := range names {
		f := flag.Lookup(n)
		require.NotNil(t, f, "glog must register the %q flag - SetupLog's logUpdate dereferences flag.Lookup", n)
		saved[n] = f.Value.String()
	}
	t.Cleanup(func() {
		for n, v := range saved {
			_ = flag.Set(n, v)
		}
	})
}

// logger.vmodule and logger.log_backtrace_at were parsed, normalized, present in the CRD and in
// both shipped YAMLs, but SetupLog never applied them - so setting either did nothing at all.
func TestSetupLogAppliesVModuleAndLogBacktraceAt(t *testing.T) {
	restoreFlags(t, "vmodule", "log_backtrace_at")

	c := &CHOp{ConfigManager: &ConfigManager{config: &api.OperatorConfig{}}}
	c.Config().Logger.VModule = "worker=3"
	c.Config().Logger.LogBacktraceAt = "worker.go:42"

	c.SetupLog()

	require.Equal(t, "worker=3", flag.Lookup("vmodule").Value.String(),
		"logger.vmodule must reach glog's vmodule flag")
	require.Equal(t, "worker.go:42", flag.Lookup("log_backtrace_at").Value.String(),
		"logger.log_backtrace_at must reach glog's log_backtrace_at flag")
}

// An unset option must not touch the corresponding flag - SetupLog guards every option on
// non-empty, so an operator that configures none of them keeps glog's defaults.
func TestSetupLogLeavesUnsetOptionsAlone(t *testing.T) {
	restoreFlags(t, "vmodule", "log_backtrace_at")

	require.NoError(t, flag.Set("vmodule", "preset=1"))

	c := &CHOp{ConfigManager: &ConfigManager{config: &api.OperatorConfig{}}}
	c.SetupLog()

	require.Equal(t, "preset=1", flag.Lookup("vmodule").Value.String(),
		"an empty logger.vmodule must not clear an already-set flag")
}
