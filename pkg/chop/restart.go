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
	"os"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
)

// RestartOnConfigChange exits the process — so Kubernetes restarts the
// container — when chopconf auto-restart is enabled
// (watch.configuration.onChange=restart). It is shared by BOTH the
// clickhouse-operator and the metrics-exporter binaries so the gate and the log
// line stay symmetric: the two containers run in the same Pod off the same
// ClickHouseOperatorConfiguration, and each must react to a config change on its
// own. The operator's process exit restarts only the operator container, so the
// exporter wires its own ClickHouseOperatorConfiguration watcher that calls this
// helper — otherwise the exporter keeps serving a stale FIPS/TLS posture until
// the whole Pod is manually recreated.
func RestartOnConfigChange(reason string) {
	if !Config().RestartOnOperatorConfigurationChange() {
		log.V(1).Info("Process restart on configuration change is disabled")
		return
	}

	log.Info("Process restart requested: %s", reason)
	os.Exit(0)
}
