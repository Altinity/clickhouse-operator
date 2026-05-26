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

package app

import (
	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	chclient "github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
	"github.com/altinity/clickhouse-operator/pkg/util/fips"
)

// fipsGate is the metrics-exporter mirror of the operator gate. The exporter
// ships in the same Pod as the operator (shared image, shared chopconf), so
// build parity is guaranteed; this gate catches the runtime ENV mismatch
// case (e.g. GODEBUG=fips140=only set on operator container but not on the
// exporter container). The gate trigger is security.fips.enforced=true
// (NOT the broader security.policy=Enforced): the cryptographic-module
// assertion is independent of the Enforced policy hardening. Pure decision
// lives in pkg/util/fips.
func fipsGate() {
	fipsEnforced := chop.Config().Security.GetFIPS().IsEnforced()
	build, runtime := fips.Enabled(), fips.Enforced()
	log.F().Info("FIPS: chopconf.fips.enforced=%t build.enabled=%t runtime.enforced=%t module=%s", fipsEnforced, build, runtime, fips.Version())
	log.F().Info("FIPS env: GODEBUG=%q DefaultGODEBUG=%q GOFIPS140=%q",
		fips.GODEBUGRaw(),
		fips.BuildSetting("DefaultGODEBUG"),
		fips.BuildSetting("GOFIPS140"))
	err, warn := fips.EvaluateGate("metrics-exporter", fipsEnforced, build, runtime, fips.BuildSetting("DefaultGODEBUG"))
	if err != nil {
		log.F().Fatal(err.Error())
	}
	if warn != "" {
		log.F().Warning(warn)
	}
	if chop.Config().Security.RequiresHardening() {
		// Either-switch: verified-TLS hardening fires when EITHER the broad
		// security.policy=Enforced OR the narrow security.fips.enforced=true
		// is set. Both imply we want verified TLS for legacy DSNs.
		// Re-register the legacy InsecureSkipVerify=true ClickHouse TLS config
		// (from connection.init() / setupTLSBasic) with a verifying config.
		// The exporter dials ClickHouse via the same package as the operator
		// and shares the global TLS registry. Mirror of the operator-side
		// enforcement in cmd/operator/app/fips_gate.go.
		chclient.EnforceVerifiedLegacyTLS()
	}
}
