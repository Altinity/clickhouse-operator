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

// fipsGate logs the FIPS posture and Fatals when the chopconf asks for
// strict FIPS but the binary/runtime cannot deliver it. Idempotent; safe to
// invoke from each binary's init path after chop.New has populated the
// config. Per-CHI rejection (insecure CH/K8s/ZK settings) runs in normalize
// via applyFIPSStrict + rejectFIPSBypass; this gate covers the operator-wide
// preconditions: was the binary built with GOFIPS140, and is crypto/fips140
// enforced at runtime via GODEBUG=fips140=only. The gate trigger is
// security.fips.enforced=true (NOT the broader security.policy=Enforced):
// the cryptographic-module assertion is independent of the Enforced policy
// hardening. Pure decision lives in pkg/util/fips so the metrics-exporter
// binary can share it.
func fipsGate() {
	fipsEnforced := chop.Config().Security.GetFIPS().IsEnforced()
	// buildLinked is the durable "GOFIPS140-linked" property derived from
	// runtime/debug.BuildInfo settings — independent of GODEBUG runtime mode.
	// fips.Enabled() ("module active right now") is reported in the banner
	// for diagnostics but is NOT the gate input: under GODEBUG=fips140=off
	// (the shipped default) Enabled() returns false even on a properly
	// GOFIPS140-linked binary, which would Fatal the pod incorrectly.
	buildLinked := fips.BuildSetting("GOFIPS140") != ""
	moduleActive, runtime := fips.Enabled(), fips.Enforced()
	log.F().Info("FIPS: chopconf.fips.enforced=%t build.linked=%t module.active=%t runtime.enforced=%t module=%s", fipsEnforced, buildLinked, moduleActive, runtime, fips.Version())
	log.F().Info("FIPS env: GODEBUG=%q DefaultGODEBUG=%q GOFIPS140=%q",
		fips.GODEBUGRaw(),
		fips.BuildSetting("DefaultGODEBUG"),
		fips.BuildSetting("GOFIPS140"))
	err, warn := fips.EvaluateGate("clickhouse-operator", fipsEnforced, buildLinked, runtime, fips.BuildSetting("DefaultGODEBUG"))
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
		// DSNs that didn't go through setupTLSAdvanced (no explicit security
		// knobs) thus get verified TLS instead of the default-insecure
		// pre-0.27.1 fallback. Under security.fips.enforced=true,
		// InsecureSkipVerify is rejected (verified TLS required).
		chclient.EnforceVerifiedLegacyTLS()
	}
}
