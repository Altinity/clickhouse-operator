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

// Package fips holds CHI/CHK-shared FIPS validation gates used by the per-CR
// normalizers. The two normalizers were byte-identical except for the status
// reason constants and the per-host image resolver; this package owns the
// common policy logic so behavior cannot drift between CHI and CHK.
package fips

import (
	"fmt"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	utilfips "github.com/altinity/clickhouse-operator/pkg/util/fips"
)

// HostsWalker is the narrow CR-shaped surface RejectFIPSBypass and
// EnforceFIPSImagePolicy actually exercise. Declared here rather than reusing
// chi.ICustomResource so the helper does not silently widen if that umbrella
// interface gains unrelated members.
type HostsWalker interface {
	WalkHosts(func(host *chi.Host) error) []error
}

// AbortFunc reports a FIPS policy violation against the CR. Implementations
// route to the CR's status (ReconcileAbortWithReason) so the operator's
// admission/abort path observes the failure exactly as the inlined CHI/CHK
// code used to. The two arguments mirror the original call:
//   - reason: api.StatusReasonFIPS{ValidationFailed,ImagePolicyViolation}
//   - msg:    free-form, prepended by the status implementation
type AbortFunc func(reason, msg string)

// RejectFIPSBypass aborts the CR via abort when the resolved cluster Security
// explicitly relaxes any FIPS-coerced TLS knob. CHOP-level applyFIPSStrict
// coerces operator-wide defaults, but chi.spec.security and cluster.security
// can still set Verify=None / MinVersion<1.2 — under FIPS that is a bypass
// attempt and must be refused.
//
// Kubernetes-API-client TLS is operator-process-scoped (one client per
// operator pod, gated at startup against the file-based chopconf), so it has
// no per-CR/per-cluster knob to bypass — enforcement lives entirely on
// OperatorConfigSecurity.Kubernetes (see RequiresStrictK8sTLS).
func RejectFIPSBypass(security *chi.ClusterSecurity, abort AbortFunc, validationReason string) {
	if security == nil || abort == nil {
		return
	}
	report := func(field, value string) {
		abort(
			validationReason,
			fmt.Sprintf("FIPS strict: %s=%s is not permitted; remove the field or set Strict", field, value),
		)
	}
	// FIPS-compatible minVersion floor is TLS 1.2 per NIST SP 800-52 Rev 2.
	// Cluster may explicitly state 1.2 or 1.3 without bypass; anything else
	// (i.e. an unrecognized value that survived normalization) is a bypass.
	minVersionBypass := func(v chi.TLSMinVersion) bool {
		if v == "" {
			return false
		}
		return (v != chi.TLSMinVersion12) && (v != chi.TLSMinVersion13)
	}
	verifyBypass := func(v chi.TLSVerify) bool {
		// Anything other than Strict at cluster level is a downgrade attempt
		// under FIPS — including explicit empty ("") and None.
		return v != chi.TLSVerifyStrict
	}
	if tls := security.GetClickHouse().GetTLS(); tls != nil {
		if verifyBypass(tls.GetVerify()) {
			report("cluster.security.clickhouse.tls.verify", string(tls.GetVerify()))
			return
		}
		if minVersionBypass(tls.GetMinVersion()) {
			report("cluster.security.clickhouse.tls.minVersion", string(tls.GetMinVersion()))
			return
		}
	}
	if zk := security.GetZookeeper().GetTLS(); zk != nil {
		if verifyBypass(zk.GetVerify()) {
			report("cluster.security.zookeeper.tls.verify", string(zk.GetVerify()))
			return
		}
		if minVersionBypass(zk.GetMinVersion()) {
			report("cluster.security.zookeeper.tls.minVersion", string(zk.GetMinVersion()))
			return
		}
	}
}

// EnforceFIPSImagePolicy aborts the CR via abort when any host's resolved
// container image lacks the FIPS marker (per pkg/util/fips/image.go). Caller
// is expected to gate on chopconf security.images.policy=Required
// before invoking — this helper assumes that decision already made.
//
// resolveImage extracts the per-host image (PodTemplate container override
// or the per-CR-kind default). containerName names the container the
// operator probes (clickhouse / clickhouse-keeper) and shows up in the abort
// message verbatim.
//
// First non-FIPS host aborts the CR; remaining hosts skip silently so the
// error stream carries one tagged entry per CR, not N (WalkHosts ignores
// callback errors, so a returned error wouldn't actually short-circuit).
func EnforceFIPSImagePolicy(
	target HostsWalker,
	resolveImage func(host *chi.Host) string,
	containerName string,
	imagePolicyReason string,
	abort AbortFunc,
) {
	if target == nil || resolveImage == nil || abort == nil {
		return
	}
	aborted := false
	target.WalkHosts(func(host *chi.Host) error {
		if aborted {
			return nil
		}
		image := resolveImage(host)
		if utilfips.ImageHasFIPSMarker(image) {
			return nil
		}
		abort(
			imagePolicyReason,
			fmt.Sprintf("host %s container %q image %q lacks 'fips' tag substring (security.images.policy=Required)",
				host.GetName(), containerName, image),
		)
		aborted = true
		return nil
	})
}
