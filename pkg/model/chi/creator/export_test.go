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

package creator

// The sidecar scripts and the probe's timing, for the external tests of the pod template, which cannot import
// managers in-package
var (
	BackupSidecarScript               = backupSidecarScript
	BackupSidecarLivenessScript       = backupSidecarLivenessScript
	BackupSidecarScriptSource         = backupSidecarScriptSource
	BackupSidecarLivenessScriptSource = backupSidecarLivenessScriptSource
)

const (
	BackupLivenessAPITimeoutSeconds = backupLivenessAPITimeoutSeconds
	BackupLivenessQuietSeconds      = backupLivenessQuietSeconds
)

// ShipBackupScripts makes the pod template carry the scripts of these sources, as it does the embedded ones, until
// the returned function restores those
func ShipBackupScripts(sidecar, liveness string) (restore func()) {
	shippedSidecar, shippedLiveness := backupSidecarScript, backupSidecarLivenessScript
	backupSidecarScript, backupSidecarLivenessScript = stripScriptComments(sidecar), stripScriptComments(liveness)
	return func() {
		backupSidecarScript, backupSidecarLivenessScript = shippedSidecar, shippedLiveness
	}
}
