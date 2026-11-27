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

// The sidecar script, for the external tests of the pod template, which cannot import managers in-package
var (
	BackupSidecarScript       = backupSidecarScript
	BackupSidecarScriptSource = backupSidecarScriptSource
)

// ShipBackupScript makes the pod template carry the script of this source, as it does the embedded one, until the
// returned function restores that
func ShipBackupScript(sidecar string) (restore func()) {
	shipped := backupSidecarScript
	backupSidecarScript = stripScriptComments(sidecar)
	return func() {
		backupSidecarScript = shipped
	}
}
