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

package v1

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// Config overrides are clickhouse-backup settings a backup is made with in place of those of the sidecar, as
// KEY=VALUE of the env var clickhouse-backup knows the setting by, e.g. S3_STORAGE_CLASS=STANDARD_IA, as the Altinity
// Cloud Manager makes a backup with --env S3_STORAGE_CLASS=...: the Job runs create_remote through the actions
// endpoint of the sidecar with an --env flag of each, which only that command takes.
const (
	// BackupConfigOverridesMaxItems bounds the overrides of an entry or a backup. Mirrored by the CRD schema maxItems.
	BackupConfigOverridesMaxItems = 16
	// BackupConfigOverrideFlag is the flag a config override may be written with, which the operator strips: it
	// renders every override as that flag.
	BackupConfigOverrideFlag = "--env="
	// backupConfigOverrideKeyMaxLength and backupConfigOverrideValueMaxLength bound the key and the value of an
	// override, as backupConfigOverrideRegexp does
	backupConfigOverrideKeyMaxLength   = 64
	backupConfigOverrideValueMaxLength = 256
)

var (
	// backupConfigOverrideRegexp bounds a config override, without the flag: every override is a single token of a
	// command, without a blank, a quote or anything a shell or JSON escapes. Mirrored by the CRD schema pattern, which
	// takes the flag too, and by the Job script.
	backupConfigOverrideRegexp = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}=[A-Za-z0-9_.,:/@+=-]{1,256}$`)
	// BackupConfigOverrideForbidden are the settings a config override must not set: those the operator pins in
	// the sidecar or sets there, the credentials of the configuration file, and the settings that tell where the
	// backups are, which the listings, the retention and the restores of the backup, made with the settings of the
	// sidecar, would not find it by. Mirrored by the Job script.
	BackupConfigOverrideForbidden = []string{
		// Pinned in the sidecar, see backupPinnedEnvVarNames of the creator
		"API_LISTEN", "API_SECURE", "API_ALLOW_PARALLEL", "RESTORE_SCHEMA_ON_CLUSTER", "CLICKHOUSE_RESTART_COMMAND",
		"RESTORE_DATABASE_MAPPING", "RESTORE_TABLE_MAPPING", "BACKUPS_TO_KEEP_REMOTE", "REBASE_DURING_DELETE",
		"REBASE_BEFORE_REMOVE_OLD_REMOTE",
		// The credentials and the file of the configuration, see backupConfigFileEnvVarNames of the creator
		"API_USERNAME", "API_PASSWORD", "CLICKHOUSE_USERNAME", "CLICKHOUSE_PASSWORD", "CLICKHOUSE_BACKUP_CONFIG",
		// Set by the operator
		"CLICKHOUSE_HOST", "CLICKHOUSE_PORT", "CLICKHOUSE_SECURE", "CLICKHOUSE_SKIP_VERIFY", "BACKUPS_TO_KEEP_LOCAL",
		"ALLOW_EMPTY_BACKUPS", "API_CREATE_INTEGRATION_TABLES",
		// Where the backups are, see PathCheckKeys of the backup model
		"REMOTE_STORAGE",
		"S3_BUCKET", "S3_ENDPOINT", "S3_PATH", "S3_OBJECT_DISK_PATH",
		"GCS_BUCKET", "GCS_ENDPOINT", "GCS_PATH", "GCS_OBJECT_DISK_PATH",
		"AZBLOB_ACCOUNT_NAME", "AZBLOB_CONTAINER", "AZBLOB_ENDPOINT_SUFFIX", "AZBLOB_PATH", "AZBLOB_OBJECT_DISK_PATH",
		"COS_URL", "COS_PATH", "COS_OBJECT_DISK_PATH",
		"FTP_ADDRESS", "FTP_PATH", "FTP_OBJECT_DISK_PATH",
		"SFTP_ADDRESS", "SFTP_PORT", "SFTP_PATH", "SFTP_OBJECT_DISK_PATH",
	}
	// backupConfigOverrideForbiddenPrefixes start the keys of settings a config override must not set: those of the
	// REST API, which the operator configures.
	backupConfigOverrideForbiddenPrefixes = []string{"API_"}
	// BackupConfigOverrideSensitiveTokens are the parts of the keys of the settings clickhouse-backup takes for
	// secrets, whose values it masks in the command it logs, sensitiveEnvNameTokens of its configuration: a secret has
	// no place in a custom resource, and a masked command would not be the one the Job follows the action of.
	// Mirrored by the Job script.
	BackupConfigOverrideSensitiveTokens = []string{
		"PASSWORD", "SECRET", "ACCESS_KEY", "SECRET_KEY", "ACCOUNT_KEY", "SSE_KEY", "SSE_CUSTOMER_KEY", "ENCRYPTION_KEY",
		"TOKEN", "CREDENTIAL", "PRIVATE_KEY", "SAS", "CONNECTION_STRING",
	}
)

// ConfigOverrides returns the overrides as the operator renders them, without the --env= flag any is written with.
func ConfigOverrides(items []string) []string {
	var overrides []string
	for _, item := range items {
		overrides = append(overrides, strings.TrimPrefix(item, BackupConfigOverrideFlag))
	}
	return overrides
}

// ValidateConfigOverrides checks the config overrides of the field: at most BackupConfigOverridesMaxItems, each
// KEY=VALUE, with or without the --env= flag, as backupConfigOverrideRegexp bounds it, of keys none of which is twice
// or forbidden.
func ValidateConfigOverrides(field string, items []string) error {
	if len(items) > BackupConfigOverridesMaxItems {
		return fmt.Errorf("%w: %s holds more than %d overrides", ErrInvalidBackupSpec, field, BackupConfigOverridesMaxItems)
	}
	keys := map[string]bool{}
	for _, item := range ConfigOverrides(items) {
		if !backupConfigOverrideRegexp.MatchString(item) {
			return fmt.Errorf("%w: %s %q must be KEY=VALUE, the env var of a clickhouse-backup setting, matching %s",
				ErrInvalidBackupSpec, field, item, backupConfigOverrideRegexp)
		}
		key := item[:strings.Index(item, "=")]
		if keys[key] {
			return fmt.Errorf("%w: %s sets %s twice", ErrInvalidBackupSpec, field, key)
		}
		keys[key] = true
		if reason := forbiddenConfigOverride(key); reason != "" {
			return fmt.Errorf("%w: %s must not set %s: %s", ErrInvalidBackupSpec, field, key, reason)
		}
	}
	return nil
}

// forbiddenConfigOverride tells why a config override must not set the setting key, "" when it may.
func forbiddenConfigOverride(key string) string {
	if slices.Contains(BackupConfigOverrideForbidden, key) {
		return "the operator sets it, or it tells where the backups are, which the restores and the retention of the " +
			"backup would not find it by"
	}
	for _, prefix := range backupConfigOverrideForbiddenPrefixes {
		if strings.HasPrefix(key, prefix) {
			return "the operator configures the REST API"
		}
	}
	for _, token := range BackupConfigOverrideSensitiveTokens {
		if strings.Contains(key, token) {
			return "it is a secret, which belongs in the Secret of the clickhouse-backup configuration"
		}
	}
	return ""
}
