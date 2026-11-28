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

const (
	BackupConfigOverridesMaxItems = 16 // keep in sync with the CRD maxItems
	BackupConfigOverrideFlag      = "--env="
	// Keep in sync with backupConfigOverrideRegexp
)

var (
	// The CRD pattern is the same but also allows the --env= prefix
	backupConfigOverrideRegexp = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}=[A-Za-z0-9_.,:/@+=-]{1,256}$`)
	// Overriding where backups are would hide the backup from listing, retention and restore, which use the sidecar settings
	BackupConfigOverrideForbidden = []string{
		// See backupPinnedEnvVarNames
		"API_LISTEN", "API_SECURE", "API_ALLOW_PARALLEL", "RESTORE_SCHEMA_ON_CLUSTER", "CLICKHOUSE_RESTART_COMMAND",
		"RESTORE_DATABASE_MAPPING", "RESTORE_TABLE_MAPPING", "BACKUPS_TO_KEEP_REMOTE", "REBASE_DURING_DELETE",
		"REBASE_BEFORE_REMOVE_OLD_REMOTE", "TZ",
		// See backupConfigFileEnvVarNames
		"API_USERNAME", "API_PASSWORD", "CLICKHOUSE_USERNAME", "CLICKHOUSE_PASSWORD", "CLICKHOUSE_BACKUP_CONFIG",
		"CLICKHOUSE_SKIP_TABLES", "CLICKHOUSE_SKIP_TABLE_ENGINES",
		"CLICKHOUSE_HOST", "CLICKHOUSE_PORT", "CLICKHOUSE_SECURE", "CLICKHOUSE_SKIP_VERIFY", "BACKUPS_TO_KEEP_LOCAL",
		"ALLOW_EMPTY_BACKUPS", "API_CREATE_INTEGRATION_TABLES", "API_INTEGRATION_TABLES_HOST",
		// See PathCheckKeys
		"REMOTE_STORAGE",
		"S3_BUCKET", "S3_ENDPOINT", "S3_PATH", "S3_OBJECT_DISK_PATH",
		"GCS_BUCKET", "GCS_ENDPOINT", "GCS_PATH", "GCS_OBJECT_DISK_PATH",
		"AZBLOB_ACCOUNT_NAME", "AZBLOB_CONTAINER", "AZBLOB_ENDPOINT_SUFFIX", "AZBLOB_PATH", "AZBLOB_OBJECT_DISK_PATH",
		"COS_URL", "COS_PATH", "COS_OBJECT_DISK_PATH",
		"FTP_ADDRESS", "FTP_PATH", "FTP_OBJECT_DISK_PATH",
		"SFTP_ADDRESS", "SFTP_PORT", "SFTP_PATH", "SFTP_OBJECT_DISK_PATH",
		// Delete reads the embedded backup disk from the sidecar settings
		"CLICKHOUSE_EMBEDDED_BACKUP_DISK",
	}
	BackupConfigOverrideSecurity = []string{
		"S3_ACL", "S3_DISABLE_CERT_VERIFICATION", "S3_DISABLE_SSL", "S3_SSE", "S3_SSE_KMS_KEY_ID",
		"S3_SSE_CUSTOMER_ALGORITHM", "S3_SSE_KMS_ENCRYPTION_CONTEXT", "S3_ASSUME_ROLE_ARN",
		"GCS_FORCE_HTTP", "GCS_SKIP_CREDENTIALS",
		"AZBLOB_ENDPOINT_SCHEMA", "AZBLOB_USE_MANAGED_IDENTITY",
		"FTP_TLS", "FTP_SKIP_TLS_VERIFY",
		"SFTP_KNOWN_HOSTS", "SFTP_KEY",
		"CLICKHOUSE_TLS_CA", "CLICKHOUSE_TLS_CERT", "CLICKHOUSE_TLS_KEY",
	}
	backupConfigOverrideForbiddenPrefixes = []string{"API_"}
	// clickhouse-backup masks these (sensitiveEnvNameTokens) in the logged command, so system.backup_actions would not match it
	BackupConfigOverrideSensitiveTokens = []string{
		"PASSWORD", "SECRET", "ACCESS_KEY", "SECRET_KEY", "ACCOUNT_KEY", "SSE_KEY", "SSE_CUSTOMER_KEY", "ENCRYPTION_KEY",
		"TOKEN", "CREDENTIAL", "PRIVATE_KEY", "SAS", "CONNECTION_STRING",
	}
)

func ConfigOverrides(items []string) []string {
	var overrides []string
	for _, item := range items {
		overrides = append(overrides, strings.TrimPrefix(item, BackupConfigOverrideFlag))
	}
	return overrides
}

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

func forbiddenConfigOverride(key string) string {
	switch key {
	case "CLICKHOUSE_SKIP_TABLES":
		return "set the tables to leave out with the skipTables field, which keeps the system databases left out"
	case "CLICKHOUSE_SKIP_TABLE_ENGINES":
		return "set the table engines to leave out with the skipTableEngines field"
	}
	if slices.Contains(BackupConfigOverrideForbidden, key) {
		return "the operator sets it, or it tells where the backups are, which the restores and the retention of the " +
			"backup would not find it by"
	}
	if slices.Contains(BackupConfigOverrideSecurity, key) {
		return "it sets the security of the storage or of the connection to it, which belongs in the Secret of the " +
			"clickhouse-backup configuration"
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
