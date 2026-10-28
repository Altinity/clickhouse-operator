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

// Package backupconfig builds the configuration file of the operator-managed clickhouse-backup sidecar. The
// sidecar reads it from a Secret the operator generates and mounts, and clickhouse-backup reads it anew for every
// operation it runs, so a change of the storage settings reaches a running sidecar without restarting it.
package backupconfig

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	"gopkg.in/yaml.v3"
)

// FileName is the name of the configuration file: the key of the storage Secret that holds the storage settings,
// the key of the generated Secret that holds the whole file, and the file the sidecar reads.
const FileName = "config.yml"

// APIFingerprintFileName is the name of the file that holds the fingerprint of the credentials of the REST API the
// configuration file sets. The sidecar restarts its server when it changes: clickhouse-backup checks the credentials
// of a request against the configuration it last loaded, which it loads anew only for a request it lets in.
const APIFingerprintFileName = "api-credentials.sha256"

// ErrInvalidStorageConfig is returned when the storage settings are no YAML mapping of the sections of the
// configuration file of clickhouse-backup.
var ErrInvalidStorageConfig = errors.New("invalid backup storage configuration")

// Sections and keys of the configuration file of clickhouse-backup
const (
	sectionGeneral    = "general"
	sectionClickHouse = "clickhouse"
	sectionAPI        = "api"

	keyDisableEnvironmentOverride = "disable_environment_override"
	keyUsername                   = "username"
	keyPassword                   = "password"
	keyPath                       = "path"
	keyObjectDiskPath             = "object_disk_path"
)

// storageSections are the sections of the storage types of clickhouse-backup, each of which takes a path of the
// backups and one of the object disk data of the backups
var storageSections = []string{"s3", "gcs", "azblob", "cos", "ftp", "sftp"}

// Settings maps the env var a setting of clickhouse-backup is known by to its section and key in the configuration
// file: the storage path check of the backup Jobs reads them by their env var names.
var Settings = map[string]Setting{
	"REMOTE_STORAGE":          {"general", "remote_storage"},
	"S3_BUCKET":               {"s3", "bucket"},
	"S3_ENDPOINT":             {"s3", "endpoint"},
	"S3_PATH":                 {"s3", keyPath},
	"S3_OBJECT_DISK_PATH":     {"s3", keyObjectDiskPath},
	"GCS_BUCKET":              {"gcs", "bucket"},
	"GCS_ENDPOINT":            {"gcs", "endpoint"},
	"GCS_PATH":                {"gcs", keyPath},
	"GCS_OBJECT_DISK_PATH":    {"gcs", keyObjectDiskPath},
	"AZBLOB_ACCOUNT_NAME":     {"azblob", "account_name"},
	"AZBLOB_CONTAINER":        {"azblob", "container"},
	"AZBLOB_ENDPOINT_SUFFIX":  {"azblob", "endpoint_suffix"},
	"AZBLOB_PATH":             {"azblob", keyPath},
	"AZBLOB_OBJECT_DISK_PATH": {"azblob", keyObjectDiskPath},
	"COS_URL":                 {"cos", "url"},
	"COS_PATH":                {"cos", keyPath},
	"COS_OBJECT_DISK_PATH":    {"cos", keyObjectDiskPath},
	"FTP_ADDRESS":             {"ftp", "address"},
	"FTP_PATH":                {"ftp", keyPath},
	"FTP_OBJECT_DISK_PATH":    {"ftp", keyObjectDiskPath},
	"SFTP_ADDRESS":            {"sftp", "address"},
	"SFTP_PORT":               {"sftp", "port"},
	"SFTP_PATH":               {"sftp", keyPath},
	"SFTP_OBJECT_DISK_PATH":   {"sftp", keyObjectDiskPath},
}

// Setting is where the configuration file holds a setting
type Setting struct {
	Section string
	Key     string
}

// Credentials are what the sidecar authenticates with: the ones of its REST API, which the backup Jobs call it
// with, and the ones it connects to ClickHouse with
type Credentials struct {
	APIUsername        string
	APIPassword        string
	ClickHouseUsername string
	ClickHousePassword string
}

// Defaults are the storage paths a configuration file takes where the storage settings name none
type Defaults struct {
	Path           string
	ObjectDiskPath string
}

// Config is a configuration file of the sidecar
type Config struct {
	// File is the configuration file
	File []byte
	// Settings holds, by env var name, the value of every setting of Settings the file sets
	Settings map[string]string
	// APIFingerprint is the fingerprint of the credentials of the REST API the file sets, never the credentials
	APIFingerprint string
}

// Build builds the configuration file of the sidecar from the storage settings, a YAML mapping of the sections of
// the configuration file of clickhouse-backup, empty for none. The file takes the default paths of every storage
// type the settings name none of, and the credentials over whatever the settings hold. The operator sets what the
// backup Jobs rely on as env vars of the sidecar, which take precedence over the file: the file never turns that
// off.
func Build(storageConfig []byte, credentials Credentials, defaults Defaults) (*Config, error) {
	file := map[string]any{}
	if err := yaml.Unmarshal(storageConfig, &file); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidStorageConfig, err)
	}
	if file == nil {
		file = map[string]any{}
	}

	for _, name := range storageSections {
		section, err := sectionOf(file, name)
		if err != nil {
			return nil, err
		}
		setDefault(section, keyPath, defaults.Path)
		setDefault(section, keyObjectDiskPath, defaults.ObjectDiskPath)
	}

	general, err := sectionOf(file, sectionGeneral)
	if err != nil {
		return nil, err
	}
	general[keyDisableEnvironmentOverride] = false

	api, err := sectionOf(file, sectionAPI)
	if err != nil {
		return nil, err
	}
	api[keyUsername] = credentials.APIUsername
	api[keyPassword] = credentials.APIPassword

	clickHouse, err := sectionOf(file, sectionClickHouse)
	if err != nil {
		return nil, err
	}
	clickHouse[keyUsername] = credentials.ClickHouseUsername
	clickHouse[keyPassword] = credentials.ClickHousePassword

	settings := map[string]string{}
	for name, setting := range Settings {
		value, found, err := settingOf(file, setting)
		if err != nil {
			return nil, err
		}
		if found {
			settings[name] = value
		}
	}

	// Marshalling sorts the keys of every mapping, so equal settings always make an equal file
	out, err := yaml.Marshal(file)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidStorageConfig, err)
	}
	return &Config{File: out, Settings: settings, APIFingerprint: apiFingerprint(credentials)}, nil
}

// apiFingerprint is the SHA-256 of the credentials of the REST API, which tells whether they changed
func apiFingerprint(credentials Credentials) string {
	sum := sha256.Sum256([]byte(credentials.APIUsername + "\x00" + credentials.APIPassword))
	return hex.EncodeToString(sum[:])
}

// sectionOf returns the section of the file, adding it when the file has none. A section the file sets null
// counts as none.
func sectionOf(file map[string]any, name string) (map[string]any, error) {
	switch section := file[name].(type) {
	case map[string]any:
		return section, nil
	case nil:
		added := map[string]any{}
		file[name] = added
		return added, nil
	default:
		return nil, fmt.Errorf("%w: %s is no mapping", ErrInvalidStorageConfig, name)
	}
}

// setDefault sets the key of the section to the value unless the section sets it to anything but null
func setDefault(section map[string]any, key, value string) {
	if section[key] == nil {
		section[key] = value
	}
}

// settingOf returns the value the file sets the setting to, as clickhouse-backup reads it from an env var
func settingOf(file map[string]any, setting Setting) (string, bool, error) {
	section, ok := file[setting.Section].(map[string]any)
	if !ok {
		return "", false, nil
	}
	switch value := section[setting.Key].(type) {
	case nil:
		return "", false, nil
	case string:
		return value, true, nil
	case bool:
		return strconv.FormatBool(value), true, nil
	case int:
		return strconv.Itoa(value), true, nil
	case uint64:
		return strconv.FormatUint(value, 10), true, nil
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64), true, nil
	default:
		return "", false, fmt.Errorf("%w: %s.%s is no scalar", ErrInvalidStorageConfig, setting.Section, setting.Key)
	}
}
