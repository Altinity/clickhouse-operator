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

package subst

import (
	"fmt"
	"path/filepath"
	"strings"

	core "k8s.io/api/core/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type settings interface {
	Has(string) bool
	Get(string) *api.Setting
	Set(string, *api.Setting) *api.Settings
	Delete(string)
	Name2Key(string) string
}

type req interface {
	GetTargetNamespace() string
	AppendAdditionalEnvVar(envVar core.EnvVar)
	AppendAdditionalVolume(volume core.Volume)
	AppendAdditionalVolumeMount(volumeMount core.VolumeMount)
	AppendSecretConfigFile(file api.SecretConfigFile)
	AppendRenderedSecretSetting(target, host, path, field, value string)
}

// substSettingsFieldWithDataFromDataSource substitute settings field with new setting built from the data source
func substSettingsFieldWithDataFromDataSource(
	settings settings,
	dataSourceDefaultNamespace string,
	dstField string,
	srcSecretRefField string,
	newSettingCreator func(types.ObjectAddress) (*api.Setting, error),
) bool {
	// Has to have source field specified
	if !settings.Has(srcSecretRefField) {
		// No substitution done
		return false
	}

	// Fetch data source address from the source setting field
	setting := settings.Get(srcSecretRefField)
	secretAddress, err := setting.FetchDataSourceAddress(dataSourceDefaultNamespace)
	if err != nil {
		// This is not necessarily an error, just no address specified, most likely setting is not data source ref
		// No substitution done
		return false
	}

	// Create setting from the secret with a provided function
	newSetting, err := newSettingCreator(secretAddress)
	if err != nil {
		// Unable to create new setting
		// No substitution done
		return false
	}

	// Set the new setting as dst.
	// Replacing src in case src name is the same as dst name.
	settings.Set(dstField, newSetting)

	// In case we are NOT replacing the same field with its new value, then remove the source field.
	// Typically non-replaced source field is not expected to be included into the final config,
	// mainly because very often these source fields are synthetic ones (do not exist in config fields list).
	if dstField != srcSecretRefField {
		settings.Delete(srcSecretRefField)
	}

	// Substitution done
	return true
}

// ApplySecretKeyRef maps a valueFrom.secretKeyRef field.
// valueFrom.mappingType=file projects the Secret key into a config directory
// ClickHouse reloads (users.d, config.d, or conf.d) and leaves the setting out
// of generated XML. mountFile is false while a setting is still being inherited
// down to a host. Every other value, including omitted mappingType, stays an
// env var + from_env.
func ApplySecretKeyRef(
	req req,
	settings settings,
	field string,
	envVarNamePrefix string,
	mountFile bool,
	target string,
	host string,
) bool {
	if settings.Get(field).IsFileMapping() {
		if !mountFile {
			return false
		}
		return mountSecretConfigFile(req, settings, field, target, host)
	}
	return ReplaceSettingsFieldWithEnvRefToSecretField(req, settings, field, field, envVarNamePrefix)
}

// RenderFileMappedSetting reads a mappingType=file settings value and records it
// for one rendered XML file. Settings that share a Secret and a top-level path
// (kafka2/sasl_username and kafka2/sasl_password) land in the same file.
// The source setting stays in place so generated settings XML skips it.
func RenderFileMappedSetting(
	req req,
	settings settings,
	field string,
	target string,
	host string,
	crName string,
	secretGet SecretGetter,
) bool {
	if secretGet == nil || !settings.Get(field).IsFileMapping() {
		return false
	}
	setting := settings.Get(field)
	secretAddress, err := setting.FetchDataSourceAddress(req.GetTargetNamespace())
	if err != nil {
		return false
	}
	value, err := FetchSecretFieldValue(secretAddress, secretGet)
	if err != nil {
		return false
	}
	section := field
	if i := strings.IndexByte(field, '/'); i >= 0 {
		section = field[:i]
	}
	path := secretSettingsFileName(secretAddress.Name, section, crName)
	if path == "" {
		return false
	}
	req.AppendRenderedSecretSetting(target, host, path, field, value)
	return true
}

func mountSecretConfigFile(req req, settings settings, field, target, host string) bool {
	setting := settings.Get(field)
	secretAddress, err := setting.FetchDataSourceAddress(req.GetTargetNamespace())
	if err != nil {
		return false
	}
	path := secretConfigFileName(secretAddress.Name, secretAddress.Key)
	if path == "" {
		return false
	}
	req.AppendSecretConfigFile(api.SecretConfigFile{
		Target: target,
		Host:   host,
		Secret: secretAddress.Name,
		Key:    secretAddress.Key,
		Path:   path,
	})
	// Leave the source setting in place. It is not a scalar, so generated XML
	// skips it, and user password normalization can still see mappingType=file
	// and refrain from substituting the default password.
	return true
}

// secretConfigFileName is the file name inside users.d.
// The secret key itself must already be a ClickHouse XML fragment.
func secretConfigFileName(secret, key string) string {
	return chopSecretFileName(secret + "-" + key)
}

// secretSettingsFileName is one rendered settings file for a Secret and a
// top-level settings section on this CR. Example:
// chop-secret-test-011-secret-kafka2-test-011-secrets.xml
func secretSettingsFileName(secret, section, crName string) string {
	return chopSecretFileName(secret + "-" + section + "-" + crName)
}

func chopSecretFileName(raw string) string {
	raw = strings.ToLower(raw)
	var b strings.Builder
	lastDash := false
	for _, r := range raw {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	name := strings.Trim(b.String(), "-")
	if name == "" {
		return ""
	}
	if len(name) > 200 {
		name = name[:200]
	}
	return "chop-secret-" + name + ".xml"
}

// ReplaceSettingsFieldWithEnvRefToSecretField substitute users settings field with ref to ENV var where value from k8s secret is stored in.
// Secrets are referenced through an ENV var on purpose - do not add a helper that inlines a
// Secret's value into a setting, as that writes the plaintext into the generated ConfigMap.
func ReplaceSettingsFieldWithEnvRefToSecretField(
	req req,
	settings settings,
	dstField string,
	srcSecretRefField string,
	envVarNamePrefix string,
) bool {
	return substSettingsFieldWithDataFromDataSource(
		settings,
		req.GetTargetNamespace(),
		dstField,
		srcSecretRefField,
		func(secretAddress types.ObjectAddress) (*api.Setting, error) {
			// ENV VAR name and value
			// In case not OK env var name will be empty and config will be incorrect. CH may not start
			envVarName, ok := util.BuildShellEnvVarName(envVarNamePrefix + "_" + settings.Name2Key(dstField))
			if !ok {
				return nil, fmt.Errorf("unable to build shell env var name for dstField: %s", dstField)
			}

			req.AppendAdditionalEnvVar(
				core.EnvVar{
					Name: envVarName,
					ValueFrom: &core.EnvVarSource{
						SecretKeyRef: &core.SecretKeySelector{
							LocalObjectReference: core.LocalObjectReference{
								Name: secretAddress.Name,
							},
							Key: secretAddress.Key,
						},
					},
				},
			)

			// Create new setting w/o value but with attribute to read from ENV var
			return api.NewSettingScalar("").SetAttribute("from_env", envVarName), nil
		})
}

func ReplaceSettingsFieldWithMountedFile(
	req req,
	settings *api.Settings,
	srcSecretRefField string,
) bool {
	var defaultMode int32 = 0644
	return substSettingsFieldWithDataFromDataSource(settings, req.GetTargetNamespace(), "", srcSecretRefField,
		func(secretAddress types.ObjectAddress) (*api.Setting, error) {
			volumeName, ok1 := util.BuildRFC1035Label(srcSecretRefField)
			volumeMountName, ok2 := util.BuildRFC1035Label(srcSecretRefField)
			filenameInSettingsOrFiles := srcSecretRefField
			filenameInMountedFS := secretAddress.Key

			if !ok1 || !ok2 {
				return nil, fmt.Errorf("unable to build k8s object name")
			}

			req.AppendAdditionalVolume(core.Volume{
				Name: volumeName,
				VolumeSource: core.VolumeSource{
					Secret: &core.SecretVolumeSource{
						SecretName: secretAddress.Name,
						Items: []core.KeyToPath{
							{
								Key:  secretAddress.Key,
								Path: filenameInMountedFS,
							},
						},
						DefaultMode: &defaultMode,
					},
				},
			})

			// TODO setting may have specified mountPath explicitly
			mountPath := filepath.Join(config.DirPathSecretFilesConfig, filenameInSettingsOrFiles, secretAddress.Name)
			// TODO setting may have specified subPath explicitly
			// Mount as file
			//subPath := filename
			// Mount as folder
			subPath := ""
			req.AppendAdditionalVolumeMount(core.VolumeMount{
				Name:      volumeMountName,
				ReadOnly:  true,
				MountPath: mountPath,
				SubPath:   subPath,
			})

			// Do not create new setting, but old setting would be deleted
			return nil, fmt.Errorf("no need to create a new setting")
		})
}

type SecretGetter func(namespace, name string) (*core.Secret, error)

var ErrSecretValueNotFound = fmt.Errorf("secret value not found")

// FetchSecretFieldValue fetches the value of the specified field in the specified Secret.
// Used by security.tls.rootCASecretRef resolution; settings substitution references Secrets
// through an ENV var instead and never reads their values here.
// TODO this is the only usage of k8s API in the normalizer. How to remove it?
func FetchSecretFieldValue(secretAddress types.ObjectAddress, secretGet SecretGetter) (string, error) {

	// Fetch the secret
	secret, err := secretGet(secretAddress.Namespace, secretAddress.Name)
	if err != nil {
		log.V(1).M(secretAddress.Namespace, secretAddress.Name).F().Info("unable to read secret %s %v", secretAddress, err)
		return "", ErrSecretValueNotFound
	}

	// Find the field within the secret
	for key, value := range secret.Data {
		if secretAddress.Key == key {
			// The field found!
			return string(value), nil
		}
	}

	log.V(1).M(secretAddress.Namespace, secretAddress.Name).F().
		Warning("unable to locate secret data by namespace/name/key: %s", secretAddress)

	return "", ErrSecretValueNotFound
}
