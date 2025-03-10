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
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"path/filepath"

	core "k8s.io/api/core/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
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
}

// substSettingsFieldWithDataFromDataSource substitute settings field with new setting built from the data source
func substSettingsFieldWithDataFromDataSource(
	settings settings,
	dataSourceDefaultNamespace string,
	dstField string,
	srcSecretRefField string,
	parseScalarString bool,
	newSettingCreator func(types.ObjectAddress) (*api.Setting, error),
) bool {
	// Has to have source field specified
	if !settings.Has(srcSecretRefField) {
		// No substitution done
		return false
	}

	// Fetch data source address from the source setting field
	setting := settings.Get(srcSecretRefField)
	secretAddress, err := setting.FetchDataSourceAddress(dataSourceDefaultNamespace, parseScalarString)
	if err != nil {
		// This is not necessarily an error, just no address specified, most likely setting is not data source ref
		// No substitution done
		return false
	}

	// Create setting from the secret with a provided function
	if newSetting, err := newSettingCreator(secretAddress); err == nil {
		// Set the new setting as dst.
		// Replacing src in case src name is the same as dst name.
		settings.Set(dstField, newSetting)
	}

	// In case we are NOT replacing the same field with its new value, then remove the source field.
	// Typically non-replaced source field is not expected to be included into the final config,
	// mainly because very often these source fields are synthetic ones (do not exist in config fields list).
	if dstField != srcSecretRefField {
		settings.Delete(srcSecretRefField)
	}

	// Substitution done
	return true
}

// ReplaceSettingsFieldWithSecretFieldValue substitute users settings field with the value read from k8s secret
func ReplaceSettingsFieldWithSecretFieldValue(
	req req,
	settings settings,
	dstField string,
	srcSecretRefField string,
	secretGet SecretGetter,
) bool {
	return substSettingsFieldWithDataFromDataSource(settings, req.GetTargetNamespace(), dstField, srcSecretRefField, true,
		func(secretAddress types.ObjectAddress) (*api.Setting, error) {
			secretFieldValue, err := fetchSecretFieldValue(secretAddress, secretGet)
			if err != nil {
				return nil, err
			}
			// Create new setting with the value
			return api.NewSettingScalar(secretFieldValue), nil
		})
}

// ReplaceSettingsFieldWithEnvRefToSecretField substitute users settings field with ref to ENV var where value from k8s secret is stored in
func ReplaceSettingsFieldWithEnvRefToSecretField(
	req req,
	settings settings,
	dstField string,
	srcSecretRefField string,
	envVarNamePrefix string,
	parseScalarString bool,
) bool {
	return substSettingsFieldWithDataFromDataSource(settings, req.GetTargetNamespace(), dstField, srcSecretRefField, parseScalarString,
		func(secretAddress types.ObjectAddress) (*api.Setting, error) {
			// ENV VAR name and value
			// In case not OK env var name will be empty and config will be incorrect. CH may not start
			envVarName, _ := util.BuildShellEnvVarName(envVarNamePrefix + "_" + settings.Name2Key(dstField))
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
	return substSettingsFieldWithDataFromDataSource(settings, req.GetTargetNamespace(), "", srcSecretRefField, false,
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

// fetchSecretFieldValue fetches the value of the specified field in the specified secret
// TODO this is the only usage of k8s API in the normalizer. How to remove it?
func fetchSecretFieldValue(secretAddress types.ObjectAddress, secretGet SecretGetter) (string, error) {

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
