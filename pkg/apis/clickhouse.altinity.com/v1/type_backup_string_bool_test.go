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
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// backupStringBoolFields are the on/off fields of the backup kinds, each read as the operator reads it.
var backupStringBoolFields = []struct {
	name string
	// unset is how the operator reads the field when the spec leaves it out
	unset bool
	// decode decodes a spec of the kind that sets the field to the JSON value, and returns how the operator
	// reads the field and what Validate makes of the spec
	decode func(t *testing.T, value string) (bool, error)
}{
	{"verify", false, func(t *testing.T, value string) (bool, error) {
		spec := decodeBackupSpec[ClickHouseBackupSpec](t, `{"clickHouseInstallation":"chi","verify":`+value+`}`)
		return spec.Verify.IsTrue(), spec.Validate()
	}},
	{"schemaOnly", false, func(t *testing.T, value string) (bool, error) {
		spec := decodeBackupSpec[ClickHouseBackupSpec](t, `{"clickHouseInstallation":"chi","schemaOnly":`+value+`}`)
		return spec.SchemaOnly.IsTrue(), spec.Validate()
	}},
	{"suspend", false, func(t *testing.T, value string) (bool, error) {
		spec := decodeBackupSpec[ClickHouseBackupScheduleSpec](t, `{"clickHouseInstallation":"chi","schedule":"0 2 * * *","suspend":`+value+`}`)
		return spec.IsSuspended(), spec.Validate()
	}},
	{"schemaOnly", false, func(t *testing.T, value string) (bool, error) {
		spec := decodeBackupSpec[ClickHouseBackupScheduleSpec](t, `{"clickHouseInstallation":"chi","schedule":"0 2 * * *","schemaOnly":`+value+`}`)
		return spec.SchemaOnly.IsTrue(), spec.Validate()
	}},
	{"schemaOnly", false, func(t *testing.T, value string) (bool, error) {
		spec := decodeBackupSpec[ClickHouseRestoreSpec](t, `{"clickHouseInstallation":"chi","backupName":"b","schemaOnly":`+value+`}`)
		return spec.SchemaOnly.IsTrue(), spec.Validate()
	}},
	{"overwrite", false, func(t *testing.T, value string) (bool, error) {
		spec := decodeBackupSpec[ClickHouseRestoreSpec](t, `{"clickHouseInstallation":"chi","backupName":"b","overwrite":`+value+`}`)
		return spec.Overwrite.IsTrue(), spec.Validate()
	}},
	{"validateTopology", true, func(t *testing.T, value string) (bool, error) {
		spec := decodeBackupSpec[ClickHouseRestoreSpec](t, `{"clickHouseInstallation":"chi","backupName":"b","validateTopology":`+value+`}`)
		return spec.IsTopologyValidationEnabled(), spec.Validate()
	}},
	{"rbac", false, func(t *testing.T, value string) (bool, error) {
		spec := decodeBackupSpec[ClickHouseRestoreSpec](t, `{"clickHouseInstallation":"chi","backupName":"b","rbac":`+value+`}`)
		return spec.RBAC.IsTrue(), spec.Validate()
	}},
}

// decodeBackupSpec decodes the JSON of a spec as the API server hands it over.
func decodeBackupSpec[T any](t *testing.T, doc string) *T {
	t.Helper()
	spec := new(T)
	require.NoError(t, json.Unmarshal([]byte(doc), spec), doc)
	return spec
}

// The on/off fields of the backup kinds take what those of an installation take: a bool, 0 or 1, or a word
// such as "yes" or "Off". Their CRD schema accepts any value, so Validate refuses one that is none of these,
// naming the field - one of another JSON type as well, which decodes as written.
func TestBackupSpecStringBools(t *testing.T) {
	for _, field := range backupStringBoolFields {
		for value, expected := range map[string]bool{
			`true`: true, `"yes"`: true, `1`: true, `"On"`: true,
			`false`: false, `"no"`: false, `0`: false, `"Off"`: false,
			`null`: field.unset,
		} {
			t.Run(fmt.Sprintf("%s=%s", field.name, value), func(t *testing.T) {
				on, err := field.decode(t, value)
				require.NoError(t, err)
				require.Equal(t, expected, on)
			})
		}
		for value, shown := range map[string]string{
			`"maybe"`: `"maybe"`, `""`: `""`, `1.5`: `"1.5"`, `{}`: `"{}"`, `[true]`: `"[true]"`,
		} {
			t.Run(fmt.Sprintf("%s=%s", field.name, value), func(t *testing.T) {
				on, err := field.decode(t, value)
				require.Equal(t, field.unset, on, "a value that is no boolean reads as the field left unset")
				require.ErrorIs(t, err, ErrInvalidBackupSpec)
				require.EqualError(t, err, fmt.Sprintf("invalid backup spec: %s %s is not a boolean", field.name, shown))
			})
		}
	}
}

// One resource whose on/off field holds a value of another JSON type must not keep the others from being read: a
// watch reads them all as one list, which would fail to decode, and so would the reconcile of every resource of
// the kind.
func TestBackupListDecodesPastAMalformedOnOffField(t *testing.T) {
	list := &ClickHouseBackupList{}
	require.NoError(t, json.Unmarshal([]byte(`{"items": [
		{"metadata": {"name": "good"}, "spec": {"clickHouseInstallation": "chi", "verify": true}},
		{"metadata": {"name": "bad"}, "spec": {"clickHouseInstallation": "chi", "verify": {"nested": 1}}}
	]}`), list))
	require.Len(t, list.Items, 2)
	require.NoError(t, list.Items[0].Spec.Validate())
	require.True(t, list.Items[0].Spec.Verify.IsTrue())
	require.ErrorIs(t, list.Items[1].Spec.Validate(), ErrInvalidBackupSpec)
}

// A copy of a spec owns its on/off fields: a change of the original, which the operator makes to a spec it
// holds, must not show in the copy the cache keeps.
func TestBackupSpecDeepCopyOwnsStringBools(t *testing.T) {
	for name, spec := range map[string]any{
		"backup":   &ClickHouseBackupSpec{},
		"schedule": &ClickHouseBackupScheduleSpec{},
		"restore":  &ClickHouseRestoreSpec{},
	} {
		t.Run(name, func(t *testing.T) {
			original := reflect.ValueOf(spec)
			fields := stringBoolFieldValues(original.Elem())
			require.NotEmpty(t, fields)
			for _, field := range fields {
				field.Set(reflect.ValueOf(types.NewStringBool(true)))
			}
			copied := original.MethodByName("DeepCopy").Call(nil)[0]
			for _, field := range fields {
				*field.Interface().(*types.StringBool) = types.StringBoolFalseFirstCapital
			}
			copiedFields := stringBoolFieldValues(copied.Elem())
			require.Len(t, copiedFields, len(fields))
			for _, field := range copiedFields {
				require.True(t, field.Interface().(*types.StringBool).IsTrue())
			}
		})
	}
}

// stringBoolType is the type of an on/off field.
var stringBoolType = reflect.TypeOf((*types.StringBool)(nil))

// stringBoolFieldValues returns the on/off fields of a struct of this package, of the structs it holds as well.
func stringBoolFieldValues(value reflect.Value) []reflect.Value {
	var fields []reflect.Value
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		switch {
		case field.Type() == stringBoolType:
			fields = append(fields, field)
		case (field.Kind() == reflect.Struct) && (field.Type().PkgPath() == value.Type().PkgPath()):
			fields = append(fields, stringBoolFieldValues(field)...)
		}
	}
	return fields
}

// stringBoolPaths returns the dotted JSON paths of the on/off fields of a struct of this package, of the
// structs it holds as well. An inline struct adds no name of its own.
func stringBoolPaths(typ reflect.Type, prefix string) []string {
	var paths []string
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		path := prefix
		if name := strings.Split(field.Tag.Get("json"), ",")[0]; name != "" {
			path = strings.TrimPrefix(prefix+"."+name, ".")
		}
		switch {
		case field.Type == stringBoolType:
			paths = append(paths, path)
		case (field.Type.Kind() == reflect.Struct) && (field.Type.PkgPath() == typ.PkgPath()):
			paths = append(paths, stringBoolPaths(field.Type, path)...)
		}
	}
	return paths
}
