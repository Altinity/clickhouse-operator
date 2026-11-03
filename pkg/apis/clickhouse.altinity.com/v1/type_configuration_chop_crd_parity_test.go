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
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// chopConfCRDPath is the generated chopconf CRD. Regenerate with `VERBOSITY=1 dev/build_manifests.sh`
// after editing deploy/builder/templates-install-bundle/*-crd-02-chopconf.yaml.
const chopConfCRDPath = "../../../../deploy/operator/parts/crd.yaml"

const chopConfCRDName = "clickhouseoperatorconfigurations.clickhouse.altinity.com"

// crdParityAllowlist are OperatorConfig paths that intentionally have no chopconf CRD counterpart.
// Every entry needs a reason - an unexplained entry hides exactly the class of bug this test
// exists to catch. A path here suppresses that path and everything under it.
var crdParityAllowlist = map[string]string{
	// Deprecated flat keys, kept only so an old ConfigMap still parses. `move()` migrates them
	// into their structured homes. Never added to the CRD, and must not be.
	"watchNamespaces":                       "deprecated flat key, migrated by move()",
	"chScheme":                              "deprecated flat key, migrated by move()",
	"chUsername":                            "deprecated flat key, migrated by move()",
	"chPassword":                            "deprecated flat key, migrated by move()",
	"chCredentialsSecretNamespace":          "deprecated flat key, migrated by move()",
	"chCredentialsSecretName":               "deprecated flat key, migrated by move()",
	"chPort":                                "deprecated flat key, migrated by move()",
	"logtostderr":                           "deprecated flat logger key, migrated by move()",
	"alsologtostderr":                       "deprecated flat logger key, migrated by move()",
	"v":                                     "deprecated flat logger key, migrated by move()",
	"stderrthreshold":                       "deprecated flat logger key, migrated by move()",
	"vmodule":                               "deprecated flat logger key, migrated by move()",
	"log_backtrace_at":                      "deprecated flat logger key, migrated by move()",
	"chConfigUserDefaultProfile":            "deprecated flat key, migrated by move()",
	"chConfigUserDefaultQuota":              "deprecated flat key, migrated by move()",
	"chConfigUserDefaultNetworksIP":         "deprecated flat key, migrated by move()",
	"chConfigUserDefaultPassword":           "deprecated flat key, migrated by move()",
	"chConfigNetworksHostRegexpTemplate":    "deprecated flat key, migrated by move()",
	"chCommonConfigsPath":                   "deprecated flat key, migrated by move()",
	"chHostConfigsPath":                     "deprecated flat key, migrated by move()",
	"chUsersConfigsPath":                    "deprecated flat key, migrated by move()",
	"chiTemplatesPath":                      "deprecated flat key, migrated by move()",
	"statefulSetUpdateTimeout":              "deprecated flat key, migrated by move()",
	"statefulSetUpdatePollPeriod":           "deprecated flat key, migrated by move()",
	"onStatefulSetCreateFailureAction":      "deprecated flat key, migrated by move()",
	"onStatefulSetUpdateFailureAction":      "deprecated flat key, migrated by move()",
	"chConfigNetworksHostRegexpTemplateOld": "deprecated flat key, migrated by move()",
	"reconcileWaitExclude":                  "deprecated flat key, migrated by move()",
	"reconcileWaitInclude":                  "deprecated flat key, migrated by move()",
	"excludeFromPropagationLabels":          "deprecated flat key, migrated by move()",
	"appendScopeLabels":                     "deprecated flat key, migrated by move()",
	"includeIntoPropagationLabels":          "deprecated flat key, migrated by move()",
	"includeIntoPropagationAnnotations":     "deprecated flat key, migrated by move()",
	"excludeFromPropagationAnnotations":     "deprecated flat key, migrated by move()",
	"reconcileThreadsNumber":                "deprecated flat key, migrated by move()",
	"revisionHistoryLimit":                  "deprecated flat duplicate of statefulSet.revisionHistoryLimit, migrated by move()",
	"terminationGracePeriod":                "deprecated flat duplicate of pod.terminationGracePeriod, migrated by move()",

	// Runtime-only state, populated by the operator at load time and never user-settable.
	"runtime": "operator runtime state, not user configuration",

	// Read from template files on disk (template.chi.path), not from the CR body.
	"chiTemplates": "populated from template files on disk, not a CR field",
}

// collectGoLeaves walks a struct by reflection and returns the dotted json path of every leaf.
func collectGoLeaves(t reflect.Type, prefix string, out map[string]bool, depth int) {
	if depth > 12 {
		return
	}
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		if prefix != "" {
			out[prefix] = true
		}
		return
	}
	// Types that serialize as scalars/maps rather than exposing their internals.
	switch t.String() {
	case "types.String", "types.StringBool", "types.Strings", "time.Time", "resource.Quantity":
		if prefix != "" {
			out[prefix] = true
		}
		return
	}

	fields := 0
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			// unexported
			continue
		}
		tag := f.Tag.Get("json")
		if tag == "-" {
			continue
		}
		name := strings.Split(tag, ",")[0]
		if name == "" {
			// embedded or untagged - descend without extending the path
			collectGoLeaves(f.Type, prefix, out, depth+1)
			fields++
			continue
		}
		p := name
		if prefix != "" {
			p = prefix + "." + name
		}
		collectGoLeaves(f.Type, p, out, depth+1)
		fields++
	}
	if fields == 0 && prefix != "" {
		out[prefix] = true
	}
}

// collectCRDPaths walks an openAPIV3Schema and returns every declared property path.
func collectCRDPaths(schema map[string]any, prefix string, out map[string]bool) {
	props, _ := schema["properties"].(map[string]any)
	for name, raw := range props {
		sub, _ := raw.(map[string]any)
		p := name
		if prefix != "" {
			p = prefix + "." + name
		}
		out[p] = true
		if sub == nil {
			continue
		}
		// A node marked preserve-unknown-fields keeps everything beneath it, so nothing under
		// it can be pruned regardless of what the schema does or does not declare.
		if preserve, ok := sub["x-kubernetes-preserve-unknown-fields"].(bool); ok && preserve {
			out[p+".*"] = true
		}
		collectCRDPaths(sub, p, out)
		// arrays: descend into items so list-of-object fields are covered
		if items, ok := sub["items"].(map[string]any); ok {
			collectCRDPaths(items, p, out)
		}
		// free-form maps accept anything below them
		if ap, ok := sub["additionalProperties"].(map[string]any); ok {
			collectCRDPaths(ap, p, out)
			out[p+".*"] = true
		}
	}
}

func loadChopConfSchema(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(chopConfCRDPath)
	require.NoError(t, err, "cannot read generated CRD - run VERBOSITY=1 dev/build_manifests.sh")

	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	for {
		var doc map[string]any
		if err := dec.Decode(&doc); err != nil {
			break
		}
		meta, _ := doc["metadata"].(map[string]any)
		if meta == nil || meta["name"] != chopConfCRDName {
			continue
		}
		spec, _ := doc["spec"].(map[string]any)
		versions, _ := spec["versions"].([]any)
		require.NotEmpty(t, versions, "chopconf CRD declares no versions")
		v0, _ := versions[0].(map[string]any)
		sch, _ := v0["schema"].(map[string]any)
		open, _ := sch["openAPIV3Schema"].(map[string]any)
		require.NotNil(t, open, "chopconf CRD has no openAPIV3Schema")
		props, _ := open["properties"].(map[string]any)
		specSchema, _ := props["spec"].(map[string]any)
		require.NotNil(t, specSchema, "chopconf openAPIV3Schema has no spec")
		return specSchema
	}
	t.Fatalf("chopconf CRD %q not found in %s", chopConfCRDName, chopConfCRDPath)
	return nil
}

func allowlisted(path string) bool {
	// `runtime` sub-objects at any depth are operator state filled at load time, never CR input.
	for _, seg := range strings.Split(path, ".") {
		if seg == "runtime" {
			return true
		}
	}
	for entry := range crdParityAllowlist {
		if path == entry || strings.HasPrefix(path, entry+".") {
			return true
		}
	}
	return false
}

// A field that exists in OperatorConfig but not in the chopconf CRD is silently PRUNED by the API
// server when the operator config is supplied as a ClickHouseOperatorConfiguration CR. The
// operator then runs with the zero value and nothing reports it - which is how the sync gate
// (CHO-732) and the reconcile-hook `events`/`failurePolicy` fields shipped broken.
func TestOperatorConfigHasNoFieldPrunedByCHOPCONFCRD(t *testing.T) {
	goLeaves := map[string]bool{}
	collectGoLeaves(reflect.TypeOf(OperatorConfig{}), "", goLeaves, 0)
	require.NotEmpty(t, goLeaves, "reflection found no OperatorConfig fields")

	crdPaths := map[string]bool{}
	collectCRDPaths(loadChopConfSchema(t), "", crdPaths)
	require.NotEmpty(t, crdPaths, "CRD walk found no properties")

	var missing []string
	for leaf := range goLeaves {
		if allowlisted(leaf) || crdPaths[leaf] {
			continue
		}
		// a free-form map ancestor accepts anything beneath it
		covered := false
		for p := leaf; strings.Contains(p, "."); {
			p = p[:strings.LastIndex(p, ".")]
			if crdPaths[p+".*"] {
				covered = true
				break
			}
		}
		if !covered {
			missing = append(missing, leaf)
		}
	}
	sort.Strings(missing)

	require.Emptyf(t, missing, "OperatorConfig fields absent from the chopconf CRD - the API server "+
		"will prune them and the operator will silently use zero values.\nAdd them to "+
		"deploy/builder/templates-install-bundle/*-crd-02-chopconf.yaml and regenerate, or allowlist "+
		"with a reason in crdParityAllowlist.\nMissing (%d):\n  %s",
		len(missing), strings.Join(missing, "\n  "))
}
