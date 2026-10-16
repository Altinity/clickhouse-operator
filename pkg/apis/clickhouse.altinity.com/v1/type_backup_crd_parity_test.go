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
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// backupCRDTemplatePath is the source of the backup CRDs. The install bundles are generated from it.
const backupCRDTemplatePath = "../../../../deploy/builder/templates-install-bundle/clickhouse-operator-install-yaml-template-01-section-crd-04-backup.yaml"

// chiCRDTemplatePath is the source of the ClickHouseInstallation and ClickHouseInstallationTemplate CRDs.
const chiCRDTemplatePath = "../../../../deploy/builder/templates-install-bundle/clickhouse-operator-install-yaml-template-01-section-crd-01-chi-chit.yaml"

// jobScriptPath is the script of the backup and restore Jobs, which checks a tables pattern again before it
// passes the pattern to clickhouse-backup.
const jobScriptPath = "../../../model/chbackup/scripts/job.sh"

// backupTargetImmutableRule keeps the target of a backup, a schedule and a restore what it was created
// with: holds, locks and records follow the target, and a retargeted restore would release the
// schedules of the installation it may have left partly restored.
const backupTargetImmutableRule = "self.clickHouseInstallation == oldSelf.clickHouseInstallation && " +
	"(has(self.cluster) ? self.cluster : '') == (has(oldSelf.cluster) ? oldSelf.cluster : '')"

// backupImmutableRule keeps the target of a one-off backup what it was created with, the installation it names or the
// schedule it runs: its clusters are pinned and its Jobs labeled and locked by the target. Either may be absent.
const backupImmutableRule = "(has(self.clickHouseInstallation) ? self.clickHouseInstallation : '') == " +
	"(has(oldSelf.clickHouseInstallation) ? oldSelf.clickHouseInstallation : '') && " +
	"(has(self.scheduleRef) ? self.scheduleRef.name : '') == (has(oldSelf.scheduleRef) ? oldSelf.scheduleRef.name : '') && " +
	"(has(self.cluster) ? self.cluster : '') == (has(oldSelf.cluster) ? oldSelf.cluster : '')"

// backupRules are the rules of a one-off backup's spec beyond its immutability: one target, the options and the name
// of a backup that runs a schedule taken from the schedule alone, but schemaOnly, which CEL cannot type and only
// Validate refuses, and no name of the series a schedule's retention prunes.
var backupRules = map[string]string{
	"has(self.clickHouseInstallation) != has(self.scheduleRef)": "exactly one of clickHouseInstallation and scheduleRef is required",
	"!has(self.scheduleRef) || !(has(self.replicaSelection) || has(self.tables) || has(self.partitions) || " +
		"has(self.activeDeadlineSeconds))": "a backup that runs a schedule takes its options from it: " +
		"replicaSelection, tables, partitions and activeDeadlineSeconds are not set with scheduleRef",
	"!has(self.scheduleRef) || (has(self.backupName) ? self.backupName : '') == ''": "a backup that runs a schedule is " +
		"named with the schedule's backupNamePrefix: backupName is not set with scheduleRef",
	"!has(self.backupName) || !self.backupName.matches('" + scheduledBackupNameRegexp.String() + "')": "backupName must not " +
		"end like the backups of a schedule, " + BackupNameScheduleSuffix + ", which the retention of a schedule of that prefix would prune",
}

// backupNamePrefixSetOnceRule keeps the backup name prefix of a schedule what it was first set to, the series its
// retention prunes: a changed prefix would leave the backups named with the former one unpruned. It reads an absent
// prefix as empty, so that setting one later is allowed and removing it is a change.
const backupNamePrefixSetOnceRule = "(has(oldSelf.backupNamePrefix) ? oldSelf.backupNamePrefix : '') == '' || " +
	"(has(self.backupNamePrefix) ? self.backupNamePrefix : '') == oldSelf.backupNamePrefix"

// restoreSourceImmutableRule keeps the installation a restore reads the backup of as the restore was created: every
// Job of the restore reads the backup from one place.
const restoreSourceImmutableRule = "(has(self.sourceInstallation) ? self.sourceInstallation : '') == " +
	"(has(oldSelf.sourceInstallation) ? oldSelf.sourceInstallation : '') && " +
	"(has(self.sourceNamespace) ? self.sourceNamespace : '') == (has(oldSelf.sourceNamespace) ? oldSelf.sourceNamespace : '')"

// loadCRDSchema returns the openAPIV3Schema of the named CRD in a template.
func loadCRDSchema(t *testing.T, path, name string) map[string]any {
	t.Helper()
	schema, _ := loadCRDVersion(t, path, name)["schema"].(map[string]any)
	openAPI, _ := schema["openAPIV3Schema"].(map[string]any)
	require.NotNil(t, openAPI)
	return openAPI
}

// loadCRDVersion returns the first version of the named CRD in a template.
func loadCRDVersion(t *testing.T, path, name string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	for {
		var doc map[string]any
		if err := dec.Decode(&doc); err != nil {
			break
		}
		metadata, _ := doc["metadata"].(map[string]any)
		if (metadata == nil) || (metadata["name"] != name) {
			continue
		}
		spec, _ := doc["spec"].(map[string]any)
		versions, _ := spec["versions"].([]any)
		require.NotEmpty(t, versions)
		version, _ := versions[0].(map[string]any)
		return version
	}
	t.Fatalf("CRD %q not found in %s", name, path)
	return nil
}

// schemaProperty returns the schema of a property at a dotted path, nil when it has none.
func schemaProperty(schema map[string]any, path string) map[string]any {
	for _, name := range strings.Split(path, ".") {
		if items, ok := schema["items"].(map[string]any); ok {
			schema = items
		}
		properties, _ := schema["properties"].(map[string]any)
		schema, _ = properties[name].(map[string]any)
		if schema == nil {
			return nil
		}
	}
	return schema
}

// crdLeaves returns the leaf property paths of a schema.
func crdLeaves(schema map[string]any) map[string]bool {
	paths := map[string]bool{}
	collectCRDPaths(schema, "", paths)
	leaves := map[string]bool{}
	for path := range paths {
		leaf := true
		for other := range paths {
			if strings.HasPrefix(other, path+".") {
				leaf = false
				break
			}
		}
		if leaf {
			leaves[path] = true
		}
	}
	return leaves
}

func sortedPaths(paths map[string]bool) []string {
	var sorted []string
	for path := range paths {
		sorted = append(sorted, path)
	}
	sort.Strings(sorted)
	return sorted
}

// The API server prunes every field of a status its CRD does not declare: a status field the schema
// lacks is lost on every write, silently. A field the schema declares and the type does not have
// is documentation of nothing. The status of the backup kinds and their schemas match exactly.
func TestBackupCRDStatusMatchesTheTypes(t *testing.T) {
	for name, status := range map[string]reflect.Type{
		"clickhousebackups.clickhouse.altinity.com":         reflect.TypeOf(ClickHouseBackupStatus{}),
		"clickhousebackupschedules.clickhouse.altinity.com": reflect.TypeOf(ClickHouseBackupScheduleStatus{}),
		"clickhouserestores.clickhouse.altinity.com":        reflect.TypeOf(ClickHouseRestoreStatus{}),
	} {
		t.Run(name, func(t *testing.T) {
			schema := schemaProperty(loadCRDSchema(t, backupCRDTemplatePath, name), "status")
			require.NotNil(t, schema)
			goLeaves := map[string]bool{}
			collectGoLeaves(status, "", goLeaves, 0)
			require.Equal(t, sortedPaths(goLeaves), sortedPaths(crdLeaves(schema)))

			clusters := schemaProperty(schema, "clusters")
			require.Equal(t, "map", clusters["x-kubernetes-list-type"], "a cluster is listed once, by name")
			require.Equal(t, []any{"name"}, clusters["x-kubernetes-list-map-keys"])
			items, _ := clusters["items"].(map[string]any)
			require.Equal(t, []any{"name"}, items["required"], "a list map key must be required")
		})
	}
}

// Every spec field of the backup kinds is declared, so that none is pruned on its way in.
func TestBackupCRDSpecDeclaresTheTypes(t *testing.T) {
	for name, spec := range map[string]reflect.Type{
		"clickhousebackups.clickhouse.altinity.com":         reflect.TypeOf(ClickHouseBackupSpec{}),
		"clickhousebackupschedules.clickhouse.altinity.com": reflect.TypeOf(ClickHouseBackupScheduleSpec{}),
		"clickhouserestores.clickhouse.altinity.com":        reflect.TypeOf(ClickHouseRestoreSpec{}),
	} {
		t.Run(name, func(t *testing.T) {
			schema := schemaProperty(loadCRDSchema(t, backupCRDTemplatePath, name), "spec")
			goLeaves := map[string]bool{}
			collectGoLeaves(spec, "", goLeaves, 0)
			declared := map[string]bool{}
			collectCRDPaths(schema, "", declared)
			for _, path := range sortedPaths(goLeaves) {
				require.True(t, declared[path], "spec field %s is not declared", path)
			}
		})
	}
}

// Every on/off field of the backup kinds is a StringBool, which the schema keeps polymorphic: a typed schema would
// refuse the values the type takes besides its own, as a boolean does "yes". A printer column that shows one is a
// string, the only column type the API server prints a bool and a string in alike.
func TestBackupCRDStringBoolsArePolymorphic(t *testing.T) {
	declared := 0
	for name, spec := range map[string]reflect.Type{
		"clickhousebackups.clickhouse.altinity.com":         reflect.TypeOf(ClickHouseBackupSpec{}),
		"clickhousebackupschedules.clickhouse.altinity.com": reflect.TypeOf(ClickHouseBackupScheduleSpec{}),
		"clickhouserestores.clickhouse.altinity.com":        reflect.TypeOf(ClickHouseRestoreSpec{}),
	} {
		t.Run(name, func(t *testing.T) {
			version := loadCRDVersion(t, backupCRDTemplatePath, name)
			schema := schemaProperty(loadCRDSchema(t, backupCRDTemplatePath, name), "spec")
			columns, _ := version["additionalPrinterColumns"].([]any)
			for _, path := range stringBoolPaths(spec, "") {
				declared++
				property := schemaProperty(schema, path)
				require.NotNil(t, property, path)
				require.Equal(t, true, property["x-kubernetes-preserve-unknown-fields"], path)
				require.NotContains(t, property, "type", path)
				require.NotEmpty(t, property["description"], path)
				for _, column := range columns {
					column, _ := column.(map[string]any)
					if column["jsonPath"] == ".spec."+path {
						require.Equal(t, "string", column["type"], "printer column %s", column["name"])
					}
				}
			}
		})
	}
	require.Equal(t, len(backupStringBoolFields), declared, "every on/off field is checked")
}

// The target of a backup, a schedule and a restore is immutable, which the API server enforces, and so are the
// backup name prefix of a schedule once set and the source of a restore. The rule compares the optional cluster as
// it reads when absent, so that adding or removing it is a change too; it is a transition rule, which a create
// never trips.
func TestBackupCRDTargetIsImmutable(t *testing.T) {
	t.Run("clickhousebackups.clickhouse.altinity.com", func(t *testing.T) {
		spec := schemaProperty(loadCRDSchema(t, backupCRDTemplatePath, "clickhousebackups.clickhouse.altinity.com"), "spec")
		rules, _ := spec["x-kubernetes-validations"].([]any)
		declared := map[string]string{}
		for _, rule := range rules {
			rule, _ := rule.(map[string]any)
			declared[rule["rule"].(string)] = rule["message"].(string)
		}
		want := map[string]string{backupImmutableRule: "clickHouseInstallation, scheduleRef and cluster are immutable; create a new resource"}
		for rule, message := range backupRules {
			want[rule] = message
		}
		require.Equal(t, want, declared)
	})
	for name, next := range map[string]struct{ rule, message string }{
		"clickhousebackupschedules.clickhouse.altinity.com": {backupNamePrefixSetOnceRule, "backupNamePrefix can be set once " +
			"and is immutable after: a changed prefix would leave the former series unpruned; create a new resource"},
		"clickhouserestores.clickhouse.altinity.com": {restoreSourceImmutableRule,
			"sourceInstallation and sourceNamespace are immutable; create a new resource"},
	} {
		t.Run(name, func(t *testing.T) {
			spec := schemaProperty(loadCRDSchema(t, backupCRDTemplatePath, name), "spec")
			rules, _ := spec["x-kubernetes-validations"].([]any)
			require.Len(t, rules, 2)
			rule, _ := rules[0].(map[string]any)
			require.Equal(t, backupTargetImmutableRule, rule["rule"])
			require.Equal(t, "clickHouseInstallation and cluster are immutable; create a new resource", rule["message"])
			rule, _ = rules[1].(map[string]any)
			require.Equal(t, next.rule, rule["rule"])
			require.Equal(t, next.message, rule["message"])
		})
	}
}

// The installation a restore, or a recovery, reads the backup of takes what the Go side does: a Kubernetes name, in
// the pattern and length the operator validates, or an explicit empty value, which names the restored installation.
func TestBackupCRDDeclaresTheSource(t *testing.T) {
	restore := loadCRDSchema(t, backupCRDTemplatePath, "clickhouserestores.clickhouse.altinity.com")
	chi := loadCRDSchema(t, chiCRDTemplatePath, "${PLURAL}.clickhouse.altinity.com")
	for _, tc := range []struct {
		schema    map[string]any
		path      string
		pattern   string
		maxLength int
	}{
		{restore, "spec.sourceInstallation", optionalPattern(backupSourceRegexp), BackupSourceInstallationMaxLength},
		{restore, "spec.sourceNamespace", optionalPattern(backupSourceRegexp), BackupSourceNamespaceMaxLength},
		{chi, "spec.backup.recovery.sourceInstallation", optionalPattern(backupSourceRegexp), BackupSourceInstallationMaxLength},
		{chi, "spec.backup.recovery.sourceNamespace", optionalPattern(backupSourceRegexp), BackupSourceNamespaceMaxLength},
		{chi, "spec.configuration.clusters.backup.recovery.sourceInstallation", optionalPattern(backupSourceRegexp), BackupSourceInstallationMaxLength},
		{chi, "spec.configuration.clusters.backup.recovery.sourceNamespace", optionalPattern(backupSourceRegexp), BackupSourceNamespaceMaxLength},
	} {
		t.Run(tc.path, func(t *testing.T) {
			field := schemaProperty(tc.schema, tc.path)
			require.NotNil(t, field)
			require.Equal(t, "string", field["type"])
			require.Equal(t, tc.pattern, field["pattern"])
			require.Equal(t, tc.maxLength, field["maxLength"])
		})
	}
}

// A recovery is declared at the installation level and on every cluster, with the backup name
// pattern that admits an explicit empty name, which leaves a cluster out.
func TestCHICRDDeclaresTheRecovery(t *testing.T) {
	schema := loadCRDSchema(t, chiCRDTemplatePath, "${PLURAL}.clickhouse.altinity.com")
	for _, path := range []string{"spec.backup.recovery.backupName", "spec.configuration.clusters.backup.recovery.backupName"} {
		backupName := schemaProperty(schema, path)
		require.NotNil(t, backupName, path)
		require.Equal(t, "string", backupName["type"], path)
		require.Equal(t, optionalPattern(backupNameRegexp), backupName["pattern"], path)
		require.Equal(t, BackupNameMaxLength, backupName["maxLength"], path)
	}
}

// The backup block of the installation and of every cluster declares exactly the fields of ClusterBackup: a
// field the schema lacks is pruned on its way in, and one the type lacks is accepted and then ignored.
func TestCHICRDBackupMatchesTheType(t *testing.T) {
	schema := loadCRDSchema(t, chiCRDTemplatePath, "${PLURAL}.clickhouse.altinity.com")
	goLeaves := map[string]bool{}
	collectGoLeaves(reflect.TypeOf(ClusterBackup{}), "", goLeaves, 0)
	for _, path := range []string{"spec.backup", "spec.configuration.clusters.backup"} {
		backup := schemaProperty(schema, path)
		require.NotNil(t, backup, path)
		declared := map[string]bool{}
		for leaf := range crdLeaves(backup) {
			// A scalar the schema keeps polymorphic, as a StringBool, is a leaf of the type all the same
			declared[strings.TrimSuffix(leaf, ".*")] = true
		}
		require.Equal(t, sortedPaths(goLeaves), sortedPaths(declared), path)
	}
}

// A field ClusterBackup omits when it is empty must not be required by the schema: the type writes an empty value
// as nothing, so the API server would reject an installation written back through the type - a cluster that
// takes no storage Secret with `name: ""`, for one.
func TestCHICRDBackupRequiresNothingTheTypeOmits(t *testing.T) {
	schema := loadCRDSchema(t, chiCRDTemplatePath, "${PLURAL}.clickhouse.altinity.com")
	for _, path := range []string{"spec.backup", "spec.configuration.clusters.backup"} {
		backup := schemaProperty(schema, path)
		require.NotNil(t, backup, path)
		require.Empty(t, requiredOmitted(backup, reflect.TypeOf(ClusterBackup{}), path), path)
	}
}

// requiredOmitted returns the dotted paths of the fields a schema requires that the type omits when empty.
func requiredOmitted(schema map[string]any, typ reflect.Type, path string) []string {
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return nil
	}
	required := map[string]bool{}
	names, _ := schema["required"].([]any)
	for _, name := range names {
		required[name.(string)] = true
	}
	properties, _ := schema["properties"].(map[string]any)
	var omitted []string
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := strings.Split(field.Tag.Get("json"), ",")
		if tag[0] == "" {
			// an embedded struct, untagged or inline, spreads its fields in place
			omitted = append(omitted, requiredOmitted(schema, field.Type, path)...)
			continue
		}
		if required[tag[0]] && slices.Contains(tag[1:], "omitempty") {
			omitted = append(omitted, path+"."+tag[0])
		}
		if property, ok := properties[tag[0]].(map[string]any); ok {
			omitted = append(omitted, requiredOmitted(property, field.Type, path+"."+tag[0])...)
		}
	}
	return omitted
}

// optionalPattern returns the CRD pattern of an optional field that must match re when set: an
// explicit empty value passes, as it does in Go.
func optionalPattern(re *regexp.Regexp) string {
	return "^(" + strings.TrimSuffix(strings.TrimPrefix(re.String(), "^"), "$") + ")?$"
}

// A tables pattern is optional in the spec of a backup, a schedule and a restore alike: the API server
// admits an explicit empty one, which the operator reads as every table, and refuses what the operator
// would refuse.
func TestBackupCRDTablesPatternMatchesTheTypes(t *testing.T) {
	for name, path := range map[string]string{
		"clickhousebackups.clickhouse.altinity.com":         "spec.tables",
		"clickhousebackupschedules.clickhouse.altinity.com": "spec.tables",
		"clickhouserestores.clickhouse.altinity.com":        "spec.tables",
	} {
		t.Run(name, func(t *testing.T) {
			tables := schemaProperty(loadCRDSchema(t, backupCRDTemplatePath, name), path)
			require.NotNil(t, tables)
			require.Equal(t, optionalPattern(backupTablesRegexp), tables["pattern"])
			require.Equal(t, backupTablesMaxLength, tables["maxLength"])
			pattern := regexp.MustCompile(tables["pattern"].(string))
			for value, valid := range map[string]bool{"": true, "db.*": true, "db.t,other.*": true, "-db.*": false, "db t": false} {
				require.Equal(t, valid, pattern.MatchString(value), value)
				require.Equal(t, valid, validateTables(value) == nil, value)
			}
		})
	}
}

// The Job script accepts exactly the tables patterns the spec validation does.
func TestJobScriptTablesPatternMatchesTheTypes(t *testing.T) {
	raw, err := os.ReadFile(jobScriptPath)
	require.NoError(t, err)
	script := string(raw)
	pattern := regexp.MustCompile(`(?m)^TABLES_RE='([^']*)'$`).FindStringSubmatch(script)
	require.NotNil(t, pattern, "the script no longer defines TABLES_RE")
	require.Equal(t, backupTablesRegexp.String(), pattern[1])
	maxLength := regexp.MustCompile(`(?m)^TABLES_MAX_LENGTH=([0-9]+)$`).FindStringSubmatch(script)
	require.NotNil(t, maxLength, "the script no longer defines TABLES_MAX_LENGTH")
	require.Equal(t, strconv.Itoa(backupTablesMaxLength), maxLength[1])
}

// The starting deadline of a backup is bounded alike by the CRD schema and the spec validation.
func TestBackupCRDBoundsTheStartingDeadline(t *testing.T) {
	deadline := schemaProperty(loadCRDSchema(t, backupCRDTemplatePath, "clickhousebackups.clickhouse.altinity.com"), "spec.startingDeadlineSeconds")
	require.NotNil(t, deadline)
	require.Equal(t, BackupStartingDeadlineMaxSeconds, deadline["maximum"])
	require.Equal(t, 0, deadline["minimum"])
}

// The retention of the Job script prunes the series a schedule's runs name, which no one-off backup name may join: the
// end of the names it takes is the one the spec validation refuses.
func TestJobScriptSeriesMatchesTheTypes(t *testing.T) {
	raw, err := os.ReadFile(jobScriptPath)
	require.NoError(t, err)
	series := regexp.MustCompile(`(?m)^\s*pattern="\^\$\{BACKUP_NAME_PREFIX//\./\[\.\]\}(.*)"$`).FindStringSubmatch(string(raw))
	require.NotNil(t, series, "the script no longer prunes the series of a prefix")
	require.Equal(t, scheduledBackupNameRegexp.String(), series[1])
}

// The Job script accepts exactly the source installations and namespaces the spec validation does.
func TestJobScriptSourcePatternMatchesTheTypes(t *testing.T) {
	raw, err := os.ReadFile(jobScriptPath)
	require.NoError(t, err)
	script := string(raw)
	pattern := regexp.MustCompile(`(?m)^SOURCE_NAME_RE='([^']*)'$`).FindStringSubmatch(script)
	require.NotNil(t, pattern, "the script no longer defines SOURCE_NAME_RE")
	require.Equal(t, backupSourceRegexp.String(), pattern[1])
	for name, want := range map[string]int{
		"SOURCE_INSTALLATION_MAX_LENGTH": BackupSourceInstallationMaxLength,
		"SOURCE_NAMESPACE_MAX_LENGTH":    BackupSourceNamespaceMaxLength,
	} {
		maxLength := regexp.MustCompile(`(?m)^` + name + `=([0-9]+)$`).FindStringSubmatch(script)
		require.NotNil(t, maxLength, "the script no longer defines %s", name)
		require.Equal(t, strconv.Itoa(want), maxLength[1], name)
	}
}
