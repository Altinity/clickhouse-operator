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

package model

import (
	"strings"

	"gopkg.in/d4l3k/messagediff.v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
)

// SettingsChangeableWithoutRestart is the set of system.server_settings names ClickHouse
// applies without a restart. Names match system.server_settings.name. An empty set,
// including a failed query, leaves the decision to configurationRestartPolicy.
type SettingsChangeableWithoutRestart map[string]struct{}

// NewSettingsChangeableWithoutRestart builds the set from system.server_settings.name.
// An empty name is left out.
func NewSettingsChangeableWithoutRestart(names []string) SettingsChangeableWithoutRestart {
	changeableWithoutRestart := make(SettingsChangeableWithoutRestart, len(names))
	for _, name := range names {
		if name == "" {
			continue
		}
		changeableWithoutRestart[name] = struct{}{}
	}
	return changeableWithoutRestart
}

// Contains reports whether name is changeable without a restart.
func (s SettingsChangeableWithoutRestart) Contains(name string) bool {
	_, ok := s[name]
	return ok
}

// serverSettingName maps a configurationRestartPolicy path onto system.server_settings.name.
// settings/logger/level becomes logger.level. Profiles, quotas, files and zookeeper paths
// are not server settings. A wildcard is not a setting name.
func serverSettingName(path string) (string, bool) {
	prefix := configurationRestartPolicyRulesSectionSettings + "/"
	if !strings.HasPrefix(path, prefix) {
		return "", false
	}
	name := strings.TrimPrefix(path, prefix)
	if name == "" || strings.Contains(name, "*") {
		return "", false
	}
	return strings.ReplaceAll(name, "/", "."), true
}

// isZookeeperChangeRequiresReboot checks two ZooKeeper configs and decides whether the
// modifications require a reboot to be applied.
//
// An edit between two configured ensembles is applied from the generated config without a
// restart, subject to configurationRestartPolicy for zookeeper/*. Turning ZooKeeper on or off
// always restarts, whatever the policy says; a change between two configs that both amount to
// "no ZooKeeper" never does.
func isZookeeperChangeRequiresReboot(host *api.Host, a, b *api.ZookeeperConfig) bool {
	if a.Equals(b) {
		return false
	}
	// Enabling or disabling ZooKeeper adds or removes the whole generated section, and with it
	// <distributed_ddl>. ClickHouse builds its DDLWorker only at startup, so the policy gets no
	// say here: without a restart, enabling leaves ON CLUSTER DDL dead on every pre-existing
	// host, and disabling leaves a DDLWorker running against a section that is gone.
	if a.IsEmpty() != b.IsEmpty() {
		return true
	}
	// Both sides amount to "no ZooKeeper" yet compare unequal - a nil config against a minted
	// zero-value one, or two node-less configs differing only in root/identity/timeouts, all of
	// which InheritZookeeperFrom can produce. getHostZookeeper returns before rendering any of
	// those fields, so both revisions generate the same zero bytes and a restart would be a
	// no-op. Guard order makes b.IsEmpty() redundant here, but stating both reads as the
	// invariant it is.
	if a.IsEmpty() && b.IsEmpty() {
		return false
	}
	return isListedChangeRequiresReboot(host, []string{
		configurationRestartPolicyRulesSectionZookeeper + "/*",
	})
}

// isSettingsChangeRequiresReboot checks whether changes between two settings requires ClickHouse reboot
func isSettingsChangeRequiresReboot(host *api.Host, configurationRestartPolicyRulesSection string, a, b *api.Settings) bool {
	diff, equal := messagediff.DeepDiff(a, b)
	if equal {
		return false
	}
	affectedPaths := api.ListAffectedSettingsPathsFromDiff(a, b, diff, configurationRestartPolicyRulesSection)
	return isListedChangeRequiresReboot(host, affectedPaths)
}

// isServerSettingsChangeRequiresReboot checks server settings. A name ClickHouse lists as
// changeable_without_restart = Yes skips configurationRestartPolicy, unless the latest matching
// rule names this exact path and says "yes". Profiles, quotas, files and ZooKeeper are not
// server settings and are not consulted here.
func isServerSettingsChangeRequiresReboot(host *api.Host, a, b *api.Settings, changeableWithoutRestart SettingsChangeableWithoutRestart) bool {
	diff, equal := messagediff.DeepDiff(a, b)
	if equal {
		return false
	}
	affectedPaths := api.ListAffectedSettingsPathsFromDiff(a, b, diff, configurationRestartPolicyRulesSectionSettings)
	paths := make([]string, 0, len(affectedPaths))
	for _, path := range affectedPaths {
		if name, ok := serverSettingName(path); ok && changeableWithoutRestart.Contains(name) {
			if _, restart, exact := getLatestConfigMatch(host, path); !(exact && restart) {
				log.V(1).Info("ChangeRequiresReboot server_settings path: %s restart: false", path)
				continue
			}
		}
		paths = append(paths, path)
	}
	return isListedChangeRequiresReboot(host, paths)
}

func versionMatches(target any, versionConstraint string) bool {
	switch typed := target.(type) {
	case *api.Host:
		host := typed
		matches := hostVersionMatches(host, versionConstraint)
		log.Info("versionConstraint host: %s version: %s constraint: %s matches: %t", host.GetName(), host.Runtime.Version.Render(), versionConstraint, matches)
		return matches
	case *api.ClickHouseInstallation:
		chi := typed
		matches := chiVersionMatches(chi, versionConstraint)
		log.Info("versionConstraint chi: %s version: %s constraint: %s matches: %t", chi.GetName(), chi.EnsureRuntime().MinVersion.Render(), versionConstraint, matches)
		return matches
	default:
		panic("unexpected flow")
	}
}

// hostVersionMatches checks whether host's ClickHouse version matches specified constraint
func hostVersionMatches(host *api.Host, versionConstraint string) bool {
	// Special version of "*" - default version - has to satisfy all versions
	// Default version will also be used in case ClickHouse version is unknown.
	return (versionConstraint == "*") || host.Runtime.Version.Matches(versionConstraint)
}

// chiVersionMatches checks whether chi's ClickHouse version matches specified constraint
func chiVersionMatches(chi *api.ClickHouseInstallation, versionConstraint string) bool {
	// Special version of "*" - default version - has to satisfy all versions
	// Default version will also be used in case ClickHouse version is unknown.
	return (versionConstraint == "*") || chi.EnsureRuntime().MinVersion.Matches(versionConstraint)
}

// ruleMatches checks whether provided rule (rule set) matches specified `path`
func ruleMatches(set api.OperatorConfigRestartPolicyRuleSet, path string) (matches bool, value bool) {
	for pattern, val := range set {
		if pattern.Match(path) {
			matches = true
			value = val.IsTrue()
			return matches, value
		}
		// Only one check has to be performed since we are expecting rule to have one entry
		matches = false
		value = false
		return matches, value
	}
	matches = false
	value = false
	return matches, value
}

// getLatestConfigMatchValue returns value of the latest match of a specified `path` in ConfigRestartPolicy.Rules
// in case match found in ConfigRestartPolicy.Rules or false
func getLatestConfigMatchValue(host *api.Host, path string) (matches bool, value bool) {
	matches, value, _ = getLatestConfigMatch(host, path)
	return matches, value
}

// getLatestConfigMatch is getLatestConfigMatchValue that also reports whether the latest matching
// rule names `path` exactly, without a wildcard.
func getLatestConfigMatch(host *api.Host, path string) (matches bool, value bool, exact bool) {
	// Check all rules
	for _, r := range chop.Config().ClickHouse.ConfigRestartPolicy.Rules {
		// Check ClickHouse version of a particular rule
		if versionMatches(host, r.Version) {
			// Yes, this is ClickHouse version of the host.
			// Check whether any rule matches specified path.
			for _, rule := range r.Rules {
				if ruleMatches, ruleValue := ruleMatches(rule, path); ruleMatches {
					// Yes, rule matches specified path.
					matches = true
					value = ruleValue
					exact = ruleNamesPath(rule, path)
				}
			}
		}
	}
	return matches, value, exact
}

// ruleNamesPath checks whether provided rule (rule set) names `path` literally
func ruleNamesPath(set api.OperatorConfigRestartPolicyRuleSet, path string) bool {
	for pattern := range set {
		return string(pattern) == path
	}
	return false
}

func GetConfigMatchSpecs(chi *api.ClickHouseInstallation) (specs []*api.ChiSpec) {
	for _, r := range chop.Config().ClickHouse.Addons.Rules {
		if versionMatches(chi, r.Version) {
			specs = append(specs, &api.ChiSpec{
				Configuration: &api.Configuration{
					Users:    api.NewSettingsScalarFromMap(r.Spec.Configuration.Users),
					Profiles: api.NewSettingsScalarFromMap(r.Spec.Configuration.Profiles),
					Quotas:   api.NewSettingsScalarFromMap(r.Spec.Configuration.Quotas),
					Settings: api.NewSettingsScalarFromMap(r.Spec.Configuration.Settings),
					Files:    api.NewSettingsScalarFromMap(r.Spec.Configuration.Files),
				},
			})
		}
	}
	return specs
}

// isListedChangeRequiresReboot checks whether any of the provided paths requires reboot to apply configuration.
func isListedChangeRequiresReboot(host *api.Host, paths []string) bool {
	for _, path := range paths {
		if matches, restart := getLatestConfigMatchValue(host, path); matches && restart {
			return true
		}
	}
	return false
}

// Set of configurationRestartPolicyRulesSection<XXX> constants specifies prefixes used in
// CHOp configuration file clickhouse.configurationRestartPolicy.rules
// Check CHOp config file for current full list.
// Ex.:
//
//	configurationRestartPolicy:
//	  rules:
//	    # IMPORTANT!
//	    # Special version of "*" - default version - has to satisfy all ClickHouse versions.
//	    # Default version will also be used in case ClickHouse version is unknown.
//	    # ClickHouse version may be unknown due to host being down - for example, because of incorrect "settings" section.
//	    # ClickHouse is not willing to start in case incorrect/unknown settings are provided in config file.
//	    - version: "*"
//	      rules:
//	        - settings/*: "yes"
//	        - settings/dictionaries_config: "no"
//	        - settings/logger: "no"
//	        - settings/macros/*: "no"
//	        - settings/max_server_memory_*: "no"
//	        - settings/max_*_to_drop: "no"
//	        - settings/max_concurrent_queries: "no"
//	        - settings/models_config: "no"
//	        - settings/user_defined_executable_functions_config: "no"
//
//	        - zookeeper/*: "no"
//
//	        - files/*.xml: "yes"
//	        - files/config.d/*.xml: "yes"
//	        - files/config.d/*dict*.xml: "no"
//
//	        - profiles/default/background_*_pool_size: "yes"
//	        - profiles/default/max_*_for_server: "yes"
const (
	configurationRestartPolicyRulesSectionProfiles  = "profiles"
	configurationRestartPolicyRulesSectionQuotas    = "quotas"
	configurationRestartPolicyRulesSectionSettings  = "settings"
	configurationRestartPolicyRulesSectionFiles     = "files"
	configurationRestartPolicyRulesSectionZookeeper = "zookeeper"
)

// IsServerSettingsChanged reports whether global or local server settings differ from the ancestor.
// Profiles, quotas, files and ZooKeeper are not server settings.
func IsServerSettingsChanged(host *api.Host) bool {
	if host == nil {
		return false
	}
	var oldGlobal, newGlobal *api.Settings
	if host.HasAncestorCR() {
		oldGlobal = host.GetAncestorCR().GetSpec().GetConfiguration().GetSettings()
	}
	if host.HasCR() {
		newGlobal = host.GetCR().GetSpec().GetConfiguration().GetSettings()
	}
	if _, equal := messagediff.DeepDiff(oldGlobal, newGlobal); !equal {
		return true
	}
	var oldLocal *api.Settings
	if host.HasAncestor() {
		oldLocal = host.GetAncestor().GetSettings()
	}
	_, equal := messagediff.DeepDiff(oldLocal, host.Settings)
	return !equal
}

// IsConfigurationChangeRequiresReboot checks whether configuration changes requires a reboot.
// changeableWithoutRestart is the set of server settings ClickHouse applies without a restart.
// An empty set leaves the decision to configurationRestartPolicy.
func IsConfigurationChangeRequiresReboot(host *api.Host, changeableWithoutRestart SettingsChangeableWithoutRestart) bool {
	// Zookeeper
	{
		var old, new *api.ZookeeperConfig
		if host.HasAncestor() {
			old = host.GetAncestor().GetZookeeper()
		}
		new = host.GetZookeeper()
		if isZookeeperChangeRequiresReboot(host, old, new) {
			log.Info("ChangeRequiresReboot isZookeeperChangeRequiresReboot: true")
			return true
		}
	}
	// Profiles Global
	{
		var old, new *api.Settings
		if host.HasAncestorCR() {
			old = host.GetAncestorCR().GetSpec().GetConfiguration().GetProfiles()
		}
		if host.HasCR() {
			new = host.GetCR().GetSpec().GetConfiguration().GetProfiles()
		}
		if isSettingsChangeRequiresReboot(host, configurationRestartPolicyRulesSectionProfiles, old, new) {
			log.Info("ChangeRequiresReboot isSettingsChangeRequiresReboot(profiles global): true")
			return true
		}
	}
	// Quotas Global
	{
		var old, new *api.Settings
		if host.HasAncestorCR() {
			old = host.GetAncestorCR().GetSpec().GetConfiguration().GetQuotas()
		}
		if host.HasCR() {
			new = host.GetCR().GetSpec().GetConfiguration().GetQuotas()
		}
		if isSettingsChangeRequiresReboot(host, configurationRestartPolicyRulesSectionQuotas, old, new) {
			log.Info("ChangeRequiresReboot isSettingsChangeRequiresReboot(quotas global): true")
			return true
		}
	}
	// Settings Global
	{
		var old, new *api.Settings
		if host.HasAncestorCR() {
			old = host.GetAncestorCR().GetSpec().GetConfiguration().GetSettings()
		}
		if host.HasCR() {
			new = host.GetCR().GetSpec().GetConfiguration().GetSettings()
		}
		if isServerSettingsChangeRequiresReboot(host, old, new, changeableWithoutRestart) {
			log.Info("ChangeRequiresReboot isServerSettingsChangeRequiresReboot(settings global): true")
			return true
		}
	}
	// Settings Local
	{
		var old, new *api.Settings
		if host.HasAncestor() {
			old = host.GetAncestor().GetSettings()
		}
		new = host.Settings
		if isServerSettingsChangeRequiresReboot(host, old, new, changeableWithoutRestart) {
			log.Info("ChangeRequiresReboot isServerSettingsChangeRequiresReboot(settings local): true")
			return true
		}
	}
	// Files Global
	{
		var old, new *api.Settings
		if host.HasAncestorCR() {
			old = host.GetAncestorCR().GetSpec().GetConfiguration().GetFiles().Filter(
				nil,
				[]api.SettingsSection{api.SectionUsers},
				true,
			)
		}
		if host.HasCR() {
			new = host.GetCR().GetSpec().GetConfiguration().GetFiles().Filter(
				nil,
				[]api.SettingsSection{api.SectionUsers},
				true,
			)
		}
		if isSettingsChangeRequiresReboot(host, configurationRestartPolicyRulesSectionFiles, old, new) {
			log.Info("ChangeRequiresReboot isSettingsChangeRequiresReboot(files global): true")
			return true
		}
	}
	// Files Local
	{
		var old, new *api.Settings
		if host.HasAncestor() {
			old = host.GetAncestor().Files.Filter(
				nil,
				[]api.SettingsSection{api.SectionUsers},
				true,
			)
		}
		new = host.Files.Filter(
			nil,
			[]api.SettingsSection{api.SectionUsers},
			true,
		)
		if isSettingsChangeRequiresReboot(host, configurationRestartPolicyRulesSectionFiles, old, new) {
			log.Info("ChangeRequiresReboot isSettingsChangeRequiresReboot(files local): true")
			return true
		}
	}

	return false
}
