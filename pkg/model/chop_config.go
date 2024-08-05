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
	"fmt"

	"gopkg.in/d4l3k/messagediff.v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
)

// isZookeeperChangeRequiresReboot checks two ZooKeeper configs and decides,
// whether config modifications require a reboot to be applied
func isZookeeperChangeRequiresReboot(host *api.Host, a, b *api.ZookeeperConfig) bool {
	return !a.Equals(b)
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

// hostVersionMatches checks whether host's ClickHouse version matches specified constraint
func hostVersionMatches(host *api.Host, versionConstraint string) bool {
	// Special version of "*" - default version - has to satisfy all host versions
	// Default version will also be used in case ClickHouse version is unknown.
	// ClickHouse version may be unknown due to host being down - for example, because of incorrect "settings" section.
	// ClickHouse is not willing to start in case incorrect/unknown settings are provided in config file.
	return (versionConstraint == "*") || host.Runtime.Version.Matches(versionConstraint)
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
	// Check all rules
	for _, r := range chop.Config().ClickHouse.ConfigRestartPolicy.Rules {
		// Check ClickHouse version of a particular rule
		_ = fmt.Sprintf("%s", r.Version)
		if hostVersionMatches(host, r.Version) {
			// Yes, this is ClickHouse version of the host.
			// Check whether any rule matches specified path.
			for _, rule := range r.Rules {
				if ruleMatches, ruleValue := ruleMatches(rule, path); ruleMatches {
					// Yes, rule matches specified path.
					matches = true
					value = ruleValue
				}
			}
		}
	}
	return matches, value
}

// isListedChangeRequiresReboot checks whether any of the provided paths requires reboot to apply configuration
func isListedChangeRequiresReboot(host *api.Host, paths []string) bool {
	// Check whether any path matches ClickHouse configuration restart policy rules requires reboot
	for _, path := range paths {
		if matches, value := getLatestConfigMatchValue(host, path); matches {
			// This path matches configuration restart policy rule
			if value {
				// And this path not only matches, but requires reboot also - no need to find any other who requires reboot
				return true
			}
		}
	}
	// Last matching path with value "yes" not found.
	// This means either:
	// 1. Last matching path has value "no"
	// 2. No matching path found
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
//	        - zookeeper/*: "yes"
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

// IsConfigurationChangeRequiresReboot checks whether configuration changes requires a reboot
func IsConfigurationChangeRequiresReboot(host *api.Host) bool {
	// Zookeeper
	{
		var old, new *api.ZookeeperConfig
		if host.HasAncestor() {
			old = host.GetAncestor().GetZookeeper()
		}
		new = host.GetZookeeper()
		if isZookeeperChangeRequiresReboot(host, old, new) {
			return true
		}
	}
	// Profiles Global
	{
		var old, new *api.Settings
		if host.HasAncestorCR() {
			old = host.GetAncestorCR().GetSpec().Configuration.Profiles
		}
		if host.HasCR() {
			new = host.GetCR().GetSpec().Configuration.Profiles
		}
		if isSettingsChangeRequiresReboot(host, configurationRestartPolicyRulesSectionProfiles, old, new) {
			return true
		}
	}
	// Quotas Global
	{
		var old, new *api.Settings
		if host.HasAncestorCR() {
			old = host.GetAncestorCR().GetSpec().Configuration.Quotas
		}
		if host.HasCR() {
			new = host.GetCR().GetSpec().Configuration.Quotas
		}
		if isSettingsChangeRequiresReboot(host, configurationRestartPolicyRulesSectionQuotas, old, new) {
			return true
		}
	}
	// Settings Global
	{
		var old, new *api.Settings
		if host.HasAncestorCR() {
			old = host.GetAncestorCR().GetSpec().Configuration.Settings
		}
		if host.HasCR() {
			new = host.GetCR().GetSpec().Configuration.Settings
		}
		if isSettingsChangeRequiresReboot(host, configurationRestartPolicyRulesSectionSettings, old, new) {
			return true
		}
	}
	// Settings Local
	{
		var old, new *api.Settings
		if host.HasAncestor() {
			old = host.GetAncestor().Settings
		}
		new = host.Settings
		if isSettingsChangeRequiresReboot(host, configurationRestartPolicyRulesSectionSettings, old, new) {
			return true
		}
	}
	// Files Global
	{
		var old, new *api.Settings
		if host.HasAncestorCR() {
			old = host.GetAncestorCR().GetSpec().Configuration.Files.Filter(
				nil,
				[]api.SettingsSection{api.SectionUsers},
				true,
			)
		}
		if host.HasCR() {
			new = host.GetCR().GetSpec().Configuration.Files.Filter(
				nil,
				[]api.SettingsSection{api.SectionUsers},
				true,
			)
		}
		if isSettingsChangeRequiresReboot(host, configurationRestartPolicyRulesSectionFiles, old, new) {
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
			return true
		}
	}

	return false
}
