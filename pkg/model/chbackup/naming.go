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

package chbackup

import (
	"fmt"
	"regexp"
	"strings"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// SlotTolerance is how long after a slot of an entry, in seconds, the time a backup of a cluster with a naming template
// is named after still counts for the slot: such a backup is named after the time its run started.
const SlotTolerance = 1800

// Naming tells how the backups of a cluster are named on its hosts. Without a template a backup has one name on every
// host, its key; with one, the name of the backup of every shard tells the shard and the time of the backup, its key,
// YYYYMMDDHHMMSS, with a T before the hour for a backup made at once.
type Naming struct {
	// Template is the naming template, "" for the default naming
	Template string
	// Cluster, Installation and Namespace are the values of {cluster}, {chi} and {namespace}
	Cluster      string
	Installation string
	Namespace    string
	// Shards are the shards of the cluster, in the order of system.clusters, by their names, the values of their {shard}
	// macro: the shard of index i has the number i+1, its shard_num
	Shards []string
	// ShardOf is the index in Shards of the shard of every host
	ShardOf map[string]int
}

// shardOf returns the name and the number, from 1, of the shard of the host, false for a host of no shard of the cluster.
func (n Naming) shardOf(host string) (name string, num int, ok bool) {
	index, ok := n.ShardOf[host]
	if !ok || (index < 0) || (index >= len(n.Shards)) {
		return "", 0, false
	}
	return n.Shards[index], index + 1, true
}

// IsTemplate reports whether the backups are named by a template.
func (n Naming) IsTemplate() bool {
	return n.Template != ""
}

// HoldsPrefix reports whether the template holds {prefix}.
func (n Naming) HoldsPrefix() bool {
	return api.BackupNamingHolds(n.Template, api.BackupNamingPrefix)
}

// Tolerance returns the slot tolerance of the backups of the naming, in seconds.
func (n Naming) Tolerance() int {
	if n.IsTemplate() {
		return SlotTolerance
	}
	return 0
}

func (n Naming) values(shard string, shardNum int, prefix, tag string) api.BackupNameValues {
	return api.BackupNameValues{Shard: shard, ShardNum: shardNum, Shards: n.Shards, Cluster: n.Cluster, Chi: n.Installation,
		Namespace: n.Namespace, Prefix: prefix, Tag: tag}
}

// Parse reads a name the template makes on the cluster, prefix for {prefix} or any when empty.
func (n Naming) Parse(name, prefix string) (api.ParsedBackupName, bool) {
	return api.ParseBackupName(n.Template, n.values("", 0, prefix, ""), name)
}

// Render returns the name the template gives the backup key of the shard of the name and the number, from 1.
func (n Naming) Render(shard string, shardNum int, key, prefix string) (string, error) {
	return api.RenderBackupName(n.Template, n.values(shard, shardNum, prefix, key))
}

// FirstName returns the name the template gives the backup key of the first shard, which tells the backup of every
// shard.
func (n Naming) FirstName(key, prefix string) (string, error) {
	first := ""
	if len(n.Shards) > 0 {
		first = n.Shards[0]
	}
	return n.Render(first, 1, key, prefix)
}

// HostName returns the name of the backup key on the host.
func (n Naming) HostName(host, key, prefix string) (string, error) {
	if !n.IsTemplate() {
		return key, nil
	}
	shard, shardNum, ok := n.shardOf(host)
	if !ok {
		return "", fmt.Errorf("the shard of %s is unknown, whose backup the naming template names by its shard", host)
	}
	name, err := n.Render(shard, shardNum, key, prefix)
	if err != nil {
		return "", fmt.Errorf("the naming template '%s' makes no valid backup name of the backup %s of %s: %w", n.Template, key, host, err)
	}
	return name, nil
}

// Key returns the key of the backup a name, or a time alone with a template, names, and the prefix of its names.
func (n Naming) Key(field, value, prefix string) (key, namingPrefix string, err error) {
	if !n.IsTemplate() {
		return value, prefix, nil
	}
	if api.BackupTagRegexp.MatchString(value) && api.BackupTagValid(strings.Replace(value, "T", "", 1)) {
		if n.HoldsPrefix() && (prefix == "") {
			return "", "", fmt.Errorf("%s %s tells the time of the backup alone, while the naming template '%s' holds {prefix}: "+
				"name the backup in full, as the name of the backup of any shard", field, value, n.Template)
		}
		return value, prefix, nil
	}
	parsed, ok := n.Parse(value, prefix)
	if !ok {
		return "", "", fmt.Errorf("%s %s is no name the naming template '%s' makes on cluster %s, nor the time of one, YYYYMMDDHHMMSS",
			field, value, n.Template, n.Cluster)
	}
	return parsed.Tag, parsed.Prefix, nil
}

// SeriesKey returns the key of a backup of the series of a schedule of the prefix the host lists, false for a backup
// of no such series.
func (n Naming) SeriesKey(host, name, prefix string) (string, bool) {
	if !n.IsTemplate() {
		return name, retentionPattern(prefix).MatchString(name)
	}
	if n.HoldsPrefix() && (prefix == "") {
		return "", false
	}
	if !n.HoldsPrefix() {
		prefix = ""
	}
	parsed, ok := n.Parse(name, prefix)
	if !ok || !n.ofHost(parsed, host) {
		return "", false
	}
	return parsed.Tag, true
}

// ofHost reports whether a parsed name is that of the backup of the shard of the host: of its name with {shard}, of its
// number with {shardNum}.
func (n Naming) ofHost(parsed api.ParsedBackupName, host string) bool {
	shard, shardNum, ok := n.shardOf(host)
	if !ok {
		return false
	}
	if api.BackupNamingHolds(n.Template, api.BackupNamingShard) && (parsed.Shard != shard) {
		return false
	}
	if api.BackupNamingHolds(n.Template, api.BackupNamingShardNum) && (parsed.ShardNum != shardNum) {
		return false
	}
	return true
}

// KeyTime returns the time the backup of a key is named after, YYYYMMDDHHMMSS, and whether it was made at once.
func (n Naming) KeyTime(key, prefix string) (string, bool) {
	value := key
	if !n.IsTemplate() {
		value = strings.TrimPrefix(key, prefix+"-")
	}
	return strings.Replace(value, "T", "", 1), strings.Contains(value, "T")
}

// retentionPattern matches the names of the backups of the series of a schedule of the prefix with the default naming.
func retentionPattern(prefix string) *regexp.Regexp {
	return regexp.MustCompile("^" + regexp.QuoteMeta(prefix) + "-[0-9]{8}T?[0-9]{6}$")
}
