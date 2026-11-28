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
	"strings"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// SlotTolerance is in seconds. A templated name uses the run start time, which can be later than the scheduled time.
const SlotTolerance = 1800

// Without a template the key is the backup name, with one it is the tag (YYYYMMDDHHMMSS or YYYYMMDDTHHMMSS).
type Naming struct {
	// Template is "" for the default naming
	Template     string
	Cluster      string
	Installation string
	Namespace    string
	// Shards are in system.clusters order, index i has shard_num i+1
	Shards  []string
	ShardOf map[string]int
}

func (n Naming) shardOf(host string) (name string, num int, ok bool) {
	index, ok := n.ShardOf[host]
	if !ok || (index < 0) || (index >= len(n.Shards)) {
		return "", 0, false
	}
	return n.Shards[index], index + 1, true
}

func (n Naming) IsTemplate() bool {
	return n.Template != ""
}

func (n Naming) HoldsPrefix() bool {
	return api.BackupNamingHolds(n.Template, api.BackupNamingPrefix)
}

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

// An empty prefix matches any {prefix}.
func (n Naming) Parse(name, prefix string) (api.ParsedBackupName, bool) {
	return api.ParseBackupName(n.Template, n.values("", 0, prefix, ""), name)
}

func (n Naming) Render(shard string, shardNum int, key, prefix string) (string, error) {
	return api.RenderBackupName(n.Template, n.values(shard, shardNum, prefix, key))
}

func (n Naming) FirstName(key, prefix string) (string, error) {
	first := ""
	if len(n.Shards) > 0 {
		first = n.Shards[0]
	}
	return n.Render(first, 1, key, prefix)
}

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

// With a template, the value can be a full name or only the tag.
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

func (n Naming) SeriesKey(host, name, prefix string) (string, bool) {
	if !n.IsTemplate() {
		tag, ok := strings.CutPrefix(name, prefix+"-")
		return name, ok && api.BackupTagRegexp.MatchString(tag)
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

// KeyTime also reports whether the tag has a T.
func (n Naming) KeyTime(key, prefix string) (string, bool) {
	value := key
	if !n.IsTemplate() {
		value = strings.TrimPrefix(key, prefix+"-")
	}
	return strings.Replace(value, "T", "", 1), strings.Contains(value, "T")
}
