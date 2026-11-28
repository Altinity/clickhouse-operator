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
	"strconv"
	"strings"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
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

const (
	// nameHashLength is the length of the hash that keeps shortened names unique.
	nameHashLength = 8
)

// boundedName joins base and suffix with a dash, shortening base and inserting a hash of it when
// the result would exceed maxLength. Equal inputs always yield equal names.
func boundedName(base, suffix string, maxLength int) string {
	name := joinName(base, suffix)
	if len(name) <= maxLength {
		return name
	}
	hash := util.HashIntoString([]byte(base))[:nameHashLength]
	return joinName(boundedHead(base, suffix, maxLength), hash, suffix)
}

// boundedHead returns what a name boundedName shortens keeps of base, ahead of the hash.
func boundedHead(base, suffix string, maxLength int) string {
	headLength := maxLength - len(joinName("", strings.Repeat("0", nameHashLength), suffix)) - 1
	return strings.TrimRight(util.StringHead(base, headLength), "-.")
}

func joinName(parts ...string) string {
	var nonEmpty []string
	for _, part := range parts {
		if part != "" {
			nonEmpty = append(nonEmpty, part)
		}
	}
	return strings.Join(nonEmpty, "-")
}

// ResolveBackupName returns the backup name of a one-off backup. A generated name is derived from the
// scheduled time, not the time of the reconcile, so every reconcile resolves the same name. A
// backup that runs a schedule, which takes no explicit name, is named with the backup name prefix it
// recorded of the schedule: the prefix lets the recovery prefix guard tell where it came from, and the
// form of the time keeps it out of the series the schedule's retention prunes. Any other backup takes
// its explicit name, or one derived from the CR name.
func ResolveBackupName(chb *api.ClickHouseBackup) string {
	due := chb.DueTime().UTC()
	if prefix := chb.Status.Schedule.GetBackupNamePrefix(); (chb.Spec.ScheduleRef != nil) && (prefix != "") {
		return joinName(prefix, due.Format(api.BackupTagLayoutAtOnce))
	}
	if chb.Spec.BackupName != "" {
		return chb.Spec.BackupName
	}
	return boundedName(chb.GetName(), strconv.FormatInt(due.Unix(), 10), api.BackupNameMaxLength)
}

// A zero BackupNaming means the default naming.
type BackupNaming struct {
	Template     string
	Cluster      string
	Installation string
	Namespace    string
	FirstShard   string
}

func (t *Topology) BackupNaming() BackupNaming {
	if t.Naming() == "" {
		return BackupNaming{}
	}
	naming := BackupNaming{Template: t.Naming(), Cluster: t.cluster(), Installation: t.installation, Namespace: t.namespace}
	if (len(t.clusters) > 0) && (len(t.clusters[0].shards) > 0) {
		naming.FirstShard = t.clusters[0].shards[0].name
	}
	return naming
}

// The name on the first shard identifies the backup of every shard.
func (n BackupNaming) Name(prefix, tag string) (string, error) {
	return api.RenderBackupName(n.Template, api.BackupNameValues{
		Shard: n.FirstShard, ShardNum: 1, Cluster: n.Cluster, Chi: n.Installation, Namespace: n.Namespace, Prefix: prefix, Tag: tag,
	})
}

func ResolveTemplateBackupName(chb *api.ClickHouseBackup, naming BackupNaming) (string, error) {
	tag := api.BackupTag(chb.DueTime().UTC(), true)
	if chb.Spec.BackupName != "" {
		if !api.BackupTagRegexp.MatchString(chb.Spec.BackupName) {
			return "", fmt.Errorf("backupName %q is no time a backup is named after, YYYYMMDDHHMMSS or YYYYMMDDTHHMMSS, as "+
				"the naming template %q of cluster %s names the backup of every shard by it", chb.Spec.BackupName, naming.Template, naming.Cluster)
		}
		tag = chb.Spec.BackupName
	}
	prefix := boundedName(chb.GetName(), "", api.BackupNamePrefixMaxLength)
	if record := chb.Status.Schedule; (chb.Spec.ScheduleRef != nil) && (record != nil) && (record.BackupNamePrefix != "") {
		prefix = record.BackupNamePrefix
	}
	name, err := naming.Name(prefix, tag)
	if err != nil {
		return "", fmt.Errorf("the naming template %q of cluster %s names no backup of backupName %q: %w", naming.Template,
			naming.Cluster, chb.Spec.BackupName, err)
	}
	return name, nil
}

// BackupNamePrefix returns the prefix of the backups a schedule makes, which also scopes its
// retention. It defaults to the namespace, installation and schedule names: two installations may
// share a storage path, a restored one with the one its backup came from above all, which
// is often of the same name in another namespace, and a schedule of the same name on each must not
// prune the backups of the other.
func BackupNamePrefix(chbs *api.ClickHouseBackupSchedule) string {
	if chbs.Spec.BackupNamePrefix != "" {
		return chbs.Spec.BackupNamePrefix
	}
	return boundedName(joinName(chbs.GetNamespace(), chbs.Spec.ClickHouseInstallation, chbs.GetName()), "", api.BackupNamePrefixMaxLength)
}

// HasDefaultBackupNamePrefix reports whether a backup name carries the backup name prefix a schedule of
// the installation defaults to, whatever the schedule: it starts with the namespace and the installation
// name as BackupNamePrefix joins them, or, for a prefix BackupNamePrefix shortens within them, with as much
// of them as the shortened prefix keeps.
func HasDefaultBackupNamePrefix(namespace, chiName, backupName string) bool {
	installation := joinName(namespace, chiName)
	for _, root := range []string{installation, boundedHead(installation, "", api.BackupNamePrefixMaxLength)} {
		if strings.HasPrefix(backupName, root+"-") {
			return true
		}
	}
	return false
}

func ScheduledBackupName(prefix string, naming Naming, slot string) (string, error) {
	if naming.IsTemplate() {
		return naming.FirstName(slot, prefix)
	}
	return joinName(prefix, slot), nil
}
