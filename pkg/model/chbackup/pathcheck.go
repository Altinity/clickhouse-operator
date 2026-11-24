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
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
)

// An embedded backup writes its data to a different location than a clickhouse-backup upload.
const (
	SettingUseEmbeddedBackupRestore = "CLICKHOUSE_USE_EMBEDDED_BACKUP_RESTORE"
	SettingEmbeddedBackupDisk       = "CLICKHOUSE_EMBEDDED_BACKUP_DISK"
)

// None of them is a credential. S3_REGION is left out: bucket names are global, so same-name buckets in two
// AWS partitions count as one storage, and the check refuses rather than misses a shared path.
var PathCheckKeys = []string{
	"REMOTE_STORAGE",
	"S3_BUCKET", "S3_ENDPOINT", "S3_PATH", "S3_OBJECT_DISK_PATH",
	"GCS_BUCKET", "GCS_ENDPOINT", "GCS_PATH", "GCS_OBJECT_DISK_PATH",
	"AZBLOB_ACCOUNT_NAME", "AZBLOB_CONTAINER", "AZBLOB_ENDPOINT_SUFFIX", "AZBLOB_PATH", "AZBLOB_OBJECT_DISK_PATH",
	"COS_URL", "COS_PATH", "COS_OBJECT_DISK_PATH",
	"FTP_ADDRESS", "FTP_PATH", "FTP_OBJECT_DISK_PATH",
	"SFTP_ADDRESS", "SFTP_PORT", "SFTP_PATH", "SFTP_OBJECT_DISK_PATH",
	SettingUseEmbeddedBackupRestore, SettingEmbeddedBackupDisk,
}

var storagePrefixes = map[string]string{"s3": "S3", "gcs": "GCS", "azblob": "AZBLOB", "cos": "COS", "ftp": "FTP", "sftp": "SFTP"}

const (
	MacroReplica         = "replica"
	MacroAllShardedShard = config.AllShardsOneReplicaClusterName + "-shard"
)

type PathHost struct {
	Name   string
	Shard  int
	Macros map[string]string
}

type PathCluster struct {
	Name   string
	Shards []string
	// Hosts are in shard order, then replica order.
	Hosts        []PathHost
	Unverifiable string
	// A nil value means unset.
	Settings map[string]*string
}

type PathCheck struct {
	Clusters []PathCluster
	// Self is the index of the target cluster.
	Self         int
	Installation string
	Namespace    string
	Operation    string
	// ReplicaSelection is empty for a restore.
	ReplicaSelection api.ReplicaSelection
	ValidateTopology bool
	// Nil means the storage root.
	DefaultPath           *string
	DefaultObjectDiskPath *string
	NamingTemplate        string
	// One KEY=VALUE list per possible run. A run without overrides is always checked too.
	ConfigOverrides [][]string
}

type PathCheckResult struct {
	// Path and ObjectDiskPath keep their macros unexpanded.
	Path           string
	ObjectDiskPath string
	StoragePrefix  string
	ClusterName    string
	Unknown        string
	Advice         []string
	Log            string
}

func identityDefault(key string) string {
	switch key {
	case "AZBLOB_ENDPOINT_SUFFIX":
		return "core.windows.net"
	case "SFTP_PORT":
		return "22"
	}
	return ""
}

func normalizeIdentity(key, value string) string {
	value = strings.TrimSpace(value)
	switch {
	case strings.HasSuffix(key, "_ENDPOINT") || (key == "COS_URL"):
		return normalizeEndpoint(value)
	case strings.HasSuffix(key, "_ADDRESS") || strings.HasSuffix(key, "_ENDPOINT_SUFFIX"):
		return strings.ToLower(strings.TrimRight(value, "/"))
	case strings.HasSuffix(key, "_PORT"):
		if port, err := strconv.Atoi(value); err == nil {
			return strconv.Itoa(port)
		}
	}
	return value
}

func normalizeEndpoint(endpoint string) string {
	u, err := url.Parse(endpoint)
	if (err != nil) || (u.Scheme == "") || (u.Host == "") {
		return strings.ToLower(strings.TrimRight(endpoint, "/"))
	}
	u.Scheme = strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Hostname())
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	if port := u.Port(); (port != "") && (port != map[string]string{"http": "80", "https": "443"}[u.Scheme]) {
		host += ":" + port
	}
	u.Host = host
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = ""
	return u.String()
}

func trimPath(storage, value string) string {
	both, trailing := " \t\r\n/", ""
	if (storage == "ftp") || (storage == "sftp") {
		both, trailing = " \t\r\n", "/"
	}
	value = strings.Trim(value, both)
	if trailing != "" {
		value = strings.TrimRight(value, trailing)
	}
	return value
}

// CleanPath works like path.Clean, but a relative path keeps its leading "..".
func CleanPath(path string) string {
	rooted := ""
	if strings.HasPrefix(path, "/") {
		rooted = "/"
	}
	var elements []string
	for _, element := range strings.Split(path, "/") {
		switch element {
		case "", ".":
		case "..":
			if (len(elements) > 0) && (elements[len(elements)-1] != "..") {
				elements = elements[:len(elements)-1]
			} else if rooted == "" {
				elements = append(elements, "..")
			}
		default:
			elements = append(elements, element)
		}
	}
	return rooted + strings.Join(elements, "/")
}

var macroRegexp = regexp.MustCompile(`\{([^{}]*)\}`)

// ExpandMacros keeps {replica}, and returns the first unknown macro or "".
func ExpandMacros(template, installation, namespace, cluster, shard string) (string, string) {
	return expandMacros(template, map[string]string{"installation": installation, "namespace": namespace, "cluster": cluster,
		"shard": shard}, MacroReplica)
}

// As clickhouse-backup does, expand system.macros in paths and leave other macros as they are.
func expandMacros(template string, values map[string]string, keep ...string) (string, string) {
	unknown := ""
	expanded := macroRegexp.ReplaceAllStringFunc(template, func(macro string) string {
		name := macro[1 : len(macro)-1]
		if slices.Contains(keep, name) {
			return macro
		}
		if value, known := values[name]; known {
			return value
		}
		if unknown == "" {
			unknown = macro
		}
		return macro
	})
	return expanded, unknown
}

func (c PathCheck) hostMacros(cluster PathCluster, host PathHost) map[string]string {
	macros := map[string]string{"installation": c.Installation, "namespace": c.Namespace, "cluster": cluster.Name}
	if (host.Shard >= 0) && (host.Shard < len(cluster.Shards)) {
		macros["shard"] = cluster.Shards[host.Shard]
	}
	for name, value := range host.Macros {
		macros[name] = value
	}
	return macros
}

// With no hosts listed, it returns one host per shard.
func hostsOf(cluster PathCluster) []PathHost {
	if len(cluster.Hosts) > 0 {
		return cluster.Hosts
	}
	var hosts []PathHost
	for i, shard := range cluster.Shards {
		hosts = append(hosts, PathHost{Name: shard, Shard: i})
	}
	return hosts
}

func setting(cluster PathCluster, key string) (string, bool) {
	if value := cluster.Settings[key]; value != nil {
		return *value, true
	}
	return "", false
}

func orDefault(value string, set bool, def *string) string {
	if set {
		return value
	}
	if def != nil {
		return *def
	}
	return ""
}

type embeddedMode struct {
	embedded bool
	disk     string
}

func (c PathCheck) embeddedModes(cluster PathCluster, self bool) []embeddedMode {
	variants := [][]string{nil}
	if self {
		variants = append(variants, c.ConfigOverrides...)
	}
	var modes []embeddedMode
	for _, overrides := range variants {
		use, _ := setting(cluster, SettingUseEmbeddedBackupRestore)
		disk, _ := setting(cluster, SettingEmbeddedBackupDisk)
		for _, override := range api.ConfigOverrides(overrides) {
			key, value, _ := strings.Cut(override, "=")
			switch key {
			case SettingUseEmbeddedBackupRestore:
				use = value
			case SettingEmbeddedBackupDisk:
				disk = value
			}
		}
		mode := embeddedMode{disk: strings.TrimSpace(disk)}
		if use = strings.TrimSpace(use); use != "" {
			// An unparsable value stops clickhouse-backup. Count it as embedded, so the check compares more paths.
			on, err := strconv.ParseBool(use)
			mode.embedded = on || (err != nil)
		}
		if !mode.embedded {
			mode.disk = ""
		}
		if !slices.Contains(modes, mode) {
			modes = append(modes, mode)
		}
	}
	return modes
}

type pathWriter struct {
	cluster            int
	index              int
	what               string
	replica            string
	shard, clusterName string
	// Writers with different names do not conflict on one path.
	name  string
	hosts []PathHost
}

type pathLocation struct {
	identity, what, path string
}

// Shards of a cluster with a naming template write different backup names, so they may share a path.
func (c PathCheck) Check() (PathCheckResult, error) {
	var result PathCheckResult
	inconclusive := func(name, what string) (PathCheckResult, error) {
		result.Unknown = what
		if len(c.Clusters) > 1 {
			return result, fmt.Errorf("the storage path check cannot tell whether cluster %s shares a storage path with another of "+
				"the %d backup-enabled clusters of the installation, which would mix their backups and restore one with the data "+
				"of the other: %s", name, len(c.Clusters), what)
		}
		result.Advice = append(result.Advice, "the storage path check is skipped: "+what)
		return result, nil
	}
	if len(c.Clusters) == 0 {
		result.Unknown = "the installation has no backup-enabled cluster"
		result.Advice = append(result.Advice, "the storage path check is skipped: "+result.Unknown)
		return result, nil
	}
	order := []int{c.Self}
	for i := range c.Clusters {
		if i != c.Self {
			order = append(order, i)
		}
	}
	everyReplica := (c.Operation == api.BackupRunOperationBackup) && (c.ReplicaSelection == api.ReplicaSelectionAllReplicas)
	owners := map[pathLocation][]pathWriter{}
	shared := false
	selfName := c.Clusters[c.Self].Name
	for _, index := range order {
		cluster := c.Clusters[index]
		self := index == c.Self
		name := cluster.Name
		if cluster.Unverifiable != "" {
			what := fmt.Sprintf("the operator cannot tell the sidecar settings of cluster %s: %s", name, cluster.Unverifiable)
			if self {
				return inconclusive(name, what)
			}
			result.Advice = append(result.Advice, fmt.Sprintf("cluster %s is left out of the storage path check: %s", name, what))
			continue
		}
		storage, _ := setting(cluster, "REMOTE_STORAGE")
		prefix := storagePrefixes[storage]
		if prefix == "" {
			what := fmt.Sprintf("it keeps its backups in %s storage, where the check cannot tell their paths", storage)
			if storage == "" {
				what = "its configuration does not set remote_storage"
			}
			if self {
				return inconclusive(name, what)
			}
			result.Advice = append(result.Advice, fmt.Sprintf("cluster %s is left out of the storage path check: %s", name, what))
			continue
		}
		identity := []string{storage}
		for _, key := range PathCheckKeys {
			if strings.HasPrefix(key, prefix+"_") && (key != prefix+"_PATH") && (key != prefix+"_OBJECT_DISK_PATH") {
				value, set := setting(cluster, key)
				if !set {
					value = identityDefault(key)
				}
				identity = append(identity, normalizeIdentity(key, value))
			}
		}
		value, set := setting(cluster, prefix+"_PATH")
		template := CleanPath(trimPath(storage, orDefault(value, set, c.DefaultPath)))
		value, set = setting(cluster, prefix+"_OBJECT_DISK_PATH")
		object := trimPath(storage, orDefault(value, set, c.DefaultObjectDiskPath))
		if object != "" {
			if object = CleanPath(object); object == "" {
				object = "."
			}
		}
		modes := c.embeddedModes(cluster, self)
		// An embedded backup without a backup disk goes directly under the object disk path (or the storage root).
		straight := slices.ContainsFunc(modes, func(mode embeddedMode) bool { return mode.embedded && (mode.disk == "") })
		if self {
			result.Path = template
			replica := strings.Contains(template, "{"+MacroReplica+"}")
			switch {
			case (c.Operation == api.BackupRunOperationBackup) && !everyReplica && replica:
				return result, fmt.Errorf("the storage path '%s' of cluster %s holds {replica}: a backup of the first replica of each "+
					"shard would not be at the path every other replica restores its schema from", template, name)
			case (c.Operation == api.BackupRunOperationRestore) && replica && !c.ValidateTopology:
				result.Advice = append(result.Advice, fmt.Sprintf("the storage path '%s' of cluster %s holds {replica}: every replica "+
					"restores its schema from its own path, which holds the backup only if it is one of every replica", template, name))
			}
		}

		paths := []struct{ what, value string }{{"storage path", template}}
		if (object != "") || straight {
			paths = append(paths, struct{ what, value string }{"object disk path", object})
		}
		hosts := hostsOf(cluster)
		var checked []struct{ what, value string }
		for _, p := range paths {
			unknownMacro := ""
			for _, host := range hosts {
				if _, macro := expandMacros(p.value, c.hostMacros(cluster, host)); macro != "" {
					unknownMacro = macro
					break
				}
			}
			if unknownMacro == "" {
				checked = append(checked, p)
				continue
			}
			if self {
				result.Unknown = fmt.Sprintf("the path '%s' of cluster %s holds %s, a macro whose value it does not know", p.value, name,
					unknownMacro)
				result.Advice = append(result.Advice, fmt.Sprintf("the storage path check skips the %s '%s' of cluster %s: it holds %s, "+
					"a macro the operator does not define, whose value it does not know; make sure it differs on every host that writes "+
					"a backup of one name at once, or they would write over each other's backup", p.what, p.value, name, unknownMacro))
				continue
			}
			result.Advice = append(result.Advice, fmt.Sprintf("cluster %s is left out of the storage path check of its %s: its path "+
				"'%s' holds %s, a macro whose value it does not know", name, p.what, p.value, unknownMacro))
		}
		if self && (result.Unknown == "") {
			result.ObjectDiskPath, result.StoragePrefix, result.ClusterName = object, prefix, name
		}

		writers := c.writersOf(index, self && everyReplica, hosts)
		if self && (c.Operation == api.BackupRunOperationBackup) {
			result.Advice = append(result.Advice, embeddedDiskAdvice(name, modes, writers)...)
		}
		for _, writer := range writers {
			for _, p := range checked {
				seen := map[string]bool{}
				for _, host := range writer.hosts {
					expanded, _ := expandMacros(p.value, c.hostMacros(cluster, host))
					path := CleanPath(expanded)
					if path == "" {
						path = "."
					}
					if seen[path] {
						continue
					}
					seen[path] = true
					location := pathLocation{strings.Join(identity, "\x00"), p.what, path}
					for _, owner := range owners[location] {
						switch {
						case (owner.cluster == index) && (owner.index == writer.index):
							continue
						case (owner.cluster != c.Self) && !self:
							// A conflict between two other clusters fails their runs, not this one.
							continue
						case (owner.cluster == index) && (owner.name != writer.name):
							shared = true
							continue
						}
						return result, c.sharedLocation(p.what, path, storageRoot(storage, cluster), writer, owner, straight)
					}
					owners[location] = append(owners[location], writer)
				}
			}
		}
	}
	switch {
	case shared:
		result.Log = fmt.Sprintf("storage path check: the shards of cluster %s share a path, where the naming template '%s' names "+
			"the backup of each apart, and no other cluster has it", selfName, c.NamingTemplate)
	case everyReplica:
		result.Log = fmt.Sprintf("storage path check: every replica of cluster %s has a path of its own", selfName)
	default:
		result.Log = fmt.Sprintf("storage path check: every shard of cluster %s has a path of its own", selfName)
	}
	return result, nil
}

func (c PathCheck) writersOf(index int, everyReplica bool, hosts []PathHost) []pathWriter {
	cluster := c.Clusters[index]
	shardName := func(host PathHost) string {
		if (host.Shard >= 0) && (host.Shard < len(cluster.Shards)) {
			return cluster.Shards[host.Shard]
		}
		return ""
	}
	nameOf := func(host PathHost) string {
		if (index == c.Self) && (c.NamingTemplate != "") {
			return shardName(host)
		}
		return ""
	}
	var writers []pathWriter
	if everyReplica {
		for i, host := range hosts {
			replica := host.Name
			if value := host.Macros[MacroReplica]; value != "" {
				replica = value
			}
			writers = append(writers, pathWriter{cluster: index, index: i, name: nameOf(host), hosts: []PathHost{host},
				what:    fmt.Sprintf("replica %s of shard %s of cluster %s", replica, shardName(host), cluster.Name),
				replica: replica, shard: shardName(host), clusterName: cluster.Name})
		}
		return writers
	}
	byShard := map[int]int{}
	for _, host := range hosts {
		at, found := byShard[host.Shard]
		if !found {
			at = len(writers)
			byShard[host.Shard] = at
			writers = append(writers, pathWriter{cluster: index, index: at, name: nameOf(host),
				what:  fmt.Sprintf("shard %s of cluster %s", shardName(host), cluster.Name),
				shard: shardName(host), clusterName: cluster.Name})
		}
		writers[at].hosts = append(writers[at].hosts, host)
	}
	return writers
}

func (c PathCheck) sharedLocation(what, path, root string, writer, owner pathWriter, straight bool) error {
	writers := fmt.Sprintf("%s and %s", owner.what, writer.what)
	writes := "write their backups"
	hint := fmt.Sprintf("give every shard of every cluster %s of its own, e.g. with {cluster} and {shard}", article(what))
	switch {
	case (owner.clusterName != writer.clusterName):
	case (owner.replica != "") && (writer.replica != "") && (owner.shard == writer.shard):
		writers = fmt.Sprintf("replicas %s and %s of shard %s of cluster %s", owner.replica, writer.replica, writer.shard,
			writer.clusterName)
		writes = "write a backup of one name"
		hint = fmt.Sprintf("give every replica %s of its own, e.g. with {replica}", article(what))
	case (owner.replica == "") && (writer.replica == ""):
		writers = fmt.Sprintf("shards %s and %s of cluster %s", owner.shard, writer.shard, writer.clusterName)
		writes = "write a backup of one name"
	default:
		writes = "write a backup of one name"
	}
	if (what == "object disk path") && straight {
		hint += ", the more so as embedded backups without a backup disk are written there whole"
	}
	return fmt.Errorf("%s '%s' is not unique: %s both %s there; %s", what, locate(root, path), writers, writes, hint)
}

var storageRoots = map[string]string{"s3": "S3_BUCKET", "gcs": "GCS_BUCKET", "azblob": "AZBLOB_CONTAINER", "cos": "COS_URL",
	"ftp": "FTP_ADDRESS", "sftp": "SFTP_ADDRESS"}

func storageRoot(storage string, cluster PathCluster) string {
	value, _ := setting(cluster, storageRoots[storage])
	value = strings.Trim(strings.TrimSpace(value), "/")
	if strings.Contains(value, "://") {
		return value
	}
	return storage + "://" + value
}

func locate(root, path string) string {
	if path == "." {
		return root + "/"
	}
	return root + "/" + strings.TrimPrefix(path, "/")
}

// The check cannot see the location of that disk, so it cannot prove the writers do not collide.
func embeddedDiskAdvice(cluster string, modes []embeddedMode, writers []pathWriter) []string {
	names := map[string]int{}
	together := 0
	for _, writer := range writers {
		names[writer.name]++
		together = max(together, names[writer.name])
	}
	if together < 2 {
		return nil
	}
	var advice []string
	for _, mode := range modes {
		if mode.embedded && (mode.disk != "") {
			advice = append(advice, fmt.Sprintf("embedded backups of cluster %s are made first on the backup disk %s, as "+
				"BACKUP ... TO Disk('%s', '<backup name>'), where the storage path check cannot see: %d hosts make a backup of one "+
				"name there at once, which write over each other when that disk is an object storage disk they share; give "+
				"every host a backup disk of its own, or leave clickhouse.embedded_backup_disk empty", cluster, mode.disk, mode.disk,
				together))
		}
	}
	return advice
}

func article(what string) string {
	if strings.HasPrefix(what, "o") {
		return "an " + what
	}
	return "a " + what
}
