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

// Settings of clickhouse-backup that tell whether, and where, a backup is made by the BACKUP statement of ClickHouse
// itself: its data then goes where the embedded backup puts it, not where clickhouse-backup uploads it.
const (
	SettingUseEmbeddedBackupRestore = "CLICKHOUSE_USE_EMBEDDED_BACKUP_RESTORE"
	SettingEmbeddedBackupDisk       = "CLICKHOUSE_EMBEDDED_BACKUP_DISK"
)

// PathCheckKeys are the clickhouse-backup settings the storage path check reads of every cluster: which type of
// storage it is, what tells one storage of its type from another, its backup and object disk paths, and whether and
// on which disk its backups are embedded ones, each by the name of the env var clickhouse-backup knows it by, see
// backupconfig.Settings. None of them is a credential. A cluster takes each as its sidecar does: from an optional key
// of the configuration file Secret of the cluster, which holds the setting of the file, overridden by the env of the
// sidecar container of its pod template. An unset one counts with the default clickhouse-backup gives it, an unset
// path with the one the configuration file takes.
// The region of an S3 storage is none of them: a bucket name is global, the region only tells where the requests to
// it go and what they are signed for. A bucket of one name in another AWS partition then counts as the same, which
// refuses a path rather than let a shared one through.
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

// storagePrefixes are the prefixes of the settings of every type of storage the check tells the paths of.
var storagePrefixes = map[string]string{"s3": "S3", "gcs": "GCS", "azblob": "AZBLOB", "cos": "COS", "ftp": "FTP", "sftp": "SFTP"}

// Macros the operator defines on every host besides {installation}, {namespace}, {cluster} and {shard}, see the
// macros the configuration generator writes: the two whose values differ between the replicas of a shard.
const (
	// MacroReplica is the {replica} macro, the host name of the pod of the host
	MacroReplica = "replica"
	// MacroAllShardedShard is the {all-sharded-shard} macro, the index of the host in the installation
	MacroAllShardedShard = config.AllShardsOneReplicaClusterName + "-shard"
)

// PathHost is a host of a backup-enabled cluster as the storage path check reads it.
type PathHost struct {
	// Name is the host as the topology lists it
	Name string
	// Shard is the index in the shards of its cluster of the shard of the host
	Shard int
	// Macros are the macros the operator gives the host besides those of its installation, namespace, cluster and
	// shard: MacroReplica and MacroAllShardedShard
	Macros map[string]string
}

// PathCluster is a backup-enabled cluster of an installation as the storage path check reads it.
type PathCluster struct {
	Name string
	// Shards are the values of the {shard} macro of the cluster, in shard order
	Shards []string
	// Hosts are the hosts of the cluster, in shard and then replica order
	Hosts []PathHost
	// Unverifiable tells why the settings of the sidecars of the cluster cannot be known, "" when they can
	Unverifiable string
	// Settings are the PathCheckKeys the sidecars of the cluster take, nil for one they leave unset
	Settings map[string]*string
}

// PathCheck is what the storage path check reads: every backup-enabled cluster of the installation.
type PathCheck struct {
	Clusters []PathCluster
	// Self is the index of the cluster the operation targets
	Self         int
	Installation string
	Namespace    string
	// Operation is the operation that checks
	Operation string
	// ReplicaSelection is the replica selection of a backup, "" for a restore
	ReplicaSelection api.ReplicaSelection
	// ValidateTopology tells a restore checks the backup on every host
	ValidateTopology bool
	// DefaultPath and DefaultObjectDiskPath are the paths the configuration files take when the settings leave them
	// unset, nil for the root of the storage
	DefaultPath           *string
	DefaultObjectDiskPath *string
	// NamingTemplate is the naming template of the cluster the operation targets
	NamingTemplate string
	// ConfigOverrides are the config overrides, KEY=VALUE, the runs of a backup of the cluster the operation targets
	// may be made with, each list those of one run: of a one-off backup, or of each entry of a schedule. A run made
	// without any is checked anyway.
	ConfigOverrides [][]string
}

// PathCheckResult is what the check tells of the paths of the cluster the operation targets.
type PathCheckResult struct {
	// Path and ObjectDiskPath are the storage path, and the object disk path, of the cluster, with their macros
	Path           string
	ObjectDiskPath string
	// StoragePrefix is the prefix of the settings of the type of storage of the cluster, "" when unknown
	StoragePrefix string
	ClusterName   string
	// Unknown tells why the paths of the cluster are unknown, "" when they are known
	Unknown string
	// Advice are the warnings of the check
	Advice []string
	// Log tells the outcome of the check
	Log string
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

// normalizeIdentity writes a setting that tells one storage from another in one form for every way to spell it:
// the scheme and the host of an endpoint in lower case, without the default port of its scheme and without the
// trailing slashes of its path, the host of an address and the suffix of an endpoint in lower case, a port as a number.
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

// normalizeEndpoint writes an endpoint URL with its scheme and host in lower case, without the default port of its
// scheme and without trailing slashes. What is no URL with a scheme and a host is left as it is, but for its case and
// slashes.
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

// CleanPath cleans a path as path.Clean does, a relative one keeping its leading "..".
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

// ExpandMacros returns the template with the macros the operator gives every host of a shard alike replaced, {replica}
// kept, and the first macro whose value it does not know, "" when it knows them all.
func ExpandMacros(template, installation, namespace, cluster, shard string) (string, string) {
	return expandMacros(template, map[string]string{"installation": installation, "namespace": namespace, "cluster": cluster,
		"shard": shard}, MacroReplica)
}

// expandMacros returns the template with the macros of the values given replaced, those named in keep kept, and the
// first macro it has no value of, "" when it has them all: clickhouse-backup replaces the macros of system.macros of
// the host in its paths, and leaves any other as it is.
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

// hostMacros returns the macros the operator gives a host of the cluster.
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

// hostsOf returns the hosts of the cluster, one without macros of its own for every shard when it lists none.
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

// embeddedMode is how the backups of a run are made: by clickhouse-backup, or embedded, by the BACKUP statement of
// ClickHouse, to the disk named, or, without one, straight to the object disk path of the remote storage.
type embeddedMode struct {
	embedded bool
	disk     string
}

// embeddedModes returns how the runs of a backup of the cluster are made: as its settings say, and, for the cluster
// the operation targets, as every list of config overrides its runs may be made with says.
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
			// A value clickhouse-backup cannot read stops it from starting: counted as embedded, the check compares
			// more paths, never fewer
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

// pathWriter is a writer of the backups of a cluster: a shard, whose replicas write one backup at the path of any of
// them, or, for a backup of every replica, a host.
type pathWriter struct {
	cluster int
	index   int
	what    string
	// replica is the replica of the writer of a backup of every replica, "" for a shard
	replica string
	// shard and clusterName are the shard and the cluster of the writer
	shard, clusterName string
	// name tells the writers of the cluster the operation targets that write a backup of one name apart from those
	// that write it of another name, the shard the naming template names it after
	name  string
	hosts []PathHost
}

// pathLocation is where a storage path, or an object disk path, expanded on a host, is.
type pathLocation struct {
	identity, what, path string
}

// Check refuses, before anything changes, a storage location that two writers of a backup share: two shards of a
// cluster, or of two clusters, or, for a backup of every replica, two replicas of the cluster the operation targets,
// which write a backup of one name at once at one storage path, or one object disk path, of one storage, every path
// expanded with the macros of each host, as clickhouse-backup expands it there. The shards of a cluster with a naming
// template write backups of different names, and may share a path.
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
		// An embedded backup without a backup disk is written by ClickHouse straight under the object disk path, the
		// root of the storage when that is empty
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
							// Two other clusters that share a path fail the runs of those clusters, not this one
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

// writersOf returns the writers of the backups of a cluster: every host, for a backup of every replica, every shard
// otherwise. Those of the cluster the operation targets with a naming template are named apart by their shards.
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

// sharedLocation tells why two writers must not share a location: the path, as root locates it in its storage, is not
// unique among the writers of the backups.
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

// storageRoots are the settings that locate the root of each type of storage.
var storageRoots = map[string]string{"s3": "S3_BUCKET", "gcs": "GCS_BUCKET", "azblob": "AZBLOB_CONTAINER", "cos": "COS_URL",
	"ftp": "FTP_ADDRESS", "sftp": "SFTP_ADDRESS"}

// storageRoot returns the root of the storage of a cluster, as a URL: s3://<bucket>, say.
func storageRoot(storage string, cluster PathCluster) string {
	value, _ := setting(cluster, storageRoots[storage])
	value = strings.Trim(strings.TrimSpace(value), "/")
	if strings.Contains(value, "://") {
		return value
	}
	return storage + "://" + value
}

// locate returns a path of a storage, in the root of that storage.
func locate(root, path string) string {
	if path == "." {
		return root + "/"
	}
	return root + "/" + strings.TrimPrefix(path, "/")
}

// embeddedDiskAdvice warns of embedded backups made to a backup disk first, whose location the check cannot see, when
// writers of the cluster write backups of one name at once.
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

// article returns the name of a path with its indefinite article.
func article(what string) string {
	if strings.HasPrefix(what, "o") {
		return "an " + what
	}
	return "a " + what
}
