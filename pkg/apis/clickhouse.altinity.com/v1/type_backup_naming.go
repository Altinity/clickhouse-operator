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
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// A backup naming template, backup.naming, names the backup of every shard of a cluster: literal characters - letters,
// digits, '.', '_' and '-' - and placeholders in braces, which the backup-orchestrator.sh script of the Jobs expands
// alike, see naming_parse there:
//   - {shard}, the number of the shard in the cluster, from 1, as the shard_num of system.clusters and the shard<N> of
//     the Altinity Cloud Manager;
//   - {cluster}, {chi} and {namespace}, the names of the cluster, of the installation and of its namespace;
//   - {prefix}, the backupNamePrefix of the schedule that makes the backup, or the name of the ClickHouseBackup that makes
//     a backup once without a schedule;
//   - the time the backup is named after, in the time zone of its schedule, UTC otherwise, by the tokens YYYY, MM, DD,
//     HH, II (minutes) and SS, each once, all of them, in braces of their own or several in one, e.g. {YYYYMMDDHHIISS}.
//
// A backup made at once - a ClickHouseBackup, of a schedule or not - has a T before the hour, as the
// "<prefix>-<YYYYMMDD>T<HHMMSS>" of the default naming: it is no backup of a slot of the schedule.
// Every backup of the cluster is the backup of each of its shards, one name for each, which differ by {shard} alone:
// the shards may keep them at one storage path, as the Altinity Cloud Manager does.
const (
	// BackupNamingShard and the others are the placeholders of a naming template
	BackupNamingShard     = "shard"
	BackupNamingCluster   = "cluster"
	BackupNamingChi       = "chi"
	BackupNamingNamespace = "namespace"
	BackupNamingPrefix    = "prefix"
	// BackupNamingACM is the naming template of the Altinity Cloud Manager
	BackupNamingACM = "shard{shard}-{YYYYMMDDHHIISS}"
	// backupNamingMaxLength bounds a naming template. Mirrored by the CRD schema maxLength.
	backupNamingMaxLength = 128
)

// backupNamingTimeTokens are the tokens of the time in a naming template, in the order of a YYYYMMDDHHMMSS tag, with the
// width of each.
var backupNamingTimeTokens = []struct {
	token string
	width int
}{{"YYYY", 4}, {"MM", 2}, {"DD", 2}, {"HH", 2}, {"II", 2}, {"SS", 2}}

var (
	// backupNamingRegexp bounds a naming template as a whole: literal characters and placeholders. Mirrored by the CRD
	// schema pattern.
	backupNamingRegexp = regexp.MustCompile(`^([A-Za-z0-9._-]|\{[A-Za-z]+\})+$`)
	// backupNamingPlaceholderRegexp finds the placeholders of a naming template.
	backupNamingPlaceholderRegexp = regexp.MustCompile(`\{([A-Za-z]+)\}`)
	// BackupTagRegexp matches the time a backup is named after, as a backup name of a cluster with a naming template
	// may be given by it alone: YYYYMMDDHHMMSS, with a T before the hour for a backup made at once.
	BackupTagRegexp = regexp.MustCompile(`^[0-9]{8}T?[0-9]{6}$`)
)

// backupNamingPart is a literal of a naming template, or one of its placeholders, or one token of its time.
type backupNamingPart struct {
	literal     string
	placeholder string
	// token is a token of the time, and width its width
	token string
	width int
}

// parseBackupNaming splits a naming template into its parts, failing for a template that is not valid: one of
// letters, digits, '.', '_', '-' and known placeholders, with {shard} once, every token of the time once, each other
// placeholder at most once, and {shard} kept apart from any other placeholder by a literal that is no digit, so a
// name tells its shard and its time whatever the values of the others.
func parseBackupNaming(template string) ([]backupNamingPart, error) {
	if (len(template) > backupNamingMaxLength) || !backupNamingRegexp.MatchString(template) {
		return nil, fmt.Errorf("%w: backup.naming %q must be at most %d letters, digits, '.', '_', '-' and placeholders "+
			"in braces", ErrInvalidBackupSpec, template, backupNamingMaxLength)
	}
	var parts []backupNamingPart
	seen := map[string]bool{}
	rest := template
	for rest != "" {
		match := backupNamingPlaceholderRegexp.FindStringSubmatchIndex(rest)
		if match == nil {
			parts = append(parts, backupNamingPart{literal: rest})
			break
		}
		if match[0] > 0 {
			parts = append(parts, backupNamingPart{literal: rest[:match[0]]})
		}
		name := rest[match[2]:match[3]]
		rest = rest[match[1]:]
		switch name {
		case BackupNamingShard, BackupNamingCluster, BackupNamingChi, BackupNamingNamespace, BackupNamingPrefix:
			if seen[name] {
				return nil, fmt.Errorf("%w: backup.naming %q holds {%s} twice", ErrInvalidBackupSpec, template, name)
			}
			seen[name] = true
			parts = append(parts, backupNamingPart{placeholder: name})
			continue
		}
		// A time: tokens alone
		for tokens := name; tokens != ""; {
			found := false
			for _, token := range backupNamingTimeTokens {
				if strings.HasPrefix(tokens, token.token) {
					if seen[token.token] {
						return nil, fmt.Errorf("%w: backup.naming %q holds %s twice", ErrInvalidBackupSpec, template, token.token)
					}
					seen[token.token] = true
					parts = append(parts, backupNamingPart{token: token.token, width: token.width})
					tokens = tokens[len(token.token):]
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("%w: backup.naming %q: {%s} is no placeholder: {shard}, {cluster}, {chi}, {namespace}, "+
					"{prefix}, or the time by YYYY, MM, DD, HH, II and SS", ErrInvalidBackupSpec, template, name)
			}
		}
	}
	if !seen[BackupNamingShard] {
		return nil, fmt.Errorf("%w: backup.naming %q lacks {shard}: the shards of a cluster may keep their backups at one "+
			"storage path, where only the shard tells them apart", ErrInvalidBackupSpec, template)
	}
	for _, token := range backupNamingTimeTokens {
		if !seen[token.token] {
			return nil, fmt.Errorf("%w: backup.naming %q lacks %s: a name tells the full time, YYYY, MM, DD, HH, II and SS, "+
				"which retention and incremental backups order the backups by", ErrInvalidBackupSpec, template, token.token)
		}
	}
	for i, part := range parts {
		if part.placeholder != BackupNamingShard {
			continue
		}
		for _, j := range []int{i - 1, i + 1} {
			if (j < 0) || (j >= len(parts)) {
				continue
			}
			if parts[j].literal == "" {
				return nil, fmt.Errorf("%w: backup.naming %q: {shard} must be kept apart from the placeholder next to it by "+
					"a literal", ErrInvalidBackupSpec, template)
			}
			edge := parts[j].literal[0]
			if j < i {
				edge = parts[j].literal[len(parts[j].literal)-1]
			}
			if (edge >= '0') && (edge <= '9') {
				return nil, fmt.Errorf("%w: backup.naming %q: {shard} must not be next to a digit", ErrInvalidBackupSpec, template)
			}
		}
	}
	return parts, nil
}

// ValidateBackupNaming checks a naming template, "" being none: the default naming.
func ValidateBackupNaming(template string) error {
	if template == "" {
		return nil
	}
	_, err := parseBackupNaming(template)
	return err
}

// BackupNameValues are the values a naming template names a backup with.
type BackupNameValues struct {
	// Shard is the number of the shard, from 1
	Shard     int
	Cluster   string
	Chi       string
	Namespace string
	Prefix    string
	// Tag is the time the backup is named after, YYYYMMDDHHMMSS, with a T before the hour for a backup made at once
	Tag string
}

// BackupTag returns the tag of a time, the wall time of t, YYYYMMDDHHMMSS, with a T before the hour when atOnce.
func BackupTag(t time.Time, atOnce bool) string {
	if atOnce {
		return t.Format("20060102T150405")
	}
	return t.Format("20060102150405")
}

// RenderBackupName names the backup of a shard by a naming template, failing for a template that is not valid or a tag
// that is not one, and for a name that is none, as a value too long or of other characters makes it.
func RenderBackupName(template string, values BackupNameValues) (string, error) {
	parts, err := parseBackupNaming(template)
	if err != nil {
		return "", err
	}
	if !BackupTagRegexp.MatchString(values.Tag) {
		return "", fmt.Errorf("%w: %q is no time a backup is named after, YYYYMMDDHHMMSS or YYYYMMDDTHHMMSS",
			ErrInvalidBackupSpec, values.Tag)
	}
	digits := strings.Replace(values.Tag, "T", "", 1)
	atOnce := len(digits) != len(values.Tag)
	var name strings.Builder
	offset := 0
	offsets := map[string]int{}
	for _, token := range backupNamingTimeTokens {
		offsets[token.token] = offset
		offset += token.width
	}
	for _, part := range parts {
		switch {
		case part.token != "":
			if (part.token == "HH") && atOnce {
				name.WriteString("T")
			}
			name.WriteString(digits[offsets[part.token] : offsets[part.token]+part.width])
		case part.placeholder == BackupNamingShard:
			name.WriteString(strconv.Itoa(values.Shard))
		case part.placeholder == BackupNamingCluster:
			name.WriteString(values.Cluster)
		case part.placeholder == BackupNamingChi:
			name.WriteString(values.Chi)
		case part.placeholder == BackupNamingNamespace:
			name.WriteString(values.Namespace)
		case part.placeholder == BackupNamingPrefix:
			name.WriteString(values.Prefix)
		default:
			name.WriteString(part.literal)
		}
	}
	if err := validateBackupName("backup name", name.String(), BackupNameMaxLength); err != nil {
		return "", err
	}
	return name.String(), nil
}

// BackupNamingHolds reports whether a naming template holds the placeholder.
func BackupNamingHolds(template, placeholder string) bool {
	return strings.Contains(template, "{"+placeholder+"}")
}
