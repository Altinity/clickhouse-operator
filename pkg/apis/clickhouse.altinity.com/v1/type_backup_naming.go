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
	"cmp"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	BackupNamingShard     = "shard"
	BackupNamingShardNum  = "shardNum"
	BackupNamingCluster   = "cluster"
	BackupNamingChi       = "chi"
	BackupNamingNamespace = "namespace"
	BackupNamingPrefix    = "prefix"
	BackupNamingShardTime = "shard{shardNum}-{YYYYMMDDHHIISS}"
	backupNamingMaxLength = 128 // keep in sync with the CRD maxLength
)

var backupNamingTimeTokens = []struct {
	token string
	width int
}{{"YYYY", 4}, {"MM", 2}, {"DD", 2}, {"HH", 2}, {"II", 2}, {"SS", 2}}

var (
	backupNamingRegexp            = regexp.MustCompile(`^([A-Za-z0-9._-]|\{[A-Za-z]+\})+$`) // keep in sync with the CRD pattern
	backupNamingPlaceholderRegexp = regexp.MustCompile(`\{([A-Za-z]+)\}`)
	BackupTagRegexp               = regexp.MustCompile(`^[0-9]{8}T?[0-9]{6}$`)
)

type backupNamingPart struct {
	literal     string
	placeholder string
	token       string
	width       int
}

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
		case BackupNamingShard, BackupNamingShardNum, BackupNamingCluster, BackupNamingChi, BackupNamingNamespace, BackupNamingPrefix:
			if seen[name] {
				return nil, fmt.Errorf("%w: backup.naming %q holds {%s} twice", ErrInvalidBackupSpec, template, name)
			}
			seen[name] = true
			parts = append(parts, backupNamingPart{placeholder: name})
			continue
		}
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
				return nil, fmt.Errorf("%w: backup.naming %q: {%s} is no placeholder: {shard}, {shardNum}, {cluster}, {chi}, {namespace}, "+
					"{prefix}, or the time by YYYY, MM, DD, HH, II and SS", ErrInvalidBackupSpec, template, name)
			}
		}
	}
	if !seen[BackupNamingShard] && !seen[BackupNamingShardNum] {
		return nil, fmt.Errorf("%w: backup.naming %q lacks {shard} and {shardNum}: the shards of a cluster may keep their "+
			"backups at one storage path, where only the shard tells them apart", ErrInvalidBackupSpec, template)
	}
	for _, token := range backupNamingTimeTokens {
		if !seen[token.token] {
			return nil, fmt.Errorf("%w: backup.naming %q lacks %s: a name tells the full time, YYYY, MM, DD, HH, II and SS, "+
				"which retention and incremental backups order the backups by", ErrInvalidBackupSpec, template, token.token)
		}
	}
	// A non-digit literal around the shard keeps the shard and the time parseable from the name
	for i, part := range parts {
		if !isShardPlaceholder(part.placeholder) {
			continue
		}
		for _, j := range []int{i - 1, i + 1} {
			if (j < 0) || (j >= len(parts)) {
				continue
			}
			if parts[j].literal == "" {
				return nil, fmt.Errorf("%w: backup.naming %q: {%s} must be kept apart from the placeholder next to it by "+
					"a literal", ErrInvalidBackupSpec, template, part.placeholder)
			}
			edge := parts[j].literal[0]
			if j < i {
				edge = parts[j].literal[len(parts[j].literal)-1]
			}
			if (edge >= '0') && (edge <= '9') {
				return nil, fmt.Errorf("%w: backup.naming %q: {%s} must not be next to a digit", ErrInvalidBackupSpec, template,
					part.placeholder)
			}
		}
	}
	return parts, nil
}

func isShardPlaceholder(placeholder string) bool {
	return (placeholder == BackupNamingShard) || (placeholder == BackupNamingShardNum)
}

func ValidateBackupNaming(template string) error {
	if template == "" {
		return nil
	}
	_, err := parseBackupNaming(template)
	return err
}

type BackupNameValues struct {
	Shard     string
	ShardNum  int
	Shards    []string
	Cluster   string
	Chi       string
	Namespace string
	Prefix    string
	Tag       string
}

// Time layouts of the tag at the end of a backup name.
const (
	// BackupTagLayout is the tag of a scheduled run.
	BackupTagLayout = "20060102150405"
	// BackupTagLayoutAtOnce is the tag of a backup run at once. The T keeps it apart from scheduled runs.
	BackupTagLayoutAtOnce = "20060102T150405"
)

func BackupTag(t time.Time, atOnce bool) string {
	if atOnce {
		return t.Format(BackupTagLayoutAtOnce)
	}
	return t.Format(BackupTagLayout)
}

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
			if values.Shard == "" {
				return "", fmt.Errorf("%w: the shard name {shard} of the naming template %q is unknown", ErrInvalidBackupSpec, template)
			}
			name.WriteString(values.Shard)
		case part.placeholder == BackupNamingShardNum:
			if values.ShardNum < 1 {
				return "", fmt.Errorf("%w: the shard number {shardNum} of the naming template %q is unknown", ErrInvalidBackupSpec,
					template)
			}
			name.WriteString(strconv.Itoa(values.ShardNum))
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

func BackupNamingHolds(template, placeholder string) bool {
	return strings.Contains(template, "{"+placeholder+"}")
}

type ParsedBackupName struct {
	Shard    string
	ShardNum int
	Prefix   string
	Tag      string
}

// An empty values.Prefix matches any prefix.
func ParseBackupName(template string, values BackupNameValues, name string) (ParsedBackupName, bool) {
	parts, err := parseBackupNaming(template)
	if err != nil {
		return ParsedBackupName{}, false
	}
	var pattern strings.Builder
	pattern.WriteString("^")
	var groups []string
	for _, part := range parts {
		switch {
		case part.token != "":
			if part.token == "HH" {
				pattern.WriteString("(T?)")
				groups = append(groups, "T")
			}
			pattern.WriteString(fmt.Sprintf("([0-9]{%d})", part.width))
			groups = append(groups, part.token)
		case part.placeholder == BackupNamingShard:
			pattern.WriteString("(" + shardNamesPattern(values.Shards) + ")")
			groups = append(groups, BackupNamingShard)
		case part.placeholder == BackupNamingShardNum:
			pattern.WriteString("([1-9][0-9]*)")
			groups = append(groups, BackupNamingShardNum)
		case part.placeholder == BackupNamingCluster:
			pattern.WriteString(regexp.QuoteMeta(values.Cluster))
		case part.placeholder == BackupNamingChi:
			pattern.WriteString(regexp.QuoteMeta(values.Chi))
		case part.placeholder == BackupNamingNamespace:
			pattern.WriteString(regexp.QuoteMeta(values.Namespace))
		case (part.placeholder == BackupNamingPrefix) && (values.Prefix != ""):
			pattern.WriteString(regexp.QuoteMeta(values.Prefix))
		case part.placeholder == BackupNamingPrefix:
			pattern.WriteString("([A-Za-z0-9._-]+)")
			groups = append(groups, BackupNamingPrefix)
		default:
			pattern.WriteString(regexp.QuoteMeta(part.literal))
		}
	}
	pattern.WriteString("$")
	match := regexp.MustCompile(pattern.String()).FindStringSubmatch(name)
	if match == nil {
		return ParsedBackupName{}, false
	}
	group := map[string]string{}
	for i, g := range groups {
		group[g] = match[i+1]
	}
	digits := group["YYYY"] + group["MM"] + group["DD"] + group["HH"] + group["II"] + group["SS"]
	if !BackupTagValid(digits) {
		return ParsedBackupName{}, false
	}
	shardNum := 0
	if value, ok := group[BackupNamingShardNum]; ok {
		if shardNum, err = strconv.Atoi(value); err != nil {
			return ParsedBackupName{}, false
		}
	}
	prefix := values.Prefix
	if p, ok := group[BackupNamingPrefix]; ok {
		prefix = p
	}
	tag := group["YYYY"] + group["MM"] + group["DD"] + group["T"] + group["HH"] + group["II"] + group["SS"]
	return ParsedBackupName{Shard: group[BackupNamingShard], ShardNum: shardNum, Prefix: prefix, Tag: tag}, true
}

func shardNamesPattern(shards []string) string {
	names := slices.Clone(shards)
	slices.SortFunc(names, func(a, b string) int { return cmp.Or(len(b)-len(a), strings.Compare(a, b)) })
	names = slices.Compact(names)
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		if name != "" {
			quoted = append(quoted, regexp.QuoteMeta(name))
		}
	}
	if len(quoted) == 0 {
		return "[^\\x00-\\x{10FFFF}]" // matches nothing
	}
	return strings.Join(quoted, "|")
}

func BackupTagValid(tag string) bool {
	if len(tag) != len(BackupTagLayout) {
		return false
	}
	_, err := time.Parse(BackupTagLayout, tag)
	return err == nil
}
