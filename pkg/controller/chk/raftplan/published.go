// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package raftplan

import (
	"encoding/xml"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// raftBlock isolates the Raft section because a common ConfigMap may also
// contain unrelated configuration files that do not parse as XML.
var raftBlock = regexp.MustCompile(`(?s)<raft_configuration(?:\s[^>]*)?\s*>.*?</raft_configuration\s*>`)

type raftSection struct {
	Servers []struct {
		Hostname string `xml:"hostname"`
	} `xml:"server"`
}

// PublishedHostnames extracts the Raft hostnames from a live common ConfigMap.
// known is false if no Raft section was found, which must never be interpreted
// as an empty published membership. A confirmed ConfigMap NotFound is handled
// by the caller, not here. Inputs are treated as read-only.
func PublishedHostnames(data map[string]string) (hostnames []string, known bool, err error) {
	if len(data) == 0 {
		return nil, false, nil
	}
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var reference []string
	for _, key := range keys {
		blocks := raftBlock.FindAllString(data[key], -1)
		for _, block := range blocks {
			var section raftSection
			if err := xml.Unmarshal([]byte(block), &section); err != nil {
				return nil, false, fmt.Errorf("ConfigMap %s Raft XML: %w", key, err)
			}
			names := make([]string, 0, len(section.Servers))
			seen := map[string]bool{}
			for _, server := range section.Servers {
				name := strings.TrimSpace(server.Hostname)
				if name == "" || seen[name] {
					return nil, false, fmt.Errorf("ConfigMap %s contains blank or duplicate Raft hostname %q", key, name)
				}
				seen[name] = true
				names = append(names, name)
			}
			if known && !equalStrings(reference, names) {
				return nil, false, fmt.Errorf("conflicting raft_configuration sections in ConfigMap")
			}
			reference, known = names, true
		}
	}
	if known && reference == nil {
		reference = []string{}
	}
	return reference, known, nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]bool, len(a))
	for _, v := range a {
		counts[v] = true
	}
	for _, v := range b {
		if !counts[v] {
			return false
		}
	}
	return true
}
