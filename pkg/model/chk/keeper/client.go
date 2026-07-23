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

package keeper

import (
	"fmt"
	"strconv"
	"strings"
)

// Member is one entry of the committed Raft cluster configuration as
// serialized by Keeper into the special znode `/keeper/config`:
//
//	server.1=host:9444;participant;1
type Member struct {
	ID       int
	Endpoint string
	Learner  bool
	Priority int
}

// ParseClusterConfig parses the payload of `get /keeper/config`.
// Unknown lines are skipped; a malformed `server.` line is an error.
func ParseClusterConfig(data string) ([]Member, error) {
	var members []Member
	for _, line := range strings.Split(data, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "server.") {
			continue
		}
		idAndRest := strings.SplitN(strings.TrimPrefix(line, "server."), "=", 2)
		if len(idAndRest) != 2 {
			return nil, fmt.Errorf("malformed server line: %q", line)
		}
		id, err := strconv.Atoi(idAndRest[0])
		if err != nil {
			return nil, fmt.Errorf("malformed server id in line: %q", line)
		}
		parts := strings.Split(idAndRest[1], ";")
		m := Member{ID: id, Endpoint: parts[0], Priority: 1}
		if len(parts) > 1 {
			m.Learner = parts[1] == "learner"
		}
		if len(parts) > 2 {
			if p, err := strconv.Atoi(parts[2]); err == nil {
				m.Priority = p
			}
		}
		members = append(members, m)
	}
	return members, nil
}

// MemberIDs returns the set of member ids.
func MemberIDs(members []Member) map[int]bool {
	ids := map[int]bool{}
	for _, m := range members {
		ids[m.ID] = true
	}
	return ids
}

// SameIDs reports whether the member list is exactly the expected id set.
func SameIDs(members []Member, expected map[int]bool) bool {
	ids := MemberIDs(members)
	if len(ids) != len(expected) {
		return false
	}
	for id := range expected {
		if !ids[id] {
			return false
		}
	}
	return true
}
