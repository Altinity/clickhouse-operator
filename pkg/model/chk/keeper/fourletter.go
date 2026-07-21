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
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// Role of a Keeper node as reported by the `srvr` four-letter command.
type Role string

const (
	RoleLeader     Role = "leader"
	RoleFollower   Role = "follower"
	RoleObserver   Role = "observer"
	RoleStandalone Role = "standalone"
	RoleUnknown    Role = "unknown"
)

// FourLetterWord sends a 4lw command (srvr, mntr, rqld, ...) over a raw TCP
// connection to the Keeper client port and returns the response.
func FourLetterWord(ctx context.Context, addr string, cmd string, timeout time.Duration) (string, error) {
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return "", err
	}
	data, err := io.ReadAll(conn)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// GetRole reports the Raft role via `srvr` ("Mode: <role>" line).
// A node with no live leader answers "This instance is not currently serving
// requests" — that surfaces as RoleUnknown with an error.
func GetRole(ctx context.Context, addr string, timeout time.Duration) (Role, error) {
	out, err := FourLetterWord(ctx, addr, "srvr", timeout)
	if err != nil {
		return RoleUnknown, err
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "Mode: ") {
			return Role(strings.TrimSpace(strings.TrimPrefix(line, "Mode: "))), nil
		}
	}
	return RoleUnknown, fmt.Errorf("no Mode in srvr response from %s: %q", addr, out)
}

// RequestLeadership asks the node at addr to become the Raft leader (`rqld`,
// present since 23.8). Best-effort: transport success does not guarantee the
// transfer; callers must poll GetRole afterwards.
func RequestLeadership(ctx context.Context, addr string, timeout time.Duration) (string, error) {
	return FourLetterWord(ctx, addr, "rqld", timeout)
}

// GetSyncedFollowers parses `mntr` zk_synced_followers (reported by the
// leader only).
func GetSyncedFollowers(ctx context.Context, addr string, timeout time.Duration) (int, error) {
	out, err := FourLetterWord(ctx, addr, "mntr", timeout)
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "zk_synced_followers" {
			return strconv.Atoi(fields[1])
		}
	}
	return 0, fmt.Errorf("no zk_synced_followers in mntr response from %s", addr)
}
