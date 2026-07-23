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
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseClusterConfig(t *testing.T) {
	data := "server.0=chk-test-keeper-0-0.ns.svc:9444;participant;1\n" +
		"server.2=chk-test-keeper-0-2.ns.svc:9444;learner;0\n"
	members, err := ParseClusterConfig(data)
	require.NoError(t, err)
	require.Len(t, members, 2)
	require.Equal(t, 0, members[0].ID)
	require.Equal(t, "chk-test-keeper-0-0.ns.svc:9444", members[0].Endpoint)
	require.False(t, members[0].Learner)
	require.Equal(t, 1, members[0].Priority)
	require.Equal(t, 2, members[1].ID)
	require.True(t, members[1].Learner)
}

func TestParseClusterConfigTolerant(t *testing.T) {
	// no type/priority suffixes, trailing newline, junk lines
	members, err := ParseClusterConfig("server.1=host:9444\n\nsomething-else\n")
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Equal(t, map[int]bool{1: true}, MemberIDs(members))
}

func TestParseClusterConfigMalformed(t *testing.T) {
	_, err := ParseClusterConfig("server.x=host:9444;participant;1")
	require.Error(t, err)
}

func TestSameIDs(t *testing.T) {
	members, _ := ParseClusterConfig("server.0=a:1;participant;1\nserver.1=b:1;participant;1\n")
	require.True(t, SameIDs(members, map[int]bool{0: true, 1: true}))
	require.False(t, SameIDs(members, map[int]bool{0: true}))
	require.False(t, SameIDs(members, map[int]bool{0: true, 1: true, 2: true}))
}

// startFake4LW starts a TCP listener answering canned 4lw responses.
func startFake4LW(t *testing.T, responses map[string]string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 4)
				if _, err := c.Read(buf); err != nil {
					return
				}
				_, _ = c.Write([]byte(responses[string(buf)]))
			}(conn)
		}
	}()
	return ln.Addr().String()
}

func TestGetRole(t *testing.T) {
	addr := startFake4LW(t, map[string]string{
		"srvr": "ClickHouse Keeper version: v25.8\nMode: leader\nNode count: 5\n",
	})
	role, err := GetRole(context.Background(), addr, time.Second)
	require.NoError(t, err)
	require.Equal(t, RoleLeader, role)
}

func TestGetRoleNotServing(t *testing.T) {
	addr := startFake4LW(t, map[string]string{
		"srvr": "This instance is not currently serving requests\n",
	})
	role, err := GetRole(context.Background(), addr, time.Second)
	require.Error(t, err)
	require.Equal(t, RoleUnknown, role)
}

func TestGetSyncedFollowers(t *testing.T) {
	addr := startFake4LW(t, map[string]string{
		"mntr": "zk_version\tv25.8\nzk_followers\t2\nzk_synced_followers\t2\n",
	})
	n, err := GetSyncedFollowers(context.Background(), addr, time.Second)
	require.NoError(t, err)
	require.Equal(t, 2, n)
}
