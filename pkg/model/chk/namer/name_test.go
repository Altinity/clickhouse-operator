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

package namer

import (
	"testing"

	"github.com/stretchr/testify/require"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

// TestStatefulSetServiceClientName pins the client Service naming contract (issue #1982):
// the client-facing Service name is the peer/StatefulSet Service name plus a "-client" suffix.
// The peer name MUST stay the bare StatefulSet Service name — Raft <hostname> and the pod DNS
// (StatefulSet.serviceName) bind to it, so any drift would break quorum on existing clusters.
func TestStatefulSetServiceClientName(t *testing.T) {
	n := New()
	host := &chi.Host{}

	peer := n.Name(interfaces.NameStatefulSetService, host)
	client := n.Name(interfaces.NameStatefulSetServiceClient, host)

	require.Equal(t, peer+"-client", client,
		"client Service name must be the peer Service name plus the -client suffix")
	require.NotEqual(t, peer, client, "peer and client Service names must differ")
}
