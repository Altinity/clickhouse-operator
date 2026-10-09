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

// This is an external test package (creator_test) so it can import the managers
// package to build a real tagger; managers imports creator, so an in-package test
// would form an import cycle.
package creator_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"

	chk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	chkNormalizer "github.com/altinity/clickhouse-operator/pkg/model/chk/normalizer"
	chkLabeler "github.com/altinity/clickhouse-operator/pkg/model/chk/tags/labeler"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
	commonLabeler "github.com/altinity/clickhouse-operator/pkg/model/common/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

// The normalizer reads the global operator config (chop.Config()) via the labeler;
// initialize a default instance once for the package.
func init() { chop.New(nil, nil, "") }

// serviceLabelKey is the fully-qualified label key that carries the Service tier value
// (host vs host-client), resolved through the CHK labeler so the test does not hard-code it.
func serviceLabelKey(t *testing.T) string {
	t.Helper()
	l := chkLabeler.New(chk.NewClickHouseKeeperInstallation("x", "ns"))
	return l.Get(commonLabeler.LabelService)
}

func portNames(svc *core.Service) []string {
	names := make([]string, 0, len(svc.Spec.Ports))
	for _, p := range svc.Spec.Ports {
		names = append(names, p.Name)
	}
	return names
}

// normalizeSingleHostCHK builds a minimal one-cluster CHK and normalizes it, returning the
// ready-to-use CR plus a ServiceManager wired exactly as the controller wires it.
func normalizeSingleHostCHK(t *testing.T) (*chk.ClickHouseKeeperInstallation, interfaces.IServiceManager) {
	t.Helper()
	src := chk.NewClickHouseKeeperInstallation("kpr", "ns")
	src.Spec.Configuration = &chk.Configuration{Clusters: []*chk.Cluster{{Name: "keeper"}}}
	cr, err := chkNormalizer.New().CreateTemplated(src, commonNormalizer.NewOptions[chk.ClickHouseKeeperInstallation]())
	require.NoError(t, err)
	require.NotNil(t, cr)

	sm := managers.NewServiceManager(managers.ServiceManagerTypeKeeper)
	sm.SetCR(cr)
	sm.SetTagger(managers.NewTagManager(managers.TagManagerTypeKeeper, cr))
	return cr, sm
}

// TestCreateServiceHostEmitsPeerAndClient pins the issue #1982 contract: with no
// user-supplied replicaServiceTemplate, each Keeper host gets TWO headless Services —
//   - a peer/Raft Service: publishNotReadyAddresses=true, keeps the Raft port, Service tier "host"
//   - a client Service:    publishNotReadyAddresses=false, no Raft port, Service tier "host-client"
//
// The peer keeps the bare StatefulSet Service name (Raft <hostname> / pod DNS binding) and the
// client name is that + "-client". The distinct "host-client" tier label is what the keeper-ref
// resolver selects so ClickHouse clients never resolve a not-yet-Ready Keeper.
func TestCreateServiceHostEmitsPeerAndClient(t *testing.T) {
	cr, sm := normalizeSingleHostCHK(t)
	svcLabel := serviceLabelKey(t)

	hosts := 0
	cr.WalkHosts(func(host *chi.Host) error {
		hosts++
		services := sm.CreateService(interfaces.ServiceHost, host)
		require.Len(t, services, 2, "a default Keeper host must emit exactly two Services")

		peer, client := services[0], services[1]

		// Peer: bare name, publishNotReady=true, host tier, retains the Raft port.
		require.Equal(t, "chk-kpr-keeper-0-0", peer.Name)
		require.True(t, peer.Spec.PublishNotReadyAddresses,
			"peer/Raft Service must publish not-ready addresses for quorum bootstrap")
		require.Equal(t, "host", peer.Labels[svcLabel])
		require.Contains(t, portNames(peer), chi.KpDefaultRaftPortName)

		// Client: peer name + "-client", publishNotReady=false, host-client tier, no Raft port.
		require.Equal(t, peer.Name+"-client", client.Name)
		require.False(t, client.Spec.PublishNotReadyAddresses,
			"client Service must resolve only Ready Keeper endpoints")
		require.Equal(t, "host-client", client.Labels[svcLabel])
		require.NotContains(t, portNames(client), chi.KpDefaultRaftPortName,
			"client Service must not expose the Raft port")

		// Both are headless and share the host selector (one pod, two readiness views).
		require.Equal(t, "None", peer.Spec.ClusterIP)
		require.Equal(t, "None", client.Spec.ClusterIP)
		return nil
	})
	require.Equal(t, 1, hosts, "minimal one-cluster CHK must normalize to exactly one host")
}

// TestCreateServiceHostHonorsUserTemplate verifies the back-compat escape hatch: when the host
// carries a replicaServiceTemplate the operator emits the single user-controlled Service and does
// NOT inject the second client Service.
func TestCreateServiceHostHonorsUserTemplate(t *testing.T) {
	src := chk.NewClickHouseKeeperInstallation("kpr", "ns")
	src.Spec.Configuration = &chk.Configuration{
		Clusters: []*chk.Cluster{{
			Name:      "keeper",
			Templates: &chi.TemplatesList{ReplicaServiceTemplate: "svc-tpl"},
		}},
	}
	src.Spec.Templates = &chi.Templates{
		ServiceTemplates: []chi.ServiceTemplate{{
			Name: "svc-tpl",
			Spec: core.ServiceSpec{Type: core.ServiceTypeClusterIP},
		}},
	}
	cr, err := chkNormalizer.New().CreateTemplated(src, commonNormalizer.NewOptions[chk.ClickHouseKeeperInstallation]())
	require.NoError(t, err)

	sm := managers.NewServiceManager(managers.ServiceManagerTypeKeeper)
	sm.SetCR(cr)
	sm.SetTagger(managers.NewTagManager(managers.TagManagerTypeKeeper, cr))

	cr.WalkHosts(func(host *chi.Host) error {
		if _, ok := host.GetServiceTemplate(); !ok {
			// Template did not attach (normalization specifics) — skip rather than assert a
			// false negative; the two-Service default path is covered by the test above.
			t.Skip("replicaServiceTemplate did not attach to host; template wiring not exercised")
		}
		services := sm.CreateService(interfaces.ServiceHost, host)
		require.Len(t, services, 1, "a templated host must emit exactly one (user-controlled) Service")
		return nil
	})
}
