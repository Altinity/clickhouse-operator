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

package chi

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	kubeFake "k8s.io/client-go/kubernetes/fake"

	chkApi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	chkNamer "github.com/altinity/clickhouse-operator/pkg/model/chk/namer"
)

// keeperRefFakeService serves the CR-level Service of every CHK it is asked for
type keeperRefFakeService struct {
	interfaces.IKubeService
}

func (f *keeperRefFakeService) Get(ctx context.Context, params ...any) (*core.Service, error) {
	return &core.Service{
		ObjectMeta: meta.ObjectMeta{Namespace: params[0].(string), Name: params[1].(string)},
		Spec:       core.ServiceSpec{Ports: []core.ServicePort{{Name: api.KpDefaultZKPortName, Port: api.KpDefaultZKPortNumber}}},
	}, nil
}

// keeperRefFakeKube exposes Service() only. IKube is embedded as a nil interface, so reaching any other
// accessor panics.
type keeperRefFakeKube struct {
	interfaces.IKube
}

func (k *keeperRefFakeKube) Service() interfaces.IKubeService { return &keeperRefFakeService{} }

// newKeeperRefWorker wires a worker whose referenced CHK is running
func newKeeperRefWorker(t *testing.T, namespace, chkName string) *worker {
	t.Helper()
	podLabels, err := labels.ConvertSelectorToLabelsMap(chkListOptions(chkName, namespace).LabelSelector)
	require.NoError(t, err)
	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{Namespace: namespace, Name: chkName + "-0", Labels: podLabels},
		Status:     core.PodStatus{Phase: core.PodRunning},
	}
	return &worker{
		c: &Controller{kube: &keeperRefFakeKube{}, kubeClient: kubeFake.NewSimpleClientset(pod)},
		a: a.NewAnnouncer(nil, nil),
	}
}

// Keeper references resolve into ZooKeeper nodes of the spec. A reconcile builds the CR more than once - anew
// once the backup Secret is complete - so resolving must leave the CR it is given as it is: resolved into that
// CR, the nodes of every build would pile up on the previous ones.
func TestResolveAllKeeperReferencesKeepsInput(t *testing.T) {
	const (
		namespace = "ns"
		chkName   = "keeper"
	)
	raw := &api.ClickHouseInstallation{}
	raw.Name = "chi"
	raw.Namespace = namespace
	raw.Spec.Configuration = &api.Configuration{
		Zookeeper: &api.ZookeeperConfig{Keeper: &api.KeeperRef{
			Name:        chkName,
			ServiceType: api.KeeperServiceTypeService,
		}},
		Clusters: []*api.Cluster{{Name: "c"}},
	}
	pristine := raw.DeepCopy()
	w := newKeeperRefWorker(t, namespace, chkName)

	wantFQDN := chkNamer.New().Name(interfaces.NameCRServiceFQDN, chkApi.NewClickHouseKeeperInstallation(chkName, namespace), raw.Spec.GetNamespaceDomainPattern())
	for build := 1; build <= 2; build++ {
		resolved, err := w.resolveAllKeeperReferences(context.Background(), raw)
		require.NoError(t, err)
		nodes := resolved.Spec.GetConfiguration().GetZookeeper().Nodes
		require.Len(t, nodes, 1, "build %d", build)
		require.Equal(t, wantFQDN, nodes[0].Host, "build %d", build)
		require.Equal(t, pristine, raw, "build %d changed the CR it was given", build)
	}
}
