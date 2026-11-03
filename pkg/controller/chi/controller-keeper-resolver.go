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

// Controller-level CHK/keeper-ref resolution.
//
// This file holds methods on *Controller that resolve a KeeperRef to the list of ZooKeeper
// endpoints the CHI should use. These are pure lookups against the kube API — no waiting,
// no mutation of CHI spec — so they can be invoked from both the reconcile worker (via the
// *worker wrapper in worker-keeper-resolver.go) and the CHK watcher gateway.

package chi

import (
	"context"
	"errors"
	"fmt"
	"sort"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chkApi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	chkNamer "github.com/altinity/clickhouse-operator/pkg/model/chk/namer"
	chkLabeler "github.com/altinity/clickhouse-operator/pkg/model/chk/tags/labeler"
	commonLabeler "github.com/altinity/clickhouse-operator/pkg/model/common/tags/labeler"
)

var (
	// ErrKeeperRefResolve indicates a failure to resolve a keeper reference (e.g., service not found).
	ErrKeeperRefResolve = errors.New("failed to resolve keeper ref")
	// ErrKeeperRefNoNodes indicates a keeper reference resolved successfully but produced zero nodes.
	ErrKeeperRefNoNodes = errors.New("keeper ref resolved to 0 nodes")
	// ErrKeeperNotReady indicates the referenced keeper's pods are not all Running yet.
	ErrKeeperNotReady = errors.New("keeper not ready")
)

// resolveKeeperNodes is the pure resolver: given a KeeperRef, returns the list of
// ZookeeperNodes that a CHI would see for that keeper in the CHK's current state.
// Does NOT mutate anything and does NOT wait for CHK readiness — callers decide whether
// to wait (the reconcile path does; the watcher gateway does not need to).
// Returns ErrKeeperRefResolve for lookup failures and ErrKeeperRefNoNodes when the lookup
// succeeds but yields zero nodes.
func (c *Controller) resolveKeeperNodes(
	ctx context.Context,
	keeper *api.KeeperRef,
	chiNamespace string,
	domainPattern *types.String,
) (api.ZookeeperNodes, error) {
	if keeper == nil || !keeper.HasName() {
		return nil, nil
	}

	ns := keeper.GetNamespace(chiNamespace)
	name := keeper.Name

	var nodes api.ZookeeperNodes
	var err error

	// Resolve keeper to either per-host services or CR-level service nodes based on service type
	switch keeper.GetServiceType() {
	case api.KeeperServiceTypeService:
		nodes, err = c.resolveKeeperByService(ctx, ns, name, domainPattern)
	case api.KeeperServiceTypeReplicas:
		nodes, err = c.resolveKeeperByReplicas(ctx, ns, name, domainPattern)
		if err != nil || len(nodes) == 0 {
			// Fallback to CR-level service if replica discovery fails
			log.V(1).Info("Keeper replica discovery failed or returned 0 nodes, falling back to CR service: %v", err)
			nodes, err = c.resolveKeeperByService(ctx, ns, name, domainPattern)
		}
	default:
		return nil, fmt.Errorf("%w: invalid keeper serviceType %q for %s/%s", ErrKeeperRefResolve, keeper.GetServiceType(), ns, name)
	}

	switch {
	case err != nil:
		return nil, fmt.Errorf("%w %s/%s: %w", ErrKeeperRefResolve, ns, name, err)
	case len(nodes) == 0:
		return nil, fmt.Errorf("%w %s/%s", ErrKeeperRefNoNodes, ns, name)
	}

	return nodes, nil
}

// resolveKeeperByService resolves using the CR-level service (single endpoint).
func (c *Controller) resolveKeeperByService(ctx context.Context, namespace, name string, domainPattern *types.String) (api.ZookeeperNodes, error) {
	// Find the CHK CR-level service in k8s
	chk := chkApi.NewClickHouseKeeperInstallation(name, namespace)
	namer := chkNamer.New()
	serviceName := namer.Name(interfaces.NameCRService, chk)
	svc, err := c.kube.Service().Get(ctx, namespace, serviceName)
	if err != nil {
		return nil, fmt.Errorf("%w: CR service %s/%s: %w", ErrKeeperRefResolve, namespace, serviceName, err)
	}

	// CHK service found, extract ZK port (with TLS auto-detection) and FQDN
	portInfo := api.ExtractZKPortInfo(svc.Spec.Ports)
	fqdn := namer.Name(interfaces.NameCRServiceFQDN, chk, domainPattern)

	return api.NewZookeeperNodes(api.NewZookeeperNodeFromPortInfo(fqdn, portInfo)), nil
}

// resolveKeeperByReplicas resolves using per-host services (one node per replica).
//
// A CHK host exposes a ready-only client Service (LabelServiceValueHostClient,
// publishNotReadyAddresses=false) alongside the peer/Raft Service (LabelServiceValueHost,
// publishNotReadyAddresses=true). ClickHouse clients must resolve only Ready Keeper nodes, so
// the client tier is selected first. The peer tier is the fallback for CHK clusters that predate
// the split (single host Service) or use a user-supplied replicaServiceTemplate.
func (c *Controller) resolveKeeperByReplicas(ctx context.Context, namespace, name string, domainPattern *types.String) (api.ZookeeperNodes, error) {
	// Prefer the ready-only client services
	services, err := c.kubeClient.CoreV1().Services(namespace).List(ctx, chkClientServiceListOptions(name, namespace))
	if (err == nil) && (len(services.Items) == 0) {
		// No client-tier services - fall back to the peer/host services (pre-split / templated CRs)
		services, err = c.kubeClient.CoreV1().Services(namespace).List(ctx, chkHostServiceListOptions(name, namespace))
	}

	// Handle errors and empty list
	switch {
	case err != nil:
		return nil, fmt.Errorf("%w: host services %s/%s: %w", ErrKeeperRefResolve, namespace, name, err)
	case len(services.Items) == 0:
		return nil, fmt.Errorf("%w: host services %s/%s", ErrKeeperRefNoNodes, namespace, name)
	}

	// We have list of host services, now build ZookeeperNodes

	// Sort for deterministic order
	sort.Slice(services.Items, func(i, j int) bool {
		return services.Items[i].Name < services.Items[j].Name
	})

	namer := chkNamer.New()
	nodes := make(api.ZookeeperNodes, 0, len(services.Items))
	for _, svc := range services.Items {
		portInfo := api.ExtractZKPortInfo(svc.Spec.Ports)
		fqdn := namer.ServiceFQDN(svc.Name, namespace, domainPattern)
		nodes = nodes.Append(api.NewZookeeperNodeFromPortInfo(fqdn, portInfo))
	}

	return nodes, nil
}

// chkListOptions builds meta.ListOptions to select all resources belonging to a CHK by name.
func chkListOptions(name, namespace string) meta.ListOptions {
	chk := chkApi.NewClickHouseKeeperInstallation(name, namespace)
	l := chkLabeler.New(chk)
	return controller.NewListOptions(map[string]string{
		l.Get(commonLabeler.LabelCRName): name,
	})
}

// chkHostServiceListOptions builds meta.ListOptions to select CHK per-host peer/Raft services.
func chkHostServiceListOptions(name, namespace string) meta.ListOptions {
	chk := chkApi.NewClickHouseKeeperInstallation(name, namespace)
	l := chkLabeler.New(chk)
	return controller.NewListOptions(map[string]string{
		l.Get(commonLabeler.LabelCRName):  name,
		l.Get(commonLabeler.LabelService): l.Get(commonLabeler.LabelServiceValueHost),
	})
}

// chkClientServiceListOptions builds meta.ListOptions to select CHK per-host client-facing
// services (publishNotReadyAddresses=false), the tier ClickHouse clients should resolve.
func chkClientServiceListOptions(name, namespace string) meta.ListOptions {
	chk := chkApi.NewClickHouseKeeperInstallation(name, namespace)
	l := chkLabeler.New(chk)
	return controller.NewListOptions(map[string]string{
		l.Get(commonLabeler.LabelCRName):  name,
		l.Get(commonLabeler.LabelService): l.Get(commonLabeler.LabelServiceValueHostClient),
	})
}
