// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package chk

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/chk/raftplan"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
	"github.com/go-zookeeper/zk"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
)

// publishStartedStagedJoiner is the only transition from bootstrap to
// published membership in the serial 1->N/2->3 admission path. Its inputs
// are live Kubernetes facts, so a retry after operator restart resumes here.
func (w *worker) publishStartedStagedJoiner(ctx context.Context, host *api.Host) error {
	if !hostIsStagedScaleUpJoin(host) || !host.GetReconcileAttributes().IsExclude() || host.IsStopped() {
		return nil
	}
	sts, err := w.stagedJoinerStatefulSet(ctx, host)
	if err != nil {
		return err
	}
	if sts == nil {
		return common.ErrCRUDDeferred
	}
	if w.c == nil || w.c.kube == nil || w.c.kube.Pod() == nil {
		return fmt.Errorf("Keeper joiner Pod observation unavailable: %s", host.GetName())
	}
	pod, err := w.c.kube.Pod().Get(ctx, host)
	if apiErrors.IsNotFound(err) {
		return common.ErrCRUDDeferred
	}
	if err != nil {
		return err
	}
	if pod == nil {
		return fmt.Errorf("nil Keeper joiner Pod: %s", host.GetName())
	}
	decision := raftplan.PlanJoin(raftplan.JoinObservation{
		StatefulSet: raftplan.PresencePresent,
		StartedKnown: true,
		Started: k8s.PodHasAllContainersStarted(pod),
		PublishedKnown: true,
		Published: false,
	})
	if decision.Action != raftplan.JoinPublish {
		return common.ErrCRUDDeferred
	}
	_, err = w.publishStagedAdmit(ctx, host)
	return err
}

// readStagedJoinerMembership probes an established member, not the joiner:
// /keeper/config on a new candidate is unavailable before it joins quorum.
// The probe is read-only and its failure never permits the next voter.
func (w *worker) readStagedJoinerMembership(ctx context.Context, host *api.Host) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	var peer *api.Host
	_ = host.GetCR().WalkHosts(func(h *api.Host) error {
		if peer == nil && h != nil && h.HasAncestor() {
			peer = h
		}
		return nil
	})
	if peer == nil {
		return false, errors.New("no established Keeper to query for Raft membership")
	}
	ns := peer.GetRuntime().GetAddress().GetNamespace()
	address := fmt.Sprintf("%s.%s.svc:2181", w.c.namer.Name(interfaces.NameInstanceHostname, peer), ns)
	conn, _, err := zk.Connect([]string{address}, 4*time.Second)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	data, _, err := conn.Get("/keeper/config")
	if err != nil {
		return false, err
	}
	want := fmt.Sprintf("server.%d=%s:%d;", host.GetRuntime().GetAddress().GetReplicaIndex(),
		w.c.namer.Name(interfaces.NameInstanceHostname, host), host.RaftPort.Value())
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), want) {
			return true, nil
		}
	}
	return false, nil
}
