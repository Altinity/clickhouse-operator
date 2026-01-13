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

package common

import (
	"context"

	"gopkg.in/d4l3k/messagediff.v1"
	apps "k8s.io/api/apps/v1"

	can "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func DumpStatefulSetDiff(host *api.Host, cur, new *apps.StatefulSet) {
	if cur == nil {
		log.V(1).M(host).Info("Cur StatefulSet is not available, nothing to compare to")
		return
	}
	if new == nil {
		log.V(1).M(host).Info("New StatefulSet is not available, nothing to compare to")
		return
	}

	if diff, equal := messagediff.DeepDiff(cur.Spec, new.Spec); equal {
		log.V(1).M(host).Info("StatefulSet.Spec ARE EQUAL")
	} else {
		log.V(1).Info(
			"StatefulSet.Spec ARE DIFFERENT:\nadded:\n%s\nmodified:\n%s\nremoved:\n%s",
			util.MessageDiffItemString("added .spec items", "none", "", diff.Added),
			util.MessageDiffItemString("modified .spec items", "none", "", diff.Modified),
			util.MessageDiffItemString("removed .spec items", "none", "", diff.Removed),
		)
	}
	if diff, equal := messagediff.DeepDiff(cur.Labels, new.Labels); equal {
		log.V(1).M(host).Info("StatefulSet.Labels ARE EQUAL")
	} else {
		if len(cur.Labels)+len(new.Labels) > 0 {
			log.V(1).Info(
				"StatefulSet.Labels ARE DIFFERENT:\nadded:\n%s\nmodified:\n%s\nremoved:\n%s",
				util.MessageDiffItemString("added .labels items", "none", "", diff.Added),
				util.MessageDiffItemString("modified .labels items", "none", "", diff.Modified),
				util.MessageDiffItemString("removed .labels items", "none", "", diff.Removed),
			)
		}
	}
	if diff, equal := messagediff.DeepDiff(cur.Annotations, new.Annotations); equal {
		log.V(1).M(host).Info("StatefulSet.Annotations ARE EQUAL")
	} else {
		if len(cur.Annotations)+len(new.Annotations) > 0 {
			log.V(1).Info(
				"StatefulSet.Annotations ARE DIFFERENT:\nadded:\n%s\nmodified:\n%s\nremoved:\n%s",
				util.MessageDiffItemString("added .annotations items", "none", "", diff.Added),
				util.MessageDiffItemString("modified .annotations items", "none", "", diff.Modified),
				util.MessageDiffItemString("removed .annotations items", "none", "", diff.Removed),
			)
		}
	}
}

// FixStuckStatefulSets checks all hosts in a CR for StatefulSets stuck at 0 replicas
// and forces them to scale up to the desired replica count.
func FixStuckStatefulSets(ctx context.Context, announcer can.Announcer, sts interfaces.IKubeSTS, cr api.ICustomResource) {
	cr.WalkHosts(func(host *api.Host) error {
		curStatefulSet := host.Runtime.CurStatefulSet
		if curStatefulSet == nil {
			return nil
		}

		desiredReplicas := host.GetStatefulSetReplicasNum(false)
		if curStatefulSet.Spec.Replicas != nil && *curStatefulSet.Spec.Replicas == 0 && desiredReplicas != nil && *desiredReplicas > 0 {
			announcer.V(1).M(host).Info("StatefulSet stuck at 0 replicas, forcing scale to %d", *desiredReplicas)
			curStatefulSet.Spec.Replicas = desiredReplicas
			if _, err := sts.Update(ctx, curStatefulSet); err != nil {
				announcer.V(1).M(host).Error("Failed to scale up stuck StatefulSet: %v", err)
			}
		}
		return nil
	})
}
