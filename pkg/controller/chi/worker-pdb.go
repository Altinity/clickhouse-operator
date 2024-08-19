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

	policy "k8s.io/api/policy/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// reconcilePDB reconciles PodDisruptionBudget
func (w *worker) reconcilePDB(ctx context.Context, cluster api.ICluster, pdb *policy.PodDisruptionBudget) error {
	cur, err := w.c.getPDB(ctx, pdb)
	switch {
	case err == nil:
		pdb.ResourceVersion = cur.ResourceVersion
		err := w.c.updatePDB(ctx, pdb)
		if err == nil {
			log.V(1).Info("PDB updated: %s", util.NamespaceNameString(pdb))
		} else {
			log.Error("FAILED to update PDB: %s err: %v", util.NamespaceNameString(pdb), err)
			return nil
		}
	case apiErrors.IsNotFound(err):
		err := w.c.createPDB(ctx, pdb)
		if err == nil {
			log.V(1).Info("PDB created: %s", util.NamespaceNameString(pdb))
		} else {
			log.Error("FAILED create PDB: %s err: %v", util.NamespaceNameString(pdb), err)
			return err
		}
	default:
		log.Error("FAILED get PDB: %s err: %v", util.NamespaceNameString(pdb), err)
		return err
	}

	return nil
}
