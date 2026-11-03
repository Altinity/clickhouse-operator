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
)

func (c *Controller) getPDB(ctx context.Context, pdb *policy.PodDisruptionBudget) (*policy.PodDisruptionBudget, error) {
	return c.kube.PDB().Get(ctx, pdb.GetNamespace(), pdb.GetName())
}

func (c *Controller) createPDB(ctx context.Context, pdb *policy.PodDisruptionBudget) error {
	_, err := c.kube.PDB().Create(ctx, pdb)

	return err
}

func (c *Controller) updatePDB(ctx context.Context, pdb *policy.PodDisruptionBudget) error {
	_, err := c.kube.PDB().Update(ctx, pdb)

	return err
}
