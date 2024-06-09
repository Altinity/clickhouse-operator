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
	"fmt"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

type Keeper struct {
}

// NewKeeper creates new namer with specified context
func NewKeeper() *Keeper {
	return &Keeper{}
}

func (n *Keeper) Name(what interfaces.NameType, params ...any) string {
	switch what {
	case interfaces.NameStatefulSetService:
		cr := params[0].(api.ICustomResource)
		return getHeadlessServiceName(cr)
	}

	panic("unknown name type")
}

func (n *Keeper) Names(what interfaces.NameType, params ...any) []string {
	return nil
}

func getHeadlessServiceName(cr api.ICustomResource) string {
	return fmt.Sprintf("%s-headless", cr.GetName())
}
