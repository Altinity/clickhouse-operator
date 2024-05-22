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

import api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

type NamerClickHouse struct {
	*namer
}

// NewClickHouse creates new namer with specified context
func NewClickHouse() *NamerClickHouse {
	return &NamerClickHouse{
		namer: New(),
	}
}

func (n *NamerClickHouse) Names(what NameType, params ...any) []string {
	switch what {
	default:
		return n.namer.Names(what, params...)
	}
	panic("unknown names type")
}

func (n *NamerClickHouse) Name(what NameType, params ...any) string {
	switch what {
	case NameConfigMapHost:
		host := params[0].(*api.Host)
		return createConfigMapHostName(host)
	case NameConfigMapCommon:
		cr := params[0].(api.ICustomResource)
		return createConfigMapCommonName(cr)
	case NameConfigMapCommonUsers:
		cr := params[0].(api.ICustomResource)
		return createConfigMapCommonUsersName(cr)
	default:
		return n.namer.Name(what, params...)
	}

	panic("unknown name type")
}
