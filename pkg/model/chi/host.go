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
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// HostIsNewOne checks whether host is a newly created
// TODO there should be better way to detect newly created CHI
// TODO unify with api host.IsNew
func HostIsNewOne(host *api.ChiHost) bool {
	return host.GetCHI().EnsureStatus().GetHostsCount() == host.GetCHI().EnsureStatus().GetHostsAddedCount()
}

// HostHasTablesCreated checks whether host has tables listed as already created
func HostHasTablesCreated(host *api.ChiHost) bool {
	return util.InArray(CreateFQDN(host), host.GetCHI().EnsureStatus().GetHostsWithTablesCreated())
}

func HostWalkPorts(host *api.ChiHost, f func(name string, port *int32, protocol core.Protocol) bool) {
	if host == nil {
		return
	}
	if f(ChDefaultTCPPortName, &host.TCPPort, core.ProtocolTCP) {
		return
	}
	if f(ChDefaultTLSPortName, &host.TLSPort, core.ProtocolTCP) {
		return
	}
	if f(ChDefaultHTTPPortName, &host.HTTPPort, core.ProtocolTCP) {
		return
	}
	if f(ChDefaultHTTPSPortName, &host.HTTPSPort, core.ProtocolTCP) {
		return
	}
	if f(ChDefaultInterserverHTTPPortName, &host.InterserverHTTPPort, core.ProtocolTCP) {
		return
	}
}

func HostWalkAssignedPorts(host *api.ChiHost, f func(name string, port *int32, protocol core.Protocol) bool) {
	if host == nil {
		return
	}
	HostWalkPorts(
		host,
		func(_name string, _port *int32, _protocol core.Protocol) bool {
			if api.IsPortAssigned(*_port) {
				return f(_name, _port, _protocol)
			}
			// Do not break, continue iterating
			return false
		},
	)
}

func HostWalkInvalidPorts(host *api.ChiHost, f func(name string, port *int32, protocol core.Protocol) bool) {
	if host == nil {
		return
	}
	HostWalkPorts(
		host,
		func(_name string, _port *int32, _protocol core.Protocol) bool {
			if api.IsPortInvalid(*_port) {
				return f(_name, _port, _protocol)
			}
			// Do not break, continue iterating
			return false
		},
	)
}
