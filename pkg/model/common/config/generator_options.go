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

package config

import (
	"fmt"
	"strings"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// HostSelector specifies options for excluding host
type HostSelector struct {
	exclude struct {
		attributes *types.ReconcileAttributes
		hosts      []*api.Host
	}
}

// NewHostSelector creates new host selector
func NewHostSelector() *HostSelector {
	return &HostSelector{}
}

// ExcludeHost adds host to the list of excluded
func (o *HostSelector) ExcludeHost(host *api.Host) *HostSelector {
	if (o == nil) || (host == nil) {
		return o
	}

	o.exclude.hosts = append(o.exclude.hosts, host)
	return o
}

// ExcludeHosts add hosts to the list of excluded
func (o *HostSelector) ExcludeHosts(hosts ...*api.Host) *HostSelector {
	if (o == nil) || (len(hosts) == 0) {
		return o
	}

	o.exclude.hosts = append(o.exclude.hosts, hosts...)
	return o
}

// ExcludeReconcileAttributes set attributes as specification to exclude
func (o *HostSelector) ExcludeReconcileAttributes(attrs *types.ReconcileAttributes) *HostSelector {
	if (o == nil) || (attrs == nil) {
		return o
	}

	o.exclude.attributes = attrs
	return o
}

func (o *HostSelector) hasExcludedHost(host *api.Host) bool {
	for _, excludedHost := range o.exclude.hosts {
		if host == excludedHost {
			// Host is in the list of excluded
			return true
		}
	}
	return false
}

// Exclude tells whether to exclude the host
func (o *HostSelector) Exclude(host *api.Host) bool {
	if o == nil {
		return false
	}

	if o.exclude.attributes.HasIntersectionWith(host.GetReconcileAttributes()) {
		// Reconcile attributes specify to exclude this host
		return true
	}

	if o.hasExcludedHost(host) {
		// Host is in the list to be excluded
		return true
	}

	return false
}

// Include tells whether to include the host
func (o *HostSelector) Include(host *api.Host) bool {
	if o == nil {
		return false
	}

	if o.exclude.attributes.HasIntersectionWith(host.GetReconcileAttributes()) {
		// Reconcile attributes specify to exclude this host
		return false
	}

	for o.hasExcludedHost(host) {
		// Host is in the list to be excluded
		return false
	}

	return true
}

// String returns string representation
func (o *HostSelector) String() string {
	if o == nil {
		return "(nil)"
	}

	var hostnames []string
	for _, host := range o.exclude.hosts {
		hostnames = append(hostnames, host.Name)
	}
	return fmt.Sprintf("exclude hosts: %s, attributes: %s", "["+strings.Join(hostnames, ",")+"]", o.exclude.attributes)
}
