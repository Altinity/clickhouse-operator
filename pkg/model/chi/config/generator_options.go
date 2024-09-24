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
)

type GeneratorOptions struct {
	DistributedDDL *api.DistributedDDL
	Users          *api.Settings
	Profiles       *api.Settings
	Quotas         *api.Settings

	Settings *api.Settings
	Files    *api.Settings
}

// RemoteServersOptions specifies options for remote-servers generator
type RemoteServersOptions struct {
	exclude struct {
		attributes *api.HostReconcileAttributes
		hosts      []*api.Host
	}
}

// NewRemoteServersOptions creates new remote-servers generator options
func NewRemoteServersOptions() *RemoteServersOptions {
	return &RemoteServersOptions{}
}

// ExcludeHost specifies to exclude a host
func (o *RemoteServersOptions) ExcludeHost(host *api.Host) *RemoteServersOptions {
	if (o == nil) || (host == nil) {
		return o
	}

	o.exclude.hosts = append(o.exclude.hosts, host)
	return o
}

// ExcludeHosts specifies to exclude list of hosts
func (o *RemoteServersOptions) ExcludeHosts(hosts ...*api.Host) *RemoteServersOptions {
	if (o == nil) || (len(hosts) == 0) {
		return o
	}

	o.exclude.hosts = append(o.exclude.hosts, hosts...)
	return o
}

// ExcludeReconcileAttributes specifies to exclude reconcile attributes
func (o *RemoteServersOptions) ExcludeReconcileAttributes(attrs *api.HostReconcileAttributes) *RemoteServersOptions {
	if (o == nil) || (attrs == nil) {
		return o
	}

	o.exclude.attributes = attrs
	return o
}

// Exclude tells whether to exclude the host
func (o *RemoteServersOptions) Exclude(host *api.Host) bool {
	if o == nil {
		return false
	}

	if o.exclude.attributes.Any(host.GetReconcileAttributes()) {
		// Reconcile attributes specify to exclude this host
		return true
	}

	for _, val := range o.exclude.hosts {
		// Host is in the list to be excluded
		if val == host {
			return true
		}
	}

	return false
}

// Include tells whether to include the host
func (o *RemoteServersOptions) Include(host *api.Host) bool {
	if o == nil {
		return false
	}

	if o.exclude.attributes.Any(host.GetReconcileAttributes()) {
		// Reconcile attributes specify to exclude this host
		return false
	}

	for _, val := range o.exclude.hosts {
		// Host is in the list to be excluded
		if val == host {
			return false
		}
	}

	return true
}

// String returns string representation
func (o *RemoteServersOptions) String() string {
	if o == nil {
		return "(nil)"
	}

	var hostnames []string
	for _, host := range o.exclude.hosts {
		hostnames = append(hostnames, host.Name)
	}
	return fmt.Sprintf("exclude hosts: %s, attributes: %s", "["+strings.Join(hostnames, ",")+"]", o.exclude.attributes)
}

// defaultRemoteServersOptions
func defaultRemoteServersOptions() *RemoteServersOptions {
	return NewRemoteServersOptions()
}
