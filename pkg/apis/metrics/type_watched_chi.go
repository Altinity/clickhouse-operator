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

package metrics

import (
	"encoding/json"
	v1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// WatchedCHI specifies watched ClickHouseInstallation
type WatchedCHI struct {
	Namespace string            `json:"namespace"`
	Name      string            `json:"name"`
	Clusters  []*WatchedCluster `json:"clusters"`
}

// WatchedCluster specifies watched cluster
type WatchedCluster struct {
	Name  string         `json:"name,omitempty"  yaml:"name,omitempty"`
	Hosts []*WatchedHost `json:"hosts,omitempty" yaml:"hosts,omitempty"`
}

// WatchedHost specifies watched host
type WatchedHost struct {
	Name      string `json:"name,omitempty"      yaml:"name,omitempty"`
	Hostname  string `json:"hostname,omitempty"  yaml:"hostname,omitempty"`
	TCPPort   int32  `json:"tcpPort,omitempty"   yaml:"tcpPort,omitempty"`
	TLSPort   int32  `json:"tlsPort,omitempty"   yaml:"tlsPort,omitempty"`
	HTTPPort  int32  `json:"httpPort,omitempty"  yaml:"httpPort,omitempty"`
	HTTPSPort int32  `json:"httpsPort,omitempty" yaml:"httpsPort,omitempty"`
}

// NewWatchedCHI creates new watched CHI
func NewWatchedCHI(c *v1.ClickHouseInstallation) *WatchedCHI {
	chi := &WatchedCHI{}
	chi.readFrom(c)
	return chi
}

func (chi *WatchedCHI) readFrom(c *v1.ClickHouseInstallation) {
	if chi == nil {
		return
	}
	chi.Namespace = c.Namespace
	chi.Name = c.Name

	c.WalkClusters(func(cl *v1.Cluster) error {
		cluster := &WatchedCluster{}
		cluster.readFrom(cl)
		chi.Clusters = append(chi.Clusters, cluster)
		return nil
	})
}

func (chi *WatchedCHI) isValid() bool {
	return !chi.empty()
}

func (chi *WatchedCHI) empty() bool {
	return (len(chi.Namespace) == 0) && (len(chi.Name) == 0) && (len(chi.Clusters) == 0)
}

func (chi *WatchedCHI) indexKey() string {
	return chi.Namespace + ":" + chi.Name
}

func (chi *WatchedCHI) walkHosts(f func(*WatchedCHI, *WatchedCluster, *WatchedHost)) {
	if chi == nil {
		return
	}
	for _, cluster := range chi.Clusters {
		for _, host := range cluster.Hosts {
			f(chi, cluster, host)
		}
	}
}

// String is a stringifier
func (chi *WatchedCHI) String() string {
	if chi == nil {
		return "nil"
	}
	bytes, _ := json.Marshal(chi)
	return string(bytes)
}

func (cluster *WatchedCluster) readFrom(c *v1.Cluster) {
	if cluster == nil {
		return
	}
	cluster.Name = c.Name

	c.WalkHosts(func(h *v1.ChiHost) error {
		host := &WatchedHost{}
		host.readFrom(h)
		cluster.Hosts = append(cluster.Hosts, host)
		return nil
	})
}

func (host *WatchedHost) readFrom(h *v1.ChiHost) {
	if host == nil {
		return
	}
	host.Name = h.Name
	host.Hostname = h.Address.FQDN
	host.TCPPort = h.TCPPort
	host.TLSPort = h.TLSPort
	host.HTTPPort = h.HTTPPort
	host.HTTPSPort = h.HTTPSPort
}
