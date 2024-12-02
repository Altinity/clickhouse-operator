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

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// WatchedCR specifies watched ClickHouseInstallation
type WatchedCR struct {
	Namespace   string            `json:"namespace"`
	Name        string            `json:"name"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	Clusters    []*WatchedCluster `json:"clusters"`
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

// NewWatchedCR creates new watched CR
func NewWatchedCR(src api.ICustomResource) *WatchedCR {
	cr := &WatchedCR{}
	cr.readFrom(src)
	return cr
}

func (cr *WatchedCR) readFrom(src api.ICustomResource) {
	if cr == nil {
		return
	}
	cr.Namespace = src.GetNamespace()
	cr.Name = src.GetName()
	cr.Labels = src.GetLabels()
	cr.Annotations = src.GetAnnotations()

	src.WalkClusters(func(cl api.ICluster) error {
		cluster := &WatchedCluster{}
		cluster.readFrom(cl)
		cr.Clusters = append(cr.Clusters, cluster)
		return nil
	})
}

func (cr *WatchedCR) IsValid() bool {
	return !cr.empty()
}

func (cr *WatchedCR) empty() bool {
	return (len(cr.Namespace) == 0) && (len(cr.Name) == 0) && (len(cr.Clusters) == 0)
}

func (cr *WatchedCR) IndexKey() string {
	return cr.Namespace + ":" + cr.Name
}

func (cr *WatchedCR) WalkHosts(f func(*WatchedCR, *WatchedCluster, *WatchedHost)) {
	if cr == nil {
		return
	}
	for _, cluster := range cr.Clusters {
		for _, host := range cluster.Hosts {
			f(cr, cluster, host)
		}
	}
}

func (cr *WatchedCR) GetName() string {
	if cr == nil {
		return ""
	}
	return cr.Name
}

func (cr *WatchedCR) GetNamespace() string {
	if cr == nil {
		return ""
	}
	return cr.Namespace
}

func (cr *WatchedCR) GetLabels() map[string]string {
	if cr == nil {
		return nil
	}
	return cr.Labels
}

func (cr *WatchedCR) GetAnnotations() map[string]string {
	if cr == nil {
		return nil
	}
	return cr.Annotations
}

// String is a stringifier
func (cr *WatchedCR) String() string {
	if cr == nil {
		return "nil"
	}
	bytes, _ := json.Marshal(cr)
	return string(bytes)
}

func (cluster *WatchedCluster) readFrom(c api.ICluster) {
	if cluster == nil {
		return
	}
	cluster.Name = c.GetName()

	c.WalkHosts(func(h *api.Host) error {
		host := &WatchedHost{}
		host.readFrom(h)
		cluster.Hosts = append(cluster.Hosts, host)
		return nil
	})
}

func (host *WatchedHost) readFrom(h *api.Host) {
	if host == nil {
		return
	}
	host.Name = h.Name
	host.Hostname = h.Runtime.Address.FQDN
	host.TCPPort = h.TCPPort.Value()
	host.TLSPort = h.TLSPort.Value()
	host.HTTPPort = h.HTTPPort.Value()
	host.HTTPSPort = h.HTTPSPort.Value()
}
