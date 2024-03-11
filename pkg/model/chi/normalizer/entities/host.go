package entities

import api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

// NormalizeHostPorts ensures api.ChiReplica.Port is reasonable
func NormalizeHostPorts(host *api.ChiHost) {
	// Deprecated
	if api.IsPortInvalid(host.Port) {
		host.Port = api.PortUnassigned()
	}

	if api.IsPortInvalid(host.TCPPort) {
		host.TCPPort = api.PortUnassigned()
	}

	if api.IsPortInvalid(host.TLSPort) {
		host.TLSPort = api.PortUnassigned()
	}

	if api.IsPortInvalid(host.HTTPPort) {
		host.HTTPPort = api.PortUnassigned()
	}

	if api.IsPortInvalid(host.HTTPSPort) {
		host.HTTPSPort = api.PortUnassigned()
	}

	if api.IsPortInvalid(host.InterserverHTTPPort) {
		host.InterserverHTTPPort = api.PortUnassigned()
	}
}
