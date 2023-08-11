package chk

import (
	"bytes"
	"fmt"
	"strings"

	v1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.com/v1alpha1"

	xmlbuilder "github.com/altinity/clickhouse-operator/pkg/model/builder/xml"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func defaultingSettings(chk *v1alpha1.ClickHouseKeeper) map[string]string {
	var props map[string]string
	if chk.Spec.Settings == nil {
		props = map[string]string{}
	} else {
		props = chk.Spec.Settings
	}

	defaultingProperty(props, "logger/level", "information")
	defaultingProperty(props, "logger/console", "1")

	defaultingProperty(props, "listen_host", "0.0.0.0")
	defaultingProperty(props, "max_connections", "4096")

	defaultingProperty(props, "keeper_server/tcp_port", "9181")
	path := chk.Spec.GetPath()
	defaultingProperty(props, "keeper_server/storage_path", path)
	defaultingProperty(props, "keeper_server/log_storage_path", fmt.Sprintf("%s/coordination/logs", path))
	defaultingProperty(props, "keeper_server/snapshot_storage_path", fmt.Sprintf("%s/coordination/snapshots", path))
	defaultingProperty(props, "keeper_server/coordination_settings/operation_timeout_ms", "10000")
	defaultingProperty(props, "keeper_server/coordination_settings/min_session_timeout_ms", "10000")
	defaultingProperty(props, "keeper_server/coordination_settings/session_timeout_ms", "100000")
	defaultingProperty(props, "keeper_server/coordination_settings/raft_logs_level", "information")
	defaultingProperty(props, "keeper_server/hostname_checks_enabled", "true")

	defaultingProperty(props, "openSSL/server/certificateFile", "/etc/clickhouse-keeper/server.crt")
	defaultingProperty(props, "openSSL/server/privateKeyFile", "/etc/clickhouse-keeper/server.key")
	defaultingProperty(props, "openSSL/server/dhParamsFile", "/etc/clickhouse-keeper/dhparam.pem")
	defaultingProperty(props, "openSSL/server/verificationMode", "none")
	defaultingProperty(props, "openSSL/server/loadDefaultCAFile", "true")
	defaultingProperty(props, "openSSL/server/cacheSessions", "true")
	defaultingProperty(props, "openSSL/server/disableProtocols", "sslv2,sslv3")
	defaultingProperty(props, "openSSL/server/preferServerCiphers", "true")

	return props
}

func defaultingProperty(props map[string]string, property_name, value string) {
	if _, ok := props[property_name]; !ok {
		props[property_name] = value
	}
}

// generateXMLConfig creates XML using map[string]string definitions
func generateXMLConfig(m map[string]string, chk *v1alpha1.ClickHouseKeeper) string {
	if len(m) == 0 {
		return ""
	}

	settings := v1.NewSettings()

	for key, value := range m {
		settings.Set(key, v1.NewSettingScalar(value))
	}

	settings.Set("keeper_server/server_id", v1.NewSettingScalar("KEEPER_ID"))
	settings.Set("keeper_server/raft_configuration/server", v1.NewSettingScalar(""))

	b := &bytes.Buffer{}
	// <clickhouse>
	// XML code
	// </clickhouse>
	util.Iline(b, 0, "<clickhouse>")
	xmlbuilder.GenerateXML(b, settings, "")
	util.Iline(b, 0, "</clickhouse>")

	raft := &bytes.Buffer{}
	raftPort := chk.Spec.GetRaftPort()
	for i := 0; i < int(chk.Spec.GetReplicas()); i++ {
		util.Iline(raft, 12, "<server>")
		util.Iline(raft, 12, "    <id>%d</id>", i)
		util.Iline(raft, 12, "    <hostname>%s-%d.%s-headless.%s.svc.cluster.local</hostname>", chk.Name, i, chk.Name, chk.Namespace)
		util.Iline(raft, 12, "    <port>%s</port>", fmt.Sprintf("%d", raftPort))
		util.Iline(raft, 12, "</server>")
	}

	tmp := b.String()

	// tmp = strings.Replace(tmp, "<server_id></server_id>", "<server_id from_env=\"KEEPER_ID\" />", 1)

	return strings.Replace(tmp, "            <server></server>\n", raft.String(), 1)
}
