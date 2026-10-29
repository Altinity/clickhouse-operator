package volume

import (
	"strings"
	"testing"

	"github.com/altinity/clickhouse-operator/pkg/model/chk/config"
)

// Keeper loads conf.d before keeper_config.d. The bootstrap replacement must
// be projected into keeper_config.d after the shared Raft configuration, and
// the projected ConfigMap must remain live-updatable (no subPath mounts).
func TestProjectedCommonConfigVolumeBootstrapOrdering(t *testing.T) {
	v := createProjectedCommonConfigVolume("common-cm", "host-cm")
	if v.Name != "common-cm" || v.Projected == nil || v.ConfigMap != nil {
		t.Fatalf("expected projected common config volume, got %#v", v)
	}
	if v.Projected.DefaultMode == nil || *v.Projected.DefaultMode != 0644 {
		t.Fatalf("unexpected projected file permissions: %#v", v.Projected.DefaultMode)
	}
	sources := v.Projected.Sources
	if len(sources) != 2 {
		t.Fatalf("expected shared and host ConfigMap sources, got %d", len(sources))
	}
	if sources[0].ConfigMap == nil || sources[0].ConfigMap.Name != "common-cm" ||
		len(sources[0].ConfigMap.Items) != 0 || sources[0].ConfigMap.Optional != nil {
		t.Fatalf("expected all common ConfigMap keys to be mandatory: %#v", sources[0])
	}
	host := sources[1].ConfigMap
	if host == nil || host.Name != "host-cm" || host.Optional == nil || !*host.Optional {
		t.Fatalf("expected optional host bootstrap source: %#v", sources[1])
	}
	if len(host.Items) != 1 {
		t.Fatalf("expected one projected bootstrap key, got %#v", host.Items)
	}
	item := host.Items[0]
	if item.Key != config.BootstrapRaftHostConfigKey || item.Path != config.BootstrapRaftProjectedFile {
		t.Fatalf("unexpected bootstrap projection: %#v", item)
	}
	if strings.HasSuffix(item.Key, ".xml") {
		t.Fatalf("host bootstrap source must not be loaded from conf.d: %q", item.Key)
	}
	if item.Path <= "chop-generated-raft.xml" || !strings.HasSuffix(item.Path, ".xml") {
		t.Fatalf("bootstrap must override shared Raft XML last: %q", item.Path)
	}
}
