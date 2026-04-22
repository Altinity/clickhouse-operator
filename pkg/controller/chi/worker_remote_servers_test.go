package chi

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

func TestIsLegacyRemoteServersMode(t *testing.T) {
	w := &worker{}

	cfg := chop.Config()
	originalThreshold := cfg.ClickHouse.Config.RemoteServers.RemoteServersSplitThresholdBytes
	cfg.ClickHouse.Config.RemoteServers.RemoteServersSplitThresholdBytes = 100
	t.Cleanup(func() {
		cfg.ClickHouse.Config.RemoteServers.RemoteServersSplitThresholdBytes = originalThreshold
	})

	tests := []struct {
		name      string
		fragments []interfaces.RemoteServersFragment
		expected  bool
	}{
		{
			name:      "zero fragments fallback to legacy mode",
			fragments: []interfaces.RemoteServersFragment{},
			expected:  true,
		},
		{
			name: "one fragment below threshold is legacy mode",
			fragments: []interfaces.RemoteServersFragment{
				{TotalBytes: 99},
			},
			expected: true,
		},
		{
			name: "one fragment at threshold is legacy mode",
			fragments: []interfaces.RemoteServersFragment{
				{TotalBytes: 100},
			},
			expected: true,
		},
		{
			name: "one fragment above threshold is fragmented mode",
			fragments: []interfaces.RemoteServersFragment{
				{TotalBytes: 101},
			},
			expected: false,
		},
		{
			name: "multiple fragments are always fragmented mode",
			fragments: []interfaces.RemoteServersFragment{
				{TotalBytes: 50},
				{TotalBytes: 50},
			},
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, w.isLegacyRemoteServersMode(test.fragments))
		})
	}
}
