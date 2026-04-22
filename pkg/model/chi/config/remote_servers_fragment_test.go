package config

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func remoteServersWrapperBytes(clusterName, secretXML string) int {
	header := fmt.Sprintf("<%s>\n    <remote_servers>\n        <%s>\n", xmlTagYandex, clusterName)
	if secretXML != "" {
		header += secretXML
	}
	footer := fmt.Sprintf("        </%s>\n    </remote_servers>\n</%s>\n", clusterName, xmlTagYandex)
	return len(header) + len(footer)
}

func TestStreamRemoteServersFragments_PackingAndAccounting(t *testing.T) {
	g := &Generator{}
	topology := remoteServersTopology{
		ClusterName: "cluster-a",
		SecretXML:   "        <secret>abc</secret>\n",
		Shards: []remoteServersShard{
			{
				Index:               0,
				InternalReplication: "false",
				ReplicasXML: []string{
					"                <replica><host>h0</host><port>9000</port></replica>\n",
				},
			},
			{
				Index:               1,
				InternalReplication: "false",
				ReplicasXML: []string{
					"                <replica><host>h1</host><port>9000</port></replica>\n",
				},
			},
			{
				Index:               2,
				InternalReplication: "false",
				ReplicasXML: []string{
					"                <replica><host>h2</host><port>9000</port></replica>\n",
				},
			},
		},
	}

	shard0Bytes := len(g.renderShardXML(topology.Shards[0]))
	shard1Bytes := len(g.renderShardXML(topology.Shards[1]))
	shard2Bytes := len(g.renderShardXML(topology.Shards[2]))
	overhead := remoteServersWrapperBytes(topology.ClusterName, topology.SecretXML)

	threshold := overhead + shard0Bytes + shard1Bytes
	fragments, err := g.streamRemoteServersFragments(topology, threshold, 10)
	require.NoError(t, err)
	require.Len(t, fragments, 2)

	first := fragments[0]
	require.Equal(t, "cluster-a", first.Cluster)
	require.Equal(t, 0, first.ShardStart)
	require.Equal(t, 1, first.ShardEnd)
	expectedPayloadFirst := shard0Bytes + shard1Bytes
	require.Equal(t, expectedPayloadFirst, first.PayloadBytes)
	require.Equal(t, expectedPayloadFirst+overhead, first.TotalBytes)
	require.Equal(t, len(first.XML), first.TotalBytes)
	require.LessOrEqual(t, first.TotalBytes, threshold)

	second := fragments[1]
	require.Equal(t, "cluster-a", second.Cluster)
	require.Equal(t, 2, second.ShardStart)
	require.Equal(t, 2, second.ShardEnd)
	expectedPayloadSecond := shard2Bytes
	require.Equal(t, expectedPayloadSecond, second.PayloadBytes)
	require.Equal(t, expectedPayloadSecond+overhead, second.TotalBytes)
	require.Equal(t, len(second.XML), second.TotalBytes)
	require.LessOrEqual(t, second.TotalBytes, threshold)
}
