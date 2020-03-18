package model

import (
	"testing"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/kubernetes-sigs/yaml"
	"github.com/stretchr/testify/require"
)

var ZookeeperOnClusterData = `
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "repl-06"
spec:
  configuration:
    zookeeper:
      nodes:
        - host: zookeeper-0.zookeepers.zoo1ns
      session_timeout_ms: 30000
      operation_timeout_ms: 10000
    clusters:
      - name: replcluster1
        layout:
          shardsCount: 3
          replicasCount: 2
      - name: replcluster2
        zookeeper:
          nodes:
            - host: zookeeper-0.zookeepers.zoo3ns
            - host: zookeeper-1.zookeepers.zoo3ns
            - host: zookeeper-2.zookeepers.zoo3ns
        layout:
          shardsCount: 3
          replicasCount: 2
`

var Cluster1ZookeeperConfigurationData = `<yandex>
    <zookeeper>
        <node>
            <host>zookeeper-0.zookeepers.zoo1ns</host>
            <port>2181</port>
        </node>
        <node>
            <host>zookeeper-1.zookeepers.zoo1ns</host>
            <port>2181</port>
        </node>
        <node>
            <host>zookeeper-2.zookeepers.zoo1ns</host>
            <port>2181</port>
        </node>
        <session_timeout_ms>30000</session_timeout_ms>
        <operation_timeout_ms>10000</operation_timeout_ms>
    </zookeeper>
    <distributed_ddl>
        <path>/clickhouse/repl-06/task_queue/ddl</path>
    </distributed_ddl>
</yandex>
`

var Cluster2ZookeeperConfigurationData = `<yandex>
    <zookeeper>
        <node>
            <host>zookeeper-0.zookeepers.zoo3ns</host>
            <port>2181</port>
        </node>
        <node>
            <host>zookeeper-1.zookeepers.zoo3ns</host>
            <port>2181</port>
        </node>
        <node>
            <host>zookeeper-2.zookeepers.zoo3ns</host>
            <port>2181</port>
        </node>
    </zookeeper>
    <distributed_ddl>
        <path>/clickhouse/repl-06/task_queue/ddl</path>
    </distributed_ddl>
</yandex>
`

func TestGetZookeeper(t *testing.T) {
	chi := new(chiv1.ClickHouseInstallation)
	err := yaml.Unmarshal([]byte(ZookeeperOnClusterData), chi)
	require.Nil(t, err, "failed to unmarshal chi")

	CHOp := chop.NewCHOp("", nil, "")
	CHOp.Init()
	normalizer := NewNormalizer(CHOp)
	chi, err = normalizer.NormalizeCHI(chi)
	require.Nil(t, err, "failed to normalize chi")

	creator := NewCreator(CHOp, chi)
	chi.WalkHostsTillError(func(host *chiv1.ChiHost) error {
		str := creator.chConfigGenerator.GetHostZookeeper(host)
		if host.Address.ClusterName == "replcluster2" {
			require.Equal(t, Cluster2ZookeeperConfigurationData, str, "unexpected zookeeper configuration")
		} else {
			require.Equal(t, Cluster1ZookeeperConfigurationData, str, "unexpected zookeeper configuration")
		}

		return nil
	})
}
