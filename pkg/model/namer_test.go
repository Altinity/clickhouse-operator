package model

import (
	"testing"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/kubernetes-sigs/yaml"
	"github.com/stretchr/testify/require"
)

var NamespaceDomainPatternData = `
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "namespace-domain-pattern"
  namespace: "kube-system"
spec:
  namespaceDomainPattern: "%s.svc"
  configuration:
    clusters:
      - name: "shard1-repl1"
        layout:
          shardsCount: 1
          replicasCount: 1
`

var NoNamespaceDomainPatternData = `
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "namespace-domain-pattern"
  namespace: "kube-system"
spec:
  configuration:
    clusters:
      - name: "shard1-repl1"
        layout:
          shardsCount: 1
          replicasCount: 1
`

var PodGenerateNameData = `
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "pod-generate-name"
  namespace: "kube-system"
spec:
  configuration: 
    clusters:
      - name: shard1-repl1
        templates:
          podTemplate: clickhouse-pod-template
        layout:
          shardsCount: 1
          replicasCount: 1
  templates:
    podTemplates:
      - name: "clickhouse-pod-template"
        generateName: "storage-chi-{chi}-{cluster}-{host}"
        spec:
          containers:
            - name: clickhouse-pod
              image: yandex/clickhouse-server:19.16.10.44
              volumeMounts:
                - name: clickhouse-storage-template
                  mountPath: /var/lib/clickhouse
`

var NoPodGenerateNameData = `
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "pod-generate-name"
  namespace: "kube-system"
spec:
  configuration:
    clusters:
      - name: shard1-repl1
        templates:
          podTemplate: clickhouse-pod-template
        layout:
          shardsCount: 1
          replicasCount: 1
  templates:
    podTemplates:
      - name: clickhouse-pod-template
        spec:
          containers:
            - name: clickhouse-pod
              image: yandex/clickhouse-server:19.16.10.44
              volumeMounts:
                - name: clickhouse-storage-template
                  mountPath: /var/lib/clickhouse
`

func TestCreateChiServiceFQDN(t *testing.T) {
	CHOp := chop.NewCHOp("", nil, "")
	CHOp.Init()
	normalizer := NewNormalizer(CHOp)

	// Test specify namespaceDomainPattern in CHI
	chi := new(chiv1.ClickHouseInstallation)
	err := yaml.Unmarshal([]byte(NamespaceDomainPatternData), chi)
	require.Nil(t, err, "failed to unmarshal chi")
	chi, err = normalizer.NormalizeCHI(chi)
	require.Nil(t, err, "failed to normalize chi")
	name := CreateCHIServiceFQDN(chi)
	require.Equal(t, "clickhouse-namespace-domain-pattern.kube-system.svc", name, "unexpected chi service fqdn")

	// Test no namespaceDomainPattern specified in CHI
	chi1 := new(chiv1.ClickHouseInstallation)
	err = yaml.Unmarshal([]byte(NoNamespaceDomainPatternData), chi1)
	require.Nil(t, err, "failed to unmarshal chi")
	chi1, err = normalizer.NormalizeCHI(chi1)
	require.Nil(t, err, "failed to normalize chi")
	name1 := CreateCHIServiceFQDN(chi1)
	require.Equal(t, "clickhouse-namespace-domain-pattern.kube-system.svc.cluster.local", name1, "unexpected chi service fqdn")
}

func TestCreatePodFQDN(t *testing.T) {
	CHOp := chop.NewCHOp("", nil, "")
	CHOp.Init()
	normalizer := NewNormalizer(CHOp)

	// Test specify namespaceDomainPattern in CHI
	chi := new(chiv1.ClickHouseInstallation)
	err := yaml.Unmarshal([]byte(NamespaceDomainPatternData), chi)
	require.Nil(t, err, "failed to unmarshal chi")
	chi, err = normalizer.NormalizeCHI(chi)
	require.Nil(t, err, "failed to normalize chi")
	chi.WalkHostsTillError(func(host *chiv1.ChiHost) error {
		name := CreatePodFQDN(host)
		require.Equal(t, "chi-namespace-domain-pattern-shard1-repl1-0-0.kube-system.svc", name, "unepected pod fqdn")
		return nil
	})

	// Test no namespaceDomainPattern specified in CHI
	chi1 := new(chiv1.ClickHouseInstallation)
	err = yaml.Unmarshal([]byte(NoNamespaceDomainPatternData), chi1)
	require.Nil(t, err, "failed to unmarshal chi")
	chi1, err = normalizer.NormalizeCHI(chi1)
	require.Nil(t, err, "failed to normalize chi")
	chi1.WalkHostsTillError(func(host *chiv1.ChiHost) error {
		name := CreatePodFQDN(host)
		require.Equal(t, "chi-namespace-domain-pattern-shard1-repl1-0-0.kube-system.svc.cluster.local", name, "unepected pod fqdn")
		return nil
	})
}

func TestCreateStatefulSetName(t *testing.T) {
	CHOp := chop.NewCHOp("", nil, "")
	CHOp.Init()
	normalizer := NewNormalizer(CHOp)

	chi := new(chiv1.ClickHouseInstallation)
	err := yaml.Unmarshal([]byte(PodGenerateNameData), chi)
	require.Nil(t, err, "failed to unmarshal chi")
	chi, err = normalizer.NormalizeCHI(chi)
	require.Nil(t, err, "failed to normalize chi")
	chi.WalkHostsTillError(func(host *chiv1.ChiHost) error {
		name := CreateStatefulSetName(host)
		require.Equal(t, "storage-chi-pod-generate-name-shard1-repl1-0-0", name, "unepected pod fqdn")
		return nil
	})

	chi1 := new(chiv1.ClickHouseInstallation)
	err = yaml.Unmarshal([]byte(NoPodGenerateNameData), chi1)
	require.Nil(t, err, "failed to unmarshal chi")
	chi1, err = normalizer.NormalizeCHI(chi1)
	require.Nil(t, err, "failed to normalize chi")
	chi1.WalkHostsTillError(func(host *chiv1.ChiHost) error {
		name := CreateStatefulSetName(host)
		require.Equal(t, "chi-pod-generate-name-shard1-repl1-0-0", name, "unepected pod fqdn")
		return nil
	})
}
