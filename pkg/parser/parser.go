package parser

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// CreateObjects returns a map of the k8s objects created based on ClickHouseInstallation Object properties
func CreateObjects(chi *chiv1.ClickHouseInstallation) (ObjectsMap, []string) {
	var (
		clusters []*chiv1.ChiCluster
		options  genOptions
	)

	setDeploymentDefaults(&chi.Spec.Defaults.Deployment, nil)
	clusters, options.dRefsMax = getNormalizedClusters(chi)

	options.ssNames = make(map[string]struct{})
	options.ssIndex = make(map[string]string)
	options.ssDeployments = make(map[string]*chiv1.ChiDeployment)

	cmData := make(map[string]string)
	cmData[remoteServersXML] = genRemoteServersConfig(chi, &options, clusters)

	includeIfNotEmpty(cmData, zookeeperXML, genZookeeperConfig(chi))
	includeIfNotEmpty(cmData, usersXML, genUsersConfig(chi))

	prefixes := make([]string, 0, len(options.ssNames))
	for p := range options.ssNames {
		prefixes = append(prefixes, p)
	}

	return ObjectsMap{
		ObjectsConfigMaps:   createConfigMapObjects(chi, cmData),
		ObjectsServices:     createServiceObjects(chi, &options),
		ObjectsStatefulSets: createStatefulSetObjects(chi, &options),
	}, prefixes
}

func getNormalizedClusters(chi *chiv1.ClickHouseInstallation) ([]*chiv1.ChiCluster, chiDeploymentRefs) {
	link := make(chan *chiClusterDataLink)
	count := len(chi.Spec.Configuration.Clusters)
	for _, cluster := range chi.Spec.Configuration.Clusters {
		go func(chin *chiv1.ClickHouseInstallation, c chiv1.ChiCluster, ch chan<- *chiClusterDataLink) {
			ch <- getNormalizedClusterLayoutData(chin, c)
		}(chi, cluster, link)
	}
	cList := make([]*chiv1.ChiCluster, 0, count)
	dRefs := make(chiDeploymentRefs)
	for i := 0; i < count; i++ {
		data := <-link
		cList = append(cList, data.cluster)
		dRefs.mergeWith(data.deployments)
	}
	return cList, dRefs
}

func getNormalizedClusterLayoutData(chi *chiv1.ClickHouseInstallation, c chiv1.ChiCluster) *chiClusterDataLink {
	n := &chiv1.ChiCluster{
		Name:       c.Name,
		Deployment: c.Deployment,
	}
	setDeploymentDefaults(&n.Deployment, &chi.Spec.Defaults.Deployment)
	d := make(chiDeploymentRefs)

	switch c.Layout.Type {
	case clusterLayoutTypeStandard:
		if c.Layout.ReplicasCount == 0 {
			c.Layout.ReplicasCount++
		}
		if c.Layout.ShardsCount == 0 {
			c.Layout.ShardsCount++
		}

		n.Layout.Shards = make([]chiv1.ChiClusterLayoutShard, c.Layout.ShardsCount)
		for i := 0; i < c.Layout.ShardsCount; i++ {
			n.Layout.Shards[i].InternalReplication = stringTrue
			n.Layout.Shards[i].Replicas = make([]chiv1.ChiClusterLayoutShardReplica, c.Layout.ReplicasCount)

			for j := 0; j < c.Layout.ReplicasCount; j++ {
				setDeploymentDefaults(&n.Layout.Shards[i].Replicas[j].Deployment, &n.Deployment)
				n.Layout.Shards[i].Replicas[j].Deployment.Key = deploymentToString(&n.Layout.Shards[i].Replicas[j].Deployment)
				d[n.Layout.Shards[i].Replicas[j].Deployment.Key]++
			}
		}
	case clusterLayoutTypeAdvanced:
		n.Layout.Shards = c.Layout.Shards
		for i := range n.Layout.Shards {
			setDeploymentDefaults(&n.Layout.Shards[i].Deployment, &n.Deployment)

			switch n.Layout.Shards[i].InternalReplication {
			case shardInternalReplicationDisabled:
				n.Layout.Shards[i].InternalReplication = stringFalse
			default:
				n.Layout.Shards[i].InternalReplication = stringTrue
			}

			if n.Layout.Shards[i].DefinitionType == shardDefinitionTypeReplicasCount {
				n.Layout.Shards[i].Replicas = make([]chiv1.ChiClusterLayoutShardReplica,
					n.Layout.Shards[i].ReplicasCount)

				for j := 0; j < n.Layout.Shards[i].ReplicasCount; j++ {
					setDeploymentDefaults(&n.Layout.Shards[i].Replicas[j].Deployment, &n.Layout.Shards[i].Deployment)
					n.Layout.Shards[i].Replicas[j].Deployment.Key = deploymentToString(&n.Layout.Shards[i].Replicas[j].Deployment)
					d[n.Layout.Shards[i].Replicas[j].Deployment.Key]++
				}
				continue
			}

			for j := range n.Layout.Shards[i].Replicas {
				setDeploymentDefaults(&n.Layout.Shards[i].Replicas[j].Deployment, &n.Layout.Shards[i].Deployment)
				n.Layout.Shards[i].Replicas[j].Deployment.Key = deploymentToString(&n.Layout.Shards[i].Replicas[j].Deployment)
				d[n.Layout.Shards[i].Replicas[j].Deployment.Key]++
			}
		}
	}

	return &chiClusterDataLink{
		cluster:     n,
		deployments: d,
	}
}

func deploymentToString(d *chiv1.ChiDeployment) string {
	var keys []string
	a := make([]string, 0, len(d.Zone.MatchLabels))
	a = append(a, fmt.Sprintf("%s::%s::%s::", d.Scenario, d.PodTemplateName, d.VolumeClaimTemplate))
	for k := range d.Zone.MatchLabels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		a = append(a, d.Zone.MatchLabels[k])
	}
	return strings.Join(a, "::")
}

func setDeploymentDefaults(d, parent *chiv1.ChiDeployment) {
	if parent != nil {
		if d.PodTemplateName == "" {
			(*d).PodTemplateName = parent.PodTemplateName
		}
		if d.VolumeClaimTemplate == "" {
			(*d).VolumeClaimTemplate = parent.VolumeClaimTemplate
		}
		if d.Scenario == "" {
			(*d).Scenario = parent.Scenario
		}
		if len(d.Zone.MatchLabels) == 0 {
			zoneCopyFrom(&d.Zone, &parent.Zone)
		}
	} else if d.Scenario == "" {
		(*d).Scenario = deploymentScenarioDefault
	}
}

func zoneCopyFrom(z, source *chiv1.ChiDeploymentZone) {
	tmp := make(map[string]string)
	for k, v := range source.MatchLabels {
		tmp[k] = v
	}
	(*z).MatchLabels = tmp
}

func (d chiDeploymentRefs) mergeWith(another chiDeploymentRefs) {
	for ak, av := range another {
		_, ok := d[ak]
		if !ok || av > d[ak] {
			d[ak] = av
		}
	}
}

func randomString() string {
	return fmt.Sprintf("%x", rand.New(rand.NewSource(time.Now().UnixNano())).Int())
}

func includeIfNotEmpty(dest map[string]string, key, src string) {
	if src == "" {
		return
	}
	dest[key] = src
}
