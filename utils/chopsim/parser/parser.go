package parser

import (
	"fmt"
)

const (
	clusterLayoutTypeStandard = "Standard"
	clusterLayoutTypeAdvanced = "Advanced"
)

const (
	shardDefinitionTypeReplicasCount = "ReplicasCount"
	shardDefinitionTypeReplicas      = "Replicas"
)

const (
	deploymentScenarioDefault      = "Default"
	deploymentScenarioNodeMonopoly = "NodeMonopoly"
)

const (
	shardInternalReplicationEnabled  = "Enabled"
	shardInternalReplicationDisabled = "Disabled"
)

// ClickHouseInstallation describes CRD object instance
type ClickHouseInstallation struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Deployment    chiDeployment    `yaml:"deployment"`
		Configuration chiConfiguration `yaml:"configuration"`
		Templates     chiTemplates     `yaml:"templates"`
	} `yaml:"spec"`
}

type chiConfiguration struct {
	Users    map[string]string `yaml:"users"`
	Settings map[string]string `yaml:"settings"`
	Clusters []chiCluster      `yaml:"clusters"`
}

type chiDeployment struct {
	PodTemplateName string            `yaml:"podTemplateName"`
	Zone            chiDeploymentZone `yaml:"zone"`
	Scenario        string            `yaml:"scenario"`
}

type chiDeploymentZone struct {
	MatchLabels map[string]string `yaml:"matchLabels"`
}

type chiCluster struct {
	Name       string           `yaml:"name"`
	Layout     chiClusterLayout `yaml:"layout"`
	Deployment chiDeployment    `yaml:"deployment"`
}

type chiClusterLayout struct {
	Type          string                  `yaml:"type"`
	ShardsCount   int                     `yaml:"shardsCount"`
	ReplicasCount int                     `yaml:"replicasCount"`
	Shards        []chiClusterLayoutShard `yaml:"shards"`
}

type chiClusterLayoutShard struct {
	DefinitionType      string                         `yaml:"definitionType"`
	ReplicasCount       int                            `yaml:"replicasCount"`
	Weight              int                            `yaml:"weight"`
	InternalReplication string                         `yaml:"internalReplication"`
	Deployment          chiDeployment                  `yaml:"deployment"`
	Replicas            []chiClusterLayoutShardReplica `yaml:"replicas"`
}

type chiClusterLayoutShardReplica struct {
	Deployment chiDeployment `yaml:"deployment"`
}

type chiTemplates struct {
	PodTemplates []chiPodTemplate `yaml:"podTemplates"`
}

type chiPodTemplate struct {
	Name       string `yaml:"name"`
	Containers []chiPodTemplatesContainerTemplate
}

type chiPodTemplatesContainerTemplate struct {
	Name      string `yaml:"name"`
	Image     string `yaml:"image"`
	Resources struct {
		Requests chiContainerResourceParams `yaml:"requests"`
		Limits   chiContainerResourceParams `yaml:"limits"`
	} `yaml:"resources"`
}

type chiContainerResourceParams struct {
	Memory string `yaml:"memory"`
	CPU    string `yaml:"cpu"`
}

type chiDeploymentRefs map[string]int8

type chiClusterDataLink struct {
	cluster     *chiCluster
	deployments chiDeploymentRefs
}

// GenerateArtifacts creates resulting (composite) manifest based on ClickHouseInstallation data provided
func GenerateArtifacts(chi *ClickHouseInstallation) string {
	chi.Spec.Deployment.setDefaults(nil)
	_, deploymentRefs := chi.getNormalizedClusters()

	// debug: start
	for k, v := range deploymentRefs {
		fmt.Printf("%s -> %d\n", k, v)
	}
	// debug: end

	return ""
}

func (chi *ClickHouseInstallation) getNormalizedClusters() ([]*chiCluster, chiDeploymentRefs) {
	link := make(chan *chiClusterDataLink)
	count := len(chi.Spec.Configuration.Clusters)
	for _, cluster := range chi.Spec.Configuration.Clusters {
		go func(c chiCluster, ch chan<- *chiClusterDataLink) {
			ch <- chi.getNormalizedClusterLayoutData(c)
		}(cluster, link)
	}
	cList := make([]*chiCluster, 0, count)
	dRefs := make(chiDeploymentRefs)
	for i := 0; i < count; i++ {
		data := <-link
		cList = append(cList, data.cluster)
		dRefs.mergeWith(data.deployments)
	}
	return cList, dRefs
}

func (chi *ClickHouseInstallation) getNormalizedClusterLayoutData(c chiCluster) *chiClusterDataLink {
	normalizedCluster := &chiCluster{
		Name:       c.Name,
		Deployment: c.Deployment,
	}
	normalizedCluster.Deployment.setDefaults(&chi.Spec.Deployment)
	deploymentRefs := make(chiDeploymentRefs)

	switch c.Layout.Type {
	case clusterLayoutTypeStandard:
		if c.Layout.ReplicasCount == 0 {
			c.Layout.ReplicasCount++
		}
		if c.Layout.ShardsCount == 0 {
			c.Layout.ShardsCount++
		}

		normalizedCluster.Layout.Shards = make([]chiClusterLayoutShard, c.Layout.ShardsCount)
		for i := 0; i < c.Layout.ShardsCount; i++ {
			normalizedCluster.Layout.Shards[i].InternalReplication = shardInternalReplicationEnabled
			normalizedCluster.Layout.Shards[i].Replicas = make([]chiClusterLayoutShardReplica, c.Layout.ReplicasCount)

			for j := 0; j < c.Layout.ReplicasCount; j++ {
				normalizedCluster.Layout.Shards[i].Replicas[j].Deployment.setDefaults(&normalizedCluster.Deployment)
				deploymentRefs[normalizedCluster.Layout.Shards[i].Replicas[j].Deployment.toString()]++
			}
		}
	case clusterLayoutTypeAdvanced:
		normalizedCluster.Layout.Shards = c.Layout.Shards
		for i := range normalizedCluster.Layout.Shards {
			normalizedCluster.Layout.Shards[i].Deployment.setDefaults(&normalizedCluster.Deployment)

			if normalizedCluster.Layout.Shards[i].DefinitionType == shardDefinitionTypeReplicasCount {
				normalizedCluster.Layout.Shards[i].Replicas = make([]chiClusterLayoutShardReplica,
					normalizedCluster.Layout.Shards[i].ReplicasCount)

				for j := 0; j < normalizedCluster.Layout.Shards[i].ReplicasCount; j++ {
					normalizedCluster.Layout.Shards[i].Replicas[j].Deployment.
						setDefaults(&normalizedCluster.Layout.Shards[i].Deployment)
					deploymentRefs[normalizedCluster.Layout.Shards[i].Replicas[j].Deployment.toString()]++
				}
				continue
			}

			for j := range normalizedCluster.Layout.Shards[i].Replicas {
				normalizedCluster.Layout.Shards[i].Replicas[j].Deployment.
					setDefaults(&normalizedCluster.Layout.Shards[i].Deployment)
				deploymentRefs[normalizedCluster.Layout.Shards[i].Replicas[j].Deployment.toString()]++
			}
		}
	}

	return &chiClusterDataLink{
		cluster:     normalizedCluster,
		deployments: deploymentRefs,
	}
}

func (d *chiDeployment) toString() string {
	return fmt.Sprintf("%v", d)
}

func (d *chiDeployment) setDefaults(parent *chiDeployment) {
	if parent == nil && d.Scenario == "" {
		d.Scenario = deploymentScenarioDefault
		return
	}
	if d.PodTemplateName == "" {
		d.PodTemplateName = parent.PodTemplateName
	}
	if d.Scenario == "" {
		d.Scenario = parent.Scenario
	}
	if len(d.Zone.MatchLabels) == 0 {
		d.Zone.copyFrom(&parent.Zone)
	}
}

func (z *chiDeploymentZone) copyFrom(source *chiDeploymentZone) {
	tmp := make(map[string]string)
	for k, v := range source.MatchLabels {
		tmp[k] = v
	}
	z.MatchLabels = tmp
}

func (d chiDeploymentRefs) mergeWith(another chiDeploymentRefs) {
	for ak, av := range another {
		dv, ok := d[ak]
		if !ok {
			d[ak] = av
			continue
		}
		d[ak] = dv + av
	}
}
