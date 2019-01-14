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

type chiClusterDataLink struct {
	cluster     *chiCluster
	deployments []*chiDeployment
}

// GenerateArtifacts creates resulting (composite) manifest based on ClickHouseInstallation data provided
func GenerateArtifacts(chi *ClickHouseInstallation) string {
	chi.Spec.Deployment.setDefaults(nil)
	clusters, deployments := chi.getNormalizedData()

	// debug: start
	fmt.Println(clusters, deployments)
	// debug: end

	return ""
}

func (chi *ClickHouseInstallation) getNormalizedData() ([]*chiCluster, []*chiDeployment) {
	link := make(chan *chiClusterDataLink)
	count := len(chi.Spec.Configuration.Clusters)
	for _, cluster := range chi.Spec.Configuration.Clusters {
		go func(c chiCluster, ch chan<- *chiClusterDataLink) {
			ch <- c.getNormalized()
		}(cluster, link)
	}
	cList := make([]*chiCluster, 0, count)
	dList := make([]*chiDeployment, 0)
	for i := 0; i < count; i++ {
		data := <-link
		cList = append(cList, data.cluster)
		for _, d := range data.deployments {
			dList = append(dList, d)
		}
	}
	return cList, dList
}

func (c *chiCluster) getNormalized() *chiClusterDataLink {
	normalizedCluster := &chiCluster{}
	// get normalized cluster's data...

	uniqDeployments := make([]*chiDeployment, 0, len(normalizedCluster.Layout.Shards))
	// get uniq deployments from the normalized cluster's data...

	return &chiClusterDataLink{
		cluster:     normalizedCluster,
		deployments: uniqDeployments,
	}
}

func (d *chiDeployment) setDefaults(parent *chiDeployment) {
	if d.Scenario == "" {
		d.Scenario = deploymentScenarioDefault
	}
	if parent != nil {
		d.PodTemplateName = parent.PodTemplateName
		d.Scenario = parent.Scenario
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
