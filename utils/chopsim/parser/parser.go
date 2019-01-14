package parser

// .spec.clusters.layout.type variants
const (
	chiClusterLayoutTypeStandard = "Standard"
	chiClusterLayoutTypeAdvanced = "Advanced"
)

//  *.deployment.scenario variants
const (
	chiDeploymentScenarioDefault      = "Default"
	chiDeploymentScenarioZoneMonopoly = "NodeMonopoly"
)

// ClickHouseInstallation describes CRD object instance
type ClickHouseInstallation struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Deployment    ChiDeployment    `yaml:"deployment"`
		Configuration ChiConfiguration `yaml:"configuration"`
		Templates     ChiTemplates     `yaml:"templates"`
	} `yaml:"spec"`
}

// ChiConfiguration defines data structure which corresponds to .spec.configuration section
type ChiConfiguration struct {
	Users    map[string]string `yaml:"users"`
	Settings map[string]string `yaml:"settings"`
	Clusters []ChiCluster      `yaml:"clusters"`
}

// ChiDeployment defines data structure used for parsing *.deployment sections
type ChiDeployment struct {
	PodTemplateName string         `yaml:"podTemplateName"`
	Zone            DeploymentZone `yaml:"zone"`
	Scenario        string         `yaml:"scenario"`
}

// DeploymentZone defines data structure used with ChiDeployment
type DeploymentZone struct {
	MatchLabels map[string]string `yaml:"matchLabels"`
}

// ChiCluster defines data structure used as .spec.configuration.clusters item
type ChiCluster struct {
	Name       string           `yaml:"name"`
	Layout     ChiClusterLayout `yaml:"layout"`
	Deployment ChiDeployment    `yaml:"deployment"`
}

// ChiClusterLayout defines data structure used as .spec.configuration.clusters.
type ChiClusterLayout struct {
	Type          string                  `yaml:"type"`
	ShardsCount   int                     `yaml:"shardsCount"`
	ReplicasCount int                     `yaml:"replicasCount"`
	Shards        []ChiClusterLayoutShard `yaml:"shards"`
}

// ChiClusterLayoutShard defines data structure used as .spec.configuration.cluster.layout.shards item
type ChiClusterLayoutShard struct {
	DefinitionType      string                         `yaml:"definitionType"`
	ReplicasCount       int                            `yaml:"replicasCount"`
	Weight              int                            `yaml:"weight"`
	InternalReplication string                         `yaml:"internalReplication"`
	Deployment          ChiDeployment                  `yaml:"deployment"`
	Replicas            []ChiClusterLayoutShardReplica `yaml:"replicas"`
}

// ChiClusterLayoutShardReplica defines data structure used as .spec.configuration.cluster.layout.shards.replicas item
type ChiClusterLayoutShardReplica struct {
	Deployment ChiDeployment `yaml:"deployment"`
}

// ChiTemplates defines data structure used as .spec.templates item
type ChiTemplates struct {
	PodTemplates []ChiPodTemplate `yaml:"podTemplates"`
}

// ChiPodTemplate defines data structure used as .spec.templates.podtemplates item
type ChiPodTemplate struct {
	Name       string `yaml:"name"`
	Containers []ChiPodTemplatesContainerTemplate
}

// ChiPodTemplatesContainerTemplate defines data structure used as .spec.templates.podtemplates.containers
type ChiPodTemplatesContainerTemplate struct {
	Name      string `yaml:"name"`
	Image     string `yaml:"image"`
	Resources struct {
		Requests ChiContainerResourceParams `yaml:"requests"`
		Limits   ChiContainerResourceParams `yaml:"limits"`
	} `yaml:"resources"`
}

// ChiContainerResourceParams defines "resources" data structure
type ChiContainerResourceParams struct {
	Memory string `yaml:"memory"`
	CPU    string `yaml:"cpu"`
}

// GenerateArtifacts creates resulting (composite) manifest based on ClickHouseInstallation data provided
func GenerateArtifacts(chi *ClickHouseInstallation) (string, error) {
	return "", nil
}
