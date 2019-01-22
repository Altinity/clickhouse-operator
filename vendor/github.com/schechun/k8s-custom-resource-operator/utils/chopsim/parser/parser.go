package parser

import (
	"fmt"
)

type chInstanceData struct {
	deployment *ChiDeployment
	id         string
}

type chInstanceDataList []*chInstanceData

// ClickHouseInstallation describes CRD object instance
type ClickHouseInstallation struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Clusters  []ChiCluster `yaml:"clusters"`
		Templates ChiTemplate  `yaml:"templates"`
	} `yaml:"spec"`
}

// ChiTemplate defines data structure used as .spec.templates element
type ChiTemplate struct {
	ContainerTemplates []struct {
		Name      string `yaml:"name"`
		Image     string `yaml:"image"`
		Resources struct {
			Requests ChiContainerResourceParams `yaml:"requests"`
			Limits   ChiContainerResourceParams `yaml:"limits"`
		} `yaml:"resources"`
	} `yaml:"containerTemplates"`
}

// ChiCluster defines data structure used as .spec.clusters array item
type ChiCluster struct {
	Name       string            `yaml:"name"`
	Layout     string            `yaml:"layout"`
	Rules      ChiClusterRules   `yaml:"rules"`
	Shards     []ChiClusterShard `yaml:"shards"`
	Deployment ChiDeployment     `yaml:"deployment"`
}

// ChiClusterShardReplica defines data structure of "replicas" element
type ChiClusterShardReplica struct {
	Deployment ChiDeployment `yaml:"deployment"`
}

// ChiClusterShard defines data structure of "shards" element
type ChiClusterShard struct {
	ReplicasCount       string                   `yaml:"replicasCount"`
	Weight              int                      `yaml:"weight"`
	InternalReplication string                   `yaml:"internalReplication"`
	Deployment          ChiDeployment            `yaml:"deployment"`
	Replicas            []ChiClusterShardReplica `yaml:"replicas"`
}

// ChiClusterRules defines "rules" data structure
type ChiClusterRules struct {
	ShardsCount   int `yaml:"shardsCount"`
	ReplicasCount int `yaml:"replicasCount"`
}

// ChiDeployment defines "deployment" data structure
type ChiDeployment struct {
	Scenario              string `yaml:"scenario"`
	ContainerTemplateName string `yaml:"containerTemplateName"`
	Zone                  struct {
		MatchLabels map[string]string `yaml:"matchLabels"`
	} `yaml:"zone"`
}

// ChiContainerResourceParams defines "resources" data structure
type ChiContainerResourceParams struct {
	Memory string `yaml:"memory"`
	CPU    string `yaml:"cpu"`
}

const (
	clusterLayoutStandard = "Standard"
	clusterLayoutAdvanced = "Advanced"

	deploymentScenarioDefault      = "Default"
	deploymentScenarioZoneMonopoly = "ZoneMonopoly"

	shardsReplicasCountInline = "Inline"
)

// GenerateResultingManifest creates resulting (composite) manifest based on ClickHouseInstallation data
func GenerateResultingManifest(chi *ClickHouseInstallation) (string, error) {
	chi.applyDefaults()
	chi.composeInstances()
	return "", nil
}

// applyDefaults fills out ClickHouseInstallation with expected default values
func (chi *ClickHouseInstallation) applyDefaults() {
	if len(chi.Spec.Clusters) == 0 {
		chi.Spec.Clusters = append(chi.Spec.Clusters, ChiCluster{
			Name:   "default",
			Layout: clusterLayoutStandard,
			Rules: ChiClusterRules{
				ReplicasCount: 1,
				ShardsCount:   1,
			},
			Deployment: ChiDeployment{
				Scenario: deploymentScenarioDefault,
			},
		})
	} else {
		for i := range chi.Spec.Clusters {
			chi.applyClusterDefaults(i)
		}
	}
}

func (chi *ClickHouseInstallation) applyClusterDefaults(index int) {
	if chi.Spec.Clusters[index].Deployment.Scenario == "" {
		chi.Spec.Clusters[index].Deployment.Scenario = deploymentScenarioDefault
	}
	switch chi.Spec.Clusters[index].Layout {
	case clusterLayoutStandard:
		chi.applyStandardLayoutDefaults(index)
	case clusterLayoutAdvanced:
		chi.applyAdvancedLayoutDefaults(index)
	}
}

func (chi *ClickHouseInstallation) applyStandardLayoutDefaults(index int) {
	if chi.Spec.Clusters[index].Rules.ShardsCount == 0 {
		chi.Spec.Clusters[index].Rules.ShardsCount++
	}
	if chi.Spec.Clusters[index].Rules.ReplicasCount == 0 {
		chi.Spec.Clusters[index].Rules.ReplicasCount++
	}
}

func (chi *ClickHouseInstallation) applyAdvancedLayoutDefaults(index int) {
	for i := range chi.Spec.Clusters[index].Shards {
		if chi.Spec.Clusters[index].Shards[i].Deployment.Scenario == "" {
			chi.Spec.Clusters[index].Shards[i].Deployment.Scenario = deploymentScenarioDefault
		}
		if chi.Spec.Clusters[index].Shards[i].ReplicasCount == shardsReplicasCountInline {
			for j := range chi.Spec.Clusters[index].Shards[i].Replicas {
				if chi.Spec.Clusters[index].Shards[i].Replicas[j].Deployment.Scenario == "" {
					chi.Spec.Clusters[index].Shards[i].Replicas[j].Deployment.Scenario = deploymentScenarioDefault
				}
			}
		}
	}
}

// composeInstances produces data to be included within resulting manifests
func (chi *ClickHouseInstallation) composeInstances() {
	instanceListChan := make(chan chInstanceDataList)
	clustersCount := len(chi.Spec.Clusters)
	for _, cluster := range chi.Spec.Clusters {
		go func(c ChiCluster, ch chan<- chInstanceDataList) {
			ch <- c.listInstances()
		}(cluster, instanceListChan)
	}
	instanceListBundle := make([]chInstanceDataList, 0, clustersCount)
	maxInstancesNum := 0
	for i := 0; i < clustersCount; i++ {
		instanceList := <-instanceListChan
		instanceListBundle = append(instanceListBundle, instanceList)
		maxInstancesNum = maxInstancesNum + len(instanceList)
	}
	if maxInstancesNum == 0 {
		return
	}
	chInstances := make(chInstanceDataList, len(instanceListBundle[0]), maxInstancesNum)
	copy(chInstances, instanceListBundle[0])
	for i := 1; i < len(instanceListBundle); i++ {
		appendUniqInstances(&chInstances, &instanceListBundle[i])
	}
	// debug
	fmt.Println("Total Instances:", len(chInstances))
	for _, instace := range chInstances {
		fmt.Println(instace.deployment)
	}
}

// appendUniqInstances creates "chInstanceDataList" composed of elements from "a" list
// + items from "b" list which have incompatible ".deployment" properties compare to items from "a" list
func appendUniqInstances(a, b *chInstanceDataList) {
	if len(*a) < len(*b) {
		*a, *b = *b, *a
	}
	for _, item := range *b {
		if matchInstances(a, item) {
			continue
		}
		*a = append(*a, item)
	}
}

// matchInstances returns true if "list" already contains compatible with "item" element
func matchInstances(list *chInstanceDataList, item *chInstanceData) bool {
	for _, listItem := range *list {
		if item.deployment.deepCompare(*listItem.deployment) {
			return true
		}
	}
	return false
}

// deepCompare return true if each field of "current" ChiDeployment
// matches values of corresponding fields from "annother" ChiDeployment
func (current *ChiDeployment) deepCompare(another ChiDeployment) bool {
	if current.ContainerTemplateName != another.ContainerTemplateName {
		return false
	}
	if current.Scenario != another.Scenario {
		return false
	}
	if !compareStringMaps(current.Zone.MatchLabels, another.Zone.MatchLabels) {
		return false
	}
	return true
}

func compareStringMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range b {
		tmp, ok := a[key]
		if !ok && tmp != value {
			return false
		}
	}
	return true
}
