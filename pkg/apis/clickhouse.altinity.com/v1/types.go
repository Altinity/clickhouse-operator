package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseInstallation describes the Installation of a ClickHouse Database Cluster
type ClickHouseInstallation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ChiSpec   `json:"spec"`
	Status            ChiStatus `json:"status"`
}

// ChiStatus defines status section of ClickHouseInstallation resource
type ChiStatus struct {
	ObjectPrefixes []string `json:"objectPrefixes"`
}

// ChiSpec defines spec section of ClickHouseInstallation resource
type ChiSpec struct {
	Deployment    ChiDeployment    `json:"deployment,omitempty"`
	Configuration ChiConfiguration `json:"configuration"`
	Templates     ChiTemplates     `json:"templates,omitempty"`
}

// ChiDeployment defines deployment section of .spec
type ChiDeployment struct {
	PodTemplateName string            `json:"podTemplateName,omitempty"`
	Zone            ChiDeploymentZone `json:"zone,omitempty"`
	Scenario        string            `json:"scenario,omitempty"`
	Key             string            // used internally by pkg/parser
}

// ChiDeploymentZone defines zone section of *.deployment
type ChiDeploymentZone struct {
	MatchLabels map[string]string `json:"matchLabels"`
}

// ChiConfiguration defines configuration section of .spec
type ChiConfiguration struct {
	Zookeeper ChiConfigurationZookeeper `json:"zookeeper"`
	Users     map[string]string         `json:"users,omitempty"`
	Settings  map[string]string         `json:"settings,omitempty"`
	Clusters  []ChiCluster              `json:"clusters,omitempty"`
}

// ChiConfigurationZookeeper defines zookeeper section of .spec.configuration
type ChiConfigurationZookeeper struct {
	Nodes []ChiConfigurationZookeeperNode `json:"nodes"`
}

// ChiConfigurationZookeeperNode defines item of nodes section of .spec.configuration.zookeeper
type ChiConfigurationZookeeperNode struct {
	Host string `json:"host"`
	Port int32  `json:"port,omitempty"`
}

// ChiCluster defines item of a clusters section of .configuration
type ChiCluster struct {
	Name       string           `json:"name"`
	Layout     ChiClusterLayout `json:"layout"`
	Deployment ChiDeployment    `json:"deployment,omitempty"`
}

// ChiClusterLayout defines layout section of .spec.configuration.clusters
type ChiClusterLayout struct {
	Type          string                  `json:"type"`
	ShardsCount   int                     `json:"shardsCount,omitempty"`
	ReplicasCount int                     `json:"replicasCount,omitempty"`
	Shards        []ChiClusterLayoutShard `json:"shards,omitempty"`
}

// ChiClusterLayoutShard defines item of a shard section of .spec.configuration.clusters[n].shards
type ChiClusterLayoutShard struct {
	DefinitionType      string                         `json:"definitionType"`
	ReplicasCount       int                            `json:"replicasCount,omitempty"`
	Weight              int                            `json:"weight,omitempty"`
	InternalReplication string                         `json:"internalReplication,omitempty"`
	Deployment          ChiDeployment                  `json:"deployment,omitempty"`
	Replicas            []ChiClusterLayoutShardReplica `json:"replicas,omitempty"`
}

// ChiClusterLayoutShardReplica defines item of a replicas section of .spec.configuration.clusters[n].shards[m]
type ChiClusterLayoutShardReplica struct {
	Deployment ChiDeployment `json:"deployment,omitempty"`
}

// ChiTemplates defines templates section of .spec
type ChiTemplates struct {
	PodTemplates []ChiPodTemplate `json:"podTemplates,omitempty"`
}

// ChiPodTemplate defines item of a podTemplates section of .spec.templates
type ChiPodTemplate struct {
	Name       string                             `json:"name"`
	Containers []ChiPodTemplatesContainerTemplate `json:"containers"`
}

// ChiPodTemplatesContainerTemplate defines item of a containers section of .spec.templates
type ChiPodTemplatesContainerTemplate struct {
	Name      string                              `json:"name"`
	Image     string                              `json:"image,omitempty"`
	Resources ChiPodTemplatesContainerTemplateRes `json:"resources,omitempty"`
}

// ChiPodTemplatesContainerTemplateRes defines resources section of .spec.templates.podTemplates[n]
type ChiPodTemplatesContainerTemplateRes struct {
	Requests ChiPodTemplatesContainerTemplateResParams `json:"requests,omitempty"`
	Limits   ChiPodTemplatesContainerTemplateResParams `json:"limits,omitempty"`
}

// ChiPodTemplatesContainerTemplateResParams defines requests/limit sections of .spec.templates.podTemplates[n].resources
type ChiPodTemplatesContainerTemplateResParams struct {
	Memory string `json:"memory,omitempty"`
	CPU    string `json:"cpu,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseInstallationList defines a list of ClickHouseInstallation resources
type ClickHouseInstallationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []ClickHouseInstallation `json:"items"`
}

const (
	// ClickHouseInstallationCRDResourceKind defines kind of CRD resource
	ClickHouseInstallationCRDResourceKind = "ClickHouseInstallation"
)
