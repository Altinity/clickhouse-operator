package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseCluster defines a ClickHouse database cluster
type ClickHouseCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ClickHouseClusterSpec `json:"spec"`
}

// ClickHouseClusterSpec defines spec section of ClickHouseCluster type
type ClickHouseClusterSpec struct {
	Type  string                        `json:"type"`
	Hosts []*ClickHouseClusterSpecHosts `json:"hosts"`
}

// ClickHouseClusterSpecHosts defines hosts section of ClickHouseClusterSpec type
type ClickHouseClusterSpecHosts struct {
	Name     string            `json:"string"`
	Selector map[string]string `json:"selector"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseClusterList defines a list of ClickHouseCluster resources
type ClickHouseClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []ClickHouseCluster `json:"items"`
}
