package v1alpha1

import (
	"strconv"

	v1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func init() {
	SchemeBuilder.Register(&ClickHouseKeeper{}, &ClickHouseKeeperList{})
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseKeeper defines a ClickHouse Keeper Cluster
type ClickHouseKeeper struct {
	metav1.TypeMeta   `json:",inline"                     yaml:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"          yaml:"metadata,omitempty"`
	Spec              ChkSpec    `json:"spec"             yaml:"spec"`
	Status            *ChkStatus `json:"status,omitempty" yaml:"status,omitempty"`
}

// ChkSpec defines spec section of ClickHouseKeeper resource
type ChkSpec struct {
	// This is used by Chi, but ignored by Chk
	Type string `json:"type,omitempty"`
	// The valid range of size is from 1 to 7.
	Replicas             int32                          `json:"replicas,omitempty"`
	PodTemplate          *ChkPodTemplate                `json:"podTemplate,omitempty"          yaml:"podTemplate,omitempty"`
	Settings             map[string]string              `json:"settings,omitempty"             yaml:"settings,omitempty"`
	VolumeClaimTemplates []corev1.PersistentVolumeClaim `json:"volumeClaimTemplates,omitempty" yaml:"volumeClaimTemplates,omitempty"`
}

func (spec *ChkSpec) GetReplicas() int32 {
	if spec.Replicas == 0 {
		return 1
	} else {
		return spec.Replicas
	}
}

func (spec *ChkSpec) GetPath() string {
	if path, ok := spec.Settings["keeper_server/storage_path"]; ok {
		return path
	} else if path, ok := spec.Settings["keeper_server/path"]; ok {
		return path
	} else {
		return "/var/lib/clickhouse_keeper"
	}
}

func (spec *ChkSpec) GetClientPort() int {
	var port int
	var err error
	if portString, ok := spec.Settings["keeper_server/tcp_port"]; ok {
		if port, err = strconv.Atoi(portString); err != nil {
			port = -1
		}
	} else {
		port = 9181
	}
	return port
}

func (spec *ChkSpec) GetRaftPort() int {
	var port int
	var err error
	if portString, ok := spec.Settings["keeper_server/raft_configuration/server/port"]; ok {
		if port, err = strconv.Atoi(portString); err != nil {
			port = -1
		}
	} else {
		port = 9234
	}
	return port
}

func (spec *ChkSpec) GetPrometheusPort() int {
	var port int
	var err error
	if portString, ok := spec.Settings["prometheus/port"]; ok {
		if port, err = strconv.Atoi(portString); err != nil {
			port = -1
		}
	} else {
		port = -1
	}
	return port
}

// ChkPodTemplate defines full Pod Template, directly used by StatefulSet
type ChkPodTemplate struct {
	ObjectMeta metav1.ObjectMeta `json:"metadata,omitempty"        yaml:"metadata,omitempty"`
	Spec       corev1.PodSpec    `json:"spec,omitempty"            yaml:"spec,omitempty"`
}

// ChkStatus defines status section of ClickHouseKeeper resource
type ChkStatus struct {
	Status string `json:"status,omitempty"                 yaml:"status,omitempty"`

	// Replicas is the number of number of desired replicas in the cluster
	Replicas int32 `json:"replicas,omitempty"`

	// ReadyReplicas is the number of number of ready replicas in the cluster
	ReadyReplicas []v1.ChiZookeeperNode `json:"readyReplicas,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseKeeperList defines a list of ClickHouseKeeper resources
type ClickHouseKeeperList struct {
	metav1.TypeMeta `json:",inline"  yaml:",inline"`
	metav1.ListMeta `json:"metadata" yaml:"metadata"`
	Items           []ClickHouseKeeper `json:"items" yaml:"items"`
}
