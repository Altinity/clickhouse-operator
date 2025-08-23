package v1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:resource:shortName=chb
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.message`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ClickHouseBackup struct {
	meta.TypeMeta   `json:",inline"                     yaml:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"          yaml:"metadata,omitempty"`

	Spec   ClickhouseBackupSpec `json:"spec,omitempty" yaml:"spec,omitempty"`
	Status Status               `json:"status,omitempty" yaml:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ClickHouseBackupList struct {
	meta.TypeMeta `json:",inline"                     yaml:",inline"`
	meta.ListMeta `json:"metadata,omitempty"          yaml:"metadata,omitempty"`
	Items         []ClickHouseBackup `json:"items" yaml:"items"`
}

// TODO we can add more matrix to the status
type Status struct {
	State      string    `json:"state,omitempty" yaml:"state,omitempty"`
	Message    string    `json:"message,omitempty" yaml:"message,omitempty"`
	BackupId   string    `json:"backupId,omitempty" yaml:"backupId,omitempty"`
	BackupName string    `json:"backupName,omitempty" yaml:"backupName,omitempty"`
	StartedAt  meta.Time `json:"startedAt,omitempty" yaml:"startedAt,omitempty"`
	StoppedAt  meta.Time `json:"stoppedAt,omitempty" yaml:"stoppedAt,omitempty"`
}

type ClickhouseBackupSpec struct {
	Backup                 BackupConfig           `json:"backup,omitempty" yaml:"backup,omitempty"`
	Method                 string                 `json:"method,omitempty" yaml:"method,omitempty"`
	PluginConfiguration    PluginConfiguration    `json:"pluginConfiguration,omitempty" yaml:"pluginConfiguration,omitempty"`
	ClickhouseInstallation ClickhouseInstallation `json:"ClickhouseInstallation,omitempty" yaml:"ClickhouseInstallation,omitempty"`
}

type BackupConfig struct {
	DBTable DBTableConfig `json:"dbTable,omitempty" yaml:"dbTable,omitempty"`
	S3      S3Config      `json:"s3,omitempty" yaml:"s3,omitempty"`
}

type DBTableConfig struct {
	WhiteList []string `json:"whiteList,omitempty" yaml:"whiteList,omitempty"`
	BlackList []string `json:"blackList,omitempty" yaml:"blackList,omitempty"`
}

type S3Config struct {
	DestinationPath string        `json:"destinationPath,omitempty" yaml:"destinationPath,omitempty"`
	EndpointURL     string        `json:"endpointURL,omitempty" yaml:"endpointURL,omitempty"`
	S3Credentials   S3Credentials `json:"s3Credentials,omitempty" yaml:"s3Credentials,omitempty"`
}

type S3Credentials struct {
	AccessKeyID     SecretKeyRef `json:"accessKeyId,omitempty" yaml:"accessKeyId,omitempty"`
	SecretAccessKey SecretKeyRef `json:"secretAccessKey,omitempty" yaml:"secretAccessKey,omitempty"`
}

type SecretKeyRef struct {
	Name string `json:"name,omitempty" yaml:"name,omitempty"`
	Key  string `json:"key,omitempty" yaml:"key,omitempty"`
}

type PluginConfiguration struct {
	Name string `json:"name,omitempty" yaml:"name,omitempty"`
}

// ClickhouseInstallation represents the CHI (ClickHouseInstallation) that a backup is tied to
type ClickhouseInstallation struct {
	Name    string            `json:"name,omitempty" yaml:"name,omitempty"`
	Cluster ClickhouseCluster `json:"cluster,omitempty" yaml:"cluster,omitempty"`
}

// ClickhouseCluster represents a single cluster inside a CHI
type ClickhouseCluster struct {
	Name string `json:"name,omitempty" yaml:"name,omitempty"`
}
