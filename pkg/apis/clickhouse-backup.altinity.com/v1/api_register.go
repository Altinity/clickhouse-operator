package v1

import (
	clickhouse_backup_altinity_com "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-backup.altinity.com"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// SchemeGroupVersion is group version used to register these objects
	SchemeGroupVersion = schema.GroupVersion{
		Group:   clickhouse_backup_altinity_com.APIGroupName,
		Version: APIVersion,
	}

	// SchemeBuilder is used to add go types to the GroupVersionKind scheme
	SchemeBuilder = &scheme.Builder{
		GroupVersion: SchemeGroupVersion,
	}

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func init() {
	SchemeBuilder.Register(
		&ClickHouseBackup{},
		&ClickHouseBackupList{},
	)
}
