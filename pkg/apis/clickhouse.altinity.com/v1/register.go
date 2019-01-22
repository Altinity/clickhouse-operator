package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	clickhousealtinitycom "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com"
)

// SchemeGroupVersion is group version used to register these objects
var SchemeGroupVersion = schema.GroupVersion{Group: clickhousealtinitycom.GroupName, Version: "v1"}

// Resource returns schema.GroupResource
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder collects scheme builder functions
	SchemeBuilder runtime.SchemeBuilder
	// AddToScheme applies SchemeBuilder functions to the specified scheme
	AddToScheme        = localSchemeBuilder.AddToScheme
	localSchemeBuilder = &SchemeBuilder
)

func init() {
	localSchemeBuilder.Register(addKnownTypes)
}

// Adds a list of known types to the api.Scheme object
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&ClickHouseInstallation{},
		&ClickHouseInstallationList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
