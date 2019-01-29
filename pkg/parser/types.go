package parser

import (
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

// ObjectKind defines k8s objects list kind
type ObjectKind uint8

// ObjectsMap defines map of a generated k8s objects
type ObjectsMap map[ObjectKind]interface{}

// ConfigMapList defines a list of the ConfigMap objects
type ConfigMapList []*corev1.ConfigMap

// StatefulSetList defines a list of the StatefulSet objects
type StatefulSetList []*apps.StatefulSet

// ServiceList defines a list of the Service objects
type ServiceList []*corev1.Service

type genOptions struct {
	ssNames       map[string]string
	ssDeployments map[string]*chiv1.ChiDeployment
	dRefsMax      chiDeploymentRefs
}

type chiClusterDataLink struct {
	cluster     *chiv1.ChiCluster
	deployments chiDeploymentRefs
}

type chiDeploymentRefs map[string]int

type vcTemplatesIndex map[string]*vcTemplatesIndexData

type vcTemplatesIndexData struct {
	useDefaultName bool
	template       *corev1.PersistentVolumeClaim
}
