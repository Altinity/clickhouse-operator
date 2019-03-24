// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"fmt"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

	"github.com/golang/glog"
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CreateCHIObjects returns a map of the k8s objects created based on ClickHouseInstallation Object properties
// and slice of all full deployment ids
func CreateCHIObjects(chi *chiv1.ClickHouseInstallation, deploymentNumber NamedNumber) (ObjectsMap, []string) {
	var options generatorOptions

	options.deploymentNumber = deploymentNumber

	// Deployments index - map full deployment ID to deployment itself
	options.fullDeploymentIDToDeployment = make(map[string]*chiv1.ChiDeployment)

	// Config files section - macros and common config
	options.fullDeploymentIDToMacrosData = make(map[string]macrosDataShardDescriptionList)

	// commonConfigSections maps section name to section XML config of the following sections:
	// 1. remote servers
	// 2. zookeeper
	// 3. settings
	// 4. listen
	options.commonConfigSections = make(map[string]string)
	// commonConfigSections maps section name to section XML config of the following sections:
	// 1. users
	// 2. quotas
	// 3. profiles
	options.commonUsersConfigSections = make(map[string]string)

	includeNonEmpty(options.commonConfigSections, filenameRemoteServersXML, generateRemoteServersConfig(chi))
	includeNonEmpty(options.commonConfigSections, filenameZookeeperXML, generateZookeeperConfig(chi))
	includeNonEmpty(options.commonConfigSections, filenameSettingsXML, generateSettingsConfig(chi))
	includeNonEmpty(options.commonConfigSections, filenameListenXML, generateListenConfig(chi))

	includeNonEmpty(options.commonUsersConfigSections, filenameUsersXML, generateUsersConfig(chi))
	includeNonEmpty(options.commonUsersConfigSections, filenameQuotasXML, generateQuotasConfig(chi))
	includeNonEmpty(options.commonUsersConfigSections, filenameProfilesXML, generateProfilesConfig(chi))

	// slice of full deployment ID's
	fullDeploymentIDs := make([]string, 0, len(options.fullDeploymentIDToDeployment))
	for p := range options.fullDeploymentIDToDeployment {
		fullDeploymentIDs = append(fullDeploymentIDs, p)
	}

	// Create k8s objects (data structures)
	return ObjectsMap{
		ObjectsServices: createServiceObjects(chi),
		// Config Maps are of two types - common and personal
		ObjectsConfigMaps: createConfigMapObjects(chi, &options),
		// Config Maps are mapped as config files in Stateful Set objects
		ObjectsStatefulSets: createStatefulSetObjects(chi),
	}, fullDeploymentIDs
}

// createConfigMapObjects returns a list of corev1.ConfigMap objects
func createConfigMapObjects(
	chi *chiv1.ClickHouseInstallation,
	options *generatorOptions,
) ConfigMapList {
	// There are two types of configs, kept in ConfigMaps:
	// 1. Common configs - for all resources in the CHI (remote servers, zookeeper setup, etc)
	//    consists of common configs and common users configs
	// 2. Personal configs - macros config
	// configMapList contains all configs so we need deploymentsNum+2 ConfigMap objects
	// personal config for each deployment and +2 for common config + common user config
	configMapList := make(ConfigMapList, 0, chi.DeploymentsCount()+2)

	// ConfigMap common for all resources in CHI
	// contains several sections, mapped as separated config files,
	// such as remote servers, zookeeper setup, etc
	configMapList = append(
		configMapList,
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      CreateConfigMapCommonName(chi.Name),
				Namespace: chi.Namespace,
				Labels: map[string]string{
					ChopGeneratedLabel: chi.Name,
					CHIGeneratedLabel:  chi.Name,
				},
			},
			// Data contains several sections which are to be several xml configs
			Data: options.commonConfigSections,
		},
	)

	// ConfigMap common for all users resources in CHI
	configMapList = append(
		configMapList,
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      CreateConfigMapCommonUsersName(chi.Name),
				Namespace: chi.Namespace,
				Labels: map[string]string{
					ChopGeneratedLabel: chi.Name,
					CHIGeneratedLabel:  chi.Name,
				},
			},
			// Data contains several sections which are to be several xml configs
			Data: options.commonUsersConfigSections,
		},
	)

	replicaProcessor := func(replica *chiv1.ChiClusterLayoutShardReplica) error {
		// Add corev1.Service object to the list
		// Add corev1.ConfigMap object to the list
		configMapList = append(
			configMapList,
			&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      CreateConfigMapMacrosName(replica),
					Namespace: chi.Namespace,
					Labels: map[string]string{
						ChopGeneratedLabel: chi.Name,
						CHIGeneratedLabel:  chi.Name,
					},
				},
				Data: map[string]string{
					filenameMacrosXML: generateHostMacros(replica),
				},
			},
		)

		return nil
	}
	chi.WalkReplicas(replicaProcessor)

	return configMapList
}

// createServiceObjects returns a list of corev1.Service objects
func createServiceObjects(chi *chiv1.ClickHouseInstallation) ServiceList {

	// We'd like to create "number of deployments" + 1 kubernetes services in order to provide access
	// to each deployment separately and one common predictably-named access point - common service
	serviceList := make(ServiceList, 0, chi.DeploymentsCount()+1)

	// Create one predictably-named service to access the whole installation
	// NAME                             TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)                      AGE
	// service/clickhouse-replcluster   ClusterIP   None         <none>        9000/TCP,9009/TCP,8123/TCP   1h
	serviceList = append(
		serviceList,
		createCHIServiceObject(chi, CreateCHIServiceName(chi.Name)),
	)

	// Create "number of deployments" service - one service for each stateful set
	// Each replica has its stateful set and each statefule set has it service
	// NAME                             TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)                      AGE
	// service/chi-01a1ce7dce-2         ClusterIP   None         <none>        9000/TCP,9009/TCP,8123/TCP   1h
	replicaProcessor := func(replica *chiv1.ChiClusterLayoutShardReplica) error {
		// Add corev1.Service object to the list
		serviceList = append(
			serviceList,
			createDeploymentServiceObject(
				chi,
				CreateStatefulSetServiceName(replica),
				CreateStatefulSetName(replica),
			),
		)

		return nil
	}
	chi.WalkReplicas(replicaProcessor)

	return serviceList
}

func createCHIServiceObject(
	chi *chiv1.ClickHouseInstallation,
	serviceName string,
) *corev1.Service {
	glog.Infof("createCHIServiceObject() for service %s\n", serviceName)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: chi.Namespace,
			Labels: map[string]string{
				ChopGeneratedLabel: chi.Name,
				CHIGeneratedLabel:  chi.Name,
			},
		},
		Spec: corev1.ServiceSpec{
			// ClusterIP: templateDefaultsServiceClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name: chDefaultHTTPPortName,
					Port: chDefaultHTTPPortNumber,
				},
				{
					Name: chDefaultClientPortName,
					Port: chDefaultClientPortNumber,
				},
				{
					Name: chDefaultInterServerPortName,
					Port: chDefaultInterServerPortNumber,
				},
			},
			Selector: map[string]string{
				CHIGeneratedLabel: chi.Name,
			},
			Type: "LoadBalancer",
		},
	}
}

func createDeploymentServiceObject(
	chi *chiv1.ClickHouseInstallation,
	serviceName string,
	statefulSetName string,
) *corev1.Service {
	glog.Infof("createDeploymentServiceObject() for service %s %s\n", serviceName, statefulSetName)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: chi.Namespace,
			Labels: map[string]string{
				ChopGeneratedLabel: chi.Name,
				CHIGeneratedLabel:  chi.Name,
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name: chDefaultHTTPPortName,
					Port: chDefaultHTTPPortNumber,
				},
				{
					Name: chDefaultClientPortName,
					Port: chDefaultClientPortNumber,
				},
				{
					Name: chDefaultInterServerPortName,
					Port: chDefaultInterServerPortNumber,
				},
			},
			Selector: map[string]string{
				chDefaultAppLabel: statefulSetName,
			},
			ClusterIP: templateDefaultsServiceClusterIP,
			Type: "ClusterIP",
		},
	}
}

// createStatefulSetObjects returns a list of apps.StatefulSet objects
func createStatefulSetObjects(chi *chiv1.ClickHouseInstallation) StatefulSetList {
	statefulSetList := make(StatefulSetList, 0, chi.DeploymentsCount())

	// Create list of apps.StatefulSet objects
	// StatefulSet is created for each replica.Deployment

	replicaProcessor := func(replica *chiv1.ChiClusterLayoutShardReplica) error {
		glog.Infof("createStatefulSetObjects() for statefulSet %s\n", CreateStatefulSetName(replica))

		// Create and setup apps.StatefulSet object
		statefulSetObject := createStatefulSetObject(replica)
		setupStatefulSetPodTemplate(statefulSetObject, chi, replica)
		setupStatefulSetVolumeClaimTemplate(statefulSetObject, chi, replica)

		// Append apps.StatefulSet to the list of stateful sets
		statefulSetList = append(statefulSetList, statefulSetObject)

		return nil
	}
	chi.WalkReplicas(replicaProcessor)

	return statefulSetList
}

func createStatefulSetObject(replica *chiv1.ChiClusterLayoutShardReplica) *apps.StatefulSet {
	statefulSetName := CreateStatefulSetName(replica)
	serviceName := CreateStatefulSetServiceName(replica)

	// Create apps.StatefulSet object
	replicasNum := int32(1)
	return &apps.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      statefulSetName,
			Namespace: replica.Address.Namespace,
			Labels: map[string]string{
				ChopGeneratedLabel: replica.Address.CHIName,
				CHIGeneratedLabel:  replica.Address.CHIName,
			},
		},
		Spec: apps.StatefulSetSpec{
			Replicas:    &replicasNum,
			ServiceName: serviceName,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					chDefaultAppLabel: statefulSetName,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name: statefulSetName,
					Labels: map[string]string{
						chDefaultAppLabel:  statefulSetName,
						ChopGeneratedLabel: replica.Address.CHIName,
						CHIGeneratedLabel:  replica.Address.CHIName,
					},
				},
				Spec: corev1.PodSpec{
					Volumes:    nil,
					Containers: nil,
				},
			},
		},
	}
}

func setupStatefulSetPodTemplate(
	statefulSetObject *apps.StatefulSet,
	chi *chiv1.ClickHouseInstallation,
	replica *chiv1.ChiClusterLayoutShardReplica,
) {
	statefulSetName := CreateStatefulSetName(replica)
	configMapMacrosName := CreateConfigMapMacrosName(replica)

	podTemplatesIndex := createPodTemplatesIndex(chi)
	podTemplate := replica.Deployment.PodTemplate

	configMapCommonName := CreateConfigMapCommonName(replica.Address.CHIName)
	configMapCommonUsersName := CreateConfigMapCommonUsersName(replica.Address.CHIName)

	// Specify pod templates - either explicitly defined or default
	if podTemplateData, ok := podTemplatesIndex[podTemplate]; ok {
		// Replica references known PodTemplate
		copyPodTemplateFrom(statefulSetObject, podTemplateData)
		glog.Infof("createStatefulSetObjects() for statefulSet %s - template: %s\n", statefulSetName, podTemplate)
	} else {
		// Replica references UNKNOWN PodTemplate
		copyPodTemplateFrom(statefulSetObject, createDefaultPodTemplatesIndexData(statefulSetName))
		glog.Infof("createStatefulSetObjects() for statefulSet %s - default template\n", statefulSetName)
	}

	// And now loop over all containers in this template and
	// append all VolumeMounts which are ConfigMap mounts
	for i := range statefulSetObject.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSetObject.Spec.Template.Spec.Containers[i]
		// Append to each Container current VolumeMount's to VolumeMount's declared in template
		container.VolumeMounts = append(
			container.VolumeMounts,
			createVolumeMountObject(configMapCommonName, dirPathConfigd),
			createVolumeMountObject(configMapCommonUsersName, dirPathUsersd),
			createVolumeMountObject(configMapMacrosName, dirPathConfd),
		)
	}

	// Add all ConfigMap objects as Pod's volumes
	statefulSetObject.Spec.Template.Spec.Volumes = append(
		statefulSetObject.Spec.Template.Spec.Volumes,
		createVolumeObjectConfigMap(configMapCommonName),
		createVolumeObjectConfigMap(configMapCommonUsersName),
		createVolumeObjectConfigMap(configMapMacrosName),
	)
}

func setupStatefulSetVolumeClaimTemplate(
	statefulSetObject *apps.StatefulSet,
	chi *chiv1.ClickHouseInstallation,
	replica *chiv1.ChiClusterLayoutShardReplica,
) {
	statefulSetName := CreateStatefulSetName(replica)
	// Templates index maps template name to (simplified) template itself
	// Used to provide named access to templates
	volumeClaimTemplatesIndex := createVolumeClaimTemplatesIndex(chi)

	volumeClaimTemplate := replica.Deployment.VolumeClaimTemplate

	// Specify volume claim templates - either explicitly defined or default
	volumeClaimTemplateData, ok := volumeClaimTemplatesIndex[volumeClaimTemplate]
	if !ok {
		// Unknown VolumeClaimTemplate
		glog.Infof("createStatefulSetObjects() for statefulSet %s - no VC templates\n", statefulSetName)
		return
	}

	// Known VolumeClaimTemplate

	statefulSetObject.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{
		*volumeClaimTemplateData.persistentVolumeClaim,
	}

	// Add default corev1.VolumeMount section for ClickHouse data
	if volumeClaimTemplateData.useDefaultName {
		statefulSetObject.Spec.Template.Spec.Containers[0].VolumeMounts = append(
			statefulSetObject.Spec.Template.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{
				Name:      chDefaultVolumeMountNameData,
				MountPath: dirPathClickHouseData,
			})
		glog.Infof("createStatefulSetObjects() for statefulSet %s - VC template.useDefaultName: %s\n", statefulSetName, volumeClaimTemplate)
	}
	glog.Infof("createStatefulSetObjects() for statefulSet %s - VC template: %s\n", statefulSetName, volumeClaimTemplate)
}

func copyPodTemplateFrom(dst *apps.StatefulSet, src *podTemplatesIndexData) {
	// Prepare .statefulSetObject.Spec.Template.Spec - fill with template's data

	// Setup Container's

	// Copy containers from pod template
	// ... snippet from .spec.templates.podTemplates
	//       containers:
	//      - name: clickhouse
	//        volumeMounts:
	//        - name: clickhouse-data-test
	//          mountPath: /var/lib/clickhouse
	//        image: yandex/clickhouse-server:18.16.2
	dst.Spec.Template.Spec.Containers = make([]corev1.Container, len(src.containers))
	copy(dst.Spec.Template.Spec.Containers, src.containers)

	// Setup Volume's
	// Copy volumes from pod template
	dst.Spec.Template.Spec.Volumes = make([]corev1.Volume, len(src.volumes))
	copy(dst.Spec.Template.Spec.Volumes, src.volumes)
}


// createDefaultPodTemplatesIndexData returns default podTemplatesIndexData
func createDefaultPodTemplatesIndexData(name string) *podTemplatesIndexData {
	return &podTemplatesIndexData{
		//	type ChiPodTemplate struct {
		//		Name       string             `json:"name"`
		//		Containers []corev1.Container `json:"containers"`
		//		Volumes    []corev1.Volume    `json:"volumes"`
		//	}

		containers: []corev1.Container{
			{
				Name:  name,
				Image: chDefaultDockerImage,
				Ports: []corev1.ContainerPort{
					{
						Name:          chDefaultHTTPPortName,
						ContainerPort: chDefaultHTTPPortNumber,
					},
					{
						Name:          chDefaultClientPortName,
						ContainerPort: chDefaultClientPortNumber,
					},
					{
						Name:          chDefaultInterServerPortName,
						ContainerPort: chDefaultInterServerPortNumber,
					},
				},
			},
		},
		volumes:    []corev1.Volume{},
	}
}

// createVolumeObjectConfigMap returns corev1.Volume object with defined name
func createVolumeObjectConfigMap(name string) corev1.Volume {
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: name,
				},
			},
		},
	}
}

// createVolumeMountObject returns corev1.VolumeMount object with name and mount path
func createVolumeMountObject(name, mountPath string) corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      name,
		MountPath: mountPath,
	}
}

// createVolumeClaimTemplatesIndex returns a map of volumeClaimTemplatesIndexData used as a reference storage for VolumeClaimTemplates
func createVolumeClaimTemplatesIndex(chi *chiv1.ClickHouseInstallation) volumeClaimTemplatesIndex {
	index := make(volumeClaimTemplatesIndex)
	for i := range chi.Spec.Templates.VolumeClaimTemplates {
		// Convenience wrapper
		volumeClaimTemplate := &chi.Spec.Templates.VolumeClaimTemplates[i]

		useDefaultName := false
		if volumeClaimTemplate.PersistentVolumeClaim.Name == useDefaultPersistentVolumeClaimMacro {
			volumeClaimTemplate.PersistentVolumeClaim.Name = chDefaultVolumeMountNameData
			useDefaultName = true
		}
		index[volumeClaimTemplate.Name] = &volumeClaimTemplatesIndexData{
			useDefaultName:        useDefaultName,
			// TODO join
			//	type ChiVolumeClaimTemplate struct {
			//		Name                  string                       `json:"name"`
			//		PersistentVolumeClaim corev1.PersistentVolumeClaim `json:"persistentVolumeClaim"`
			//	}
			persistentVolumeClaim: &volumeClaimTemplate.PersistentVolumeClaim,
		}
	}

	return index
}

// createPodTemplatesIndex returns a map of podTemplatesIndexData used as a reference storage for PodTemplates
func createPodTemplatesIndex(chi *chiv1.ClickHouseInstallation) podTemplatesIndex {
	index := make(podTemplatesIndex)
	for i := range chi.Spec.Templates.PodTemplates {
		// Convenience wrapper
		podTemplate := &chi.Spec.Templates.PodTemplates[i]
		index[podTemplate.Name] = &podTemplatesIndexData{
			// TODO join
			//	type ChiPodTemplate struct {
			//		Name       string             `json:"name"`
			//		Containers []corev1.Container `json:"containers"`
			//		Volumes    []corev1.Volume    `json:"volumes"`
			//	}
			containers: podTemplate.Containers,
			volumes:    podTemplate.Volumes,
		}
	}

	return index
}

// CreateConfigMapMacrosName returns a name for a ConfigMap (CH macros) resource based on predefined pattern
func CreateConfigMapMacrosName(replica *chiv1.ChiClusterLayoutShardReplica) string {
	return fmt.Sprintf(
		configMapMacrosNamePattern,
		replica.Address.CHIName,
		replica.Address.ClusterIndex,
		replica.Address.ShardIndex,
		replica.Address.ReplicaIndex,
	)
}

// CreateConfigMapCommonName returns a name for a ConfigMap resource based on predefined pattern
func CreateConfigMapCommonName(chiName string) string {
	return fmt.Sprintf(configMapCommonNamePattern, chiName)
}

// CreateConfigMapCommonUsersName returns a name for a ConfigMap resource based on predefined pattern
func CreateConfigMapCommonUsersName(chiName string) string {
	return fmt.Sprintf(configMapCommonUsersNamePattern, chiName)
}

// CreateInstServiceName creates a name of a Installation Service resource
// prefix is a fullDeploymentID
func CreateCHIServiceName(prefix string) string {
	return fmt.Sprintf(chiServiceNamePattern, prefix)
}

// CreateStatefulSetName creates a name of a StatefulSet resource
// prefix is a fullDeploymentID
func CreateStatefulSetName(replica *chiv1.ChiClusterLayoutShardReplica) string {
	return fmt.Sprintf(
		statefulSetNamePattern,
		replica.Address.CHIName,
		replica.Address.ClusterIndex,
		replica.Address.ShardIndex,
		replica.Address.ReplicaIndex,
	)
}

// CreateStatefulSetServiceName creates a name of a pod Service resource
// prefix is a fullDeploymentID
func CreateStatefulSetServiceName(replica *chiv1.ChiClusterLayoutShardReplica) string {
	return fmt.Sprintf(
		statefulSetServiceNamePattern,
		replica.Address.CHIName,
		replica.Address.ClusterIndex,
		replica.Address.ShardIndex,
		replica.Address.ReplicaIndex,
	)
}

// CreatePodHostname creates a name of a Pod resource
// prefix is a fullDeploymentID
// ss-1eb454-2-0
func CreatePodHostname(replica *chiv1.ChiClusterLayoutShardReplica) string {
	return fmt.Sprintf(
		podHostnamePattern,
		replica.Address.CHIName,
		replica.Address.ClusterIndex,
		replica.Address.ShardIndex,
		replica.Address.ReplicaIndex,
	)
}

// CreateNamespaceDomainName creates domain name of a namespace
// .my-dev-namespace.svc.cluster.local
func CreateNamespaceDomainName(chiNamespace string) string {
	return fmt.Sprintf(namespaceDomainPattern, chiNamespace)
}

// CreatePodFQDN creates a fully qualified domain name of a pod
// prefix is a fullDeploymentID
// ss-1eb454-2-0.my-dev-domain.svc.cluster.local
func CreatePodFQDN(chiNamespace, prefix string) string {
	return fmt.Sprintf(
		podFQDNPattern,
		prefix,
		chiNamespace,
	)
}
