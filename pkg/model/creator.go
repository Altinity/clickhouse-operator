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

package model

import (
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/config"
	"github.com/altinity/clickhouse-operator/pkg/util"

	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/golang/glog"
)

// Reconciler is the base struct to create k8s objects
type Reconciler struct {
	appVersion                string
	chi                       *chiv1.ClickHouseInstallation
	chopConfig                *config.Config
	chConfigGenerator         *ClickHouseConfigGenerator
	chConfigSectionsGenerator *configSections
	labeler                   *Labeler
	funcs                     *ReconcileFuncs
}

type ReconcileFuncs struct {
	ReconcileConfigMap   func(configMap *corev1.ConfigMap) error
	ReconcileService     func(service *corev1.Service) error
	ReconcileStatefulSet func(newStatefulSet *apps.StatefulSet, replica *chiv1.ChiReplica) error
}

// NewReconciler creates new creator
func NewReconciler(
	chi *chiv1.ClickHouseInstallation,
	chopConfig *config.Config,
	appVersion string,
	funcs *ReconcileFuncs,
) *Reconciler {
	reconciler := &Reconciler{
		chi:               chi,
		chopConfig:        chopConfig,
		appVersion:        appVersion,
		chConfigGenerator: NewClickHouseConfigGenerator(chi),
		labeler:           NewLabeler(appVersion, chi),
		funcs:             funcs,
	}
	reconciler.chConfigSectionsGenerator = NewConfigSections(reconciler.chConfigGenerator, reconciler.chopConfig)

	return reconciler
}

// Reconcile runs reconcile process
func (r *Reconciler) Reconcile() error {

	// Reconcile CHI
	if err := r.reconcileChiService(r.chi); err != nil {
		return err
	}

	if err := r.reconcileChiConfigMaps(); err != nil {
		return err
	}

	// Reconcile Clusters
	if err := r.reconcileReplicas(); err != nil {
		return err
	}

	return nil
}

// reconcileChiService reconciles global Services belonging to CHI
func (r *Reconciler) reconcileChiService(chi *chiv1.ClickHouseInstallation) error {
	service := r.createChiService(chi)
	return r.funcs.ReconcileService(service)
}

// reconcileChiConfigMaps reconciles global ConfigMaps belonging to CHI
func (r *Reconciler) reconcileChiConfigMaps() error {
	r.chConfigSectionsGenerator.CreateConfigsUsers()
	r.chConfigSectionsGenerator.CreateConfigsCommon()

	// ConfigMap common for all resources in CHI
	// contains several sections, mapped as separated chopConfig files,
	// such as remote servers, zookeeper setup, etc
	configMapCommon := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CreateConfigMapCommonName(r.chi),
			Namespace: r.chi.Namespace,
			Labels:    r.labeler.getLabelsCommonObject(),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: r.chConfigSectionsGenerator.commonConfigSections,
	}
	if err := r.funcs.ReconcileConfigMap(configMapCommon); err != nil {
		return err
	}

	// ConfigMap common for all users resources in CHI
	configMapUsers := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CreateConfigMapCommonUsersName(r.chi),
			Namespace: r.chi.Namespace,
			Labels:    r.labeler.getLabelsCommonObject(),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: r.chConfigSectionsGenerator.commonUsersConfigSections,
	}
	if err := r.funcs.ReconcileConfigMap(configMapUsers); err != nil {
		return err
	}

	return nil
}

// reconcileReplicas reconciles all replicas
func (r *Reconciler) reconcileReplicas() error {
	replicaProcessor := func(replica *chiv1.ChiReplica) error {
		// Add replica's Service
		service := r.createService(replica)
		if err := r.funcs.ReconcileService(service); err != nil {
			return err
		}

		// Add replica's ConfigMap
		configMap := r.createConfigMap(replica)
		if err := r.funcs.ReconcileConfigMap(configMap); err != nil {
			return err
		}

		// Add replica's StatefulSet
		statefulSet := r.createStatefulSet(replica)
		if err := r.funcs.ReconcileStatefulSet(statefulSet, replica); err != nil {
			return err
		}

		return nil
	}

	return r.chi.WalkReplicasTillError(replicaProcessor)
}

// createChiService creates new corev1.Service
func (r *Reconciler) createChiService(chi *chiv1.ClickHouseInstallation) *corev1.Service {
	serviceName := CreateChiServiceName(chi)

	glog.V(1).Infof("createChiService(%s/%s)", chi.Namespace, serviceName)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: r.chi.Namespace,
			Labels:    r.labeler.getLabelsCommonObject(),
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
			},
			Selector: r.labeler.getSelectorCommonObject(),
			Type:     "LoadBalancer",
		},
	}
}

// createService creates new corev1.Service
func (r *Reconciler) createService(replica *chiv1.ChiReplica) *corev1.Service {
	serviceName := CreateStatefulSetServiceName(replica)
	statefulSetName := CreateStatefulSetName(replica)

	glog.V(1).Infof("createService(%s/%s) for Set %s", replica.Address.Namespace, serviceName, statefulSetName)
	if template, ok := replica.Chi.GetServiceTemplate(replica.Templates.ServiceTemplate); ok {
		// .templates.ServiceTemplate specified
		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      serviceName,
				Namespace: replica.Address.Namespace,
				Labels:    r.labeler.getLabelsReplica(replica, false),
			},
			Spec: *template.Spec.DeepCopy(),
		}
		service.Spec.Selector = util.MergeStringMaps(service.Spec.Selector, r.labeler.getSelectorReplica(replica))

		return service
	} else {
		// Incorrect/unknown .templates.ServiceTemplate specified
		return &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      serviceName,
				Namespace: replica.Address.Namespace,
				Labels:    r.labeler.getLabelsReplica(replica, false),
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
				Selector:  r.labeler.getSelectorReplica(replica),
				ClusterIP: templateDefaultsServiceClusterIP,
				Type:      "ClusterIP",
			},
		}
	}
}

// createConfigMap creates new corev1.ConfigMap
func (r *Reconciler) createConfigMap(replica *chiv1.ChiReplica) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CreateConfigMapPodName(replica),
			Namespace: replica.Address.Namespace,
			Labels:    r.labeler.getLabelsReplica(replica, false),
		},
		Data: r.chConfigSectionsGenerator.CreateConfigsPod(replica),
	}
}

// createStatefulSet creates new apps.StatefulSet
func (r *Reconciler) createStatefulSet(replica *chiv1.ChiReplica) *apps.StatefulSet {
	statefulSetName := CreateStatefulSetName(replica)
	serviceName := CreateStatefulSetServiceName(replica)

	// Create apps.StatefulSet object
	replicasNum := int32(1)
	// StatefulSet has additional label - ZK config fingerprint
	statefulSet := &apps.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      statefulSetName,
			Namespace: replica.Address.Namespace,
			Labels:    r.labeler.getLabelsReplica(replica, true),
		},
		Spec: apps.StatefulSetSpec{
			Replicas:    &replicasNum,
			ServiceName: serviceName,
			Selector: &metav1.LabelSelector{
				MatchLabels: r.labeler.getSelectorReplica(replica),
			},
			// IMPORTANT
			// VolumeClaimTemplates are to be setup later
			VolumeClaimTemplates: nil,

			// IMPORTANT
			// Template is to be setup later
			Template: corev1.PodTemplateSpec{},
		},
	}

	r.setupStatefulSetPodTemplate(statefulSet, replica)
	r.setupStatefulSetVolumeClaimTemplates(statefulSet, replica)

	return statefulSet
}

// setupStatefulSetPodTemplate performs PodTemplate setup of StatefulSet
func (r *Reconciler) setupStatefulSetPodTemplate(statefulSetObject *apps.StatefulSet, replica *chiv1.ChiReplica) {
	statefulSetName := CreateStatefulSetName(replica)
	podTemplateName := replica.Templates.PodTemplate

	// Initial PodTemplateSpec value
	// All the rest fields would be filled later
	statefulSetObject.Spec.Template = corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: r.labeler.getLabelsReplica(replica, true),
		},
	}

	// Specify pod templates - either explicitly defined or default
	if podTemplate, ok := r.chi.GetPodTemplate(podTemplateName); ok {
		// Replica references known PodTemplate
		copyPodTemplateFrom(statefulSetObject, podTemplate)
		glog.V(1).Infof("createStatefulSetObjects() for statefulSet %s - template: %s", statefulSetName, podTemplateName)
	} else {
		// Replica references UNKNOWN PodTemplate
		copyPodTemplateFrom(statefulSetObject, createDefaultPodTemplate(statefulSetName))
		glog.V(1).Infof("createStatefulSetObjects() for statefulSet %s - default template", statefulSetName)
	}

	r.setupConfigMapVolumes(statefulSetObject, replica)
}

// setupConfigMapVolumes adds to each container in the Pod VolumeMount objects with
func (r *Reconciler) setupConfigMapVolumes(statefulSetObject *apps.StatefulSet, replica *chiv1.ChiReplica) {
	configMapMacrosName := CreateConfigMapPodName(replica)
	configMapCommonName := CreateConfigMapCommonName(r.chi)
	configMapCommonUsersName := CreateConfigMapCommonUsersName(r.chi)

	// Add all ConfigMap objects as Volume objects of type ConfigMap
	statefulSetObject.Spec.Template.Spec.Volumes = append(
		statefulSetObject.Spec.Template.Spec.Volumes,
		createVolumeForConfigMap(configMapCommonName),
		createVolumeForConfigMap(configMapCommonUsersName),
		createVolumeForConfigMap(configMapMacrosName),
	)

	// And reference these Volumes in each Container via VolumeMount
	// So Pod will have ConfigMaps mounted as Volumes
	for i := range statefulSetObject.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSetObject.Spec.Template.Spec.Containers[i]
		// Append to each Container current VolumeMount's to VolumeMount's declared in template
		container.VolumeMounts = append(
			container.VolumeMounts,
			createVolumeMount(configMapCommonName, dirPathConfigd),
			createVolumeMount(configMapCommonUsersName, dirPathUsersd),
			createVolumeMount(configMapMacrosName, dirPathConfd),
		)
	}
}

// setupStatefulSetVolumeClaimTemplates performs VolumeClaimTemplate setup for Containers in PodTemplate of a StatefulSet
func (r *Reconciler) setupStatefulSetVolumeClaimTemplates(
	statefulSet *apps.StatefulSet,
	replica *chiv1.ChiReplica,
) {
	// Append VolumeClaimTemplates, that are referenced in Containers' VolumeMount object(s)
	// to StatefulSet's Spec.VolumeClaimTemplates slice, so these
	statefulSetName := CreateStatefulSetName(replica)
	for i := range statefulSet.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSet.Spec.Template.Spec.Containers[i]
		for j := range container.VolumeMounts {
			// Convenience wrapper
			volumeMount := &container.VolumeMounts[j]
			if volumeClaimTemplate, ok := r.chi.GetVolumeClaimTemplate(volumeMount.Name); ok {
				// Found VolumeClaimTemplate to mount by VolumeMount
				appendVolumeClaimTemplateFrom(statefulSet, volumeClaimTemplate)
			}
		}
	}

	// Now deal with .templates.VolumeClaimTemplate
	//
	// We want to mount this default VolumeClaimTemplate into /var/lib/clickhouse in case:
	// 1. This default VolumeClaimTemplate is not already mounted with any VolumeMount
	// 2. And /var/lib/clickhouse is not already mounted with any VolumeMount

	defaultVolumeClaimTemplateName := replica.Templates.VolumeClaimTemplate

	if defaultVolumeClaimTemplateName == "" {
		// No .templates.VolumeClaimTemplate specified
		return
	}

	if _, ok := r.chi.GetVolumeClaimTemplate(defaultVolumeClaimTemplateName); !ok {
		// Incorrect/unknown .templates.VolumeClaimTemplate specified
		return
	}

	// 1. Check explicit usage - whether this default VolumeClaimTemplate is already listed in VolumeMount
	clickHouseContainer := getClickHouseContainer(statefulSet)
	for i := range clickHouseContainer.VolumeMounts {
		// Convenience wrapper
		volumeMount := &clickHouseContainer.VolumeMounts[i]
		if volumeMount.Name == defaultVolumeClaimTemplateName {
			// This .templates.VolumeClaimTemplate is already used in VolumeMount
			glog.V(1).Infof("createStatefulSetObjects() for statefulSet %s - VC template 1: %s", statefulSetName, volumeMount.Name)
			return
		}
	}

	// This default VolumeClaimTemplate is not used by name - it is unused - what's it's purpose, then?
	// So we want to mount it to /var/lib/clickhouse even more now, because it is unused.
	// However, mount point /var/lib/clickhouse may be used already explicitly. Need to check

	// 2. Check whether /var/lib/clickhouse is already mounted
	for i := range clickHouseContainer.VolumeMounts {
		// Convenience wrapper
		volumeMount := &clickHouseContainer.VolumeMounts[i]
		if volumeMount.MountPath == dirPathClickHouseData {
			// /var/lib/clickhouse is already mounted
			glog.V(1).Infof("createStatefulSetObjects() for statefulSet %s - VC template 2: /var/lib/clickhouse already mounted", statefulSetName)
			return
		}
	}

	// This default volumeClaimTemplate is not used explicitly by name and /var/lib/clickhouse is not mounted also.
	// Let's mount this default VolumeClaimTemplate into /var/lib/clickhouse
	if template, ok := r.chi.GetVolumeClaimTemplate(defaultVolumeClaimTemplateName); ok {
		// Add VolumeClaimTemplate to StatefulSet
		appendVolumeClaimTemplateFrom(statefulSet, template)
		// Add VolumeMount to ClickHouse container to /var/lib/clickhouse point
		clickHouseContainer.VolumeMounts = append(
			clickHouseContainer.VolumeMounts,
			createVolumeMount(replica.Templates.VolumeClaimTemplate, dirPathClickHouseData),
		)
	}

	glog.V(1).Infof("createStatefulSetObjects() for statefulSet %s - VC template.useDefaultName: %s", statefulSetName, defaultVolumeClaimTemplateName)
}

// copyPodTemplateFrom fills StatefulSet.Spec.Template with data from provided 'src' ChiPodTemplate
func copyPodTemplateFrom(dst *apps.StatefulSet, src *chiv1.ChiPodTemplate) {
	dst.Spec.Template.Name = src.Name
	dst.Spec.Template.Spec = *src.Spec.DeepCopy()
}

// appendVolumeClaimTemplateFrom appends to StatefulSet.Spec.VolumeClaimTemplates new entry with data from provided 'src' ChiVolumeClaimTemplate
func appendVolumeClaimTemplateFrom(dst *apps.StatefulSet, src *chiv1.ChiVolumeClaimTemplate) {
	// Ensure VolumeClaimTemplates slice is in place
	if dst.Spec.VolumeClaimTemplates == nil {
		dst.Spec.VolumeClaimTemplates = make([]corev1.PersistentVolumeClaim, 0, 0)
	}

	// Append copy of PersistentVolumeClaimSpec
	dst.Spec.VolumeClaimTemplates = append(dst.Spec.VolumeClaimTemplates, corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: src.Name,
		},
		Spec: *src.Spec.DeepCopy(),
	})
}

// createDefaultPodTemplate returns default Pod Template to be used with StatefulSet
func createDefaultPodTemplate(name string) *chiv1.ChiPodTemplate {
	return &chiv1.ChiPodTemplate{
		Name: name,
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "clickhouse",
					Image: defaultClickHouseDockerImage,
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
					ReadinessProbe: &corev1.Probe{
						Handler: corev1.Handler{
							HTTPGet: &corev1.HTTPGetAction{
								Path: "/ping",
								Port: intstr.Parse(chDefaultHTTPPortName),
							},
						},
						InitialDelaySeconds: 10,
						PeriodSeconds:       10,
					},
				},
			},
			Volumes: []corev1.Volume{},
		},
	}
}

// createVolumeForConfigMap returns corev1.Volume object with defined name
func createVolumeForConfigMap(name string) corev1.Volume {
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

// createVolumeMount returns corev1.VolumeMount object with name and mount path
func createVolumeMount(name, mountPath string) corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      name,
		MountPath: mountPath,
	}
}

// getClickHouseContainer finds Container with ClickHouse amond all containers of Pod specified in StatefulSet
func getClickHouseContainer(statefulSet *apps.StatefulSet) *corev1.Container {
	return &statefulSet.Spec.Template.Spec.Containers[ClickHouseContainerIndex]
}
