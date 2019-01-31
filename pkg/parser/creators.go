package parser

import (
	"fmt"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func createConfigMapObjects(chi *chiv1.ClickHouseInstallation, data map[string]string, o *genOptions) ConfigMapList {
	c := len(o.ssNames)
	cmList := make(ConfigMapList, 1, c+1)
	cmList[0] = &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf(configMapNamePattern, chi.Name),
			Namespace: chi.Namespace,
		},
		Data: data,
	}
	for ssName := range o.ssNames {
		cmList = append(cmList, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf(configMapMacrosNamePattern, chi.Name, ssName),
				Namespace: chi.Namespace,
			},
			Data: map[string]string{
				macrosXML: generateHostMacros(chi.Name, ssName, o.macrosDataIndex[ssName]),
			},
		})
	}
	return cmList
}

func createServiceObjects(chi *chiv1.ClickHouseInstallation, o *genOptions) ServiceList {
	svcList := make(ServiceList, 0, len(o.ssNames))
	for ssNameID := range o.ssNames {
		ssName := fmt.Sprintf(ssNamePattern, ssNameID)
		svcName := fmt.Sprintf(svcNamePattern, ssNameID)
		svcList = append(svcList, &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      svcName,
				Namespace: chi.Namespace,
			},
			Spec: corev1.ServiceSpec{
				ClusterIP: templateDefaultsServiceClusterIP,
				Selector: map[string]string{
					chDefaultAppLabel: ssName,
				},
				Ports: []corev1.ServicePort{
					{
						Name: chDefaultRPCPortName,
						Port: chDefaultRPCPortNumber,
					},
					{
						Name: chDefaultInterServerPortName,
						Port: chDefaultInterServerPortNumber,
					},
					{
						Name: chDefaultRestPortName,
						Port: chDefaultRestPortNumber,
					},
				},
			},
		})
	}
	return svcList
}

func createStatefulSetObjects(chi *chiv1.ClickHouseInstallation, o *genOptions) StatefulSetList {
	rNum := int32(1)
	cmName := fmt.Sprintf(configMapNamePattern, chi.Name)
	ssList := make(StatefulSetList, 0, len(o.ssNames))
	index := createVolumeClaimTemplatesIndex(chi)
	for ssNameID, key := range o.ssNames {
		ssName := fmt.Sprintf(ssNamePattern, ssNameID)
		cmMacros := fmt.Sprintf(configMapMacrosNamePattern, chi.Name, ssNameID)
		templateName := o.ssDeployments[key].VolumeClaimTemplate
		svcName := fmt.Sprintf(svcNamePattern, ssNameID)
		statefulSetObject := &apps.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ssName,
				Namespace: chi.Namespace,
			},
			Spec: apps.StatefulSetSpec{
				Replicas:    &rNum,
				ServiceName: svcName,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						chDefaultAppLabel: ssName,
					},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Name: ssName,
						Labels: map[string]string{
							chDefaultAppLabel: ssName,
						},
					},
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{
							{
								Name: cmName,
								VolumeSource: corev1.VolumeSource{
									ConfigMap: &corev1.ConfigMapVolumeSource{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: cmName,
										},
									},
								},
							},
							{
								Name: cmMacros,
								VolumeSource: corev1.VolumeSource{
									ConfigMap: &corev1.ConfigMapVolumeSource{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: cmMacros,
										},
									},
								},
							},
						},
						Containers: []corev1.Container{
							{
								Name:  ssName,
								Image: chDefaultDockerImage,
								Ports: []corev1.ContainerPort{
									{
										Name:          chDefaultRPCPortName,
										ContainerPort: chDefaultRPCPortNumber,
									},
									{
										Name:          chDefaultInterServerPortName,
										ContainerPort: chDefaultInterServerPortNumber,
									},
									{
										Name:          chDefaultRestPortName,
										ContainerPort: chDefaultRestPortNumber,
									},
								},
								VolumeMounts: []corev1.VolumeMount{
									{
										Name:      cmName,
										MountPath: fullPathRemoteServersXML,
										SubPath:   remoteServersXML,
									},
									{
										Name:      cmName,
										MountPath: fullPathZookeeperXML,
										SubPath:   zookeeperXML,
									},
									{
										Name:      cmMacros,
										MountPath: fullPathMacrosXML,
										SubPath:   macrosXML,
									},
								},
							},
						},
					},
				},
			},
		}
		if data, ok := index[templateName]; ok {
			statefulSetObject.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{
				*index[templateName].template,
			}
			if data.useDefaultName {
				statefulSetObject.Spec.Template.Spec.Containers[0].VolumeMounts = append(
					statefulSetObject.Spec.Template.Spec.Containers[0].VolumeMounts,
					corev1.VolumeMount{
						Name:      chDefaultVolumeMountNameData,
						MountPath: fullPathClickHouseData,
					})
			}
		}
		ssList = append(ssList, statefulSetObject)
	}
	return ssList
}

func createVolumeClaimTemplatesIndex(chi *chiv1.ClickHouseInstallation) vcTemplatesIndex {
	index := make(vcTemplatesIndex)
	for i := range chi.Spec.Templates.VolumeClaimTemplates {
		flag := false
		if chi.Spec.Templates.VolumeClaimTemplates[i].Template.Name == useDefaultNamePlaceholder {
			chi.Spec.Templates.VolumeClaimTemplates[i].Template.Name = chDefaultVolumeMountNameData
			flag = true
		}
		index[chi.Spec.Templates.VolumeClaimTemplates[i].Name] = &vcTemplatesIndexData{
			template:       &chi.Spec.Templates.VolumeClaimTemplates[i].Template,
			useDefaultName: flag,
		}
	}
	return index
}
