package main

import (
	. "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	corev1 "k8s.io/api/core/v1"
)

func exCHI1() (ChiSpec, ChiSpec) {
	a := ChiSpec{

		// useless
		Defaults: ChiDefaults{
			ReplicasUseFQDN: 0,
			DistributedDDL: ChiDistributedDDL{
				Profile: "",
			},
			Deployment: ChiDeployment{
				PodTemplate:         "",
				VolumeClaimTemplate: "",
				Zone: ChiDeploymentZone{
					MatchLabels: nil, // map[string]string,
				},
				Scenario:    "",
				Fingerprint: "",
			},
		},

		Configuration: Configuration{

			Zookeeper: ChiZookeeperConfig{
				Nodes: []ChiZookeeperNode{
					{
						Host: "host1",
						Port: 123,
					},
				},
			},
			Users:    nil,
			Profiles: nil,
			Quotas:   nil,
			Settings: nil,

			Clusters: []ChiCluster{
				{
					Name: "cluster1",

					// useless
					Deployment: ChiDeployment{
						PodTemplate:         "",
						VolumeClaimTemplate: "",
						Zone: ChiDeploymentZone{
							MatchLabels: nil, // map[string]string,
						},
						Scenario:    "",
						Fingerprint: "",
					},

					Layout: ChiClusterLayout{
						// useless
						Type: "",
						// useless
						ShardsCount: 1,
						// useless
						ReplicasCount: 1,

						Shards: []ChiShard{
							{
								// useless
								DefinitionType: "",
								// useless
								ReplicasCount: 1,

								Weight:              1,
								InternalReplication: "yes",

								// useless
								Deployment: ChiDeployment{
									PodTemplate:         "",
									VolumeClaimTemplate: "",
									Zone: ChiDeploymentZone{
										MatchLabels: nil, // map[string]string,
									},
									Scenario:    "",
									Fingerprint: "",
								},

								Hosts: []ChiReplica{
									// Replica 0
									{
										Port: 9000,

										Deployment: ChiDeployment{
											PodTemplate:         "podTemplate1",
											VolumeClaimTemplate: "volumeClaimTemplate1",
											Zone: ChiDeploymentZone{
												MatchLabels: nil, // map[string]string,
											},
											Scenario:    "",
											Fingerprint: "",
										},
									},
								},
							},
						},
					},
				},
			},
		},

		Templates: ChiTemplates{
			PodTemplates: []ChiPodTemplate{
				{
					Name:       "podTemplate1",
					Containers: []corev1.Container{},
					Volumes:    []corev1.Volume{},
				},
			},
			VolumeClaimTemplates: []ChiVolumeClaimTemplate{
				{
					Name:                  "volumeClaimTemplate1",
					PersistentVolumeClaim: corev1.PersistentVolumeClaim{},
				},
			},
		},
	}

	b := ChiSpec{

		// useless
		Defaults: ChiDefaults{
			ReplicasUseFQDN: 0,
			DistributedDDL: ChiDistributedDDL{
				Profile: "",
			},
			Deployment: ChiDeployment{
				PodTemplate:         "",
				VolumeClaimTemplate: "",
				Zone: ChiDeploymentZone{
					MatchLabels: nil, // map[string]string,
				},
				Scenario:    "",
				Fingerprint: "",
			},
		},

		Configuration: Configuration{

			Zookeeper: ChiZookeeperConfig{
				Nodes: []ChiZookeeperNode{
					{
						Host: "host1",
						Port: 123,
					},
				},
			},
			Users:    nil,
			Profiles: nil,
			Quotas:   nil,
			Settings: nil,

			Clusters: []ChiCluster{
				{
					Name: "cluster1",

					// useless
					Deployment: ChiDeployment{
						PodTemplate:         "",
						VolumeClaimTemplate: "",
						Zone: ChiDeploymentZone{
							MatchLabels: nil, // map[string]string,
						},
						Scenario:    "",
						Fingerprint: "",
					},

					Layout: ChiClusterLayout{
						// useless
						Type: "",
						// useless
						ShardsCount: 1,
						// useless
						ReplicasCount: 1,

						Shards: []ChiShard{
							{
								// useless
								DefinitionType: "",
								// useless
								ReplicasCount: 1,

								Weight:              1,
								InternalReplication: "yes",

								// useless
								Deployment: ChiDeployment{
									PodTemplate:         "",
									VolumeClaimTemplate: "",
									Zone: ChiDeploymentZone{
										MatchLabels: nil, // map[string]string,
									},
									Scenario:    "",
									Fingerprint: "",
								},

								Hosts: []ChiReplica{
									// Replica 0
									{
										Port: 9000,

										Deployment: ChiDeployment{
											PodTemplate:         "podTemplate1",
											VolumeClaimTemplate: "volumeClaimTemplate1",
											Zone: ChiDeploymentZone{
												MatchLabels: nil, // map[string]string,
											},
											Scenario:    "",
											Fingerprint: "",
										},
									},
									// Replica 1
									{
										Port: 9000,

										Deployment: ChiDeployment{
											PodTemplate:         "podTemplate1",
											VolumeClaimTemplate: "volumeClaimTemplate1",
											Zone: ChiDeploymentZone{
												MatchLabels: nil, // map[string]string,
											},
											Scenario:    "",
											Fingerprint: "",
										},
									},
								},
							},
						},
					},
				},
			},
		},

		Templates: ChiTemplates{
			PodTemplates: []ChiPodTemplate{
				{
					Name:       "podTemplate1",
					Containers: []corev1.Container{},
					Volumes:    []corev1.Volume{},
				},
			},
			VolumeClaimTemplates: []ChiVolumeClaimTemplate{
				{
					Name:                  "volumeClaimTemplate1",
					PersistentVolumeClaim: corev1.PersistentVolumeClaim{},
				},
			},
		},
	}

	return a, b
}
