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

package main

import (
	"fmt"
	. "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/r3labs/diff"
	corev1 "k8s.io/api/core/v1"
)

type Order struct {
	ID    string `diff:"id"`
	Items []int  `diff:"items"`
}

func main() {

	a, b := ex4()

	fmt.Printf("test diff\n")
	changelog, err := diff.Diff(a, b)

	if err != nil {
		fmt.Printf("Error %v\n", err)
	}

	fmt.Printf("cl: %v", changelog)
}

func ex4() ([]int, []int) {
	a := []int{1, 1}
	b := []int{1}

	return a, b
}

func ex3() ([]int, []int) {
	a := []int{1, 2, 3, 4}
	b := []int{1, 2, 3}

	return a, b
}

func ex2() (ChiSpec, ChiSpec) {
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

func ex1() (Order, Order) {
	a := Order{
		ID:    "1234",
		Items: []int{1, 2, 3, 4},
	}

	b := Order{
		ID:    "1234",
		Items: []int{1, 2, 4},
	}

	return a, b
}
