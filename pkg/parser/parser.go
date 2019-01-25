package parser

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	clusterLayoutTypeStandard = "Standard"
	clusterLayoutTypeAdvanced = "Advanced"
)

const (
	shardDefinitionTypeReplicasCount = "ReplicasCount"
	shardDefinitionTypeReplicas      = "Replicas"
)

const (
	deploymentScenarioDefault      = "Default"
	deploymentScenarioNodeMonopoly = "NodeMonopoly"
)

const (
	shardInternalReplicationDisabled = "Disabled"
	stringTrue                       = "true"
	stringFalse                      = "false"
)

const (
	remoteServersXML = "remote_servers.xml"
	zookeeperXML     = "zookeeper.xml"
	usersXML         = "users.xml"
)

const (
	ssNamePattern           = "chi-%s-%s-i%d"
	svcNamePattern          = "%s-service"
	hostnamePattern         = "%s-0.%[1]s-service.%s.svc.cluster.local"
	configMapNamePattern    = "chi-%s-configd"
	vmClickHouseDataPattern = "chi-%s-data"
)

const (
	// ObjectsConfigMaps defines a category of the ConfigMap objects list
	ObjectsConfigMaps ObjectKind = iota + 1
	// ObjectsStatefulSets defines a category of the StatefulSet objects list
	ObjectsStatefulSets
	// ObjectsServices defines a category of the Service objects list
	ObjectsServices
)

const (
	callSplitter = "---\n"
)

const (
	clickhouseDefaultDockerImage = "yandex/clickhouse-server:latest"
)

const (
	fullPathRemoteServersXML = "/etc/clickhouse-server/config.d/" + remoteServersXML
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
	ssNames       map[string]struct{}
	ssDeployments map[string]*chiv1.ChiDeployment
	dRefsMax      chiDeploymentRefs
}

type chiClusterDataLink struct {
	cluster     *chiv1.ChiCluster
	deployments chiDeploymentRefs
}

type chiDeploymentRefs map[string]int

// CreateObjects returns a map of the k8s objects created based on ClickHouseInstallation Object properties
func CreateObjects(chi *chiv1.ClickHouseInstallation) (ObjectsMap, []string) {
	var (
		clusters []*chiv1.ChiCluster
		options  genOptions
	)
	setDefaults(chi)
	setDeploymentDefaults(&chi.Spec.Deployment, nil)
	clusters, options.dRefsMax = getNormalizedClusters(chi)
	options.ssNames = make(map[string]struct{})
	options.ssDeployments = make(map[string]*chiv1.ChiDeployment)
	cmData := make(map[string]string)
	cmData[remoteServersXML] = genRemoteServersConfig(chi, &options, clusters)
	cmData[zookeeperXML] = genZookeeperConfig(chi)

	prefixes := make([]string, 0, len(options.ssNames))
	for p := range options.ssNames {
		prefixes = append(prefixes, p)
	}

	return ObjectsMap{
		ObjectsConfigMaps:   createConfigMapObjects(chi, cmData),
		ObjectsServices:     createServiceObjects(chi, &options),
		ObjectsStatefulSets: createStatefulSetObjects(chi, &options),
	}, prefixes
}

func createConfigMapObjects(chi *chiv1.ClickHouseInstallation, data map[string]string) ConfigMapList {
	cmList := make(ConfigMapList, 1)
	cmList[0] = &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf(configMapNamePattern, chi.Name),
			Namespace: chi.Namespace,
		},
		Data: data,
	}
	return cmList
}

func createServiceObjects(chi *chiv1.ClickHouseInstallation, o *genOptions) ServiceList {
	svcList := make(ServiceList, 0, len(o.ssNames))
	for ssName := range o.ssNames {
		svcName := fmt.Sprintf(svcNamePattern, ssName)
		svcList = append(svcList, &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      svcName,
				Namespace: chi.Namespace,
			},
			Spec: corev1.ServiceSpec{
				ClusterIP: "None",
				Selector: map[string]string{
					"app": ssName,
				},
				Ports: []corev1.ServicePort{
					{
						Name: "rpc",
						Port: 9000,
					},
					{
						Name: "interserver",
						Port: 9009,
					},
					{
						Name: "rest",
						Port: 8123,
					},
				},
			},
		})
	}
	return svcList
}

func createStatefulSetObjects(chi *chiv1.ClickHouseInstallation, o *genOptions) StatefulSetList {
	rNum := int32(1)
	ssList := make(StatefulSetList, 0, len(o.ssNames))
	cmName := fmt.Sprintf(configMapNamePattern, chi.Name)
	vmName := fmt.Sprintf(vmClickHouseDataPattern, chi.Name)
	for ssName := range o.ssNames {
		svcName := fmt.Sprintf(svcNamePattern, ssName)
		ssList = append(ssList, &apps.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ssName,
				Namespace: chi.Namespace,
			},
			Spec: apps.StatefulSetSpec{
				Replicas:    &rNum,
				ServiceName: svcName,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app": ssName,
					},
				},
				VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: vmName,
						},
						Spec: corev1.PersistentVolumeClaimSpec{
							AccessModes: []corev1.PersistentVolumeAccessMode{
								"ReadWriteOnce",
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceStorage: resource.MustParse("100Mi"),
								},
							},
						},
					},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Name: ssName,
						Labels: map[string]string{
							"app": ssName,
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
						},
						Containers: []corev1.Container{
							{
								Name:  ssName,
								Image: clickhouseDefaultDockerImage,
								Ports: []corev1.ContainerPort{
									{
										Name:          "rpc",
										ContainerPort: 9000,
									},
									{
										Name:          "interserver",
										ContainerPort: 9009,
									},
									{
										Name:          "rest",
										ContainerPort: 8123,
									},
								},
								VolumeMounts: []corev1.VolumeMount{
									{
										Name:      vmName,
										MountPath: "/clickhouse",
									},
									{
										Name:      cmName,
										MountPath: fullPathRemoteServersXML,
										SubPath:   remoteServersXML,
									},
								},
							},
						},
					},
				},
			},
		})
	}
	return ssList
}

func genZookeeperConfig(chi *chiv1.ClickHouseInstallation) string {
	b := &bytes.Buffer{}
	c := len(chi.Spec.Configuration.Zookeeper.Nodes)
	if c == 0 {
		return ""
	}
	fmt.Fprintf(b, "<yandex>\n%4s<zookeeper>\n", " ")
	for i := 0; i < c; i++ {
		fmt.Fprintf(b, "%8s<node>\n%12[1]s<host>%s</host>\n", " ", chi.Spec.Configuration.Zookeeper.Nodes[i].Host)
		if chi.Spec.Configuration.Zookeeper.Nodes[i].Port > 0 {
			fmt.Fprintf(b, "%12s<port>%d</port>\n", " ", chi.Spec.Configuration.Zookeeper.Nodes[i].Port)
		}
		fmt.Fprintf(b, "%8s</node>\n", " ")
	}
	fmt.Fprintf(b, "%4s</zookeeper>\n</yandex>\n", " ")
	return b.String()
}

func genRemoteServersConfig(chi *chiv1.ClickHouseInstallation, o *genOptions, c []*chiv1.ChiCluster) string {
	b := &bytes.Buffer{}
	dRefIndex := make(map[string]int)
	dID := make(map[string]string)
	for k := range o.dRefsMax {
		dID[k] = randomString()
	}
	fmt.Fprintf(b, "<yandex>\n%4s<remote_servers>\n", " ")
	for i := range c {
		fmt.Fprintf(b, "%8s<%s>\n", " ", c[i].Name)
		for j := range c[i].Layout.Shards {
			fmt.Fprintf(b, "%12s<shard>\n%16[1]s<internal_replication>%s</internal_replication>\n",
				" ", c[i].Layout.Shards[j].InternalReplication)
			if c[i].Layout.Shards[j].Weight > 0 {
				fmt.Fprintf(b, "%16s<weight>%d</weight>\n", " ", c[i].Layout.Shards[j].Weight)
			}
			for _, r := range c[i].Layout.Shards[j].Replicas {
				k := r.Deployment.Key
				idx, ok := dRefIndex[k]
				if !ok {
					idx = 1
				} else {
					idx++
					if idx > o.dRefsMax[k] {
						idx = 1
					}
				}
				dRefIndex[k] = idx
				prefix := fmt.Sprintf(ssNamePattern, chi.Name, dID[k], idx)
				o.ssNames[prefix] = struct{}{}
				o.ssDeployments[k] = &r.Deployment
				fmt.Fprintf(b, "%16s<replica>\n%20[1]s<host>%s</host>\n", " ", instanceHostname(chi, prefix))
				fmt.Fprintf(b, "%20s<port>9000</port>\n%16[1]s</replica>\n", " ")
			}
			fmt.Fprintf(b, "%12s</shard>\n", " ")
		}
		fmt.Fprintf(b, "%8s</%s>\n", " ", c[i].Name)
	}
	fmt.Fprintf(b, "%4s</remote_servers>\n</yandex>\n", " ")
	return b.String()
}

func instanceHostname(chi *chiv1.ClickHouseInstallation, prefix string) string {
	return fmt.Sprintf(hostnamePattern, prefix, chi.Namespace)
}

func getNormalizedClusters(chi *chiv1.ClickHouseInstallation) ([]*chiv1.ChiCluster, chiDeploymentRefs) {
	link := make(chan *chiClusterDataLink)
	count := len(chi.Spec.Configuration.Clusters)
	for _, cluster := range chi.Spec.Configuration.Clusters {
		go func(chin *chiv1.ClickHouseInstallation, c chiv1.ChiCluster, ch chan<- *chiClusterDataLink) {
			ch <- getNormalizedClusterLayoutData(chin, c)
		}(chi, cluster, link)
	}
	cList := make([]*chiv1.ChiCluster, 0, count)
	dRefs := make(chiDeploymentRefs)
	for i := 0; i < count; i++ {
		data := <-link
		cList = append(cList, data.cluster)
		dRefs.mergeWith(data.deployments)
	}
	return cList, dRefs
}

func getNormalizedClusterLayoutData(chi *chiv1.ClickHouseInstallation, c chiv1.ChiCluster) *chiClusterDataLink {
	n := &chiv1.ChiCluster{
		Name:       c.Name,
		Deployment: c.Deployment,
	}
	setDeploymentDefaults(&n.Deployment, &chi.Spec.Deployment)
	d := make(chiDeploymentRefs)

	switch c.Layout.Type {
	case clusterLayoutTypeStandard:
		if c.Layout.ReplicasCount == 0 {
			c.Layout.ReplicasCount++
		}
		if c.Layout.ShardsCount == 0 {
			c.Layout.ShardsCount++
		}

		n.Layout.Shards = make([]chiv1.ChiClusterLayoutShard, c.Layout.ShardsCount)
		for i := 0; i < c.Layout.ShardsCount; i++ {
			n.Layout.Shards[i].InternalReplication = stringTrue
			n.Layout.Shards[i].Replicas = make([]chiv1.ChiClusterLayoutShardReplica, c.Layout.ReplicasCount)

			for j := 0; j < c.Layout.ReplicasCount; j++ {
				setDeploymentDefaults(&n.Layout.Shards[i].Replicas[j].Deployment, &n.Deployment)
				n.Layout.Shards[i].Replicas[j].Deployment.Key = deploymentToString(&n.Layout.Shards[i].Replicas[j].Deployment)
				d[n.Layout.Shards[i].Replicas[j].Deployment.Key]++

			}
		}
	case clusterLayoutTypeAdvanced:
		n.Layout.Shards = c.Layout.Shards
		for i := range n.Layout.Shards {
			setDeploymentDefaults(&n.Layout.Shards[i].Deployment, &n.Deployment)

			switch n.Layout.Shards[i].InternalReplication {
			case shardInternalReplicationDisabled:
				n.Layout.Shards[i].InternalReplication = stringFalse
			default:
				n.Layout.Shards[i].InternalReplication = stringTrue
			}

			if n.Layout.Shards[i].DefinitionType == shardDefinitionTypeReplicasCount {
				n.Layout.Shards[i].Replicas = make([]chiv1.ChiClusterLayoutShardReplica,
					n.Layout.Shards[i].ReplicasCount)

				for j := 0; j < n.Layout.Shards[i].ReplicasCount; j++ {
					setDeploymentDefaults(&n.Layout.Shards[i].Replicas[j].Deployment, &n.Layout.Shards[i].Deployment)
					n.Layout.Shards[i].Replicas[j].Deployment.Key = deploymentToString(&n.Layout.Shards[i].Replicas[j].Deployment)
					d[n.Layout.Shards[i].Replicas[j].Deployment.Key]++
				}
				continue
			}

			for j := range n.Layout.Shards[i].Replicas {
				setDeploymentDefaults(&n.Layout.Shards[i].Replicas[j].Deployment, &n.Layout.Shards[i].Deployment)
				n.Layout.Shards[i].Replicas[j].Deployment.Key = deploymentToString(&n.Layout.Shards[i].Replicas[j].Deployment)
				d[n.Layout.Shards[i].Replicas[j].Deployment.Key]++
			}
		}
	}

	return &chiClusterDataLink{
		cluster:     n,
		deployments: d,
	}
}

func setDefaults(chi *chiv1.ClickHouseInstallation) {
}

func deploymentToString(d *chiv1.ChiDeployment) string {
	var keys []string
	a := make([]string, 0, len(d.Zone.MatchLabels))
	a = append(a, fmt.Sprintf("%s::%s::", d.Scenario, d.PodTemplateName))
	for k := range d.Zone.MatchLabels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		a = append(a, d.Zone.MatchLabels[k])
	}
	return strings.Join(a, "::")
}

func setDeploymentDefaults(d, parent *chiv1.ChiDeployment) {
	if parent != nil {
		if d.PodTemplateName == "" {
			(*d).PodTemplateName = parent.PodTemplateName
		}
		if d.Scenario == "" {
			(*d).Scenario = parent.Scenario
		}
		if len(d.Zone.MatchLabels) == 0 {
			zoneCopyFrom(&d.Zone, &parent.Zone)
		}
	} else if d.Scenario == "" {
		(*d).Scenario = deploymentScenarioDefault
	}
}

func zoneCopyFrom(z, source *chiv1.ChiDeploymentZone) {
	tmp := make(map[string]string)
	for k, v := range source.MatchLabels {
		tmp[k] = v
	}
	(*z).MatchLabels = tmp
}

func (d chiDeploymentRefs) mergeWith(another chiDeploymentRefs) {
	for ak, av := range another {
		_, ok := d[ak]
		if !ok || av > d[ak] {
			d[ak] = av
		}
	}
}

func randomString() string {
	return fmt.Sprintf("%x",
		rand.New(
			rand.NewSource(
				time.Now().
					UnixNano())).
			Int())
}
