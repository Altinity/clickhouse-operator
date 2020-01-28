package parser

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"
)

const (
	clusterLayoutTypeStandard = "Standard"
	clusterLayoutTypeAdvanced = "Advanced"
)

const (
	shardDefinitionTypeReplicasCount = "ReplicasCount"
	shardDefinitionTypeReplicas      = "Hosts"
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
	defaultNamespace = "default"
	remoteServersXML = "remote_servers.xml"
	zookeeperXML     = "zookeeper.xml"
	usersXML         = "users.xml"
)

const (
	ssNamePattern           = "chi-%s-i%d"
	svcNamePattern          = "%s-service"
	chiSvcNamePattern       = "clickhouse-%s"
	hostnamePattern         = "%s-0.%[1]s-service.%s.svc.cluster.local"
	configMapNamePattern    = "chi-%s-configd"
	vmClickHouseDataPattern = "chi-%s-data"
)

const (
	objectsConfigMaps objectKind = iota + 1
	objectsStatefulSets
	objectsServices
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

// ObjectMarshaller expresses common behaviour of the object marshaling
type ObjectMarshaller interface {
	Marshal(buffer *bytes.Buffer, object interface{})
}

// ClickHouseInstallation defines structure of the Custom Resource Object
type ClickHouseInstallation struct {
	Metadata struct {
		Name      string `yaml:"name"`
		Namespace string `yaml:"namespace"`
	} `yaml:"metadata"`
	Spec struct {
		Deployment    chiDeployment    `yaml:"deployment"`
		Configuration chiConfiguration `yaml:"configuration"`
		Templates     chiTemplates     `yaml:"templates"`
	} `yaml:"spec"`
}

type chiConfiguration struct {
	Users    map[string]string `yaml:"users"`
	Settings map[string]string `yaml:"settings"`
	Clusters []chiCluster      `yaml:"clusters"`
}

type chiDeployment struct {
	PodTemplate string            `yaml:"podTemplate"`
	Zone        chiDeploymentZone `yaml:"zone"`
	Scenario    string            `yaml:"scenario"`
	key         string
}

type chiDeploymentZone struct {
	MatchLabels map[string]string `yaml:"matchLabels"`
}

type chiCluster struct {
	Name       string           `yaml:"name"`
	Layout     chiClusterLayout `yaml:"layout"`
	Deployment chiDeployment    `yaml:"deployment"`
}

type chiClusterLayout struct {
	Type          string                  `yaml:"type"`
	ShardsCount   int                     `yaml:"shardsCount"`
	ReplicasCount int                     `yaml:"replicasCount"`
	Shards        []chiClusterLayoutShard `yaml:"shards"`
}

type chiClusterLayoutShard struct {
	DefinitionType      string                         `yaml:"definitionType"`
	ReplicasCount       int                            `yaml:"replicasCount"`
	Weight              int                            `yaml:"weight"`
	InternalReplication string                         `yaml:"internalReplication"`
	Deployment          chiDeployment                  `yaml:"deployment"`
	Replicas            []chiClusterLayoutShardReplica `yaml:"replicas"`
}

type chiClusterLayoutShardReplica struct {
	Deployment chiDeployment `yaml:"deployment"`
}

type chiTemplates struct {
	PodTemplates []chiPodTemplate `yaml:"podTemplates"`
}

type chiPodTemplate struct {
	Name       string `yaml:"name"`
	Containers []chiPodTemplatesContainerTemplate
}

type chiPodTemplatesContainerTemplate struct {
	Name      string `yaml:"name"`
	Image     string `yaml:"image"`
	Resources struct {
		Requests chiContainerResourceParams `yaml:"requests"`
		Limits   chiContainerResourceParams `yaml:"limits"`
	} `yaml:"resources"`
}

type chiContainerResourceParams struct {
	Memory string `yaml:"memory"`
	CPU    string `yaml:"cpu"`
}

type chiDeploymentRefs map[string]int

type chiClusterDataLink struct {
	cluster     *chiCluster
	deployments chiDeploymentRefs
}

type configMap struct {
	APIVersion string            `yaml:"apiVersion"`
	Kind       string            `yaml:"kind"`
	Metadata   metaData          `yaml:"metadata"`
	Data       map[string]string `yaml:"data"`
}

type statefullSet struct {
	APIVersion string           `yaml:"apiVersion"`
	Kind       string           `yaml:"kind"`
	Metadata   metaData         `yaml:"metadata"`
	Spec       statefullSetSpec `yaml:"spec"`
}

type statefullSetSpec struct {
	Replicas            int                 `yaml:"replicas"`
	ServiceName         string              `yaml:"serviceName"`
	Selector            labelSelector       `yaml:"selector"`
	Template            podTemplate         `yaml:"template"`
	VolumeClaimTemplate volumeClaimTemplate `yaml:"volumeClaimTemplate"`
}

type podTemplate struct {
	Metadata metaData        `yaml:"metadata"`
	Spec     podTemplateSpec `yaml:"spec"`
}

type podTemplateSpec struct {
	Volumes    []podTemplateVolume `yaml:"volumes"`
	Containers []containerTemplate `yaml:"containers"`
}

type containerTemplate struct {
	Name         string          `yaml:"name"`
	Image        string          `yaml:"image"`
	Ports        []containerPort `yaml:"ports"`
	VolumeMounts []volumeMount   `yaml:"volumeMounts"`
}

type volumeMount struct {
	Name      string `yaml:"name"`
	MountPath string `yaml:"mountPath"`
	SubPath   string `yaml:"subPath,omitempty"`
}

type containerPort struct {
	Name          string `yaml:"name"`
	ContainerPort int    `yaml:"containerPort"`
}

type podTemplateVolume struct {
	Name      string                     `yaml:"name"`
	ConfigMap podTemplateVolumeConfigMap `yaml:"configMap"`
}

type podTemplateVolumeConfigMap struct {
	Name string `yaml:"name"`
}

type volumeClaimTemplate struct {
	Metadata metaData                `yaml:"metadata"`
	Spec     volumeClaimTemplateSpec `yaml:"spec"`
}

type volumeClaimTemplateSpec struct {
	AccessModes []string                     `yaml:"accessModes"`
	Resources   volumeClaimTemplateResources `yaml:"resources"`
}

type volumeClaimTemplateResources struct {
	Requests volumeClaimTemplateResourcesRequests `yaml:"requests"`
}

type volumeClaimTemplateResourcesRequests struct {
	Storage string `yaml:"storage"`
}

type labelSelector struct {
	MatchLabels map[string]string `yaml:"matchLabels"`
}

type service struct {
	APIVersion string      `yaml:"apiVersion"`
	Kind       string      `yaml:"kind"`
	Metadata   metaData    `yaml:"metadata"`
	Spec       serviceSpec `yaml:"spec"`
}

type serviceSpec struct {
	Ports     []serviceSpecPort `yaml:"ports"`
	ClusterIP string            `yaml:"clusterIP"`
	Selector  map[string]string `yaml:"selector"`
}

type serviceSpecPort struct {
	Name string `yaml:"name"`
	Port int    `yaml:"port"`
}

type metaData struct {
	Name      string            `yaml:"name"`
	Namespace string            `yaml:"namespace,omitempty"`
	Labels    map[string]string `yaml:"labels,omitempty"`
}

type genOptions struct {
	ssNames       map[string]struct{}
	ssDeployments map[string]*chiDeployment
	dRefsMax      chiDeploymentRefs
}

type objectKind uint8
type objectsMap map[objectKind]interface{}
type configMapList []*configMap
type statefulSetList []*statefullSet
type serviceList []*service

// GenerateArtifacts returns resulting (composite) manifest
func GenerateArtifacts(chi *ClickHouseInstallation, om ObjectMarshaller) string {
	b := &bytes.Buffer{}
	objects := chi.createObjects()
	n := len(objects) - 1
	i := 0
	for _, o := range objects {
		switch v := o.(type) {
		case configMapList:
			for j, m := range v {
				om.Marshal(b, m)
				l := len(v) - 1
				if j != l {
					fmt.Fprintf(b, callSplitter)
				}
			}
		case serviceList:
			for j, m := range v {
				om.Marshal(b, m)
				l := len(v) - 1
				if j != l {
					fmt.Fprintf(b, callSplitter)
				}
			}
		case statefulSetList:
			for j, m := range v {
				om.Marshal(b, m)
				l := len(v) - 1
				if j != l {
					fmt.Fprintf(b, callSplitter)
				}
			}
		}
		if i != n {
			fmt.Fprintf(b, callSplitter)
		}
		i++
	}
	return b.String()
}

func (chi *ClickHouseInstallation) createObjects() objectsMap {
	var (
		clusters []*chiCluster
		options  genOptions
	)
	chi.setDefaults()
	chi.Spec.Deployment.setDefaults(nil)
	clusters, options.dRefsMax = chi.getNormalizedClusters()
	options.ssNames = make(map[string]struct{})
	options.ssDeployments = make(map[string]*chiDeployment)
	cmData := make(map[string]string)
	cmData[remoteServersXML] = chi.genRemoteServersConfig(&options, clusters)
	cmData[zookeeperXML] = chi.genZookeeperConfig()

	return objectsMap{
		objectsConfigMaps:   chi.createConfigMapObjects(cmData),
		objectsServices:     chi.createServiceObjects(&options),
		objectsStatefulSets: chi.createStatefulSetObjects(&options),
	}
}

func (chi *ClickHouseInstallation) createConfigMapObjects(data map[string]string) configMapList {
	cmList := make(configMapList, 1)
	cmList[0] = &configMap{
		APIVersion: "v1",
		Kind:       "ConfigMap",
		Metadata: metaData{
			Name:      fmt.Sprintf(configMapNamePattern, chi.Metadata.Name),
			Namespace: chi.Metadata.Namespace,
		},
		Data: data,
	}
	return cmList
}

// Returns list of services:
// one service per pod with internal name, and one service for installation itself that should finally bind to:
// clickhouse-<installation_name>.<namespace>.svc.cluster.local
func (chi *ClickHouseInstallation) createServiceObjects(o *genOptions) serviceList {
	svcList := make(serviceList, 0, len(o.ssNames)+1)
	ports := []serviceSpecPort{
		{
			Name: "http",
			Port: 8123,
		},
		{
			Name: "client",
			Port: 9000,
		},
		{
			Name: "interserver",
			Port: 9009,
		},
	}
	for ssName := range o.ssNames {
		svcList = append(svcList, &service{
			APIVersion: "v1",
			Kind:       "Service",
			Metadata: metaData{
				Name:      fmt.Sprintf(svcNamePattern, ssName),
				Namespace: chi.Metadata.Namespace,
			},
			Spec: serviceSpec{
				ClusterIP: "None",
				Selector: map[string]string{
					"app": ssName,
				},
				Ports: ports,
			},
		})
	}
	svcList = append(svcList, &service{
		APIVersion: "v1",
		Kind:       "Service",
		Metadata: metaData{
			Name:      fmt.Sprintf(chiSvcNamePattern, chi.Metadata.Name),
			Namespace: chi.Metadata.Namespace,
		},
		Spec: serviceSpec{
			ClusterIP: "None",
			Selector: map[string]string{
				"chi": chi.Metadata.Name,
			},
			Ports: ports,
		},
	})
	return svcList
}

func (chi *ClickHouseInstallation) createStatefulSetObjects(o *genOptions) statefulSetList {
	ssList := make(statefulSetList, 0, len(o.ssNames))
	cmName := fmt.Sprintf(configMapNamePattern, chi.Metadata.Name)
	vmName := fmt.Sprintf(vmClickHouseDataPattern, chi.Metadata.Name)
	for ssName := range o.ssNames {
		svcName := fmt.Sprintf(svcNamePattern, ssName)
		ssList = append(ssList, &statefullSet{
			APIVersion: "apps/v1",
			Kind:       "StatefulSet",
			Metadata: metaData{
				Name:      ssName,
				Namespace: chi.Metadata.Namespace,
			},
			Spec: statefullSetSpec{
				Replicas:    1,
				ServiceName: svcName,
				Selector: labelSelector{
					MatchLabels: map[string]string{
						"app": ssName,
					},
				},
				VolumeClaimTemplate: volumeClaimTemplate{
					Metadata: metaData{
						Name: vmName,
					},
					Spec: volumeClaimTemplateSpec{
						AccessModes: []string{
							"ReadWriteOnce",
						},
						Resources: volumeClaimTemplateResources{
							Requests: volumeClaimTemplateResourcesRequests{
								Storage: "1Gb",
							},
						},
					},
				},
				Template: podTemplate{
					Metadata: metaData{
						Name: ssName,
						Labels: map[string]string{
							"app": ssName,
						},
					},
					Spec: podTemplateSpec{
						Volumes: []podTemplateVolume{
							{
								Name: cmName,
								ConfigMap: podTemplateVolumeConfigMap{
									Name: cmName,
								},
							},
						},
						Containers: []containerTemplate{
							{
								Name:  ssName,
								Image: clickhouseDefaultDockerImage,
								Ports: []containerPort{
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
								VolumeMounts: []volumeMount{
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

func (chi *ClickHouseInstallation) genZookeeperConfig() string {
	b := &bytes.Buffer{}
	return b.String()
}

func (chi *ClickHouseInstallation) genRemoteServersConfig(o *genOptions, c []*chiCluster) string {
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
				k := r.Deployment.key
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
				prefix := fmt.Sprintf(ssNamePattern, dID[k], idx)
				o.ssNames[prefix] = struct{}{}
				o.ssDeployments[k] = &r.Deployment
				fmt.Fprintf(b, "%16s<replica>\n%20[1]s<host>%s</host>\n", " ", chi.instanceHostname(prefix))
				fmt.Fprintf(b, "%20s<port>9000</port>\n%16[1]s</replica>\n", " ")
			}
			fmt.Fprintf(b, "%12s</shard>\n", " ")
		}
		fmt.Fprintf(b, "%8s</%s>\n", " ", c[i].Name)
	}
	fmt.Fprintf(b, "%4s</remote_servers>\n</yandex>\n", " ")
	return b.String()
}

func (chi *ClickHouseInstallation) instanceHostname(prefix string) string {
	return fmt.Sprintf(hostnamePattern, prefix, chi.Metadata.Namespace)
}

func (chi *ClickHouseInstallation) getNormalizedClusters() ([]*chiCluster, chiDeploymentRefs) {
	link := make(chan *chiClusterDataLink)
	count := len(chi.Spec.Configuration.Clusters)
	for _, cluster := range chi.Spec.Configuration.Clusters {
		go func(c chiCluster, ch chan<- *chiClusterDataLink) {
			ch <- chi.getNormalizedClusterLayoutData(c)
		}(cluster, link)
	}
	cList := make([]*chiCluster, 0, count)
	dRefs := make(chiDeploymentRefs)
	for i := 0; i < count; i++ {
		data := <-link
		cList = append(cList, data.cluster)
		dRefs.mergeWith(data.deployments)
	}
	return cList, dRefs
}

func (chi *ClickHouseInstallation) getNormalizedClusterLayoutData(c chiCluster) *chiClusterDataLink {
	n := &chiCluster{
		Name:       c.Name,
		Deployment: c.Deployment,
	}
	n.Deployment.setDefaults(&chi.Spec.Deployment)
	d := make(chiDeploymentRefs)

	switch c.Layout.Type {
	case clusterLayoutTypeStandard:
		if c.Layout.ReplicasCount == 0 {
			c.Layout.ReplicasCount++
		}
		if c.Layout.ShardsCount == 0 {
			c.Layout.ShardsCount++
		}

		n.Layout.Shards = make([]chiClusterLayoutShard, c.Layout.ShardsCount)
		for i := 0; i < c.Layout.ShardsCount; i++ {
			n.Layout.Shards[i].InternalReplication = stringTrue
			n.Layout.Shards[i].Replicas = make([]chiClusterLayoutShardReplica, c.Layout.ReplicasCount)

			for j := 0; j < c.Layout.ReplicasCount; j++ {
				n.Layout.Shards[i].Replicas[j].Deployment.setDefaults(&n.Deployment)
				n.Layout.Shards[i].Replicas[j].Deployment.key = n.Layout.Shards[i].Replicas[j].Deployment.toString()
				d[n.Layout.Shards[i].Replicas[j].Deployment.key]++

			}
		}
	case clusterLayoutTypeAdvanced:
		n.Layout.Shards = c.Layout.Shards
		for i := range n.Layout.Shards {
			n.Layout.Shards[i].Deployment.setDefaults(&n.Deployment)

			switch n.Layout.Shards[i].InternalReplication {
			case shardInternalReplicationDisabled:
				n.Layout.Shards[i].InternalReplication = stringFalse
			default:
				n.Layout.Shards[i].InternalReplication = stringTrue
			}

			if n.Layout.Shards[i].DefinitionType == shardDefinitionTypeReplicasCount {
				n.Layout.Shards[i].Replicas = make([]chiClusterLayoutShardReplica,
					n.Layout.Shards[i].ReplicasCount)

				for j := 0; j < n.Layout.Shards[i].ReplicasCount; j++ {
					n.Layout.Shards[i].Replicas[j].Deployment.setDefaults(&n.Layout.Shards[i].Deployment)
					n.Layout.Shards[i].Replicas[j].Deployment.key = n.Layout.Shards[i].Replicas[j].Deployment.toString()
					d[n.Layout.Shards[i].Replicas[j].Deployment.key]++
				}
				continue
			}

			for j := range n.Layout.Shards[i].Replicas {
				n.Layout.Shards[i].Replicas[j].Deployment.setDefaults(&n.Layout.Shards[i].Deployment)
				n.Layout.Shards[i].Replicas[j].Deployment.key = n.Layout.Shards[i].Replicas[j].Deployment.toString()
				d[n.Layout.Shards[i].Replicas[j].Deployment.key]++
			}
		}
	}

	return &chiClusterDataLink{
		cluster:     n,
		deployments: d,
	}
}

func (chi *ClickHouseInstallation) setDefaults() {
	if chi.Metadata.Namespace == "" {
		chi.Metadata.Namespace = defaultNamespace
	}
}

func (d *chiDeployment) toString() string {
	var keys []string
	a := make([]string, 0, len(d.Zone.MatchLabels))
	a = append(a, fmt.Sprintf("%s::%s::", d.Scenario, d.PodTemplate))
	for k := range d.Zone.MatchLabels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		a = append(a, d.Zone.MatchLabels[k])
	}
	return strings.Join(a, "::")
}

func (d *chiDeployment) setDefaults(parent *chiDeployment) {
	if parent == nil && d.Scenario == "" {
		d.Scenario = deploymentScenarioDefault
		return
	}
	if d.PodTemplate == "" {
		d.PodTemplate = parent.PodTemplate
	}
	if d.Scenario == "" {
		d.Scenario = parent.Scenario
	}
	if len(d.Zone.MatchLabels) == 0 {
		d.Zone.copyFrom(&parent.Zone)
	}
}

func (z *chiDeploymentZone) copyFrom(source *chiDeploymentZone) {
	tmp := make(map[string]string)
	for k, v := range source.MatchLabels {
		tmp[k] = v
	}
	z.MatchLabels = tmp
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
