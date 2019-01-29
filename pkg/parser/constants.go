package parser

const (
	// ObjectsConfigMaps defines a category of the ConfigMap objects list
	ObjectsConfigMaps ObjectKind = iota + 1
	// ObjectsStatefulSets defines a category of the StatefulSet objects list
	ObjectsStatefulSets
	// ObjectsServices defines a category of the Service objects list
	ObjectsServices
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
	ssNameIDPattern      = "d%si%d"
	ssNamePattern        = "ch-%s"
	svcNamePattern       = "%ss"
	domainPattern        = ".%s.svc.cluster.local"
	hostnamePattern      = ssNamePattern + "-0.%[1]ss%s"
	configMapNamePattern = "chi-%s-configd"
)

const (
	chDefaultDockerImage         = "yandex/clickhouse-server:latest"
	chDefaultVolumeMountNameData = "clickhouse-data"
)

const (
	useDefaultNamePlaceholder = "USE_DEFAULT_NAME"
)

const (
	chDefaultRPCPortName           = "rpc"
	chDefaultRPCPortNumber         = 9000
	chDefaultInterServerPortName   = "interserver"
	chDefaultInterServerPortNumber = 9009
	chDefaultRestPortName          = "rest"
	chDefaultRestPortNumber        = 8123
	chDefaultAppLabel              = "app"
)

const (
	configdPath              = "/etc/clickhouse-server/config.d/"
	fullPathRemoteServersXML = configdPath + remoteServersXML
	fullPathZookeeperXML     = configdPath + zookeeperXML
	fullPathClickHouseData   = "/var/lib/clickhouse"
)

const (
	templateDefaultsServiceClusterIP = "None"
)
