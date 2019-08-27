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

package app

import (
	"context"
	"flag"
	"fmt"
	"github.com/altinity/clickhouse-operator/pkg/apis/metrics"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/altinity/clickhouse-operator/pkg/config"
	"github.com/altinity/clickhouse-operator/pkg/version"

	chopclientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	chopinformers "github.com/altinity/clickhouse-operator/pkg/client/informers/externalversions"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeinformers "k8s.io/client-go/informers"
	kube "k8s.io/client-go/kubernetes"
	kuberest "k8s.io/client-go/rest"
	kubeclientcmd "k8s.io/client-go/tools/clientcmd"

	"github.com/golang/glog"
)

// Prometheus exporter defaults
const (
	defaultMetricsEndpoint             = ":8888"
	metricsPath                        = "/metrics"
	defaultInformerFactoryResyncPeriod = 10 * time.Second
)

// Default number of controller threads running concurrently (used in case no other specified in config)
const (
	defaultControllerThreadsNum = 10
)

// CLI parameter variables
var (
	// versionRequest defines request for clickhouse-operator version report. Operator should exit after version printed
	versionRequest bool

	// chopConfigFile defines path to clickhouse-operator config file to be used
	chopConfigFile string

	// kubeConfigFile defines path to kube config file to be used
	kubeConfigFile string

	// masterURL defines URL of kubernetes master to be used
	masterURL string

	// metricsEP defines metrics end-point IP address
	metricsEP string

	// Setting to 0 disables resync
	// Informer fires Update() func to periodically verify current state
	kubeInformetFactoryResyncPeriod = defaultInformerFactoryResyncPeriod
	chopInformerFactoryResyncPeriod = defaultInformerFactoryResyncPeriod
)

var (
	runtimeParams        map[string]string
	controllerThreadsNum = defaultControllerThreadsNum
)

func init() {
	flag.BoolVar(&versionRequest, "version", false, "Display clickhouse-operator version and exit")
	flag.StringVar(&chopConfigFile, "config", "", "Path to clickhouse-operator config file.")
	flag.StringVar(&kubeConfigFile, "kubeconfig", "", "Path to kubernetes config file. Only required if called outside of the cluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Only required if called outside of the cluster and not being specified in kube config file.")
	flag.StringVar(&metricsEP, "metrics-endpoint", defaultMetricsEndpoint, "The Prometheus exporter endpoint.")
	flag.Parse()
}

// getConfig creates kuberest.Config object based on current environment
func getConfig(kubeConfigFile, masterURL string) (*kuberest.Config, error) {
	if len(kubeConfigFile) > 0 {
		// kube config file specified as CLI flag
		return kubeclientcmd.BuildConfigFromFlags(masterURL, kubeConfigFile)
	}

	if len(os.Getenv("KUBECONFIG")) > 0 {
		// kube config file specified as ENV var
		return kubeclientcmd.BuildConfigFromFlags(masterURL, os.Getenv("KUBECONFIG"))
	}

	if conf, err := kuberest.InClusterConfig(); err == nil {
		// in-cluster configuration found
		return conf, nil
	}

	usr, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("user not found")
	}

	// OS user found. Parse ~/.kube/config file
	conf, err := kubeclientcmd.BuildConfigFromFlags("", filepath.Join(usr.HomeDir, ".kube", "config"))
	if err != nil {
		return nil, fmt.Errorf("~/.kube/config not found")
	}

	// ~/.kube/config found
	return conf, nil
}

// createClientsets creates Clientset objects
func createClientsets(config *kuberest.Config) (*kube.Clientset, *chopclientset.Clientset) {

	kubeClientset, err := kube.NewForConfig(config)
	if err != nil {
		glog.Fatalf("Unable to initialize kubernetes API clientset: %s", err.Error())
	}

	chopClientset, err := chopclientset.NewForConfig(config)
	if err != nil {
		glog.Fatalf("Unable to initialize clickhouse-operator clientset: %s", err.Error())
	}

	return kubeClientset, chopClientset
}

// getRuntimeParamNames return list of ENV VARS parameter names
func getRuntimeParamNames() []string {
	// This list of ENV VARS is specified in operator .yaml manifest, section "kind: Deployment"
	return []string{
		// spec.nodeName: ip-172-20-52-62.ec2.internal
		"OPERATOR_POD_NODE_NAME",
		// metadata.name: clickhouse-operator-6f87589dbb-ftcsf
		"OPERATOR_POD_NAME",
		// metadata.namespace: kube-system
		"OPERATOR_POD_NAMESPACE",
		// status.podIP: 100.96.3.2
		"OPERATOR_POD_IP",
		// spec.serviceAccount: clickhouse-operator
		// spec.serviceAccountName: clickhouse-operator
		"OPERATOR_POD_SERVICE_ACCOUNT",

		// .containers.resources.requests.cpu
		"OPERATOR_CONTAINER_CPU_REQUEST",
		// .containers.resources.limits.cpu
		"OPERATOR_CONTAINER_CPU_LIMIT",
		// .containers.resources.requests.memory
		"OPERATOR_CONTAINER_MEM_REQUEST",
		// .containers.resources.limits.memory
		"OPERATOR_CONTAINER_MEM_LIMIT",

		// What namespaces to watch
		"WATCH_NAMESPACE",
		"WATCH_NAMESPACES",
	}
}

// getRuntimeParams returns map[string]string of ENV VARS with some runtime parameters
func getRuntimeParams() map[string]string {
	params := make(map[string]string)
	// Extract parameters from ENV VARS
	for _, varName := range getRuntimeParamNames() {
		params[varName] = os.Getenv(varName)
	}

	return params
}

// logRuntimeParams writes runtime parameters into log
func logRuntimeParams() {
	// Log params according to sorted names
	// So we need to
	// 1. Extract and sort names aka keys
	// 2. Walk over keys and log params

	runtimeParams = getRuntimeParams()

	// Sort names aka keys
	var keys []string
	for k := range runtimeParams {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Walk over sorted names aka keys
	glog.V(1).Infof("Parameters num: %d\n", len(runtimeParams))
	for _, k := range keys {
		glog.V(1).Infof("%s=%s\n", k, runtimeParams[k])
	}
}

// logConfig writes Config into log
func logConfig(chopConfig *config.Config) {
	glog.V(1).Infof("Config:\n%s", chopConfig.String())
}

// Run is an entry point of the application
func Run() {
	if versionRequest {
		fmt.Printf("%s\n", version.Version)
		os.Exit(0)
	}

	//
	// Prepare configuration
	//
	glog.V(1).Infof("Starting clickhouse-operator. Version:%s GitSHA:%s\n", version.Version, version.GitSHA)
	logRuntimeParams()

	chopConfig, err := config.GetConfig(chopConfigFile)
	if err != nil {
		glog.Fatalf("Unable to build config file %v\n", err)
		os.Exit(1)
	}
	logConfig(chopConfig)

	// Set OS signals and termination context
	ctx, cancelFunc := context.WithCancel(context.Background())
	stopChan := make(chan os.Signal, 2)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stopChan
		cancelFunc()
		<-stopChan
		os.Exit(1)
	}()

	// Initialize ClientSets and Informers
	kubeConfig, err := getConfig(kubeConfigFile, masterURL)
	if err != nil {
		glog.Fatalf("Unable to build kube conf: %s", err.Error())
		os.Exit(1)
	}

	kubeClient, chopClient := createClientsets(kubeConfig)

	//
	// Create Informers
	//

	// Namespace where informers would watch notifications from
	namespace := metav1.NamespaceAll
	if len(chopConfig.WatchNamespaces) == 1 {
		// We have exactly one watch namespace specified
		// This scenario is implemented in go-client
		// In any other case just keep metav1.NamespaceAll

		// This contradicts current implementation of multiple namespaces in config's watchNamespaces field,
		// but k8s has possibility to specify one/all namespaces only, no 'multiple namespaces' option
		namespace = chopConfig.WatchNamespaces[0]
	}

	kubeInformerFactory := kubeinformers.NewSharedInformerFactoryWithOptions(
		kubeClient,
		kubeInformetFactoryResyncPeriod,
		kubeinformers.WithNamespace(namespace),
	)
	chopInformerFactory := chopinformers.NewSharedInformerFactoryWithOptions(
		chopClient,
		chopInformerFactoryResyncPeriod,
		chopinformers.WithNamespace(namespace),
	)

	//
	// Create resource Controller
	//

	chiController := chi.NewController(
		version.Version,
		runtimeParams,
		chopConfig,
		chopClient,
		kubeClient,
		chopInformerFactory.Clickhouse().V1().ClickHouseInstallations(),
		chopInformerFactory.Clickhouse().V1().ClickHouseInstallationTemplates(),
		kubeInformerFactory.Core().V1().Services(),
		kubeInformerFactory.Core().V1().Endpoints(),
		kubeInformerFactory.Core().V1().ConfigMaps(),
		kubeInformerFactory.Apps().V1().StatefulSets(),
		kubeInformerFactory.Core().V1().Pods(),
		metrics.StartMetricsREST(
			chopConfig.ChUsername, chopConfig.ChPassword, chopConfig.ChPort,
			metricsEP, metricsPath,
			metricsEP, "/chi",
		),
	)
	chiController.AddEventHandlers(
		chopInformerFactory.Clickhouse().V1().ClickHouseInstallations(),
		chopInformerFactory.Clickhouse().V1().ClickHouseInstallationTemplates(),
		kubeInformerFactory.Core().V1().Services(),
		kubeInformerFactory.Core().V1().Endpoints(),
		kubeInformerFactory.Core().V1().ConfigMaps(),
		kubeInformerFactory.Apps().V1().StatefulSets(),
		kubeInformerFactory.Core().V1().Pods(),
	)

	//
	// Start Informers and Controllers
	//

	// Start Informers
	kubeInformerFactory.Start(ctx.Done())
	chopInformerFactory.Start(ctx.Done())

	// Start CHI resource Controller
	glog.V(1).Info("Starting CHI controller\n")
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		chiController.Run(ctx, controllerThreadsNum)
	}()
	<-ctx.Done()
	wg.Wait()
}
