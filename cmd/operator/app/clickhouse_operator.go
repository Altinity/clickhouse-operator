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
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi"
	"github.com/altinity/clickhouse-operator/pkg/version"

	chopclientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	chopinformers "github.com/altinity/clickhouse-operator/pkg/client/informers/externalversions"

	kubeinformers "k8s.io/client-go/informers"
	kube "k8s.io/client-go/kubernetes"
	kuberest "k8s.io/client-go/rest"
	kubeclientcmd "k8s.io/client-go/tools/clientcmd"

	"github.com/golang/glog"
)

// Prometheus exporter defaults
const (
	defaultMetricsEndpoint                  = ":8888"
	metricsPath                             = "/metrics"
	defaultInformerFactoryResyncPeriod      = 60 * time.Second
	defaultInformerFactoryResyncDebugPeriod = 60 * time.Second
)

const (
	// TODO probably this should be added as a CLI/Config param
	// Default number of controller threads running concurrently (used in case no other specified in config)
	defaultControllerThreadsNum = 1
)

// CLI parameter variables
var (
	// versionRequest defines request for clickhouse-operator version report. Operator should exit after version printed
	versionRequest bool

	// debugRequest defines request for clickhouse-operator debug run
	debugRequest bool

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
	kubeInformerFactoryResyncPeriod = defaultInformerFactoryResyncPeriod
	chopInformerFactoryResyncPeriod = defaultInformerFactoryResyncPeriod
)

var (
	controllerThreadsNum = defaultControllerThreadsNum
)

func init() {
	flag.BoolVar(&versionRequest, "version", false, "Display clickhouse-operator version and exit")
	flag.BoolVar(&debugRequest, "debug", false, "Debug run")
	flag.StringVar(&chopConfigFile, "config", "", "Path to clickhouse-operator config file.")
	flag.StringVar(&kubeConfigFile, "kubeconfig", "", "Path to kubernetes config file. Only required if called outside of the cluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Only required if called outside of the cluster and not being specified in kube config file.")
	flag.StringVar(&metricsEP, "metrics-endpoint", defaultMetricsEndpoint, "The Prometheus exporter endpoint.")
	flag.Parse()
}

// getKubeConfig creates kuberest.Config object based on current environment
func getKubeConfig(kubeConfigFile, masterURL string) (*kuberest.Config, error) {
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
		glog.Fatalf("Unable to initialize clickhouse-operator API clientset: %s", err.Error())
	}

	return kubeClientset, chopClientset
}

// Run is an entry point of the application
func Run() {
	if versionRequest {
		fmt.Printf("%s\n", version.Version)
		os.Exit(0)
	}

	if debugRequest {
		kubeInformerFactoryResyncPeriod = defaultInformerFactoryResyncDebugPeriod
		chopInformerFactoryResyncPeriod = defaultInformerFactoryResyncDebugPeriod
	}

	glog.V(1).Infof("Starting clickhouse-operator. Version:%s GitSHA:%s\n", version.Version, version.GitSHA)

	//
	// Initialize k8s API clients
	//
	kubeConfig, err := getKubeConfig(kubeConfigFile, masterURL)
	if err != nil {
		glog.Fatalf("Unable to build kubeconf: %s", err.Error())
		os.Exit(1)
	}
	kubeClient, chopClient := createClientsets(kubeConfig)

	//
	// Create operator instance
	//
	chop := chop.NewCHOp(version.Version, chopClient, chopConfigFile)
	if err := chop.Init(); err != nil {
		glog.Fatalf("Unable to init CHOP instance %v\n", err)
		os.Exit(1)
	}

	//
	// Create Informers
	//
	kubeInformerFactory := kubeinformers.NewSharedInformerFactoryWithOptions(
		kubeClient,
		kubeInformerFactoryResyncPeriod,
		kubeinformers.WithNamespace(chop.Config().GetInformerNamespace()),
	)
	chopInformerFactory := chopinformers.NewSharedInformerFactoryWithOptions(
		chopClient,
		chopInformerFactoryResyncPeriod,
		chopinformers.WithNamespace(chop.Config().GetInformerNamespace()),
	)

	//
	// Create Controller
	//
	chiController := chi.NewController(
		chop,
		chopClient,
		kubeClient,
		chopInformerFactory,
		kubeInformerFactory,
	)

	// Setup OS signals and termination context
	ctx, cancelFunc := context.WithCancel(context.Background())
	stopChan := make(chan os.Signal, 2)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stopChan
		cancelFunc()
		<-stopChan
		os.Exit(1)
	}()

	//
	// Start Informers
	//
	kubeInformerFactory.Start(ctx.Done())
	chopInformerFactory.Start(ctx.Done())

	//
	// Start Controller
	//
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
