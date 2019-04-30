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
	"github.com/altinity/clickhouse-operator/pkg/config"
	"net/http"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	chopmetrics "github.com/altinity/clickhouse-operator/pkg/apis/metrics"
	chopclientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	chopinformers "github.com/altinity/clickhouse-operator/pkg/client/informers/externalversions"
	"github.com/altinity/clickhouse-operator/pkg/controllers/chi"
	kubeinformers "k8s.io/client-go/informers"
	kube "k8s.io/client-go/kubernetes"
	kuberest "k8s.io/client-go/rest"
	kubeclientcmd "k8s.io/client-go/tools/clientcmd"

	chopmodels "github.com/altinity/clickhouse-operator/pkg/models"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
)

// Version defines current build versionRequest
const Version = "0.2.2"

// Prometheus exporter defaults
const (
	defaultMetricsEndpoint = ":8888"
	metricsPath            = "/metrics"
	informerFactoryResync  = 30 * time.Second
)

var (
	// versionRequest defines versionRequest request
	versionRequest bool

	// chopConfigFile defines path to clickhouse-operator config file to be used
	chopConfigFile string

	// kubeConfigFile defines path to kube config file to be used
	kubeConfigFile string

	// masterURL defines URL of kubernetes master to be used
	masterURL string

	// metricsEP defines metrics end-point IP address
	metricsEP string
)

func init() {
	flag.BoolVar(&versionRequest, "version", false, "Display versionRequest and exit")
	flag.StringVar(&chopConfigFile, "config", "", "Path to clickhouse-operator config file.")
	flag.StringVar(&kubeConfigFile, "kube-config", "", "Path to kubernetes config file. Only required if called outside of the cluster.")
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

	if len(os.Getenv("KUBE_CONFIG")) > 0 {
		// kube config file specified as ENV var
		return kubeclientcmd.BuildConfigFromFlags(masterURL, os.Getenv("KUBE_CONFIG"))
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

// Run is an entry point of the application
func Run() {
	if versionRequest {
		fmt.Printf("%s\n", Version)
		os.Exit(0)
	}

	glog.V(1).Infof("Starting clickhouse-operator versionRequest '%s'\n", Version)
	chopConfig, err := config.GetConfig(chopConfigFile)
	if err != nil {
		glog.Fatalf("Unable to build config file %v\n", err)
		os.Exit(1)
	}

	chopmodels.SetAppVersion(Version)

	// Initializing Prometheus Metrics Exporter
	glog.V(1).Infof("Starting metrics exporter at '%s%s'\n", metricsEP, metricsPath)
	metricsExporter := chopmetrics.CreateExporter()
	prometheus.MustRegister(metricsExporter)
	http.Handle(metricsPath, prometheus.Handler())
	go http.ListenAndServe(metricsEP, nil)

	// Setting OS signals and termination context
	ctx, cancelFunc := context.WithCancel(context.Background())
	stopChan := make(chan os.Signal, 2)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stopChan
		cancelFunc()
		<-stopChan
		os.Exit(1)
	}()

	// Initializing ClientSets and Informers
	kubeConfig, err := getConfig(kubeConfigFile, masterURL)
	if err != nil {
		glog.Fatalf("Unable to build kube conf: %s", err.Error())
		os.Exit(1)
	}

	kubeClient, chopClient := createClientsets(kubeConfig)
	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeClient, informerFactoryResync)
	chopInformerFactory := chopinformers.NewSharedInformerFactory(chopClient, informerFactoryResync)

	// Creating resource Controller
	chiController := chi.CreateController(
		chopConfig,
		chopClient,
		kubeClient,
		chopInformerFactory.Clickhouse().V1().ClickHouseInstallations(),
		kubeInformerFactory.Core().V1().Services(),
		kubeInformerFactory.Core().V1().Endpoints(),
		kubeInformerFactory.Core().V1().ConfigMaps(),
		kubeInformerFactory.Apps().V1().StatefulSets(),
		kubeInformerFactory.Core().V1().Pods(),
		metricsExporter,
	)

	// Starting Informers
	kubeInformerFactory.Start(ctx.Done())
	chopInformerFactory.Start(ctx.Done())

	// Starting CHI resource Controller
	glog.V(1).Info("Starting CHI controller\n")
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		chiController.Run(ctx, 10)
	}()
	<-ctx.Done()
	wg.Wait()
}
