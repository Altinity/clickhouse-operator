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
	"sync"
	"syscall"
	"time"

	kubeinformers "k8s.io/client-go/informers"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	chopinformers "github.com/altinity/clickhouse-operator/pkg/client/informers/externalversions"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi"
	"github.com/altinity/clickhouse-operator/pkg/version"
)

// Prometheus exporter defaults
const (
	defaultMetricsEndpoint                  = ":8888"
	metricsPath                             = "/metrics"
	defaultInformerFactoryResyncPeriod      = 60 * time.Second
	defaultInformerFactoryResyncDebugPeriod = 60 * time.Second
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

func init() {
	flag.BoolVar(&versionRequest, "version", false, "Display clickhouse-operator version and exit")
	flag.BoolVar(&debugRequest, "debug", false, "Debug run")
	flag.StringVar(&chopConfigFile, "config", "", "Path to clickhouse-operator config file.")
	flag.StringVar(&kubeConfigFile, "kubeconfig", "", "Path to custom kubernetes config file. Makes sense if runs outside of the cluster only.")
	flag.StringVar(&masterURL, "master", "", "The address of custom Kubernetes API server. Makes sense if runs outside of the cluster and not being specified in kube config file only.")
	flag.StringVar(&metricsEP, "metrics-endpoint", defaultMetricsEndpoint, "The Prometheus exporter endpoint.")
	flag.Parse()
}

// Run is an entry point of the application
func Run() {
	if versionRequest {
		fmt.Printf("%s\n", version.Version)
		os.Exit(0)
	}

	log.S().P()
	defer log.E().P()

	if debugRequest {
		kubeInformerFactoryResyncPeriod = defaultInformerFactoryResyncDebugPeriod
		chopInformerFactoryResyncPeriod = defaultInformerFactoryResyncDebugPeriod
	}

	log.F().Info("Starting clickhouse-operator. Version:%s GitSHA:%s BuiltAt:%s", version.Version, version.GitSHA, version.BuiltAt)

	// Initialize k8s API clients
	kubeClient, extClient, chopClient := chop.GetClientset(kubeConfigFile, masterURL)

	// Create operator instance
	chop.New(kubeClient, chopClient, chopConfigFile)
	log.V(1).F().Info("Log options parsed")
	log.Info(chop.Config().String(true))

	// Create Informers
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

	// Create Controller
	chiController := chi.NewController(
		chopClient,
		extClient,
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
	log.V(1).F().Info("Starting CHI controller")
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		chiController.Run(ctx)
	}()
	<-ctx.Done()
	wg.Wait()
}
