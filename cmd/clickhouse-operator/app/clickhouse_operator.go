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

	clientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	informers "github.com/altinity/clickhouse-operator/pkg/client/informers/externalversions"

	"github.com/altinity/clickhouse-operator/pkg/controllers/chi"
	"github.com/golang/glog"

	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Version defines current build version
const Version = "0.1.3beta"

var (
	kubeconfig string
	masterURL  string
)

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "",
		"Paths to kubeconfig. Only required if called outside of the cluster.")
	flag.StringVar(&masterURL, "master", "",
		"The address of the Kubernetes API server. Only required if called outside of the cluster.")
	flag.Parse()
}

// getConfig creates rest.Config object based on current environment
func getConfig() (*rest.Config, error) {
	if len(kubeconfig) > 0 {
		return clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	}
	if len(os.Getenv("KUBECONFIG")) > 0 {
		return clientcmd.BuildConfigFromFlags(masterURL, os.Getenv("KUBECONFIG"))
	}
	if conf, err := rest.InClusterConfig(); err == nil {
		return conf, nil
	}
	if usr, err := user.Current(); err == nil {
		if conf, err := clientcmd.BuildConfigFromFlags(
			"", filepath.Join(usr.HomeDir, ".kube", "config")); err == nil {
			return conf, nil
		}
	}
	return nil, fmt.Errorf("kubeconfig not found")
}

// createClientsets creates Clientset objects
func createClientsets() (*kubernetes.Clientset, *clientset.Clientset) {
	config, err := getConfig()
	if err != nil {
		glog.Fatalf("Unable to initialize cluster configuration: %s", err.Error())
	}
	kubeClientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		glog.Fatalf("Unable to initialize kubernetes API clientset: %s", err.Error())
	}
	customClientset, err := clientset.NewForConfig(config)
	if err != nil {
		glog.Fatalf("Unable to initialize Custom Resource API clientset: %s", err.Error())
	}
	return kubeClientset, customClientset
}

// Run is an entry point of the application
func Run() {
	glog.V(1).Infof("Starting clickhouse-operator version '%s'\n", Version)
	// Setting OS signals and termination context
	ctx, cancelFunc := context.WithCancel(context.Background())
	stop := make(chan os.Signal, 2)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		cancelFunc()
		<-stop
		os.Exit(1)
	}()
	// Initializing ClientSets and Informers
	kubeClient, chiClient := createClientsets()
	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeClient, time.Second*30)
	chiInformerFactory := informers.NewSharedInformerFactory(chiClient, time.Second*30)
	// Creating resource Controller
	chiController := chi.CreateController(
		chiClient, kubeClient,
		chiInformerFactory.Clickhouse().V1().ClickHouseInstallations(),
		kubeInformerFactory.Apps().V1().StatefulSets(),
		kubeInformerFactory.Core().V1().ConfigMaps(),
		kubeInformerFactory.Core().V1().Services())
	// Starting Informers
	kubeInformerFactory.Start(ctx.Done())
	chiInformerFactory.Start(ctx.Done())
	// Starting resource Controller
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		chiController.Run(ctx, 10)
	}()
	<-ctx.Done()
	wg.Wait()
}
