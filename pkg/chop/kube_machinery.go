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

package chop

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"

	log "github.com/golang/glog"
	// log "k8s.io/klog"

	chopclientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	"github.com/altinity/clickhouse-operator/pkg/version"

	kube "k8s.io/client-go/kubernetes"
	kuberest "k8s.io/client-go/rest"
	kubeclientcmd "k8s.io/client-go/tools/clientcmd"
)

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

// GetClientset gets k8s API clients - both kube native client and our custom client
func GetClientset(kubeConfigFile, masterURL string) (*kube.Clientset, *chopclientset.Clientset) {
	kubeConfig, err := getKubeConfig(kubeConfigFile, masterURL)
	if err != nil {
		log.Fatalf("Unable to build kubeconf: %s", err.Error())
		os.Exit(1)
	}

	kubeClientset, err := kube.NewForConfig(kubeConfig)
	if err != nil {
		log.Fatalf("Unable to initialize kubernetes API clientset: %s", err.Error())
	}

	chopClientset, err := chopclientset.NewForConfig(kubeConfig)
	if err != nil {
		log.Fatalf("Unable to initialize clickhouse-operator API clientset: %s", err.Error())
	}

	return kubeClientset, chopClientset
}

// chopClient can be nil, in this case CHOp will not be able to use any ConfigMap(s) with configuration
func GetCHOp(chopClient *chopclientset.Clientset, initCHOpConfigFilePath string) *CHOp {
	// Create operator instance
	chop := NewCHOp(version.Version, chopClient, initCHOpConfigFilePath)
	if err := chop.Init(); err != nil {
		log.Fatalf("Unable to init CHOP instance %v\n", err)
		os.Exit(1)
	}

	return chop
}
