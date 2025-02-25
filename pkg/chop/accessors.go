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
	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	v1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopclientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	"github.com/altinity/clickhouse-operator/pkg/version"
	kube "k8s.io/client-go/kubernetes"
	"os"
)

var chop *CHOp

// New creates new chop instance
// chopClient can be nil, in this case CHOp will not be able to use any ConfigMap(s) with configuration
func New(kubeClient *kube.Clientset, chopClient *chopclientset.Clientset, initCHOpConfigFilePath string) {
	// Create operator instance
	chop = newCHOp(version.Version, version.GitSHA, version.BuiltAt, kubeClient, chopClient, initCHOpConfigFilePath)
	if err := chop.Init(); err != nil {
		log.F().Fatal("Unable to init CHOP instance %v", err)
		os.Exit(1)
	}
	chop.SetupLog()
}

// Get gets global CHOp
func Get() *CHOp {
	return chop
}

// Config gets global CHOp config
func Config() *v1.OperatorConfig {
	return Get().Config()
}

// GetRuntimeParam returns operator runtime parameter by name
func GetRuntimeParam(name string) (string, bool) {
	return Get().GetRuntimeParam(name)
}
