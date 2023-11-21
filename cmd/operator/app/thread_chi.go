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
	"time"

	kubeinformers "k8s.io/client-go/informers"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	chopinformers "github.com/altinity/clickhouse-operator/pkg/client/informers/externalversions"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi"
)

// Prometheus exporter defaults
const (
	defaultInformerFactoryResyncPeriod      = 60 * time.Second
	defaultInformerFactoryResyncDebugPeriod = 60 * time.Second
)

// CLI parameter variables
var (
	// Setting to 0 disables resync
	// Informer fires Update() func to periodically verify current state
	kubeInformerFactoryResyncPeriod = defaultInformerFactoryResyncPeriod
	chopInformerFactoryResyncPeriod = defaultInformerFactoryResyncPeriod
)

func init() {
}

var chiController *chi.Controller

// initClickHouse is an entry point of the application
func initClickHouse(ctx context.Context) {
	log.S().P()
	defer log.E().P()

	if debugRequest {
		kubeInformerFactoryResyncPeriod = defaultInformerFactoryResyncDebugPeriod
		chopInformerFactoryResyncPeriod = defaultInformerFactoryResyncDebugPeriod
	}

	// Initialize k8s API clients
	kubeClient, extClient, chopClient := chop.GetClientset(kubeConfigFile, masterURL)

	// Create operator instance
	chop.New(kubeClient, chopClient, chopConfigFile)
	log.V(1).F().Info("Config parsed:")
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
	chiController = chi.NewController(
		chopClient,
		extClient,
		kubeClient,
		chopInformerFactory,
		kubeInformerFactory,
	)

	// Start Informers
	kubeInformerFactory.Start(ctx.Done())
	chopInformerFactory.Start(ctx.Done())
}

// runClickHouse is an entry point of the application
func runClickHouse(ctx context.Context) {
	log.S().P()
	defer log.E().P()

	// Start main CHI controller
	log.V(1).F().Info("Starting CHI controller")
	chiController.Run(ctx)
}
