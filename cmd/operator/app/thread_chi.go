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
	kubeClient, extClient, chopClient, dynamicClient := chop.GetClientset(kubeConfigFile, masterURL)

	// Create operator instance. The chopconf load inside chop.New gates on
	// clickhouse.security.kubernetes.allowInsecure BEFORE the first network call,
	// so an insecure kubeconfig forbidden by chopconf will Fatal here rather than
	// after the first List has already crossed the wire.
	chop.New(kubeClient, chopClient, chopConfigFile)
	log.V(1).F().Info("Config parsed:")
	log.Info("\n" + chop.Config().String(true))

	// Validate the runtime FIPS posture against chopconf security.policy.
	// Fatals when chopconf requires FIPS but the binary/runtime can't deliver.
	fipsGate()

	// Provision the operator↔exporter IPC token. No-op in Plain mode (default).
	// In Secure mode this writes a fresh random token to the shared-volume path
	// before the metrics-exporter sidecar polls for it.
	if err := chop.ProvisionIPCToken(); err != nil {
		log.F().Fatal("IPC token provisioning failed: %s", err.Error())
	}
	if chop.Config().RestartOnOperatorConfigurationChange() {
		log.Info("Auto-restart on ClickHouseOperatorConfiguration change is enabled")
	} else {
		log.Info("Auto-restart on ClickHouseOperatorConfiguration change is disabled")
	}

	// Log namespace deny list configuration
	if chop.Config().Watch.Namespaces.Exclude.Len() > 0 {
		log.Info("Namespace deny list configured: %v - these namespaces will NOT be reconciled", chop.Config().Watch.Namespaces.Exclude.Value())
	} else {
		log.V(1).Info("No namespace deny list configured - all watched namespaces will be reconciled")
	}

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
	chopConfigInformerFactory := chopinformers.NewSharedInformerFactoryWithOptions(
		chopClient,
		chopInformerFactoryResyncPeriod,
		chopinformers.WithNamespace(chop.Config().Runtime.Namespace),
	)

	// Create Controller
	chiController = chi.NewController(
		chopClient,
		extClient,
		kubeClient,
		dynamicClient,
		chopConfigInformerFactory,
		chopInformerFactory,
		kubeInformerFactory,
	)

	// Start CHK watcher (if enabled by config)
	chiController.StartCHKWatcher(ctx)

	// Start Informers
	kubeInformerFactory.Start(ctx.Done())
	chopInformerFactory.Start(ctx.Done())
	chopConfigInformerFactory.Start(ctx.Done())
}

// runClickHouse is an entry point of the application
func runClickHouse(ctx context.Context) {
	log.S().P()
	defer log.E().P()

	// Start main CHI controller
	log.V(1).F().Info("Starting CHI controller")
	chiController.Run(ctx)
}
