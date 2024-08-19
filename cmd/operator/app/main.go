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

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/version"
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
)

func init() {
	flag.BoolVar(&versionRequest, "version", false, "Display clickhouse-operator version and exit")
	flag.BoolVar(&debugRequest, "debug", false, "Debug run")
	flag.StringVar(&chopConfigFile, "config", "", "Path to clickhouse-operator config file.")
	flag.StringVar(&masterURL, "master", "", "The address of custom Kubernetes API server. Makes sense if runs outside of the cluster and not being specified in kube config file only.")
}

// Run is an entry point of the application
func Run() {
	flag.Parse()

	if versionRequest {
		fmt.Printf("%s\n", version.Version)
		os.Exit(0)
	}

	log.S().P()
	defer log.E().P()

	log.F().Info("Starting clickhouse-operator. Version:%s GitSHA:%s BuiltAt:%s", version.Version, version.GitSHA, version.BuiltAt)

	// Create main context with cancel
	ctx, cancelFunc := context.WithCancel(context.Background())

	// Setup notification signals with cancel
	setupSignalsNotification(cancelFunc)

	var wg sync.WaitGroup

	launchClickHouse(ctx, &wg)
	launchClickHouseReconcilerMetricsExporter(ctx, &wg)
	launchKeeper(ctx, &wg)

	// Wait for completion
	<-ctx.Done()
	wg.Wait()
}

func launchClickHouse(ctx context.Context, wg *sync.WaitGroup) {
	initClickHouse(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		runClickHouse(ctx)
	}()
}

func launchClickHouseReconcilerMetricsExporter(ctx context.Context, wg *sync.WaitGroup) {
	initClickHouseReconcilerMetricsExporter(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		runClickHouseReconcilerMetricsExporter(ctx)
	}()
}

func launchKeeper(ctx context.Context, wg *sync.WaitGroup) {
	keeperErr := initKeeper(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if keeperErr == nil {
			log.Info("Starting keeper")
			keeperErr = runKeeper(ctx)
			if keeperErr == nil {
				log.Info("Starting keeper OK")
			} else {
				log.Warning("Starting keeper FAILED with err: %v", keeperErr)
			}
		} else {
			log.Warning("Starting keeper skipped due to failed initialization with err: %v", keeperErr)
		}
	}()
}

// setupSignalsNotification sets up OS signals
func setupSignalsNotification(cancel context.CancelFunc) {
	stopChan := make(chan os.Signal, 2)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stopChan
		cancel()
		<-stopChan
		os.Exit(1)
	}()
}
