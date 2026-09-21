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
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/util"
	"github.com/altinity/clickhouse-operator/pkg/util/fips"
	"github.com/altinity/clickhouse-operator/pkg/version"
)

// CLI parameter variables
var (
	// versionRequest defines request for clickhouse-operator version report. Operator should exit after version printed
	versionRequest bool

	// fipsInfoRequest dumps the binary's FIPS build + runtime posture
	// (GOFIPS140, DefaultGODEBUG, fips140.Enabled/Enforced/Version, env
	// GODEBUG, Go version, OS/arch) and exits. Designed for offline audit
	// and e2e verification without a local `go version -m` toolchain.
	fipsInfoRequest bool

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
	flag.BoolVar(&fipsInfoRequest, "fips-info", false, "Display FIPS build/runtime info and exit (no Go toolchain required).")
	flag.BoolVar(&debugRequest, "debug", false, "Debug run")
	flag.StringVar(&chopConfigFile, "config", "", "Path to clickhouse-operator config file.")
	flag.StringVar(&masterURL, "master", "", "The address of custom Kubernetes API server. Makes sense if runs outside of the cluster and not being specified in kube config file only.")
}

// Run is an entry point of the application
func Run() {
	// ACVP responder trampoline: if argv[0] is *-acvp and the binary was
	// built with -tags acvp_wrapper, hand control to the ACVP stdin/stdout
	// responder before any operator-side initialization (flag.Parse, k8s
	// client construction, signal handlers, goroutine launches). In default
	// builds this is a no-op stub. See acvp_dispatch_{on,off}.go.
	if TryACVPDispatch() {
		return // unreachable: TryACVPDispatch calls os.Exit on dispatch
	}

	flag.Parse()

	if versionRequest {
		fmt.Printf("%s\n", version.Version)
		os.Exit(0)
	}

	if fipsInfoRequest {
		fips.PrintInfo(os.Stdout, "clickhouse-operator", version.Version, version.GitSHA, version.BuiltAt)
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

// keeperInitRetryBackoff paces the retry of initKeeper. Narrowing the Keeper caches by label makes
// controller-runtime resolve a REST mapping for every narrowed type while the manager is built, so
// initKeeper now reaches the API server where it previously did not: its failures are no longer
// only deterministic programming faults, they include whatever the API server is doing at boot.
//
// Retried indefinitely, and deliberately so. The two alternatives are both worse:
//
//   - Giving up after a fixed budget leaves Keeper reconciliation dead for the lifetime of the
//     process because initKeeper runs only at start-up, so an outage a minute longer than the
//     budget costs an operator restart that nothing asks for.
//   - Exiting the process to force that restart is worse still: a permanently failing environment
//     then crash-loops the pod, and since the container dies faster than the kubelet's back-off
//     resets, ClickHouse ends up down most of the time. Keeper is dead either way in that
//     scenario; taking ClickHouse down with it buys nothing.
//
// So the cost of failure stays proportional: ClickHouse keeps reconciling, and Keeper starts as
// soon as the API server lets it. The cap bounds the log noise of an environment that never will.
var keeperInitRetryBackoff = wait.Backoff{
	Duration: time.Second,
	Factor:   2.0,
	Jitter:   0.1,
	Cap:      5 * time.Minute,
	// Steps only bounds the doubling; Step() keeps returning the cap afterwards, which is what
	// makes the retry unbounded in time while staying bounded in frequency.
	Steps: 9,
}

// initKeeperWithRetry calls init until it succeeds, hits a failure that retrying cannot clear, or
// ctx is done. A nil return means Keeper is initialized; any other outcome leaves it disabled for
// the lifetime of the process, which the caller reports.
func initKeeperWithRetry(ctx context.Context, init func(context.Context) error) error {
	backoff := keeperInitRetryBackoff // copy - Step() mutates the receiver

	for {
		// Each attempt rebuilds the manager and reassigns the package-level manager, scheme and
		// logger. A failed one abandons a manager that was built but never started, so there is
		// nothing to unwind; its HTTP transport is client-go's globally cached one, which the next
		// attempt gets back rather than leaking, and ctrl.SetLogger is a no-op after the first
		// call. The retry is not a clean slate; it does not need to be.
		err := init(ctx)
		if err == nil {
			return nil
		}
		// Retry by default, and stop only on what initKeeper itself marks as deterministic.
		// Deliberately NOT IsTransientAPIError: that classifier serves reconcile-time reads, where
		// NotFound legitimately means "create it", and it reports Forbidden and a missing REST
		// mapping as terminal. Here that is worth waiting out - apps/v1 briefly absent from
		// discovery resolves on its own. Classifying the failure where it is raised leaves
		// nothing to guess.
		if errors.Is(err, errKeeperInitTerminal) {
			return err
		}

		delay := backoff.Step()
		log.Warning("init keeper FAILED, retrying in %s, err: %v", delay, err)
		if util.WaitContextDoneOrTimeout(ctx, delay) {
			return err
		}
	}
}

func launchKeeper(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		// In the goroutine rather than before it: initKeeper retries until the API server lets it
		// through, and start-up must not block on that. The ClickHouse controller is already
		// running by this point and stays independent of whatever happens here.
		if err := initKeeperWithRetry(ctx, newKeeperInitializer()); err != nil {
			if util.IsContextDone(ctx) {
				log.Warning("Starting keeper ABORTED - shutting down, err: %v", err)
				return
			}
			// Not Fatal: see keeperInitRetryBackoff. The operator keeps reconciling ClickHouse,
			// and this log is the only signal that Keeper is not being reconciled - the shipped
			// Deployment defines no readinessProbe, so the pod stays Ready regardless.
			log.Warning("Starting keeper FAILED - keeper reconciliation is DISABLED for the "+
				"lifetime of this process, err: %v", err)
			return
		}

		log.Info("Starting keeper")
		// Warning, not Fatal - and an absent ClickHouseKeeperInstallation CRD fails HERE rather
		// than in init: initKeeper's only API access is REST-mapping discovery, which lists the
		// cluster's API groups and then resolves apps/v1 - all built-in - so it succeeds on a
		// cluster that has never seen the Keeper CRD. The missing CRD surfaces when the manager
		// starts its watches, which is the normal state of a ClickHouse-only installation.
		if err := runKeeper(ctx); err != nil {
			log.Warning("Starting keeper FAILED with err: %v", err)
			return
		}
		log.Info("Starting keeper OK")
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
