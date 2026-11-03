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
	"syscall"

	log "github.com/golang/glog"
	// log "k8s.io/klog"

	"k8s.io/client-go/tools/cache"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	chopclientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	chopinformers "github.com/altinity/clickhouse-operator/pkg/client/informers/externalversions"
	"github.com/altinity/clickhouse-operator/pkg/metrics/clickhouse"
	"github.com/altinity/clickhouse-operator/pkg/util/fips"
	"github.com/altinity/clickhouse-operator/pkg/version"
)

// Prometheus exporter defaults
const (
	defaultMetricsEndpoint = ":8888"
	defaultChiListEndPoint = ":8888"

	metricsPath = "/metrics"
	chiListPath = "/chi"
)

// CLI parameter variables
var (
	// versionRequest defines request for clickhouse-operator version report. Operator should exit after version printed
	versionRequest bool

	// fipsInfoRequest dumps the binary's FIPS build + runtime posture
	// (GOFIPS140, DefaultGODEBUG, fips140.Enabled/Enforced/Version, env
	// GODEBUG, Go version, OS/arch) and exits. Mirror of the operator-side
	// flag — same diagnostic available from the metrics-exporter container
	// without a local `go version -m` toolchain.
	fipsInfoRequest bool

	// chopConfigFile defines path to clickhouse-operator config file to be used
	chopConfigFile string

	// kubeConfigFile defines path to kube config file to be used
	kubeConfigFile string

	// masterURL defines URL of kubernetes master to be used
	masterURL string

	// metricsEP defines metrics end-point IP address
	metricsEP string

	chiListEP string
)

func init() {
	flag.BoolVar(&versionRequest, "version", false, "Display clickhouse-operator version and exit")
	flag.BoolVar(&fipsInfoRequest, "fips-info", false, "Display FIPS build/runtime info and exit (no Go toolchain required).")
	flag.StringVar(&chopConfigFile, "config", "", "Path to clickhouse-operator config file.")
	flag.StringVar(&kubeConfigFile, "kubeconfig", "", "Path to custom kubernetes config file. Makes sense if runs outside of the cluster only.")
	flag.StringVar(&masterURL, "master", "", "The address of custom Kubernetes API server. Makes sense if runs outside of the cluster and not being specified in kube config file only.")
	flag.StringVar(&metricsEP, "metrics-endpoint", defaultMetricsEndpoint, "The Prometheus exporter endpoint.")
	flag.StringVar(&chiListEP, "chi-list-endpoint", defaultChiListEndPoint, "The CHI list endpoint.")
	flag.Parse()
}

// Run is an entry point of the application
func Run() {
	// ACVP responder trampoline: if argv[0] is *-acvp (e.g.
	// metrics-exporter-acvp) and the binary was built with -tags acvp_wrapper,
	// hand control to the ACVP stdin/stdout responder before any exporter-side
	// initialization (chop.Config, k8s client construction, Prometheus HTTP
	// endpoint, CHI discovery). In default builds this is a no-op stub that
	// keeps the responder package out of the production binary. Mirror of the
	// operator-side trampoline in cmd/operator/app/main.go. See
	// acvp_dispatch_{on,off}.go. Note: flag.Parse runs in this package's
	// init() and has already completed by the time Run() executes, but the
	// ACVP responder ignores flags and only reads argv[0] / stdin, so the
	// ordering is harmless.
	if TryACVPDispatch() {
		return // unreachable: TryACVPDispatch calls os.Exit on dispatch
	}

	if versionRequest {
		fmt.Printf("%s\n", version.Version)
		os.Exit(0)
	}

	if fipsInfoRequest {
		fips.PrintInfo(os.Stdout, "metrics-exporter", version.Version, version.GitSHA, version.BuiltAt)
		os.Exit(0)
	}

	// Set OS signals and termination context
	ctx, cancelFunc := context.WithCancel(context.Background())
	stopChan := make(chan os.Signal, 2)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stopChan
		cancelFunc()
		<-stopChan
		os.Exit(1)
	}()

	log.Infof("Starting metrics exporter. Version:%s GitSHA:%s BuiltAt:%s\n", version.Version, version.GitSHA, version.BuiltAt)

	// Initialize k8s API clients
	kubeClient, _, chopClient, _ := chop.GetClientset(kubeConfigFile, masterURL, chopConfigFile)

	// Create operator instance
	chop.New(kubeClient, chopClient, chopConfigFile)
	log.Info(chop.Config().String(true))

	// Validate the runtime FIPS posture against chopconf security.policy.
	// Same gate the operator binary runs; both share the chopconf singleton via
	// the chop package, and the exporter ships in the same Pod / same image, so
	// build-mode parity is guaranteed but the runtime mismatch (e.g. missing
	// GODEBUG on the exporter container) is caught here.
	fipsGate()

	exporter := clickhouse.StartMetricsREST(
		metricsEP,
		metricsPath,
		chop.Config().ClickHouse.Metrics.Timeouts.Collect,

		chiListEP,
		chiListPath,
	)

	exporter.DiscoveryWatchedCHIs(kubeClient, chopClient)

	// Watch ClickHouseOperatorConfiguration and restart this container on a
	// genuine change, mirroring the operator. The operator reacts to chopconf
	// changes by exiting its process (controller.restartOperatorOnConfigChange),
	// but that restarts ONLY the operator container — this exporter sibling in
	// the same Pod would otherwise keep serving its stale startup FIPS/TLS
	// posture until the whole Pod is recreated. Gated by the same
	// watch.configuration.onChange=restart switch as the operator.
	startChopConfigRestartWatcher(ctx, chopClient)

	<-ctx.Done()
}

// startChopConfigRestartWatcher wires a ClickHouseOperatorConfiguration informer
// that exits the exporter process on a genuine config change so Kubernetes
// restarts the container with the new merged config. It mirrors the operator's
// chopconf handlers (pkg/controller/chi/controller.go addEventHandlersChopConfig
// + addChopConfig/updateChopConfig) so both containers react identically.
// deletedChopConfig decodes an informer delete payload, unwrapping a client-go
// DeletedFinalStateUnknown tombstone - delivered when a watch delete is missed and
// a relist finds the object gone. Returns nil for anything that does not decode:
// this handler exits the process, so an unrecognized payload must not take the
// container down with it. Mirrors deleteHandler in pkg/controller/chi/controller.go,
// duplicated rather than shared because importing package chi would pull the whole
// operator controller into the exporter binary.
func deletedChopConfig(obj interface{}) *api.ClickHouseOperatorConfiguration {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	// A tombstone can carry a typed nil, which satisfies the assertion - check it.
	cfg, ok := obj.(*api.ClickHouseOperatorConfiguration)
	if !ok || (cfg == nil) {
		log.Errorf("chopConfigInformer.DeleteFunc: unexpected object type %T", obj)
		return nil
	}
	return cfg
}

func startChopConfigRestartWatcher(ctx context.Context, chopClient *chopclientset.Clientset) {
	factory := chopinformers.NewSharedInformerFactoryWithOptions(
		chopClient,
		0, // no resync; the ResourceVersion guard below also covers any resync
		chopinformers.WithNamespace(chop.Config().Runtime.Namespace),
	)
	factory.Clickhouse().V1().ClickHouseOperatorConfigurations().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			cfg, ok := obj.(*api.ClickHouseOperatorConfiguration)
			// Skip the initial list-sync replay of the config already loaded at
			// startup (same ns+name+ResourceVersion is in ConfigManager's list),
			// so the exporter does not exit on boot. Mirrors operator addChopConfig.
			if !ok || chop.Get().ConfigManager.IsConfigListed(cfg) {
				return
			}
			chop.RestartOnConfigChange("ClickHouseOperatorConfiguration added")
		},
		UpdateFunc: func(old, new interface{}) {
			o, ok1 := old.(*api.ClickHouseOperatorConfiguration)
			n, ok2 := new.(*api.ClickHouseOperatorConfiguration)
			// Skip resync no-ops. Mirrors operator updateChopConfig.
			if !ok1 || !ok2 || o.GetResourceVersion() == n.GetResourceVersion() {
				return
			}
			chop.RestartOnConfigChange("ClickHouseOperatorConfiguration updated")
		},
		DeleteFunc: func(obj interface{}) {
			cfg := deletedChopConfig(obj)
			if cfg == nil {
				return
			}
			chop.RestartOnConfigChange("ClickHouseOperatorConfiguration deleted")
		},
	})
	factory.Start(ctx.Done())
}
