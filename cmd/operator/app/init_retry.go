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
	"time"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/cache"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// initRetryBackoff paces the retry of a controller-runtime manager initialization, Keeper's and
// backup's alike. Narrowing a manager's caches by label makes controller-runtime resolve a REST
// mapping for every narrowed type while the manager is built, so the initialization reaches the
// API server: its failures are not only deterministic programming faults, they include whatever
// the API server is doing at boot.
//
// Retried indefinitely, and deliberately so. The two alternatives are both worse:
//
//   - Giving up after a fixed budget leaves the controllers dead for the lifetime of the process
//     because the initialization runs only at start-up, so an outage a minute longer than the
//     budget costs an operator restart that nothing asks for.
//   - Exiting the process to force that restart is worse still: a permanently failing environment
//     then crash-loops the pod, and since the container dies faster than the kubelet's back-off
//     resets, ClickHouse ends up down most of the time. The failing controllers are dead either
//     way in that scenario; taking ClickHouse down with them buys nothing.
//
// So the cost of failure stays proportional: ClickHouse keeps reconciling, and the failing
// controllers start as soon as the API server lets them. The cap bounds the log noise of an
// environment that never will.
var initRetryBackoff = wait.Backoff{
	Duration: time.Second,
	Factor:   2.0,
	Jitter:   0.1,
	Cap:      5 * time.Minute,
	// Steps only bounds the doubling; Step() keeps returning the cap afterwards, which is what
	// makes the retry unbounded in time while staying bounded in frequency.
	Steps: 9,
}

// errInitTerminal marks an initialization failure that no retry can clear, as opposed to one that
// merely reflects the state of the API server at this instant. An initializer attaches it to its
// own deterministic failures so the retry never has to infer intent from an error's shape.
var errInitTerminal = errors.New("init cannot succeed on retry")

// initWithRetry calls init until it succeeds, hits a failure that retrying cannot clear, or ctx is
// done. A nil return means the component is initialized; any other outcome leaves it disabled for
// the lifetime of the process, which the caller reports.
func initWithRetry(ctx context.Context, component string, init func(context.Context) error) error {
	backoff := initRetryBackoff // copy - Step() mutates the receiver

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
		// Retry by default, and stop only on what the initializer itself marks as deterministic.
		// Deliberately NOT IsTransientAPIError: that classifier serves reconcile-time reads, where
		// NotFound legitimately means "create it", and it reports Forbidden and a missing REST
		// mapping as terminal. Here that is worth waiting out - apps/v1 briefly absent from
		// discovery resolves on its own. Classifying the failure where it is raised leaves
		// nothing to guess. A shutdown, which an initializer that waits reports as its error,
		// ends the retry without announcing one.
		if errors.Is(err, errInitTerminal) || util.IsContextDone(ctx) {
			return err
		}

		delay := backoff.Step()
		log.Warning("init %s FAILED, retrying in %s, err: %v", component, delay, err)
		if util.WaitContextDoneOrTimeout(ctx, delay) {
			return err
		}
	}
}

// newCacheNamespaces scopes a manager cache to the watched namespaces.
//
// GetCacheNamespaces returns exact namespace names when all configured watch namespaces are valid
// DNS labels, enabling per-namespace cache scoping. It falls back to NamespaceAll when the include
// list is unset or any watch namespace is a regexp pattern (the controllers' Reconcile guard
// handles filtering in that case). ByObject entries that leave Namespaces nil inherit this scoping.
//
// NamespaceAll alone yields nil, so the cache is a single cluster-wide one. A lone "" key would make
// controller-runtime build a multi-namespace cache whose List rejects every concrete namespace.
func newCacheNamespaces(cacheNamespaces []string) map[string]cache.Config {
	if len(cacheNamespaces) == 1 && cacheNamespaces[0] == meta.NamespaceAll {
		return nil
	}
	defaultNamespaces := make(map[string]cache.Config)
	for _, ns := range cacheNamespaces {
		defaultNamespaces[ns] = cache.Config{}
	}
	return defaultNamespaces
}
