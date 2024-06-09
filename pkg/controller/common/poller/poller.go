// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
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

package poller

import (
	"context"
	"fmt"
	"time"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func Poll(
	ctx context.Context,
	namespace, name string,
	opts *Options,
	main *Functions,
	background *BackgroundFunctions,
) error {
	opts = opts.Ensure()
	start := time.Now()
	for {
		if util.IsContextDone(ctx) {
			log.V(2).Info("task is done")
			return nil
		}

		item, err := main.CallGet(ctx)
		switch {
		case err == nil:
			// Object is found - process it
			if main.CallIsDone(ctx, item) {
				// All is good, job is done, exit
				log.V(1).M(namespace, name).F().Info("OK %s/%s", namespace, name)
				return nil
			}
			// Object is found, but processor function says we need to continue polling
		case main.CallShouldContinue(ctx, item, err):
			// Object is not found - it either failed to be created or just still not created
			if (opts.GetErrorTimeout > 0) && (time.Since(start) >= opts.GetErrorTimeout) {
				// No more wait for the object to be created. Consider create process as failed.
				log.V(1).M(namespace, name).F().Error("Get() FAILED - item is not available and get timeout reached. Abort")
				return err
			}
			// Object is not found - create timeout is not reached, we need to continue polling
		default:
			// Some kind of total error, abort polling
			log.M(namespace, name).F().Error("%s/%s Get() FAILED", namespace, name)
			return err
		}

		// Continue polling

		// May be time has come to abort polling?
		if time.Since(start) >= opts.Timeout {
			// Timeout reached, no good result available, time to abort
			log.V(1).M(namespace, name).F().Info("poll(%s/%s) - TIMEOUT reached", namespace, name)
			return fmt.Errorf("poll(%s/%s) - wait timeout", namespace, name)
		}

		// Continue polling

		// May be time has come to start bothers into logs?
		if time.Since(start) >= opts.StartBotheringAfterTimeout {
			// Start bothering with log messages after some time only
			log.V(1).M(namespace, name).F().Info("WAIT:%s/%s", namespace, name)
		}

		// Wait some more time and lauch background process(es)
		log.V(2).M(namespace, name).F().P()
		sleepAndRunBackgroundProcess(ctx, opts, background)
	} // for
}

func sleepAndRunBackgroundProcess(ctx context.Context, opts *Options, background *BackgroundFunctions) {
	if ctx == nil {
		ctx = context.Background()
	}
	switch {
	case opts.BackgroundInterval > 0:
		mainIntervalTimeout := time.After(opts.MainInterval)
		backgroundIntervalTimeout := time.After(opts.BackgroundInterval)
		for {
			select {
			case <-ctx.Done():
				// Context is done, nothing to do here more
				return
			case <-mainIntervalTimeout:
				// Timeout reached, nothing to do here more
				return
			case <-backgroundIntervalTimeout:
				// Function interval reached, time to call the func
				if background != nil {
					if background.F != nil {
						background.F(ctx)
					}
				}
				backgroundIntervalTimeout = time.After(opts.BackgroundInterval)
			}
		}
	default:
		util.WaitContextDoneOrTimeout(ctx, opts.MainInterval)
	}
}
