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

package controller

import (
	"context"
	"fmt"
	"time"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

const (
	waitStatefulSetGenerationTimeoutBeforeStartBothering = 60
	waitStatefulSetGenerationTimeoutToCreateStatefulSet  = 30
)

// PollerOptions specifies polling options
type PollerOptions struct {
	StartBotheringAfterTimeout time.Duration
	GetErrorTimeout            time.Duration
	Timeout                    time.Duration
	MainInterval               time.Duration
	BackgroundInterval         time.Duration
}

// NewPollerOptions creates new poll options
func NewPollerOptions() *PollerOptions {
	return &PollerOptions{}
}

// Ensure ensures poll options do exist
func (o *PollerOptions) Ensure() *PollerOptions {
	if o == nil {
		return NewPollerOptions()
	}
	return o
}

// FromConfig makes poll options from config
func (o *PollerOptions) FromConfig(config *api.OperatorConfig) *PollerOptions {
	if o == nil {
		return nil
	}
	o.StartBotheringAfterTimeout = time.Duration(waitStatefulSetGenerationTimeoutBeforeStartBothering) * time.Second
	o.GetErrorTimeout = time.Duration(waitStatefulSetGenerationTimeoutToCreateStatefulSet) * time.Second
	o.Timeout = time.Duration(config.Reconcile.StatefulSet.Update.Timeout) * time.Second
	o.MainInterval = time.Duration(config.Reconcile.StatefulSet.Update.PollInterval) * time.Second
	o.BackgroundInterval = 1 * time.Second
	return o
}

// SetCreateTimeout sets create timeout
func (o *PollerOptions) SetGetErrorTimeout(timeout time.Duration) *PollerOptions {
	if o == nil {
		return nil
	}
	o.GetErrorTimeout = timeout
	return o
}

type PollerFunctions struct {
	Get            func(context.Context) (any, error)
	IsDone         func(context.Context, any) bool
	ShouldContinue func(context.Context, any, error) bool
}

func (p *PollerFunctions) CallGet(c context.Context) (any, error) {
	if p == nil {
		return nil, nil
	}
	if p.Get == nil {
		return nil, nil
	}
	return p.Get(c)
}

func (p *PollerFunctions) CallIsDone(c context.Context, a any) bool {
	if p == nil {
		return false
	}
	if p.IsDone == nil {
		return false
	}
	return p.IsDone(c, a)
}

func (p *PollerFunctions) CallShouldContinue(c context.Context, a any, e error) bool {
	if p == nil {
		return false
	}
	if p.ShouldContinue == nil {
		return false
	}
	return p.ShouldContinue(c, a, e)
}

type PollerBackgroundFunctions struct {
	F func(context.Context)
}

func Poll(
	ctx context.Context,
	namespace, name string,
	opts *PollerOptions,
	main *PollerFunctions,
	background *PollerBackgroundFunctions,
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

func sleepAndRunBackgroundProcess(ctx context.Context, opts *PollerOptions, background *PollerBackgroundFunctions) {
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
				if background.F != nil {
					background.F(ctx)
				}
				backgroundIntervalTimeout = time.After(opts.BackgroundInterval)
			}
		}
	default:
		util.WaitContextDoneOrTimeout(ctx, opts.MainInterval)
	}
}
