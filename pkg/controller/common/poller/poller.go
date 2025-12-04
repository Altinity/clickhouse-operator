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

type Poller interface {
	Poll() error
	WithOptions(opts *Options) Poller
	WithFunctions(functions *Functions) Poller
}

type poller struct {
	ctx       context.Context
	name      string
	opts      *Options
	functions *Functions
}

func New(ctx context.Context, name string) Poller {
	return &poller{
		ctx:  ctx,
		name: name,
	}
}

func (p *poller) WithOptions(opts *Options) Poller {
	p.opts = opts
	return p
}

func (p *poller) WithFunctions(functions *Functions) Poller {
	p.functions = functions
	return p
}

func (p *poller) preparePoll() {
	p.opts = p.opts.Ensure()
	if p.ctx == nil {
		p.ctx = context.Background()
	}
}

func (p *poller) Poll() error {
	p.preparePoll()
	start := time.Now()
	for {
		if util.IsContextDone(p.ctx) {
			log.V(1).Info("poll is aborted. Host")
			return nil
		}

		item, err := p.functions.CallGet(p.ctx)
		switch {

		// Object is found or getter function is not specified
		case err == nil:
			if p.functions.CallIsDone(p.ctx, item) {
				// All is good, job is done, exit
				log.V(1).M(p.name).F().Info("OK %s", p.name)
				return nil
			}
			// Object is found, but processor function says we should continue polling
			// exit switch

		// Object is not found - it is either failed to be created or just still not created yet
		case p.functions.CallShouldContinue(p.ctx, item, err):
			// Error has happened but we should continue
			if (p.opts.GetErrorTimeout > 0) && (time.Since(start) >= p.opts.GetErrorTimeout) {
				// No more wait for the object to be created. Consider create process as failed.
				log.V(1).M(p.name).F().Error("Poller.Get() FAILED because item is not available and get timeout reached for: %s. Abort", p.name)
				return err
			}
			// Timeout not reached, we should continue
			// exit switch

		// Error has happened and we should not continue, abort polling
		default:
			log.M(p.name).F().Error("Poller.Get() FAILED for: %s", p.name)
			return err
		}

		// Continue polling

		// May be time has come to abort polling?
		if time.Since(start) >= p.opts.Timeout {
			// Timeout reached, no good result available, time to abort
			log.V(1).M(p.name).F().Info("poll(%s) - TIMEOUT reached", p.name)
			return fmt.Errorf("poll(%s) - wait timeout", p.name)
		}

		// Continue polling

		// May be time has come to start bothering with log messages?
		if time.Since(start) >= p.opts.StartBotheringAfterTimeout {
			// Start bothering with log messages after some time only
			log.V(1).M(p.name).F().Info("WAIT: %s", p.name)
		}

		// Wait some more time and launch background process(es)
		log.V(2).M(p.name).F().Info("poll iteration")
		p.sleepAndRunBackgroundProcess()
	} // for
}

func (p *poller) sleepAndRunBackgroundProcess() {
	switch {
	case p.opts.BackgroundInterval > 0:
		mainIntervalTimeout := time.After(p.opts.MainInterval)
		backgroundIntervalTimeout := time.After(p.opts.BackgroundInterval)
		for {
			select {
			case <-p.ctx.Done():
				// Context is done, nothing to do here more
				return

			case <-mainIntervalTimeout:
				// Timeout reached, nothing to do here more
				return

			case <-backgroundIntervalTimeout:
				// Function interval reached, time to call the func
				p.functions.CallBackground(p.ctx)
				// Reload timeout
				backgroundIntervalTimeout = time.After(p.opts.BackgroundInterval)
				// continue for loop
			}
		}
	default:
		util.WaitContextDoneOrTimeout(p.ctx, p.opts.MainInterval)
	}
}
