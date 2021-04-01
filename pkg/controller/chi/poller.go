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

package chi

import (
	"context"
	"errors"
	"fmt"
	"time"

	apps "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

const (
	waitStatefulSetGenerationTimeoutBeforeStartBothering = 60
	waitStatefulSetGenerationTimeoutToCreateStatefulSet  = 30
)

// waitHostNotReady polls host's StatefulSet for not exists or not ready
func (c *Controller) waitHostNotReady(ctx context.Context, host *chop.ChiHost) error {
	err := c.pollStatefulSet(ctx, host, NewStatefulSetPollOptionsConfigNoCreate(c.chop.Config()), model.IsStatefulSetNotReady, nil)
	if apierrors.IsNotFound(err) {
		err = nil
	}

	return err
}

// waitHostReady polls host's StatefulSet until it is ready
func (c *Controller) waitHostReady(ctx context.Context, host *chop.ChiHost) error {
	// Wait for StatefulSet to reach generation
	err := c.pollStatefulSet(
		ctx,
		host.StatefulSet,
		nil,
		func(sts *apps.StatefulSet) bool {
			if sts == nil {
				return false
			}
			_ = c.deleteLabelReady(ctx, host)
			return model.IsStatefulSetGeneration(sts, sts.Generation)
		},
		func() {
			_ = c.deleteLabelReady(ctx, host)
		},
	)
	if err != nil {
		return err
	}

	// Wait StatefulSet to reach ready status
	return c.pollStatefulSet(
		ctx,
		host.StatefulSet,
		nil,
		func(sts *apps.StatefulSet) bool {
			_ = c.deleteLabelReady(ctx, host)
			return model.IsStatefulSetReady(sts)
		},
		func() {
			_ = c.deleteLabelReady(ctx, host)
		},
	)
}

// waitHostDeleted polls host's StatefulSet until it is not available
func (c *Controller) waitHostDeleted(host *chop.ChiHost) {
	for {
		// TODO
		// Probably there would be better way to wait until k8s reported StatefulSet deleted
		if _, err := c.getStatefulSet(host); err == nil {
			log.V(2).Info("cache NOT yet synced")
			time.Sleep(15 * time.Second)
		} else {
			log.V(1).Info("cache synced")
			return
		}
	}
}

// waitHostRunning polls host for `Running` state
func (c *Controller) waitHostRunning(host *chop.ChiHost) error {
	namespace := host.Address.Namespace
	name := host.Address.HostName
	// Wait for some limited time for StatefulSet to reach target generation
	// Wait timeout is specified in c.chopConfig.StatefulSetUpdateTimeout in seconds
	start := time.Now()
	for {
		if c.isHostRunning(host) {
			// All is good, job done, exit
			log.V(1).M(host).F().Info("%s/%s-OK", namespace, name)
			return nil
		}

		// Object is found, function not positive
		if time.Since(start) >= (time.Duration(waitStatefulSetGenerationTimeoutBeforeStartBothering) * time.Second) {
			// Start bothering with log messages after some time only
			log.V(1).M(host).F().Info("%s/%s-WAIT", namespace, name)
		}

		if time.Since(start) >= (time.Duration(c.chop.Config().StatefulSetUpdateTimeout) * time.Second) {
			// Timeout reached, no good result available, time to quit
			log.V(1).M(host).F().Error("%s/%s-TIMEOUT reached", namespace, name)
			return errors.New(fmt.Sprintf("waitHostRunning(%s/%s) - wait timeout", namespace, name))
		}

		// Wait some more time
		log.V(2).M(host).F().Info("%s/%s", namespace, name)
		select {
		case <-time.After(time.Duration(c.chop.Config().StatefulSetUpdatePollPeriod) * time.Second):
		}
	}

	return fmt.Errorf("unexpected flow")
}

type StatefulSetPollOptions struct {
	StartBotheringAfterTimeout time.Duration
	CreateTimeout              time.Duration
	Timeout                    time.Duration
	MainInterval               time.Duration
	BackgroundInterval         time.Duration
}

func NewStatefulSetPollOptions() *StatefulSetPollOptions {
	return &StatefulSetPollOptions{}
}

func NewStatefulSetPollOptionsConfig(config *chop.OperatorConfig) *StatefulSetPollOptions {
	return &StatefulSetPollOptions{
		StartBotheringAfterTimeout: time.Duration(waitStatefulSetGenerationTimeoutBeforeStartBothering) * time.Second,
		CreateTimeout:              time.Duration(waitStatefulSetGenerationTimeoutToCreateStatefulSet) * time.Second,
		Timeout:                    time.Duration(config.StatefulSetUpdateTimeout) * time.Second,
		MainInterval:               time.Duration(config.StatefulSetUpdatePollPeriod) * time.Second,
		BackgroundInterval:         1 * time.Second,
	}
}

func NewStatefulSetPollOptionsConfigNoCreate(config *chop.OperatorConfig) *StatefulSetPollOptions {
	return &StatefulSetPollOptions{
		StartBotheringAfterTimeout: time.Duration(waitStatefulSetGenerationTimeoutBeforeStartBothering) * time.Second,
		//CreateTimeout:              time.Duration(waitStatefulSetGenerationTimeoutToCreateStatefulSet) * time.Second,
		Timeout:            time.Duration(config.StatefulSetUpdateTimeout) * time.Second,
		MainInterval:       time.Duration(config.StatefulSetUpdatePollPeriod) * time.Second,
		BackgroundInterval: 1 * time.Second,
	}
}

// pollStatefulSet polls StatefulSet with poll callback function.
func (c *Controller) pollStatefulSet(
	ctx context.Context,
	entity interface{},
	opts *StatefulSetPollOptions,
	mainFn func(set *apps.StatefulSet) bool,
	backFn func(),
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}
	if opts == nil {
		opts = NewStatefulSetPollOptionsConfig(c.chop.Config())
	}
	namespace := ""
	name := ""

	switch entity.(type) {
	case *apps.StatefulSet:
		sts := entity.(*apps.StatefulSet)
		namespace = sts.Namespace
		name = sts.Name
	case *chop.ChiHost:
		h := entity.(*chop.ChiHost)
		namespace = h.Address.Namespace
		name = h.Address.StatefulSet
	}

	// Wait for some limited time for StatefulSet to reach target generation
	// Wait timeout is specified in c.chopConfig.StatefulSetUpdateTimeout in seconds
	start := time.Now()
	for {
		if util.IsContextDone(ctx) {
			log.V(2).Info("ctx is done")
			return nil
		}

		if statefulSet, err := c.kubeClient.AppsV1().StatefulSets(namespace).Get(ctx, name, newGetOptions()); err == nil {
			// Object is found
			if mainFn(statefulSet) {
				// All is good, job done, exit
				log.V(1).M(namespace, name).F().Info("OK  :%s", model.StrStatefulSetStatus(&statefulSet.Status))
				return nil
			}

			// Object is found, but function is not positive
			if time.Since(start) >= opts.StartBotheringAfterTimeout {
				// Start bothering with log messages after some time only
				log.V(1).M(namespace, name).F().Info("WAIT:%s", model.StrStatefulSetStatus(&statefulSet.Status))
			}
		} else if apierrors.IsNotFound(err) {
			// Object is not found - it either failed to be created or just still not created
			if time.Since(start) >= opts.CreateTimeout {
				// No more wait for object to be created. Consider create as failed.
				if opts.CreateTimeout > 0 {
					log.V(1).M(namespace, name).F().Error("Get() FAILED - StatefulSet still not found, abort")
				} else {
					log.V(1).M(namespace, name).F().Info("Get() NEUTRAL StatefulSet not found and no wait required")
				}
				return err
			}
			// Object with such name not found - may be is still being created - wait for it
			log.V(1).M(namespace, name).F().Info("WAIT: object not found. Not created yet?")
		} else {
			// Some kind of total error
			log.M(namespace, name).A().Error("%s/%s Get() FAILED", namespace, name)
			return err
		}

		// StatefulSet is either not created or generation is not yet reached

		if time.Since(start) >= opts.Timeout {
			// Timeout reached, no good result available, time to quit
			log.V(1).M(namespace, name).F().Info("%s/%s - TIMEOUT reached")
			return errors.New(fmt.Sprintf("waitStatefulSet(%s/%s) - wait timeout", namespace, name))
		}

		// Wait some more time
		log.V(2).M(namespace, name).F().P()
		pollback(ctx, opts, backFn)
	}

	return fmt.Errorf("unexpected flow")
}

func pollback(ctx context.Context, opts *StatefulSetPollOptions, fn func()) {
	if ctx == nil {
		ctx = context.Background()
	}
	mainIntervalTimeout := time.After(opts.MainInterval)
	run := true
	for run {
		backgroundIntervalTimeout := time.After(opts.BackgroundInterval)
		select {
		case <-ctx.Done():
			// Context is done, nothing to do here more
			run = false
		case <-mainIntervalTimeout:
			// Timeout reached, nothing to do here more
			run = false
		case <-backgroundIntervalTimeout:
			// Function interval reached, time to call the func
			if fn != nil {
				fn()
			}
		}
	}
}

// pollHost polls host with poll callback function.
func (c *Controller) pollHostContext(
	ctx context.Context,
	host *chop.ChiHost,
	opts *StatefulSetPollOptions,
	f func(ctx context.Context, host *chop.ChiHost) bool,
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	if opts == nil {
		opts = NewStatefulSetPollOptionsConfig(c.chop.Config())
	}
	namespace := host.Address.Namespace
	name := host.Address.HostName

	// Wait timeout is specified in c.chopConfig.StatefulSetUpdateTimeout in seconds
	start := time.Now()
	for {
		if util.IsContextDone(ctx) {
			log.V(2).Info("ctx is done")
			return nil
		}

		if f(ctx, host) {
			// All is good, job done, exit
			log.V(1).M(host).F().Info("%s/%s-OK", namespace, name)
			return nil
		}

		if util.IsContextDone(ctx) {
			log.V(2).Info("ctx is done")
			return nil
		}

		// Object is found, but function is not positive
		if time.Since(start) >= opts.StartBotheringAfterTimeout {
			// Start bothering with log messages after some time only
			log.V(1).M(host).F().Info("%s/%s-WAIT", namespace, name)
		}

		if time.Since(start) >= opts.Timeout {
			// Timeout reached, no good result available, time to quit
			log.V(1).M(host).F().Error("%s/%s-TIMEOUT reached", namespace, name)
			return errors.New(fmt.Sprintf("pollHost(%s/%s) - wait timeout", namespace, name))
		}

		// Wait some more time
		log.V(2).M(host).F().Info("%s/%s", namespace, name)
		util.WaitContextDoneOrTimeout(ctx, opts.MainInterval)
	}

	return fmt.Errorf("unexpected flow")
}