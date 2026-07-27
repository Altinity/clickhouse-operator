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

// Package kube holds Kubernetes-client helpers shared by the CHI and CHK kube
// drivers. It lives under controller/common so both controllers reuse one copy.
package kube

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
	"time"

	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// transientGetRetryMaxAttempts bounds how many times a transient Get is retried
// (including the first try) before the last error is surfaced to the caller.
const transientGetRetryMaxAttempts = 5

// transientGetRetryBackoff is the retry schedule: 500ms base, doubling, capped at
// 5s, with 10% jitter so a fleet of hosts hitting the same apiserver blip does not
// retry in lockstep (thundering herd). Steps = attempts-1 because the first attempt
// is not preceded by a back-off. It is a var (not a const) so tests can shrink it;
// treat it as read-only at runtime — GetWithRetry copies it before calling Step().
var transientGetRetryBackoff = wait.Backoff{
	Duration: 500 * time.Millisecond,
	Factor:   2.0,
	Jitter:   0.1,
	Cap:      5 * time.Second,
	Steps:    transientGetRetryMaxAttempts - 1,
}

// IsTransientAPIError reports whether err is a transient Kubernetes API or network
// error worth retrying, as opposed to a terminal/semantic error (NotFound, Conflict,
// Forbidden, Invalid, ...) that will never succeed on retry.
func IsTransientAPIError(err error) bool {
	if err == nil {
		return false
	}

	// Terminal/semantic API errors - never retry, surface immediately. This runs
	// before any network check so a NotFound (which callers rely on to mean
	// "create it") is never mistaken for a transient failure.
	switch {
	case apiErrors.IsNotFound(err),
		apiErrors.IsAlreadyExists(err),
		apiErrors.IsConflict(err),
		apiErrors.IsForbidden(err),
		apiErrors.IsUnauthorized(err),
		apiErrors.IsBadRequest(err),
		apiErrors.IsInvalid(err),
		apiErrors.IsMethodNotSupported(err):
		return false
	}

	// Context cancellation means the reconcile is being torn down - do not retry.
	if errors.Is(err, context.Canceled) {
		return false
	}

	// Transient API-status errors: apiserver overloaded, restarting, or a 5xx.
	if apiErrors.IsServerTimeout(err) ||
		apiErrors.IsTimeout(err) ||
		apiErrors.IsTooManyRequests(err) ||
		apiErrors.IsInternalError(err) ||
		apiErrors.IsServiceUnavailable(err) ||
		apiErrors.IsUnexpectedServerError(err) {
		return true
	}

	// Transient transport/network errors. These are not k8s API status errors, so
	// apiErrors.Is* does not catch them (this is the connection-refused case).
	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ETIMEDOUT) ||
		errors.Is(err, syscall.EPIPE) {
		return true
	}
	// Any other net.Error only if it is a TIMEOUT. A permanent net error such as a
	// DNS "no such host" (net.DNSError with Timeout()==false) will never succeed on
	// retry, so blanket-retrying every net.Error would just waste the whole back-off
	// budget before failing.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}

	return false
}

// GetWithRetry runs get and, on a transient API/network error, retries it with a
// bounded, jittered exponential back-off. Success or a terminal error returns
// immediately. When retries are exhausted the last error is returned unchanged, so
// callers behave exactly as before for genuine (non-transient or sustained) outages.
//
// ctx must be the caller's real reconcile context (NOT a detached background ctx):
// the back-off waits honor ctx cancellation, so a shutting-down or cancelled
// reconcile stops retrying promptly instead of burning the full back-off budget.
//
// No object label is passed in: client-go errors are self-describing (they embed the
// full request URL, e.g. `Get "https://.../namespaces/x/configmaps/y": ...`), so the
// logged error already identifies the verb, kind, namespace and name.
func GetWithRetry[T any](ctx context.Context, get func() (T, error)) (T, error) {
	var (
		result T
		err    error
	)
	backoff := transientGetRetryBackoff // copy — Step() mutates the receiver
	for attempt := 1; ; attempt++ {
		result, err = get()
		if err == nil || !IsTransientAPIError(err) {
			return result, err
		}
		if attempt >= transientGetRetryMaxAttempts {
			log.V(1).F().Warning("transient API error: giving up after %d attempt(s), err: %v", attempt, err)
			return result, err
		}
		delay := backoff.Step()
		log.V(2).F().Warning("transient API error: retrying (attempt %d/%d) in %s, err: %v", attempt, transientGetRetryMaxAttempts, delay, err)
		if util.WaitContextDoneOrTimeout(ctx, delay) {
			// ctx cancelled/expired during back-off — stop retrying and surface the
			// last error (matches pre-retry behavior: the caller always gets a real error).
			return result, err
		}
	}
}
