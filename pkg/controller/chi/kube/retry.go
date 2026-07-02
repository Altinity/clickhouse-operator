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

package kube

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
	"time"

	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
)

// transientGetRetryMaxAttempts bounds how many times a transient Get is retried
// (including the first try) before the last error is surfaced to the caller.
const transientGetRetryMaxAttempts = 5

// Back-off schedule between retries. These are vars (not consts) so tests can shrink
// them; treat them as read-only at runtime.
var (
	// transientGetRetryBaseDelay is the first back-off pause; it doubles each attempt.
	transientGetRetryBaseDelay = 500 * time.Millisecond
	// transientGetRetryMaxDelay caps the exponential back-off between attempts.
	transientGetRetryMaxDelay = 5 * time.Second
)

// isTransientAPIError reports whether err is a transient Kubernetes API or network
// error worth retrying, as opposed to a terminal/semantic error (NotFound, Conflict,
// Forbidden, Invalid, ...) that will never succeed on retry.
func isTransientAPIError(err error) bool {
	if err == nil {
		return false
	}

	// Terminal/semantic API errors - never retry, surface immediately.
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
	// apiErrors.Is* does not catch them (this is the connection-refused case above).
	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ETIMEDOUT) ||
		errors.Is(err, syscall.EPIPE) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	return false
}

// getWithRetry runs get and, on a transient API/network error, retries it with a
// bounded exponential back-off. Success or a terminal error returns immediately.
// When retries are exhausted the last error is returned unchanged, so callers behave
// exactly as before for genuine (non-transient or sustained) outages.
//
// No object label is passed in: client-go errors are self-describing (they embed the
// full request URL, e.g. `Get "https://.../namespaces/x/configmaps/y": ...`), so the
// logged error already identifies the verb, kind, namespace and name.
func getWithRetry[T any](ctx context.Context, get func() (T, error)) (T, error) {
	var (
		result T
		err    error
	)
	delay := transientGetRetryBaseDelay
	for attempt := 1; ; attempt++ {
		result, err = get()
		if err == nil || !isTransientAPIError(err) {
			return result, err
		}
		if attempt >= transientGetRetryMaxAttempts {
			log.V(1).F().Warning("transient API error: giving up after %d attempt(s), err: %v", attempt, err)
			return result, err
		}
		log.V(2).F().Warning("transient API error: retrying (attempt %d/%d) in %s, err: %v", attempt, transientGetRetryMaxAttempts, delay, err)
		select {
		case <-ctx.Done():
			return result, err
		case <-time.After(delay):
		}
		if delay *= 2; delay > transientGetRetryMaxDelay {
			delay = transientGetRetryMaxDelay
		}
	}
}
