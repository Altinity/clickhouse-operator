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
	"net/url"
	"os"
	"syscall"
	"testing"
	"time"

	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// connRefused reproduces the error client-go returns on an apiserver blip:
// `Get "https://10.x.x.x:443/...": dial tcp 10.x.x.x:443: connect: connection refused`.
func connRefused() error {
	return &url.Error{
		Op:  "Get",
		URL: "https://10.0.0.1:443/api/v1/namespaces/x/configmaps/y",
		Err: &net.OpError{
			Op:  "dial",
			Net: "tcp",
			Err: os.NewSyscallError("connect", syscall.ECONNREFUSED),
		},
	}
}

func TestIsTransientAPIError(t *testing.T) {
	gr := schema.GroupResource{Resource: "configmaps"}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		// Not transient - terminal/semantic, or no error.
		{"nil", nil, false},
		{"not found", apiErrors.NewNotFound(gr, "y"), false},
		{"conflict", apiErrors.NewConflict(gr, "y", errors.New("x")), false},
		{"forbidden", apiErrors.NewForbidden(gr, "y", errors.New("x")), false},
		{"invalid", apiErrors.NewInvalid(schema.GroupKind{Kind: "ConfigMap"}, "y", nil), false},
		{"context canceled", context.Canceled, false},
		// Transient - network / control-plane blips worth retrying.
		{"connection refused", connRefused(), true},
		{"eof", io.EOF, true},
		{"unexpected eof", io.ErrUnexpectedEOF, true},
		{"too many requests", apiErrors.NewTooManyRequests("busy", 1), true},
		{"service unavailable", apiErrors.NewServiceUnavailable("down"), true},
		{"internal error", apiErrors.NewInternalError(errors.New("boom")), true},
		{"server timeout", apiErrors.NewServerTimeout(gr, "get", 1), true},
		{"dns timeout", &net.DNSError{Err: "timeout", IsTimeout: true}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isTransientAPIError(tc.err); got != tc.want {
				t.Fatalf("isTransientAPIError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestGetWithRetry(t *testing.T) {
	// Shrink the back-off so the test runs fast.
	origBase, origMax := transientGetRetryBaseDelay, transientGetRetryMaxDelay
	transientGetRetryBaseDelay, transientGetRetryMaxDelay = time.Millisecond, 4*time.Millisecond
	defer func() { transientGetRetryBaseDelay, transientGetRetryMaxDelay = origBase, origMax }()

	t.Run("success on first try", func(t *testing.T) {
		calls := 0
		got, err := getWithRetry(context.Background(), func() (int, error) {
			calls++
			return 42, nil
		})
		if err != nil || got != 42 || calls != 1 {
			t.Fatalf("got=%d err=%v calls=%d, want 42/nil/1", got, err, calls)
		}
	})

	t.Run("retries transient then succeeds", func(t *testing.T) {
		calls := 0
		got, err := getWithRetry(context.Background(), func() (int, error) {
			calls++
			if calls < 3 {
				return 0, connRefused()
			}
			return 7, nil
		})
		if err != nil || got != 7 || calls != 3 {
			t.Fatalf("got=%d err=%v calls=%d, want 7/nil/3", got, err, calls)
		}
	})

	t.Run("terminal error returns immediately without retry", func(t *testing.T) {
		calls := 0
		_, err := getWithRetry(context.Background(), func() (int, error) {
			calls++
			return 0, apiErrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, "y")
		})
		if !apiErrors.IsNotFound(err) || calls != 1 {
			t.Fatalf("err=%v calls=%d, want NotFound/1", err, calls)
		}
	})

	t.Run("exhausts retries and returns last error", func(t *testing.T) {
		calls := 0
		_, err := getWithRetry(context.Background(), func() (int, error) {
			calls++
			return 0, connRefused()
		})
		if err == nil || calls != transientGetRetryMaxAttempts {
			t.Fatalf("err=%v calls=%d, want non-nil/%d", err, calls, transientGetRetryMaxAttempts)
		}
	})

	t.Run("stops on context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		_, err := getWithRetry(ctx, func() (int, error) {
			calls++
			return 0, connRefused()
		})
		// First attempt runs; the transient wait is then short-circuited by ctx.Done().
		if err == nil || calls != 1 {
			t.Fatalf("err=%v calls=%d, want non-nil/1", err, calls)
		}
	})
}
