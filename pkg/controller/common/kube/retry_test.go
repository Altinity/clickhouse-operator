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
	"k8s.io/apimachinery/pkg/util/wait"
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
		// A permanent (non-timeout) net error must NOT be retried.
		{"dns not-found (permanent)", &net.DNSError{Err: "no such host", IsNotFound: true}, false},
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
			if got := IsTransientAPIError(tc.err); got != tc.want {
				t.Fatalf("IsTransientAPIError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestGetWithRetry(t *testing.T) {
	// Shrink the back-off (and disable jitter for determinism) so the test runs fast.
	orig := transientGetRetryBackoff
	transientGetRetryBackoff = wait.Backoff{
		Duration: time.Millisecond,
		Factor:   2.0,
		Cap:      4 * time.Millisecond,
		Steps:    transientGetRetryMaxAttempts - 1,
	}
	defer func() { transientGetRetryBackoff = orig }()

	t.Run("success on first try", func(t *testing.T) {
		calls := 0
		got, err := GetWithRetry(context.Background(), func() (int, error) {
			calls++
			return 42, nil
		})
		if err != nil || got != 42 || calls != 1 {
			t.Fatalf("got=%d err=%v calls=%d, want 42/nil/1", got, err, calls)
		}
	})

	t.Run("retries transient then succeeds", func(t *testing.T) {
		calls := 0
		got, err := GetWithRetry(context.Background(), func() (int, error) {
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
		_, err := GetWithRetry(context.Background(), func() (int, error) {
			calls++
			return 0, apiErrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, "y")
		})
		if !apiErrors.IsNotFound(err) || calls != 1 {
			t.Fatalf("err=%v calls=%d, want NotFound/1", err, calls)
		}
	})

	t.Run("exhausts retries and returns last error", func(t *testing.T) {
		calls := 0
		_, err := GetWithRetry(context.Background(), func() (int, error) {
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
		_, err := GetWithRetry(ctx, func() (int, error) {
			calls++
			return 0, connRefused()
		})
		// First attempt runs; the transient wait is then short-circuited by ctx.Done().
		if err == nil || calls != 1 {
			t.Fatalf("err=%v calls=%d, want non-nil/1", err, calls)
		}
	})
}

// ownerRefPending is the error OwnerReferencesPermissionEnforcement returns right after the
// owner's CRD is created (https://github.com/Altinity/clickhouse-operator/issues/2094).
func ownerRefPending() error {
	return apiErrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, "y", errors.New(
		"cannot set blockOwnerDeletion in this case because cannot find RESTMapping for APIVersion "+
			"clickhouse.altinity.com/v1 Kind ClickHouseInstallation: no matches for kind"))
}

func TestCreateWithRetry(t *testing.T) {
	orig := createRetryBackoff
	createRetryBackoff = wait.Backoff{Duration: time.Millisecond, Factor: 2.0, Cap: 4 * time.Millisecond, Steps: 3}
	defer func() { createRetryBackoff = orig }()
	gr := schema.GroupResource{Resource: "configmaps"}

	tests := []struct {
		name      string
		errs      []error // returned by successive calls; nil once exhausted
		wantCalls int
		wantErr   func(error) bool
	}{
		{"owner-ref admission pending, then accepted", []error{ownerRefPending(), ownerRefPending()}, 3, nil},
		{"transient is not retried", []error{connRefused()}, 1, IsTransientAPIError},
		{"service unavailable is not retried", []error{apiErrors.NewServiceUnavailable("down")}, 1, apiErrors.IsServiceUnavailable},
		{"other forbidden is terminal", []error{apiErrors.NewForbidden(gr, "y", errors.New("RBAC"))}, 1, apiErrors.IsForbidden},
		{"already exists is terminal", []error{apiErrors.NewAlreadyExists(gr, "y")}, 1, apiErrors.IsAlreadyExists},
		{"gives up after Steps+1 attempts", []error{ownerRefPending(), ownerRefPending(), ownerRefPending(), ownerRefPending(), ownerRefPending()}, 4, IsOwnerRefAdmissionPending},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			_, err := CreateWithRetry(context.Background(), func() (int, error) {
				calls++
				if calls <= len(tt.errs) {
					return 0, tt.errs[calls-1]
				}
				return 1, nil
			})
			if calls != tt.wantCalls {
				t.Fatalf("calls=%d, want %d", calls, tt.wantCalls)
			}
			if (tt.wantErr == nil) != (err == nil) || (tt.wantErr != nil && !tt.wantErr(err)) {
				t.Fatalf("unexpected err: %v", err)
			}
		})
	}

	t.Run("keeps retrying after the back-off hits Cap", func(t *testing.T) {
		// wait.Backoff zeroes Steps once Duration passes Cap; retryOn must count attempts itself.
		createRetryBackoff = wait.Backoff{Duration: time.Millisecond, Factor: 2.0, Cap: 2 * time.Millisecond, Steps: 5}
		calls := 0
		_, _ = CreateWithRetry(context.Background(), func() (int, error) {
			calls++
			return 0, ownerRefPending()
		})
		if calls != 6 {
			t.Fatalf("calls=%d, want 6", calls)
		}
	})

	t.Run("cancelled ctx stops retrying", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		_, err := CreateWithRetry(ctx, func() (int, error) {
			calls++
			return 0, ownerRefPending()
		})
		if calls != 1 || !IsOwnerRefAdmissionPending(err) {
			t.Fatalf("calls=%d err=%v, want 1/owner-ref pending", calls, err)
		}
	})
}

func TestIsOwnerRefAdmissionPending(t *testing.T) {
	gr := schema.GroupResource{Resource: "configmaps"}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"owner-ref admission pending", ownerRefPending(), true},
		{"plain forbidden", apiErrors.NewForbidden(gr, "y", errors.New("RBAC")), false},
		{"forbidden mentioning only RESTMapping", apiErrors.NewForbidden(gr, "y", errors.New("webhook: cannot find RESTMapping")), false},
		{"other blockOwnerDeletion rejection", apiErrors.NewForbidden(gr, "y", errors.New(
			"cannot set blockOwnerDeletion if an ownerReference refers to a resource you can't set finalizers on")), false},
		{"not forbidden", apiErrors.NewInternalError(errors.New("cannot set blockOwnerDeletion: cannot find RESTMapping")), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsOwnerRefAdmissionPending(tc.err); got != tc.want {
				t.Fatalf("IsOwnerRefAdmissionPending(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestCreateRetryBackoffBudget pins the production schedule: #2094 needed up to ~25s, and a
// CHK ConfigMap took 6 tries, so the total wait must stay well above that.
func TestCreateRetryBackoffBudget(t *testing.T) {
	b := createRetryBackoff
	b.Jitter = 0
	var total time.Duration
	for i := 0; i < createRetryBackoff.Steps; i++ {
		total += b.Step()
	}
	if total < 60*time.Second {
		t.Fatalf("create retry budget = %s, want >= 60s", total)
	}
}

func TestUpdateWithRetry(t *testing.T) {
	orig := createRetryBackoff
	createRetryBackoff = wait.Backoff{Duration: time.Millisecond, Factor: 2.0, Cap: 4 * time.Millisecond, Steps: 3}
	defer func() { createRetryBackoff = orig }()

	t.Run("owner-ref admission pending, then accepted", func(t *testing.T) {
		calls := 0
		_, err := UpdateWithRetry(context.Background(), func() (int, error) {
			calls++
			if calls < 3 {
				return 0, ownerRefPending()
			}
			return 1, nil
		})
		if err != nil || calls != 3 {
			t.Fatalf("err=%v calls=%d, want nil/3", err, calls)
		}
	})

	t.Run("conflict is not retried", func(t *testing.T) {
		calls := 0
		_, err := UpdateWithRetry(context.Background(), func() (int, error) {
			calls++
			return 0, apiErrors.NewConflict(schema.GroupResource{Resource: "statefulsets"}, "y", errors.New("x"))
		})
		if !apiErrors.IsConflict(err) || calls != 1 {
			t.Fatalf("err=%v calls=%d, want Conflict/1", err, calls)
		}
	})
}
