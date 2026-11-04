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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// TestValidateCreateUsesDryRunRequest exercises the real client-go HTTP call,
// not the fake clientset (which discards CreateOptions in this client-go version).
func TestValidateCreateUsesDryRunRequest(t *testing.T) {
	request := make(chan struct {
		method string
		path string
		dryRun string
	}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request <- struct {
			method string
			path string
			dryRun string
		}{
			method: r.Method,
			path: r.URL.Path,
			dryRun: r.URL.Query().Get("dryRun"),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_ = json.NewEncoder(w).Encode(&meta.Status{
			TypeMeta: meta.TypeMeta{APIVersion: "v1", Kind: "Status"},
			Status: meta.StatusFailure,
			Reason: meta.StatusReasonInvalid,
			Code: http.StatusUnprocessableEntity,
			Message: "StatefulSet is invalid: spec.template.labels: Invalid value: \"/metrics\"",
			Details: &meta.StatusDetails{Group: "apps", Kind: "StatefulSet", Name: "example"},
		})
	}))
	defer server.Close()

	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	require.NoError(t, err)
	sts := NewSTS(client, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = sts.ValidateCreate(ctx, &apps.StatefulSet{ObjectMeta: meta.ObjectMeta{Name: "example", Namespace: "ns"}})
	require.True(t, apiErrors.IsInvalid(err), "expected API-server invalid response, got %v", err)

	select {
	case got := <-request:
		require.Equal(t, http.MethodPost, got.method)
		require.Equal(t, "/apis/apps/v1/namespaces/ns/statefulsets", got.path)
		require.Equal(t, meta.DryRunAll, got.dryRun)
	case <-ctx.Done():
		t.Fatal("no preflight create request observed")
	}
}
