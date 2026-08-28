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

package zookeeper

import (
	"context"
	"testing"

	"github.com/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// TestPathManagerEnsure covers what Ensure promises: the path is present when it returns
// nil, and a genuine failure to make it present is reported rather than swallowed.
//
// Precreating the root is a convenience - ClickHouse creates it on first DDL - so callers
// treat the error as non-fatal. That makes false positives the expensive direction: a
// reported failure for a path that is actually present sends a healthy cluster down an
// error path for nothing.
func TestPathManagerEnsure(t *testing.T) {
	const path = "/clickhouse/task_queue/ddl"
	components := []string{"/clickhouse", "/clickhouse/task_queue", "/clickhouse/task_queue/ddl"}

	tests := []struct {
		name      string
		setupMock func(*MockZKClient)
		wantErr   bool
	}{
		{
			name: "success: every component created",
			setupMock: func(m *MockZKClient) {
				for _, c := range components {
					m.On("Exists", c).Return(false, (*zk.Stat)(nil), nil).Once()
					m.On("Create", c, mock.Anything, mock.Anything, mock.Anything).Return(c, nil).Once()
				}
			},
		},
		{
			name: "success: created concurrently between Exists and Create",
			setupMock: func(m *MockZKClient) {
				for _, c := range components {
					m.On("Exists", c).Return(false, (*zk.Stat)(nil), nil).Once()
					m.On("Create", c, mock.Anything, mock.Anything, mock.Anything).
						Return("", zk.ErrNodeExists).Once()
				}
			},
		},
		{
			// Exists failing is not itself a failure to ensure: the Create that follows
			// still puts the component in place. Reporting an error here would abort a
			// reconcile over a path that is present.
			name: "success: Exists errored but Create succeeded",
			setupMock: func(m *MockZKClient) {
				for _, c := range components {
					m.On("Exists", c).Return(false, (*zk.Stat)(nil), zk.ErrAPIError).Once()
					m.On("Create", c, mock.Anything, mock.Anything, mock.Anything).Return(c, nil).Once()
				}
			},
		},
		{
			// The client reports exists=true for most errors, so an errored Exists must
			// not short-circuit the component - Create still has to run.
			name: "success: Exists reported true alongside an error",
			setupMock: func(m *MockZKClient) {
				for _, c := range components {
					m.On("Exists", c).Return(true, (*zk.Stat)(nil), zk.ErrNoAuth).Once()
					m.On("Create", c, mock.Anything, mock.Anything, mock.Anything).Return(c, nil).Once()
				}
			},
		},
		{
			name: "failure: ensemble unreachable, Create exhausts its retries",
			setupMock: func(m *MockZKClient) {
				m.On("Exists", mock.Anything).Return(false, (*zk.Stat)(nil), zk.ErrAPIError)
				m.On("Create", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return("", zk.ErrAPIError)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockZKClient)
			tt.setupMock(mockClient)
			conn := newTestConnection(api.ZookeeperNodes{}, mockClient, &ConnectionParams{MaxRetriesNum: 1})

			err := NewPathManager(conn).Ensure(context.Background(), path)

			if tt.wantErr {
				assert.ErrorIs(t, err, zk.ErrAPIError)
			} else {
				assert.NoError(t, err)
				mockClient.AssertExpectations(t)
			}
		})
	}
}

// TestPathManagerEnsureStopsOnCancelledContext pins that a superseded reconcile stops the
// walk instead of spending the retry budget - roughly 8 minutes per ZK operation, two per
// component - dialing an ensemble nobody is waiting on any more.
func TestPathManagerEnsureStopsOnCancelledContext(t *testing.T) {
	mockClient := new(MockZKClient)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	conn := newTestConnection(api.ZookeeperNodes{}, mockClient, &ConnectionParams{MaxRetriesNum: 30})
	err := NewPathManager(conn).Ensure(ctx, "/clickhouse/task_queue/ddl")

	assert.ErrorIs(t, err, context.Canceled)
	mockClient.AssertNotCalled(t, "Create", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}
