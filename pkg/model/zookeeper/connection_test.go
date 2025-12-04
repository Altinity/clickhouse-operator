package zookeeper

import (
	"context"
	"testing"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestConnection_Get(t *testing.T) {
	// Sample stat for successful responses
	sampleStat := &zk.Stat{
		Czxid:          123,
		Mzxid:          124,
		Ctime:          1234567890,
		Mtime:          1234567891,
		Version:        1,
		Cversion:       0,
		Aversion:       0,
		EphemeralOwner: 0,
		DataLength:     4,
		NumChildren:    0,
		Pzxid:          125,
	}

	tests := []struct {
		name           string
		path           string
		setupMock      func(*MockZKClient)
		expectedData   []byte
		expectedStat   *zk.Stat
		expectedErrors []error
		retries        int
	}{
		{
			name: "success: successful get operation",
			path: "/test/path",
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Get", "/test/path").Return([]byte("test data"), sampleStat, nil).Once()
			},
			expectedData: []byte("test data"),
			expectedStat: sampleStat,
			retries:      1, // 1 means a single attempt
		},
		{
			name: "success: retry scenario - 2 errors then success",
			path: "/test/retry-path",
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Get", "/test/retry-path").Return([]byte(nil), (*zk.Stat)(nil), zk.ErrAPIError).Once()
				mockClient.On("Get", "/test/retry-path").Return([]byte(nil), (*zk.Stat)(nil), zk.ErrNoAuth).Once()
				mockClient.On("Get", "/test/retry-path").Return([]byte("retry success"), sampleStat, nil).Once()
			},
			expectedData: []byte("retry success"),
			expectedStat: sampleStat,
			retries:      3,
		},
		{
			name: "error: persistent connection error, no retries",
			path: "/nonexistent/path",
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Get", "/nonexistent/path").Return([]byte(nil), (*zk.Stat)(nil), zk.ErrNoNode).Once()
			},
			expectedData:   nil,
			expectedStat:   nil,
			expectedErrors: []error{zk.ErrNoNode},
			retries:        1,
		},

		{
			name: "error: persistent connection error, with retries",
			path: "/nonexistent/path",
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Get", "/nonexistent/path").Return([]byte(nil), (*zk.Stat)(nil), zk.ErrNoNode).Once()
				mockClient.On("Get", "/nonexistent/path").Return([]byte(nil), (*zk.Stat)(nil), zk.ErrAPIError).Once()
				mockClient.On("Get", "/nonexistent/path").Return([]byte(nil), (*zk.Stat)(nil), zk.ErrNoAuth).Once()
			},
			expectedData:   nil,
			expectedStat:   nil,
			expectedErrors: []error{zk.ErrNoNode, zk.ErrAPIError, zk.ErrNoAuth},
			retries:        3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock client
			mockClient := new(MockZKClient)

			// Setup mock expectations
			tt.setupMock(mockClient)

			// Create connection with mock client
			conn := newTestConnection(api.ZookeeperNodes{}, mockClient, &ConnectionParams{MaxRetriesNum: tt.retries})

			// Call the method under test
			ctx := context.Background()
			data, stat, err := conn.Get(ctx, tt.path)

			// Assert expectations
			if len(tt.expectedErrors) > 0 {
				assert.Error(t, err)
				// Verify that all expected errors are present in the aggregated error
				for _, expectedError := range tt.expectedErrors {
					assert.ErrorIs(t, err, expectedError, "Expected error %v should be present in aggregated error", expectedError)
				}
				assert.Nil(t, data)
				assert.Nil(t, stat)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedData, data)
				assert.Equal(t, tt.expectedStat, stat)
			}

			// Verify all mock expectations were met
			mockClient.AssertExpectations(t)
		})
	}
}

func TestConnection_Exists(t *testing.T) {
	// Sample stat for successful responses
	sampleStat := &zk.Stat{
		Czxid:          123,
		Mzxid:          124,
		Ctime:          1234567890,
		Mtime:          1234567891,
		Version:        1,
		Cversion:       0,
		Aversion:       0,
		EphemeralOwner: 0,
		DataLength:     4,
		NumChildren:    0,
		Pzxid:          125,
	}

	tests := []struct {
		name           string
		path           string
		setupMock      func(*MockZKClient)
		expectedExists bool
		expectedErrors []error
		retries        int
	}{
		{
			name: "success: node exists",
			path: "/test/path",
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Exists", "/test/path").Return(true, sampleStat, nil).Once()
			},
			expectedExists: true,
			retries:        1,
		},
		{
			name: "success: node does not exist",
			path: "/nonexistent/path",
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Exists", "/nonexistent/path").Return(false, (*zk.Stat)(nil), nil).Once()
			},
			expectedExists: false,
			retries:        1,
		},
		{
			name: "success: retry scenario - 2 errors then success",
			path: "/test/retry-path",
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Exists", "/test/retry-path").Return(false, (*zk.Stat)(nil), zk.ErrAPIError).Once()
				mockClient.On("Exists", "/test/retry-path").Return(false, (*zk.Stat)(nil), zk.ErrNoAuth).Once()
				mockClient.On("Exists", "/test/retry-path").Return(true, sampleStat, nil).Once()
			},
			expectedExists: true,
			retries:        3,
		},
		{
			name: "error: persistent connection error, no retries",
			path: "/test/path",
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Exists", "/test/path").Return(false, (*zk.Stat)(nil), zk.ErrConnectionClosed).Once()
			},
			expectedExists: false,
			expectedErrors: []error{zk.ErrConnectionClosed},
			retries:        1,
		},
		{
			name: "error: all retries fail with different errors",
			path: "/test/path",
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Exists", "/test/path").Return(false, (*zk.Stat)(nil), zk.ErrAPIError).Once()
				mockClient.On("Exists", "/test/path").Return(false, (*zk.Stat)(nil), zk.ErrNoAuth).Once()
				mockClient.On("Exists", "/test/path").Return(false, (*zk.Stat)(nil), zk.ErrBadVersion).Once()
			},
			expectedExists: false,
			expectedErrors: []error{zk.ErrAPIError, zk.ErrNoAuth, zk.ErrBadVersion},
			retries:        3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock client
			mockClient := new(MockZKClient)

			// Setup mock expectations
			tt.setupMock(mockClient)

			// Create connection with mock client
			conn := newTestConnection(api.ZookeeperNodes{}, mockClient, &ConnectionParams{MaxRetriesNum: tt.retries})

			// Call the method under test
			ctx := context.Background()
			exists, err := conn.Exists(ctx, tt.path)

			// Assert expectations
			if len(tt.expectedErrors) > 0 {
				assert.Error(t, err)
				// Verify that all expected errors are present in the aggregated error
				for _, expectedError := range tt.expectedErrors {
					assert.ErrorIs(t, err, expectedError, "Expected error %v should be present in aggregated error", expectedError)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedExists, exists)
			}

			// Verify all mock expectations were met
			mockClient.AssertExpectations(t)
		})
	}
}

func TestConnection_Details(t *testing.T) {
	// Sample stat for successful responses
	sampleStat := &zk.Stat{
		Czxid:          123,
		Mzxid:          124,
		Ctime:          1234567890,
		Mtime:          1234567891,
		Version:        1,
		Cversion:       0,
		Aversion:       0,
		EphemeralOwner: 0,
		DataLength:     4,
		NumChildren:    0,
		Pzxid:          125,
	}

	tests := []struct {
		name           string
		path           string
		setupMock      func(*MockZKClient)
		expectedExists bool
		expectedStat   *zk.Stat
		expectedErrors []error
		retries        int
	}{
		{
			name: "success: node exists with stat",
			path: "/test/path",
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Exists", "/test/path").Return(true, sampleStat, nil).Once()
			},
			expectedExists: true,
			expectedStat:   sampleStat,
			retries:        1,
		},
		{
			name: "success: node does not exist",
			path: "/nonexistent/path",
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Exists", "/nonexistent/path").Return(false, (*zk.Stat)(nil), nil).Once()
			},
			expectedExists: false,
			expectedStat:   nil,
			retries:        1,
		},
		{
			name: "success: retry scenario - 2 errors then success",
			path: "/test/retry-path",
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Exists", "/test/retry-path").Return(false, (*zk.Stat)(nil), zk.ErrAPIError).Once()
				mockClient.On("Exists", "/test/retry-path").Return(false, (*zk.Stat)(nil), zk.ErrNoAuth).Once()
				mockClient.On("Exists", "/test/retry-path").Return(true, sampleStat, nil).Once()
			},
			expectedExists: true,
			expectedStat:   sampleStat,
			retries:        3,
		},
		{
			name: "error: persistent error, no retries",
			path: "/test/path",
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Exists", "/test/path").Return(false, (*zk.Stat)(nil), zk.ErrConnectionClosed).Once()
			},
			expectedExists: false,
			expectedStat:   nil,
			expectedErrors: []error{zk.ErrConnectionClosed},
			retries:        1,
		},
		{
			name: "error: all retries fail with different errors",
			path: "/test/path",
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Exists", "/test/path").Return(false, (*zk.Stat)(nil), zk.ErrAPIError).Once()
				mockClient.On("Exists", "/test/path").Return(false, (*zk.Stat)(nil), zk.ErrNoAuth).Once()
				mockClient.On("Exists", "/test/path").Return(false, (*zk.Stat)(nil), zk.ErrBadVersion).Once()
			},
			expectedExists: false,
			expectedStat:   nil,
			expectedErrors: []error{zk.ErrAPIError, zk.ErrNoAuth, zk.ErrBadVersion},
			retries:        3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock client
			mockClient := new(MockZKClient)

			// Setup mock expectations
			tt.setupMock(mockClient)

			// Create connection with mock client
			conn := newTestConnection(api.ZookeeperNodes{}, mockClient, &ConnectionParams{MaxRetriesNum: tt.retries})

			// Call the method under test
			ctx := context.Background()
			exists, stat, err := conn.Details(ctx, tt.path)

			// Assert expectations
			if len(tt.expectedErrors) > 0 {
				assert.Error(t, err)
				// Verify that all expected errors are present in the aggregated error
				for _, expectedError := range tt.expectedErrors {
					assert.ErrorIs(t, err, expectedError, "Expected error %v should be present in aggregated error", expectedError)
				}
				assert.False(t, exists)
				assert.Nil(t, stat)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedExists, exists)
				assert.Equal(t, tt.expectedStat, stat)
			}

			// Verify all mock expectations were met
			mockClient.AssertExpectations(t)
		})
	}
}

func TestConnection_Create(t *testing.T) {
	tests := []struct {
		name           string
		path           string
		value          []byte
		flags          int32
		acl            []zk.ACL
		setupMock      func(*MockZKClient)
		expectedPath   string
		expectedErrors []error
		retries        int
	}{
		{
			name:  "success: create node",
			path:  "/test/path",
			value: []byte("test data"),
			flags: 0,
			acl:   zk.WorldACL(zk.PermAll),
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Create", "/test/path", []byte("test data"), int32(0), zk.WorldACL(zk.PermAll)).Return("/test/path", nil).Once()
			},
			expectedPath: "/test/path",
			retries:      1,
		},
		{
			name:  "success: retry scenario - 2 errors then success",
			path:  "/test/retry-path",
			value: []byte("retry data"),
			flags: 0,
			acl:   zk.WorldACL(zk.PermAll),
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Create", "/test/retry-path", []byte("retry data"), int32(0), zk.WorldACL(zk.PermAll)).Return("", zk.ErrAPIError).Once()
				mockClient.On("Create", "/test/retry-path", []byte("retry data"), int32(0), zk.WorldACL(zk.PermAll)).Return("", zk.ErrNoAuth).Once()
				mockClient.On("Create", "/test/retry-path", []byte("retry data"), int32(0), zk.WorldACL(zk.PermAll)).Return("/test/retry-path", nil).Once()
			},
			expectedPath: "/test/retry-path",
			retries:      3,
		},
		{
			name:  "error: persistent error",
			path:  "/test/error",
			value: []byte("data"),
			flags: 0,
			acl:   zk.WorldACL(zk.PermAll),
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Create", "/test/error", []byte("data"), int32(0), zk.WorldACL(zk.PermAll)).Return("", zk.ErrNoNode).Once()
			},
			expectedPath:   "",
			expectedErrors: []error{zk.ErrNoNode},
			retries:        1,
		},
		{
			name:  "error: all retries fail with different errors",
			path:  "/test/error-path",
			value: []byte("data"),
			flags: 0,
			acl:   zk.WorldACL(zk.PermAll),
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Create", "/test/error-path", []byte("data"), int32(0), zk.WorldACL(zk.PermAll)).Return("", zk.ErrNodeExists).Once()
				mockClient.On("Create", "/test/error-path", []byte("data"), int32(0), zk.WorldACL(zk.PermAll)).Return("", zk.ErrAPIError).Once()
				mockClient.On("Create", "/test/error-path", []byte("data"), int32(0), zk.WorldACL(zk.PermAll)).Return("", zk.ErrNoAuth).Once()
			},
			expectedPath:   "",
			expectedErrors: []error{zk.ErrNodeExists, zk.ErrAPIError, zk.ErrNoAuth},
			retries:        3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock client
			mockClient := new(MockZKClient)

			// Setup mock expectations
			tt.setupMock(mockClient)

			// Create connection with mock client
			conn := newTestConnection(api.ZookeeperNodes{}, mockClient, &ConnectionParams{MaxRetriesNum: tt.retries})

			// Call the method under test
			ctx := context.Background()
			pathCreated, err := conn.Create(ctx, tt.path, tt.value, tt.flags, tt.acl)

			// Assert expectations
			if len(tt.expectedErrors) > 0 {
				assert.Error(t, err)
				// Verify that all expected errors are present in the aggregated error
				for _, expectedError := range tt.expectedErrors {
					assert.ErrorIs(t, err, expectedError, "Expected error %v should be present in aggregated error", expectedError)
				}
				assert.Empty(t, pathCreated)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedPath, pathCreated)
			}

			// Verify all mock expectations were met
			mockClient.AssertExpectations(t)
		})
	}
}

func TestConnection_Set(t *testing.T) {
	// Sample stat for successful responses
	sampleStat := &zk.Stat{
		Czxid:          123,
		Mzxid:          124,
		Ctime:          1234567890,
		Mtime:          1234567891,
		Version:        2,
		Cversion:       0,
		Aversion:       0,
		EphemeralOwner: 0,
		DataLength:     9,
		NumChildren:    0,
		Pzxid:          125,
	}

	tests := []struct {
		name           string
		path           string
		value          []byte
		version        int32
		setupMock      func(*MockZKClient)
		expectedStat   *zk.Stat
		expectedErrors []error
		retries        int
	}{
		{
			name:    "success: set data",
			path:    "/test/path",
			value:   []byte("new data"),
			version: 1,
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Set", "/test/path", []byte("new data"), int32(1)).Return(sampleStat, nil).Once()
			},
			expectedStat: sampleStat,
			retries:      1,
		},
		{
			name:    "success: retry scenario - 2 errors then success",
			path:    "/test/retry-path",
			value:   []byte("retry data"),
			version: 1,
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Set", "/test/retry-path", []byte("retry data"), int32(1)).Return((*zk.Stat)(nil), zk.ErrAPIError).Once()
				mockClient.On("Set", "/test/retry-path", []byte("retry data"), int32(1)).Return((*zk.Stat)(nil), zk.ErrNoAuth).Once()
				mockClient.On("Set", "/test/retry-path", []byte("retry data"), int32(1)).Return(sampleStat, nil).Once()
			},
			expectedStat: sampleStat,
			retries:      3,
		},
		{
			name:    "error: node not found",
			path:    "/nonexistent/path",
			value:   []byte("data"),
			version: -1,
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Set", "/nonexistent/path", []byte("data"), int32(-1)).Return((*zk.Stat)(nil), zk.ErrNoNode).Once()
			},
			expectedStat:   nil,
			expectedErrors: []error{zk.ErrNoNode},
			retries:        1,
		},
		{
			name:    "error: persistent connection error",
			path:    "/test/path",
			value:   []byte("data"),
			version: 1,
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Set", "/test/path", []byte("data"), int32(1)).Return((*zk.Stat)(nil), zk.ErrConnectionClosed).Once()
			},
			expectedStat:   nil,
			expectedErrors: []error{zk.ErrConnectionClosed},
			retries:        1,
		},
		{
			name:    "error: all retries fail with different errors",
			path:    "/test/error-path",
			value:   []byte("data"),
			version: 1,
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Set", "/test/error-path", []byte("data"), int32(1)).Return((*zk.Stat)(nil), zk.ErrBadVersion).Once()
				mockClient.On("Set", "/test/error-path", []byte("data"), int32(1)).Return((*zk.Stat)(nil), zk.ErrNoNode).Once()
				mockClient.On("Set", "/test/error-path", []byte("data"), int32(1)).Return((*zk.Stat)(nil), zk.ErrAPIError).Once()
			},
			expectedStat:   nil,
			expectedErrors: []error{zk.ErrBadVersion, zk.ErrNoNode, zk.ErrAPIError},
			retries:        3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock client
			mockClient := new(MockZKClient)

			// Setup mock expectations
			tt.setupMock(mockClient)

			// Create connection with mock client
			conn := newTestConnection(api.ZookeeperNodes{}, mockClient, &ConnectionParams{MaxRetriesNum: tt.retries})

			// Call the method under test
			ctx := context.Background()
			stat, err := conn.Set(ctx, tt.path, tt.value, tt.version)

			// Assert expectations
			if len(tt.expectedErrors) > 0 {
				assert.Error(t, err)
				// Verify that all expected errors are present in the aggregated error
				for _, expectedError := range tt.expectedErrors {
					assert.ErrorIs(t, err, expectedError, "Expected error %v should be present in aggregated error", expectedError)
				}
				assert.Nil(t, stat)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedStat, stat)
			}

			// Verify all mock expectations were met
			mockClient.AssertExpectations(t)
		})
	}
}

func TestConnection_Delete(t *testing.T) {
	tests := []struct {
		name           string
		path           string
		version        int32
		setupMock      func(*MockZKClient)
		expectedErrors []error
		retries        int
	}{
		{
			name:    "success: delete",
			path:    "/test/path",
			version: 1,
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Delete", "/test/path", int32(1)).Return(nil).Once()
			},
			retries: 1,
		},
		{
			name:    "success: retry scenario - 2 errors then success",
			path:    "/test/retry-path",
			version: 1,
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Delete", "/test/retry-path", int32(1)).Return(zk.ErrAPIError).Once()
				mockClient.On("Delete", "/test/retry-path", int32(1)).Return(zk.ErrNoAuth).Once()
				mockClient.On("Delete", "/test/retry-path", int32(1)).Return(nil).Once()
			},
			retries: 3,
		},
		{
			name:    "error: persistent connection error",
			path:    "/test/path",
			version: 1,
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Delete", "/test/path", int32(1)).Return(zk.ErrConnectionClosed).Once()
			},
			expectedErrors: []error{zk.ErrConnectionClosed},
			retries:        1,
		},
		{
			name:    "error: all retries fail with different errors",
			path:    "/test/error-path",
			version: 1,
			setupMock: func(mockClient *MockZKClient) {
				mockClient.On("Delete", "/test/error-path", int32(1)).Return(zk.ErrNotEmpty).Once()
				mockClient.On("Delete", "/test/error-path", int32(1)).Return(zk.ErrBadVersion).Once()
				mockClient.On("Delete", "/test/error-path", int32(1)).Return(zk.ErrAPIError).Once()
			},
			expectedErrors: []error{zk.ErrNotEmpty, zk.ErrBadVersion, zk.ErrAPIError},
			retries:        3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock client
			mockClient := new(MockZKClient)

			// Setup mock expectations
			tt.setupMock(mockClient)

			// Create connection with mock client
			conn := newTestConnection(api.ZookeeperNodes{}, mockClient, &ConnectionParams{MaxRetriesNum: tt.retries})

			// Call the method under test
			ctx := context.Background()
			err := conn.Delete(ctx, tt.path, tt.version)

			// Assert expectations
			if len(tt.expectedErrors) > 0 {
				assert.Error(t, err)
				// Verify that all expected errors are present in the aggregated error
				for _, expectedError := range tt.expectedErrors {
					assert.ErrorIs(t, err, expectedError, "Expected error %v should be present in aggregated error", expectedError)
				}
			} else {
				assert.NoError(t, err)
			}

			// Verify all mock expectations were met
			mockClient.AssertExpectations(t)
		})
	}
}

func TestConnection_Close(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*MockZKClient) *Connection
	}{
		{
			name: "success: close connection",
			setup: func(mockClient *MockZKClient) *Connection {
				mockClient.On("Close").Return().Once()
				return newTestConnection(api.ZookeeperNodes{}, mockClient, &ConnectionParams{MaxRetriesNum: 1})
			},
		},
		{
			name: "success: handle empty connection",
			setup: func(mockClient *MockZKClient) *Connection {
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock client
			mockClient := new(MockZKClient)

			// Setup mock expectations
			conn := tt.setup(mockClient)

			// Call the method under test
			err := conn.Close()

			// Assert expectations - Close should always succeed
			assert.NoError(t, err)

			// Verify all mock expectations were met
			mockClient.AssertExpectations(t)
		})
	}
}

// newTestConnection creates a Connection with a custom ZKClient for testing
func newTestConnection(nodes api.ZookeeperNodes, client ZKClient, _params ...*ConnectionParams) *Connection {
	conn := NewConnection(nodes, _params...)
	conn.connection = client
	conn.retryDelayFn = func(i int) {} // Disable retry delay for tests
	return conn
}

// MockZKClient is a mock implementation of the ZKClient interface for testing
type MockZKClient struct {
	mock.Mock
}

func (m *MockZKClient) Get(path string) ([]byte, *zk.Stat, error) {
	args := m.Called(path)
	return args.Get(0).([]byte), args.Get(1).(*zk.Stat), args.Error(2)
}

func (m *MockZKClient) Set(path string, data []byte, version int32) (*zk.Stat, error) {
	args := m.Called(path, data, version)
	return args.Get(0).(*zk.Stat), args.Error(1)
}

func (m *MockZKClient) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	args := m.Called(path, data, flags, acl)
	return args.String(0), args.Error(1)
}

func (m *MockZKClient) Delete(path string, version int32) error {
	args := m.Called(path, version)
	return args.Error(0)
}

func (m *MockZKClient) Exists(path string) (bool, *zk.Stat, error) {
	args := m.Called(path)
	return args.Bool(0), args.Get(1).(*zk.Stat), args.Error(2)
}

func (m *MockZKClient) AddAuth(scheme string, auth []byte) error {
	args := m.Called(scheme, auth)
	return args.Error(0)
}

func (m *MockZKClient) Close() {
	m.Called()
}
