// //go:build race
package v1

import (
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

var normalizedChiA = &ClickHouseInstallation{}
var fillParamsA = &FillStatusParams{
	CHOpIP:              "1.2.3.4",
	ClustersCount:       1,
	ShardsCount:         2,
	HostsCount:          3,
	TaskID:              "task-a",
	HostsUpdatedCount:   4,
	HostsAddedCount:     5,
	HostsCompletedCount: 6,
	HostsDeleteCount:    7,
	HostsDeletedCount:   8,
	Pods:                []string{"pod-a-1", "pod-a-2"},
	FQDNs:               []string{"fqdns-a-1", "fqdns-a-2"},
	Endpoint:            "endpoint-a",
	NormalizedCHI:       normalizedChiA, // fields not recursively checked, this is only used as a pointer
}

var normalizedChiB = &ClickHouseInstallation{}
var fillParamsB = &FillStatusParams{
	CHOpIP:              "5.6.7.8",
	ClustersCount:       10,
	ShardsCount:         20,
	HostsCount:          30,
	TaskID:              "task-b",
	HostsUpdatedCount:   40,
	HostsAddedCount:     50,
	HostsCompletedCount: 60,
	HostsDeleteCount:    70,
	HostsDeletedCount:   80,
	Pods:                []string{"pod-b-1", "pod-b-2"},
	FQDNs:               []string{"fqdns-b-1", "fqdns-b-2"},
	Endpoint:            "endpoint-b",
	NormalizedCHI:       normalizedChiB, // fields not recursively checked, this is only used as a pointer
}

var copyTestStatusFrom = &ChiStatus{
	CHOpVersion:            "version-a",
	CHOpCommit:             "commit-a",
	CHOpDate:               "date-a",
	CHOpIP:                 "ip-a",
	ClustersCount:          1,
	ShardsCount:            2,
	ReplicasCount:          3,
	HostsCount:             4,
	Status:                 "status-a",
	TaskID:                 "task-a",
	TaskIDsStarted:         []string{"task-a-started-1", "task-a-started-2"},
	TaskIDsCompleted:       []string{"task-a-completed-1", "task-a-completed-2"},
	Action:                 "action-a",
	Actions:                []string{"action-a", "action-a-another"},
	Error:                  "error-a",
	Errors:                 []string{"error-a", "error-a-another"},
	HostsUpdatedCount:      5,
	HostsAddedCount:        6,
	HostsUnchangedCount:    7,
	HostsFailedCount:       8,
	HostsCompletedCount:    9,
	HostsDeletedCount:      10,
	HostsDeleteCount:       11,
	Pods:                   []string{"pod-a-1", "pod-a-2"},
	PodIPs:                 []string{"podIP-a-1", "podIP-a-2"},
	FQDNs:                  []string{"fqdns-a-1", "fqdns-a-2"},
	Endpoint:               "endpt-a",
	NormalizedCHI:          normalizedChiA,
	NormalizedCHICompleted: normalizedChiA,
	HostsWithTablesCreated: []string{"host-a-1", "host-a-2"},
}

// NB: These tests mostly exist to exercise synchronization and detect regressions related to them via the
// Golang race detector. See: https://go.dev/blog/race-detector
// In short, add -race to the go test flags when running this.
func Test_ChiStatus_BasicOperations_SingleStatus_ConcurrencyTest(t *testing.T) {
	type testCase struct {
		name                       string
		goRoutineA                 func(s *ChiStatus)
		goRoutineB                 func(s *ChiStatus)
		postConditionsVerification func(tt *testing.T, s *ChiStatus)
	}
	for _, tc := range []testCase{
		{
			name: "PushAction",
			goRoutineA: func(s *ChiStatus) {
				s.PushAction("foo")
			},
			goRoutineB: func(s *ChiStatus) {
				s.PushAction("bar")
			},
			postConditionsVerification: func(tt *testing.T, s *ChiStatus) {
				actual := s.GetActions()
				require.Len(tt, actual, 2)
				require.Contains(tt, actual, "foo")
				require.Contains(tt, actual, "bar")
			},
		},
		{
			name: "PushError",
			goRoutineA: func(s *ChiStatus) {
				s.PushError("errA")
				s.PushError("errB")
			},
			goRoutineB: func(s *ChiStatus) {
				s.PushError("errC")
			},
			postConditionsVerification: func(tt *testing.T, s *ChiStatus) {
				actual := s.GetErrors()
				require.Len(t, actual, 3)
				require.Contains(tt, actual, "errA")
				require.Contains(tt, actual, "errB")
				require.Contains(tt, actual, "errC")
			},
		},
		{
			name: "Fill",
			goRoutineA: func(s *ChiStatus) {
				s.Fill(fillParamsA)
			},
			goRoutineB: func(s *ChiStatus) {
				s.Fill(fillParamsB)
			},
			postConditionsVerification: func(tt *testing.T, s *ChiStatus) {
				// Fill performs hard updates (overwrites), not pushing/adding extra data.
				// The winning goroutine should basically determine the resultant post-condition for every "filled" field.
				var expectedParams *FillStatusParams
				if s.CHOpIP == fillParamsA.CHOpIP {
					expectedParams = fillParamsA
				} else if s.CHOpIP == fillParamsB.CHOpIP {
					expectedParams = fillParamsB
				} else {
					require.Fail(t, "Unexpected CHOpIP after FillStatus: %s", s.CHOpIP)
				}
				require.Equal(tt, expectedParams.CHOpIP, s.CHOpIP)
				require.Equal(tt, expectedParams.ClustersCount, s.ClustersCount)
				require.Equal(tt, expectedParams.ShardsCount, s.ShardsCount)
				require.Equal(tt, expectedParams.HostsCount, s.HostsCount)
				require.Equal(tt, expectedParams.TaskID, s.TaskID)
				require.Equal(tt, expectedParams.HostsUpdatedCount, s.HostsUpdatedCount)
				require.Equal(tt, expectedParams.HostsAddedCount, s.HostsAddedCount)
				require.Equal(tt, expectedParams.HostsCompletedCount, s.HostsCompletedCount)
				require.Equal(tt, expectedParams.HostsDeleteCount, s.HostsDeleteCount)
				require.Equal(tt, expectedParams.HostsDeletedCount, s.HostsDeletedCount)
				require.Equal(tt, expectedParams.Pods, s.Pods)
				require.Equal(tt, expectedParams.FQDNs, s.FQDNs)
				require.Equal(tt, expectedParams.Endpoint, s.Endpoint)
				require.Equal(tt, expectedParams.NormalizedCHI, s.NormalizedCHI)
			},
		},
		{
			name: "CopyFrom",
			goRoutineA: func(s *ChiStatus) {
				s.PushAction("always-present-action") // CopyFrom preserves existing actions (does not clobber)
				s.CopyFrom(copyTestStatusFrom, CopyCHIStatusOptions{
					Actions:           true,
					Errors:            true,
					MainFields:        true,
					WholeStatus:       true,
					InheritableFields: true,
				})
			},
			goRoutineB: func(s *ChiStatus) {
				s.PushAction("additional-action") // this may or may not win the race, but the race will be sync
			},
			postConditionsVerification: func(tt *testing.T, s *ChiStatus) {
				if len(s.GetActions()) == len(copyTestStatusFrom.GetActions())+2 {
					require.Equal(tt, copyTestStatusFrom.GetActions(), s.GetActions())
					require.Contains(tt, s.GetActions(), "always-present-action")
					require.Contains(tt, s.GetActions(), "additional-action")
					for _, action := range copyTestStatusFrom.GetActions() {
						require.Contains(tt, s.GetActions(), action)
					}
				} else {
					require.Equal(tt, len(copyTestStatusFrom.GetActions())+1, len(s.GetActions()))
					require.Contains(tt, s.GetActions(), "additional-action")
					for _, action := range copyTestStatusFrom.GetActions() {
						require.Contains(tt, s.GetActions(), action)
					}
				}
				require.Equal(tt, copyTestStatusFrom.GetAction(), s.GetAction())
				require.Equal(tt, copyTestStatusFrom.GetCHOpCommit(), s.GetCHOpCommit())
				require.Equal(tt, copyTestStatusFrom.GetCHOpDate(), s.GetCHOpDate())
				require.Equal(tt, copyTestStatusFrom.GetCHOpIP(), s.GetCHOpIP())
				require.Equal(tt, copyTestStatusFrom.GetCHOpVersion(), s.GetCHOpVersion())
				require.Equal(tt, copyTestStatusFrom.GetClustersCount(), s.GetClustersCount())
				require.Equal(tt, copyTestStatusFrom.GetEndpoint(), s.GetEndpoint())
				require.Equal(tt, copyTestStatusFrom.GetError(), s.GetError())
				require.Equal(tt, copyTestStatusFrom.GetErrors(), s.GetErrors())
				require.Equal(tt, copyTestStatusFrom.GetErrors(), s.GetErrors())
				require.Equal(tt, copyTestStatusFrom.GetFQDNs(), s.GetFQDNs())
				require.Equal(tt, copyTestStatusFrom.GetHostsAddedCount(), s.GetHostsAddedCount())
				require.Equal(tt, copyTestStatusFrom.GetHostsCompletedCount(), s.GetHostsCompletedCount())
				require.Equal(tt, copyTestStatusFrom.GetHostsCount(), s.GetHostsCount())
				require.Equal(tt, copyTestStatusFrom.GetHostsDeleteCount(), s.GetHostsDeleteCount())
				require.Equal(tt, copyTestStatusFrom.GetHostsDeletedCount(), s.GetHostsDeletedCount())
				require.Equal(tt, copyTestStatusFrom.GetHostsUpdatedCount(), s.GetHostsUpdatedCount())
				require.Equal(tt, copyTestStatusFrom.GetHostsWithTablesCreated(), s.GetHostsWithTablesCreated())
				require.Equal(tt, copyTestStatusFrom.GetHostsWithTablesCreated(), s.GetHostsWithTablesCreated())
				require.Equal(tt, copyTestStatusFrom.GetHostsWithTablesCreated(), s.GetHostsWithTablesCreated())
				require.Equal(tt, copyTestStatusFrom.GetNormalizedCHI(), s.GetNormalizedCHI())
				require.Equal(tt, copyTestStatusFrom.GetNormalizedCHICompleted(), s.GetNormalizedCHICompleted())
				require.Equal(tt, copyTestStatusFrom.GetPodIPs(), s.GetPodIPs())
				require.Equal(tt, copyTestStatusFrom.GetPods(), s.GetPods())
				require.Equal(tt, copyTestStatusFrom.GetReplicasCount(), s.GetReplicasCount())
				require.Equal(tt, copyTestStatusFrom.GetShardsCount(), s.GetShardsCount())
				require.Equal(tt, copyTestStatusFrom.GetStatus(), s.GetStatus())
				require.Equal(tt, copyTestStatusFrom.GetTaskID(), s.GetTaskID())
				require.Equal(tt, copyTestStatusFrom.GetTaskIDsCompleted(), s.GetTaskIDsCompleted())
				require.Equal(tt, copyTestStatusFrom.GetTaskIDsStarted(), s.GetTaskIDsStarted())
			},
		},
	} {
		t.Run(tc.name, func(tt *testing.T) {
			status := &ChiStatus{}
			startWg := sync.WaitGroup{}
			doneWg := sync.WaitGroup{}
			startWg.Add(2) // We will make sure both goroutines begin execution, i.e., that they don't execute sequentially.
			doneWg.Add(2)  // We also need to synchronize the test over the completion of both.

			go func() {
				startWg.Done()
				startWg.Wait() // Block until the other goroutine has begun execution
				tc.goRoutineA(status)
				doneWg.Done()
			}()

			go func() {
				startWg.Done()
				startWg.Wait() // Block until the other goroutine has begun execution

				tc.goRoutineB(status)
				doneWg.Done()
			}()

			doneWg.Wait() // Block until both goroutines have completed execution

			// Verify post-conditions
			tc.postConditionsVerification(tt, status)
		})
	}
}
