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

package v1

// ChiStatus defines status section of ClickHouseInstallation resource
type ChiStatus struct {
	Version           string   `json:"version"`
	ClustersCount     int      `json:"clusters"`
	ShardsCount       int      `json:"shards"`
	ReplicasCount     int      `json:"replicas"`
	HostsCount        int      `json:"hosts"`
	Status            string   `json:"status"`
	Action            string   `json:"action"`
	Actions           []string `json:"actions"`
	Error             string   `json:"error"`
	Errors            []string `json:"errors"`
	UpdatedHostsCount int      `json:"updated"`
	AddedHostsCount   int      `json:"added"`
	DeletedHostsCount int      `json:"deleted"`
	DeleteHostsCount  int      `json:"delete"`
	Pods              []string `json:"pods"`
	FQDNs             []string `json:"fqdns"`
	Endpoint          string   `json:"endpoint"`
	NormalizedCHI     ChiSpec  `json:"normalized"`
}

const (
	maxActions = 100
	maxErrors  = 100
)

func (s *ChiStatus) PushAction(action string) {
	s.Actions = append(s.Actions, action)
	if len(s.Actions) > maxActions {
		s.Actions = s.Actions[1:]
	}
}

func (s *ChiStatus) SetAndPushError(error string) {
	s.Error = error
	s.Errors = append(s.Errors, error)
	if len(s.Errors) > maxErrors {
		s.Errors = s.Errors[1:]
	}
}

func (s *ChiStatus) ReconcileStart(DeleteHostsCount int) {
	s.Status = StatusInProgress
	s.UpdatedHostsCount = 0
	s.AddedHostsCount = 0
	s.DeletedHostsCount = 0
	s.DeleteHostsCount = DeleteHostsCount
}

func (s *ChiStatus) ReconcileComplete() {
	s.Status = StatusCompleted
	s.Action = ""
}

func (s *ChiStatus) DeleteStart() {
	s.Status = StatusTerminating
	s.UpdatedHostsCount = 0
	s.AddedHostsCount = 0
	s.DeletedHostsCount = 0
	s.DeleteHostsCount = 0
}
