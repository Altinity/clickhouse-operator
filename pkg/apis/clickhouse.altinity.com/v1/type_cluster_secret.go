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

import (
	corev1 "k8s.io/api/core/v1"
)

// ClusterSecret defines the shared secret to for nodes to authenticate each other with
type ClusterSecret struct {
	Value     string               `json:"value,omitempty"     yaml:"value,omitempty"`
	ValueFrom *ClusterSecretSource `json:"valueFrom,omitempty" yaml:"valueFrom,omitempty"`
}

type ClusterSecretSource struct {
	SecretKeyRef *corev1.SecretKeySelector `json:"secretKeyRef,omitempty" yaml:"secretKeyRef,omitempty"`
}

func (s *ClusterSecret) HasValue() bool {
	if s == nil {
		return false
	}
	return s.Value != ""
}

func (s *ClusterSecret) GetSecretKeyRef() *corev1.SecretKeySelector {
	if s == nil {
		return nil
	}
	if s.ValueFrom == nil {
		return nil
	}
	return s.ValueFrom.SecretKeyRef
}

func (s *ClusterSecret) HasSecretKeyRef() bool {
	return s.GetSecretKeyRef() != nil
}

func (s *ClusterSecret) Get() (string, bool) {
	// Sanity check
	if s == nil {
		return "", false
	}

	// Explicit value has priority
	if s.Value != "" {
		return s.Value, false
	}

	// From now on explicit value is empty, check for SecretSource

	if s.ValueFrom == nil {
		return "", false
	}

	if s.ValueFrom.SecretKeyRef == nil {
		return "", false
	}

	return "", true
}
