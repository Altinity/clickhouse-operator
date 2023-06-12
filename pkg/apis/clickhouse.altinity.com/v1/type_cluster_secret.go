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

// ClusterSecret defines the shared secret for nodes to authenticate each other with
type ClusterSecret struct {
	Auto      *StringBool          `json:"auto,omitempty"      yaml:"auto,omitempty"`
	Value     string               `json:"value,omitempty"     yaml:"value,omitempty"`
	ValueFrom *ClusterSecretSource `json:"valueFrom,omitempty" yaml:"valueFrom,omitempty"`
}

// ClusterSecretSource mirrors k8s SecretSource type
type ClusterSecretSource struct {
	SecretKeyRef *corev1.SecretKeySelector `json:"secretKeyRef,omitempty" yaml:"secretKeyRef,omitempty"`
}

// ClusterSecretSourceName specifies name of the source where secret is provided
type ClusterSecretSourceName string

// Possible values for ClusterSecretSourceName secret sources
const (
	ClusterSecretSourcePlaintext   ClusterSecretSourceName = "plaintext"
	ClusterSecretSourceSecretRef   ClusterSecretSourceName = "secret_ref"
	ClusterSecretSourceAuto        ClusterSecretSourceName = "auto"
	ClusterSecretSourceUnspecified ClusterSecretSourceName = ""
)

// Source returns name of the source where secret is provided
func (s *ClusterSecret) Source() ClusterSecretSourceName {
	if s == nil {
		// No secret is specified at all
		return ClusterSecretSourceUnspecified
	}

	if s.HasValue() {
		// Secret has explicit value specified
		return ClusterSecretSourcePlaintext
	}

	if s.HasSecretKeyRef() {
		// Secret has SecretKeyRef specified
		return ClusterSecretSourceSecretRef
	}

	if s.Auto.IsTrue() {
		// Secret is auto-generated
		return ClusterSecretSourceAuto
	}

	// No secret is specified at all
	return ClusterSecretSourceUnspecified
}

// HasValue checks whether explicit plaintext value is specified
func (s *ClusterSecret) HasValue() bool {
	if s == nil {
		return false
	}
	return s.Value != ""
}

// GetSecretKeyRef gets SecretKeySelector (typically named as SecretKeyRef) or nil
func (s *ClusterSecret) GetSecretKeyRef() *corev1.SecretKeySelector {
	if s == nil {
		return nil
	}
	if s.ValueFrom == nil {
		return nil
	}
	return s.ValueFrom.SecretKeyRef
}

// HasSecretKeyRef checks whether SecretKeySelector (typically named as SecretKeyRef) is available
func (s *ClusterSecret) HasSecretKeyRef() bool {
	return s.GetSecretKeyRef() != nil
}

// GetAutoSecretKeyRef gets SecretKeySelector (typically named as SecretKeyRef) of an auto-generated secret or nil
func (s *ClusterSecret) GetAutoSecretKeyRef(name string) *corev1.SecretKeySelector {
	return &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{
			Name: name,
		},
		Key: "secret",
	}
}
