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

package labeler

import (
	"fmt"
)

var (
	ErrEnvVarNotSpecified = fmt.Errorf("ENV var not specified")
	// ErrOperatorPodNotSpecified specifies error when there is not namespace/name pair provided pointing to operator pod
	ErrOperatorPodNotSpecified = fmt.Errorf("operator pod not specfied")
	ErrUnableToLabelPod        = fmt.Errorf("unable to label pod")
	ErrUnableToLabelReplicaSet = fmt.Errorf("unable to label replica set")
	ErrUnableToLabelDeployment = fmt.Errorf("unable to label deployment")
)
