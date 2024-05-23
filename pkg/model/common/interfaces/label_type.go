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

package interfaces

type LabelType string

const (
	LabelConfigMapCommon      LabelType = "Label cm common"
	LabelConfigMapCommonUsers LabelType = "Label cm common users"
	LabelConfigMapHost        LabelType = "Label cm host"
)

const (
	LabelServiceCR      LabelType = "Label svc chi"
	LabelServiceCluster LabelType = "Label svc cluster"
	LabelServiceShard   LabelType = "Label svc shard"
	LabelServiceHost    LabelType = "Label svc host"

	LabelExistingPV  LabelType = "Label existing pv"
	LabelNewPVC      LabelType = "Label new pvc"
	LabelExistingPVC LabelType = "Label existing pvc"

	LabelPDB LabelType = "Label pdb"

	LabelSTS LabelType = "Label STS"

	LabelPodTemplate LabelType = "Label PodTemplate"
)
