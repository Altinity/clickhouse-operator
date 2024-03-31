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

package creator

import (
	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	model "github.com/altinity/clickhouse-operator/pkg/model/chi"
)

// Creator specifies creator object
type Creator struct {
	chi                    *api.ClickHouseInstallation
	chConfigFilesGenerator *model.ClickHouseConfigFilesGenerator
	labels                 *model.Labeler
	annotations            *model.Annotator
	a                      log.Announcer
}

// NewCreator creates new Creator object
func NewCreator(chi *api.ClickHouseInstallation) *Creator {
	return &Creator{
		chi:                    chi,
		chConfigFilesGenerator: model.NewClickHouseConfigFilesGenerator(model.NewClickHouseConfigGenerator(chi), chop.Config()),
		labels:                 model.NewLabeler(chi),
		annotations:            model.NewAnnotator(chi),
		a:                      log.M(chi),
	}
}
