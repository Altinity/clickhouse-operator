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

package managers

import (
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	chiAnnotator "github.com/altinity/clickhouse-operator/pkg/model/chi/tags/annotator"
	chiLabeler "github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	chkAnnotator "github.com/altinity/clickhouse-operator/pkg/model/chk/tags/annotator"
	chkLabeler "github.com/altinity/clickhouse-operator/pkg/model/chk/tags/labeler"
	commonAnnotator "github.com/altinity/clickhouse-operator/pkg/model/common/tags/annotator"
	commonLabeler "github.com/altinity/clickhouse-operator/pkg/model/common/tags/labeler"
)

type TagManagerType string

const (
	TagManagerTypeClickHouse TagManagerType = "clickhouse"
	TagManagerTypeKeeper     TagManagerType = "keeper"
)

func NewTagManager(what TagManagerType, cr api.ICustomResource) interfaces.ITagger {
	switch what {
	case TagManagerTypeClickHouse:
		return newTaggerClickHouse(cr)
	case TagManagerTypeKeeper:
		return newTaggerKeeper(cr)
	}
	panic("unknown volume manager type")
}

type tagger struct {
	annotator interfaces.IAnnotator
	labeler   interfaces.ILabeler
}

func newTaggerClickHouse(cr api.ICustomResource) *tagger {
	return &tagger{
		annotator: chiAnnotator.NewAnnotatorClickHouse(cr, commonAnnotator.Config{
			Include: chop.Config().Annotation.Include,
			Exclude: chop.Config().Annotation.Exclude,
		}),
		labeler: chiLabeler.NewLabelerClickHouse(cr, commonLabeler.Config{
			AppendScope: chop.Config().Label.Runtime.AppendScope,
			Include:     chop.Config().Label.Include,
			Exclude:     chop.Config().Label.Exclude,
		}),
	}
}

func newTaggerKeeper(cr api.ICustomResource) *tagger {
	return &tagger{
		annotator: chkAnnotator.NewAnnotatorKeeper(cr, commonAnnotator.Config{
			Include: chop.Config().Annotation.Include,
			Exclude: chop.Config().Annotation.Exclude,
		}),
		labeler: chkLabeler.NewLabelerKeeper(cr, commonLabeler.Config{
			AppendScope: chop.Config().Label.Runtime.AppendScope,
			Include:     chop.Config().Label.Include,
			Exclude:     chop.Config().Label.Exclude,
		}),
	}
}

func (t *tagger) Annotate(what interfaces.AnnotateType, params ...any) map[string]string {
	return t.annotator.Annotate(what, params...)
}

func (t *tagger) Label(what interfaces.LabelType, params ...any) map[string]string {
	return t.labeler.Label(what, params...)
}

func (t *tagger) Selector(what interfaces.SelectorType, params ...any) map[string]string {
	return t.labeler.Selector(what, params...)
}
