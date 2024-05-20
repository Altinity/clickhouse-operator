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

package tags

import (
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags/annotator"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
)

type iAnnotator interface {
	Annotate(what annotator.AnnotateType, params ...any) map[string]string
}

type iLabeler interface {
	Label(what labeler.LabelType, params ...any) map[string]string
	Selector(what labeler.SelectorType, params ...any) map[string]string
}

type tagger struct {
	annotator iAnnotator
	labeler   iLabeler
}

func NewTagger(cr api.ICustomResource) *tagger {
	return &tagger{
		annotator: annotator.NewAnnotatorClickHouse(cr, annotator.Config{
			Include: chop.Config().Annotation.Include,
			Exclude: chop.Config().Annotation.Exclude,
		}),
		labeler: labeler.NewLabelerClickHouse(cr, labeler.Config{
			AppendScope: chop.Config().Label.Runtime.AppendScope,
			Include:     chop.Config().Label.Include,
			Exclude:     chop.Config().Label.Exclude,
		}),
	}
}

func (t *tagger) Annotate(what annotator.AnnotateType, params ...any) map[string]string {
	return t.annotator.Annotate(what, params...)
}

func (t *tagger) Label(what labeler.LabelType, params ...any) map[string]string {
	return t.labeler.Label(what, params...)
}

func (t *tagger) Selector(what labeler.SelectorType, params ...any) map[string]string {
	return t.labeler.Selector(what, params...)
}
