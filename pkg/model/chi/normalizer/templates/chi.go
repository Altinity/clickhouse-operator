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

package templates

import (
	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

const (
	// .spec.useTemplate.useType
	UseTypeMerge = "merge"
)

type TemplateSubject interface {
	GetNamespace() string
	GetLabels() map[string]string
	GetUsedTemplates() []*api.TemplateRef
}

// prepareListOfTemplates prepares list of CHI templates to be used by the CHI
func prepareListOfTemplates(subj TemplateSubject) (templates []*api.TemplateRef) {
	// 1. Get list of auto templates available
	templates = append(templates, prepareListOfAutoTemplates(subj)...)
	// 2. Append templates which are explicitly requested by the CHI
	templates = append(templates, prepareListOfManualTemplates(subj)...)
	// 3 Normalize list of templates
	templates = NormalizeTemplatesList(templates)

	log.V(1).M(subj).F().Info("Found applicable templates num: %d", len(templates))
	return templates
}

func getListOfAutoTemplates() []*api.ClickHouseInstallation {
	return chop.Config().GetAutoTemplates()
}

func getTemplate(templateRef *api.TemplateRef, fallbackNamespace string) *api.ClickHouseInstallation {
	return chop.Config().FindTemplate(templateRef, fallbackNamespace)
}

func prepareListOfAutoTemplates(subj any) (templates []*api.TemplateRef) {
	// 1. Get list of auto templates available
	if autoTemplates := getListOfAutoTemplates(); len(autoTemplates) > 0 {
		log.V(1).M(subj).F().Info("Found auto-templates num: %d", len(autoTemplates))
		for _, template := range autoTemplates {
			log.V(1).M(subj).F().Info(
				"Adding auto-template to the list of applicable templates: %s/%s ",
				template.Namespace, template.Name)
			templates = append(templates, &api.TemplateRef{
				Name:      template.Name,
				Namespace: template.Namespace,
				UseType:   UseTypeMerge,
			})
		}
	}

	return templates
}

func prepareListOfManualTemplates(subj TemplateSubject) (templates []*api.TemplateRef) {
	if len(subj.GetUsedTemplates()) > 0 {
		log.V(1).M(subj).F().Info("Found manual-templates num: %d", len(subj.GetUsedTemplates()))
		templates = append(templates, subj.GetUsedTemplates()...)
	}

	return templates
}

// ApplyTemplates applies templates provided by 'subj' over 'target'
func ApplyTemplates(target *api.ClickHouseInstallation, subj TemplateSubject) (appliedTemplates []*api.TemplateRef) {
	// Prepare list of templates to be applied to the target
	templates := prepareListOfTemplates(subj)

	// Apply templates from the list and count applied templates - just to make nice log entry
	for _, template := range templates {
		if applyTemplate(target, template, subj) {
			appliedTemplates = append(appliedTemplates, template)
		}
	}

	log.V(1).M(subj).F().Info("Applied templates num: %d", len(appliedTemplates))
	return appliedTemplates
}

// applyTemplate applies a template over target n.ctx.chi
// `initiator` is used to determine whether the template should be applied or not
func applyTemplate(target *api.ClickHouseInstallation, templateRef *api.TemplateRef, subj TemplateSubject) bool {
	if templateRef == nil {
		log.Warning("unable to apply template - nil templateRef provided")
		// Template is not applied
		return false
	}

	// What template are we going to apply?
	defaultNamespace := subj.GetNamespace()
	template := getTemplate(templateRef, defaultNamespace)
	if template == nil {
		log.V(1).M(templateRef).F().Warning(
			"skip template - UNABLE to find by templateRef: %s/%s",
			templateRef.Namespace, templateRef.Name)
		// Template is not applied
		return false
	}

	// What target(s) this template wants to be applied to?
	// This is determined by matching selector of the template and target's labels
	// Convenience wrapper
	selector := template.GetSpec().Templating.GetSelector()
	if !selector.Matches(subj.GetLabels()) {
		// This template does not want to be applied to this CHI
		log.V(1).M(templateRef).F().Info(
			"Skip template: %s/%s. Selector: %v does not match labels: %v",
			templateRef.Namespace, templateRef.Name, selector, subj.GetLabels())
		// Template is not applied
		return false
	}

	//
	// Template is found and wants to be applied on the target
	//

	log.V(1).M(templateRef).F().Info(
		"Apply template: %s/%s. Selector: %v matches labels: %v",
		templateRef.Namespace, templateRef.Name, selector, subj.GetLabels())

	//  Let's apply template and append used template to the list of used templates
	mergeFromTemplate(target, template)

	// Template is applied
	return true
}

func mergeFromTemplate(target, template *api.ClickHouseInstallation) *api.ClickHouseInstallation {
	// Merge template's Labels over target's Labels
	target.Labels = util.MergeStringMapsOverwrite(
		target.Labels,
		util.CopyMapFilter(
			template.Labels,
			chop.Config().Label.Include,
			chop.Config().Label.Exclude,
		),
	)

	// Merge template's Annotations over target's Annotations
	target.Annotations = util.MergeStringMapsOverwrite(
		target.Annotations,
		util.CopyMapFilter(
			template.Annotations,
			chop.Config().Annotation.Include,
			append(chop.Config().Annotation.Exclude, util.ListSkippedAnnotations()...),
		),
	)

	// Merge template's Spec over target's Spec
	target.GetSpec().MergeFrom(template.GetSpec(), api.MergeTypeOverrideByNonEmptyValues)

	return target
}

// NormalizeTemplatesList normalizes list of templates use specifications
func NormalizeTemplatesList(templates []*api.TemplateRef) []*api.TemplateRef {
	for i := range templates {
		templates[i] = normalizeTemplateRef(templates[i])
	}
	return templates
}

// normalizeTemplateRef normalizes TemplateRef
func normalizeTemplateRef(templateRef *api.TemplateRef) *api.TemplateRef {
	// Check Name
	if templateRef.Name == "" {
		// This is strange, don't know what to do in this case
	}

	// Check Namespace
	if templateRef.Namespace == "" {
		// So far do nothing with empty namespace
	}

	// Ensure UseType
	switch templateRef.UseType {
	case UseTypeMerge:
		// Known use type, all is fine, do nothing
	default:
		// Unknown use type - overwrite with default value
		templateRef.UseType = UseTypeMerge
	}

	return templateRef
}
