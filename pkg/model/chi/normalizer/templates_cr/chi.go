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

package templates_cr

import (
	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

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

func getListOfAutoTemplates() []*api.ClickHouseInstallation {
	return chop.Config().GetAutoTemplates()
}

func getTemplate(templateRef *api.TemplateRef, fallbackNamespace string) *api.ClickHouseInstallation {
	return chop.Config().FindTemplate(templateRef, fallbackNamespace)
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

// applyTemplate finds and applies a template over target
// 'subj' is used to determine whether the template should be applied or not
func applyTemplate(target *api.ClickHouseInstallation, templateRef *api.TemplateRef, subj TemplateSubject) bool {
	// Find and apply (merge) template
	if template := findApplicableTemplate(templateRef, subj); template != nil {
		mergeFromTemplate(target, template)
		return true
	}

	return false
}

func findApplicableTemplate(templateRef *api.TemplateRef, subj TemplateSubject) *api.ClickHouseInstallation {
	if templateRef == nil {
		log.Warning("unable to apply template - nil templateRef provided")
		// Template is not applied
		return nil
	}

	// What template are we going to apply?
	defaultNamespace := subj.GetNamespace()
	template := getTemplate(templateRef, defaultNamespace)
	if template == nil {
		log.V(1).M(templateRef).F().Warning(
			"skip template - UNABLE to find by templateRef: %s/%s",
			templateRef.Namespace, templateRef.Name)
		// Template is not applied
		return nil
	}

	// What target(s) this template wants to be applied to?
	// This is determined by matching selector of the template and target's labels
	// Convenience wrapper
	selector := template.GetSpecT().Templating.GetSelector()
	if !selector.Matches(subj.GetLabels()) {
		// This template does not want to be applied to this CHI
		log.V(1).M(templateRef).F().Info(
			"Skip template: %s/%s. Selector: %v does not match labels: %v",
			templateRef.Namespace, templateRef.Name, selector, subj.GetLabels())
		// Template is not applied
		return nil
	}

	//
	// Template is found and wants to be applied on the target
	//

	log.V(1).M(templateRef).F().Info(
		"Apply template: %s/%s. Selector: %v matches labels: %v",
		templateRef.Namespace, templateRef.Name, selector, subj.GetLabels())

	return template
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
	target.GetSpecT().MergeFrom(template.GetSpecT(), api.MergeTypeOverrideByNonEmptyValues)

	return target
}
