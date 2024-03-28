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

// prepareListOfTemplates prepares list of CHI templates to be used by the CHI
func prepareListOfTemplates(chi *api.ClickHouseInstallation) (templates []*api.ChiTemplateRef) {
	// 1. Get list of auto templates available
	templates = append(templates, prepareListOfAutoTemplates(chi)...)
	// 2. Append templates which are explicitly requested by the CHI
	templates = append(templates, prepareListOfManualTemplates(chi)...)
	// 3 Normalize list of templates
	templates = NormalizeTemplatesList(templates)

	log.V(1).M(chi).F().Info("Found applicable templates num: %d", len(templates))
	return templates
}

func prepareListOfAutoTemplates(chi *api.ClickHouseInstallation) (templates []*api.ChiTemplateRef) {
	// 1. Get list of auto templates available
	if autoTemplates := chop.Config().GetAutoTemplates(); len(autoTemplates) > 0 {
		log.V(1).M(chi).F().Info("Found auto-templates num: %d", len(autoTemplates))
		for _, template := range autoTemplates {
			log.V(1).M(chi).F().Info(
				"Adding auto-template to the list of applicable templates: %s/%s ",
				template.Namespace, template.Name)
			templates = append(templates, &api.ChiTemplateRef{
				Name:      template.Name,
				Namespace: template.Namespace,
				UseType:   UseTypeMerge,
			})
		}
	}

	return templates
}

func prepareListOfManualTemplates(chi *api.ClickHouseInstallation) (templates []*api.ChiTemplateRef) {
	if len(chi.Spec.UseTemplates) > 0 {
		log.V(1).M(chi).F().Info("Found manual-templates num: %d", len(chi.Spec.UseTemplates))
		templates = append(templates, chi.Spec.UseTemplates...)
	}

	return templates
}

// ApplyCHITemplates applies templates over target n.ctx.chi
func ApplyCHITemplates(target, chi *api.ClickHouseInstallation) (appliedTemplates []*api.ChiTemplateRef) {
	// Prepare list of templates to be applied to the CHI
	templates := prepareListOfTemplates(chi)

	// Apply templates from the list and count applied templates - just to make nice log entry
	for i := range templates {
		template := templates[i]
		if applyTemplate(target, template, chi) {
			appliedTemplates = append(appliedTemplates, template)
		}
	}

	log.V(1).M(chi).F().Info("Applied templates num: %d", len(appliedTemplates))
	return appliedTemplates
}

// applyTemplate applies a template over target n.ctx.chi
// `chi *api.ClickHouseInstallation` is used to determine whether the template should be applied or not only
func applyTemplate(target *api.ClickHouseInstallation, templateRef *api.ChiTemplateRef, chi *api.ClickHouseInstallation) bool {
	if templateRef == nil {
		log.Warning("unable to apply template - nil templateRef provided")
		// Template is not applied
		return false
	}

	// What template are we going to apply?
	defaultNamespace := chi.Namespace
	template := chop.Config().FindTemplate(templateRef, defaultNamespace)
	if template == nil {
		log.V(1).M(templateRef.Namespace, templateRef.Name).F().Warning(
			"skip template - UNABLE to find by templateRef: %s/%s",
			templateRef.Namespace, templateRef.Name)
		// Template is not applied
		return false
	}

	// What target(s) this template wants to be applied to?
	// This is determined by matching selector of the template and target's labels
	// Convenience wrapper
	selector := template.Spec.Templating.GetSelector()
	if !selector.Matches(chi.Labels) {
		// This template does not want to be applied to this CHI
		log.V(1).M(templateRef.Namespace, templateRef.Name).F().Info(
			"Skip template: %s/%s. Selector: %v does not match labels: %v",
			templateRef.Namespace, templateRef.Name, selector, chi.Labels)
		// Template is not applied
		return false
	}

	//
	// Template is found and wants to be applied on the target
	//

	log.V(1).M(templateRef.Namespace, templateRef.Name).F().Info(
		"Apply template: %s/%s. Selector: %v matches labels: %v",
		templateRef.Namespace, templateRef.Name, selector, chi.Labels)

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
		target.Annotations, util.CopyMapFilter(
			template.Annotations,
			chop.Config().Annotation.Include,
			append(chop.Config().Annotation.Exclude, util.ListSkippedAnnotations()...),
		),
	)

	// Merge template's Spec over target's Spec
	(&target.Spec).MergeFrom(&template.Spec, api.MergeTypeOverrideByNonEmptyValues)

	return target
}

// NormalizeTemplatesList normalizes list of templates use specifications
func NormalizeTemplatesList(templates []*api.ChiTemplateRef) []*api.ChiTemplateRef {
	for i := range templates {
		templates[i] = normalizeTemplateRef(templates[i])
	}
	return templates
}

// normalizeTemplateRef normalizes ChiTemplateRef
func normalizeTemplateRef(templateRef *api.ChiTemplateRef) *api.ChiTemplateRef {
	// Check Name
	if templateRef.Name == "" {
		// This is strange
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
