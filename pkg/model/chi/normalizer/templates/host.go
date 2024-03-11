package templates

import (
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer/entities"
)

// NormalizeHostTemplate normalizes .spec.templates.hostTemplates
func NormalizeHostTemplate(template *api.ChiHostTemplate) {
	// Name

	// PortDistribution

	if template.PortDistribution == nil {
		// In case no PortDistribution provided - setup default one
		template.PortDistribution = []api.ChiPortDistribution{
			{Type: deployment.PortDistributionUnspecified},
		}
	}
	// Normalize PortDistribution
	for i := range template.PortDistribution {
		portDistribution := &template.PortDistribution[i]
		switch portDistribution.Type {
		case
			deployment.PortDistributionUnspecified,
			deployment.PortDistributionClusterScopeIndex:
			// distribution is known
		default:
			// distribution is not known
			portDistribution.Type = deployment.PortDistributionUnspecified
		}
	}

	// Spec
	normalizeHostTemplateSpec(&template.Spec)
}

// normalizeHostTemplateSpec is the same as normalizeHost but for a template
func normalizeHostTemplateSpec(host *api.ChiHost) {
	entities.NormalizeHostPorts(host)
}
