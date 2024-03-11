package templates

import api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

// NormalizeServiceTemplate normalizes .spec.templates.volumeClaimTemplates
func NormalizeServiceTemplate(template *api.ChiServiceTemplate) {
	// Check name
	// Check GenerateName
	// Check ObjectMeta
	// Check Spec
}
