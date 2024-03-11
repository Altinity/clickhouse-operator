package templates

import api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

// NormalizeVolumeClaimTemplate normalizes .spec.templates.volumeClaimTemplates
func NormalizeVolumeClaimTemplate(template *api.ChiVolumeClaimTemplate) {
	// Check name
	// Skip for now

	// StorageManagement
	normalizeStorageManagement(&template.StorageManagement)

	// Check Spec
	// Skip for now
}

// normalizeStorageManagement normalizes StorageManagement
func normalizeStorageManagement(storage *api.StorageManagement) {
	// Check PVCProvisioner
	if !storage.PVCProvisioner.IsValid() {
		storage.PVCProvisioner = api.PVCProvisionerUnspecified
	}

	// Check PVCReclaimPolicy
	if !storage.PVCReclaimPolicy.IsValid() {
		storage.PVCReclaimPolicy = api.PVCReclaimPolicyUnspecified
	}
}
