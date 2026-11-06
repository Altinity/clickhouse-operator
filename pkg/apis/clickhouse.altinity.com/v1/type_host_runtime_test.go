package v1

import "testing"

func TestHostForceReplicaCatchUpDefaultsFalseAndCanBeSet(t *testing.T) {
	host := &Host{}
	if host.IsForceReplicaCatchUp() {
		t.Fatalf("force replica catch-up must default to false")
	}

	host.SetForceReplicaCatchUp(true)
	if !host.IsForceReplicaCatchUp() {
		t.Fatalf("force replica catch-up must be true after SetForceReplicaCatchUp(true)")
	}

	host.SetForceReplicaCatchUp(false)
	if host.IsForceReplicaCatchUp() {
		t.Fatalf("force replica catch-up must be false after SetForceReplicaCatchUp(false)")
	}
}
