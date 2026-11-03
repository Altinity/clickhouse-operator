package app

import (
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/event"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// keeperPredicate intentionally does NOT pre-filter on Spec.Suspend (mirrors the
// behavior of pkg/controller/chi/ShouldEnqueue). The CHK reconciler handles suspend
// itself; pre-filtering at the informer level would prevent the reconciler from
// observing suspend-driven state transitions.

func Test_keeperPredicateCreate(t *testing.T) {
	tests := []struct {
		name string
		want bool
		evt  event.CreateEvent
	}{
		{
			name: "queues create for a non-suspended CHK",
			want: true,
			evt: event.CreateEvent{
				Object: &api.ClickHouseKeeperInstallation{
					Spec: api.ChkSpec{
						Suspend: types.NewStringBool(false),
					},
				},
			},
		},
		{
			name: "queues create even when suspended (reconciler handles suspend)",
			want: true,
			evt: event.CreateEvent{
				Object: &api.ClickHouseKeeperInstallation{
					Spec: api.ChkSpec{
						Suspend: types.NewStringBool(true),
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			predicate := keeperPredicate()
			if got := predicate.Create(tt.evt); tt.want != got {
				t.Errorf("keeperPredicate.Create() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_keeperPredicateUpdate(t *testing.T) {
	tests := []struct {
		name string
		want bool
		evt  event.UpdateEvent
	}{
		{
			name: "queues update for a non-suspended CHK",
			want: true,
			evt: event.UpdateEvent{
				ObjectNew: &api.ClickHouseKeeperInstallation{
					Spec: api.ChkSpec{
						Suspend: types.NewStringBool(false),
					},
				},
			},
		},
		{
			name: "queues update even when suspended (reconciler handles suspend)",
			want: true,
			evt: event.UpdateEvent{
				ObjectNew: &api.ClickHouseKeeperInstallation{
					Spec: api.ChkSpec{
						Suspend: types.NewStringBool(true),
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			predicate := keeperPredicate()
			if got := predicate.Update(tt.evt); tt.want != got {
				t.Errorf("keeperPredicate.Update() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_keeperPredicateDelete(t *testing.T) {
	tests := []struct {
		name string
		want bool
		evt  event.DeleteEvent
	}{
		{
			name: "deletes even when suspended",
			want: true,
			evt: event.DeleteEvent{
				Object: &api.ClickHouseKeeperInstallation{
					Spec: api.ChkSpec{
						Suspend: types.NewStringBool(true),
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			predicate := keeperPredicate()
			if got := predicate.Delete(tt.evt); tt.want != got {
				t.Errorf("keeperPredicate.Delete() = %v, want %v", got, tt.want)
			}
		})
	}
}
