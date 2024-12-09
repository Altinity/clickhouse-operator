package app

import (
	"testing"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"sigs.k8s.io/controller-runtime/pkg/event"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
)

func Test_keeperPredicateCreate(t *testing.T) {
	tests := []struct {
		name string
		want bool
		evt  event.CreateEvent
	}{
		{
			name: "skips create when suspended",
			want: false,
			evt: event.CreateEvent{
				Object: &api.ClickHouseKeeperInstallation{
					Spec: api.ChkSpec{
						Suspend: types.NewStringBool(true),
					},
				},
			},
		},
		{
			name: "queues create when not suspended",
			want: true,
			evt: event.CreateEvent{
				Object: &api.ClickHouseKeeperInstallation{
					Spec: api.ChkSpec{
						Suspend: types.NewStringBool(false),
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
			name: "skips update when suspended",
			want: false,
			evt: event.UpdateEvent{
				ObjectNew: &api.ClickHouseKeeperInstallation{
					Spec: api.ChkSpec{
						Suspend: types.NewStringBool(true),
					},
				},
			},
		},
		{
			name: "queues update when not suspended",
			want: true,
			evt: event.UpdateEvent{
				ObjectNew: &api.ClickHouseKeeperInstallation{
					Spec: api.ChkSpec{
						Suspend: types.NewStringBool(false),
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
