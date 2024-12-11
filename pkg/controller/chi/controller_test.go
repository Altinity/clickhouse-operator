package chi

import (
	"testing"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
)

func init() {
	chop.New(nil, nil, "")
}

func Test_shouldEnqueue(t *testing.T) {
	tests := []struct {
		name string
		chi  *api.ClickHouseInstallation
		want bool
	}{
		{
			name: "skips when chi is suspended",
			chi: &api.ClickHouseInstallation{
				Spec: api.ChiSpec{
					Suspend: types.NewStringBool(true),
				},
			},
			want: false,
		},
		{
			name: "enqueues when chi is not suspended",
			chi: &api.ClickHouseInstallation{
				Spec: api.ChiSpec{
					Suspend: types.NewStringBool(false),
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldEnqueue(tt.chi); got != tt.want {
				t.Errorf("shouldEnqueue() = %v, want %v", got, tt.want)
			}
		})
	}
}
