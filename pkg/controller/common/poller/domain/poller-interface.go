package domain

import (
	"context"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

type readyMarkDeleter interface {
	DeleteReadyMarkOnPodAndService(ctx context.Context, host *api.Host) error
}

type defaultReadyMarkDeleter struct{}

func newDefaultReadyMarkDeleter() *defaultReadyMarkDeleter {
	return &defaultReadyMarkDeleter{}
}

func (x *defaultReadyMarkDeleter) DeleteReadyMarkOnPodAndService(ctx context.Context, host *api.Host) error {
	return nil
}
