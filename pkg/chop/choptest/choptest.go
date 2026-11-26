// Package choptest provides shared helpers for tests exercising the global chop config.
package choptest

import (
	"testing"

	"github.com/altinity/clickhouse-operator/pkg/chop"
)

// ShardLabelKey is the shard label key used across watch label selector tests.
const ShardLabelKey = "example.com/clickhouse-shard"

// EnsureInit initializes the global chop singleton if no test package has done so yet.
func EnsureInit() {
	if chop.Get() == nil {
		chop.New(nil, nil, "")
	}
}

// SetWatchLabelSelector sets watch.labelSelector on the global chop config for one test,
// restoring the selector-less default afterwards.
func SetWatchLabelSelector(t *testing.T, selector string) {
	t.Helper()
	EnsureInit()
	chop.Config().Watch.LabelSelector = selector
	if err := chop.Config().ParseWatchLabelSelector(); err != nil {
		t.Fatalf("ParseWatchLabelSelector(%q) unexpected error: %v", selector, err)
	}
	t.Cleanup(func() {
		chop.Config().Watch.LabelSelector = ""
		_ = chop.Config().ParseWatchLabelSelector()
	})
}
