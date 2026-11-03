// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrimHostListToFQDNs(t *testing.T) {
	t.Parallel()
	t.Run("nil fqdns leaves hosts unchanged", func(t *testing.T) {
		t.Parallel()
		hosts := []string{"h1", "h2"}
		require.Equal(t, hosts, TrimHostListToFQDNs(hosts, nil))
	})
	t.Run("intersect and normalize", func(t *testing.T) {
		t.Parallel()
		fqdns := []string{"a.ns.svc.", "b.ns.svc"}
		hosts := []string{"a.ns.svc", "gone.ns.svc", "b.ns.svc."}
		got := TrimHostListToFQDNs(hosts, fqdns)
		require.ElementsMatch(t, []string{"a.ns.svc", "b.ns.svc"}, got)
	})
	t.Run("empty hosts", func(t *testing.T) {
		t.Parallel()
		require.Nil(t, TrimHostListToFQDNs(nil, []string{"a"}))
	})
}
