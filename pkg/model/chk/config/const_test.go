package config

import "testing"

func TestImageFromEnv(t *testing.T) {
	t.Setenv("CLICKHOUSE_OPERATOR_TEST_KEEPER_IMAGE", "registry.example/image@sha256:test")
	if got := imageFromEnv("CLICKHOUSE_OPERATOR_TEST_KEEPER_IMAGE", "fallback"); got != "registry.example/image@sha256:test" {
		t.Fatalf("imageFromEnv() = %q", got)
	}
	if got := imageFromEnv("CLICKHOUSE_OPERATOR_UNSET_TEST_KEEPER_IMAGE", "fallback"); got != "fallback" {
		t.Fatalf("imageFromEnv() fallback = %q", got)
	}
}
