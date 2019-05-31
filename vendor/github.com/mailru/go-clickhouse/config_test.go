package clickhouse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseDSN(t *testing.T) {
	dsn := "http://username:password@localhost:8123/test?timeout=1s&idle_timeout=2s&read_timeout=3s" +
		"&write_timeout=4s&location=Local&max_execution_time=10&debug=1"
	cfg, err := ParseDSN(dsn)
	if assert.NoError(t, err) {
		assert.Equal(t, "username", cfg.User)
		assert.Equal(t, "password", cfg.Password)
		assert.Equal(t, "http", cfg.Scheme)
		assert.Equal(t, "localhost:8123", cfg.Host)
		assert.Equal(t, "test", cfg.Database)
		assert.Equal(t, time.Second, cfg.Timeout)
		assert.Equal(t, 2*time.Second, cfg.IdleTimeout)
		assert.Equal(t, 3*time.Second, cfg.ReadTimeout)
		assert.Equal(t, 4*time.Second, cfg.WriteTimeout)
		assert.Equal(t, time.Local, cfg.Location)
		assert.True(t, cfg.Debug)
		assert.Equal(t, map[string]string{"max_execution_time": "10"}, cfg.Params)
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := NewConfig()
	assert.Equal(t, "http", cfg.Scheme)
	assert.Equal(t, "localhost:8123", cfg.Host)
	assert.Empty(t, cfg.Database)
	assert.Empty(t, cfg.User)
	assert.Empty(t, cfg.Password)
	assert.False(t, cfg.Debug)
	assert.Equal(t, time.UTC, cfg.Location)
	assert.EqualValues(t, 0, cfg.ReadTimeout)
	assert.EqualValues(t, 0, cfg.WriteTimeout)
	assert.Equal(t, time.Hour, cfg.IdleTimeout)
	assert.Empty(t, cfg.Params)

	dsn := cfg.FormatDSN()
	expected := "http://localhost:8123/?idle_timeout=1h0m0s"
	assert.Equal(t, expected, dsn)
}

func TestParseWrongDSN(t *testing.T) {
	testCases := []string{
		"http://localhost:8123/?database=test",
		"http://localhost:8123/?default_format=Native",
		"http://localhost:8123/?query=SELECT%201",
	}
	for _, tc := range testCases {
		_, err := ParseDSN(tc)
		assert.Error(t, err)
	}
}

func TestFormatDSN(t *testing.T) {
	dsn := "http://username:password@localhost:8123/test?timeout=1s&idle_timeout=2s&read_timeout=3s" +
		"&write_timeout=4s&location=Europe%2FMoscow&max_execution_time=10&debug=1"
	cfg, err := ParseDSN(dsn)
	if assert.NoError(t, err) {
		dsn2 := cfg.FormatDSN()
		assert.Equal(t, len(dsn), len(dsn2))
		assert.Contains(t, dsn2, "http://username:password@localhost:8123/test?")
		assert.Contains(t, dsn2, "timeout=1s")
		assert.Contains(t, dsn2, "idle_timeout=2s")
		assert.Contains(t, dsn2, "read_timeout=3s")
		assert.Contains(t, dsn2, "write_timeout=4s")
		assert.Contains(t, dsn2, "location=Europe%2FMoscow")
		assert.Contains(t, dsn2, "max_execution_time=10")
		assert.Contains(t, dsn2, "debug=1")
	}
}

func TestConfigURL(t *testing.T) {
	dsn := "http://username:password@localhost:8123/test?timeout=1s&idle_timeout=2s&read_timeout=3s" +
		"&write_timeout=4s&location=Local&max_execution_time=10"
	cfg, err := ParseDSN(dsn)
	if assert.NoError(t, err) {
		u1 := cfg.url(nil, true).String()
		assert.Equal(t, "http://username:password@localhost:8123/test?max_execution_time=10", u1)
		u2 := cfg.url(map[string]string{"default_format": "Native"}, false).String()
		assert.Contains(t, u2, "http://username:password@localhost:8123/?")
		assert.Contains(t, u2, "default_format=Native")
		assert.Contains(t, u2, "database=test")
	}
}

func TestDefaultPort(t *testing.T) {
	testCases := []struct {
		in  string
		out string
	}{
		{"http://localhost/test", "http://localhost:8123/test"},
		{"http://de:ad:be:ef::ca:fe/test", "http://[de:ad:be:ef::ca:fe]:8123/test"},
	}
	var (
		cfg *Config
		err error
	)
	for _, tc := range testCases {
		cfg, err = ParseDSN(tc.in)
		if assert.NoError(t, err) {
			assert.Equal(t, tc.out, cfg.url(nil, true).String())
		}
	}
}
