package normalizer

import api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

// Context specifies CHI-related normalization context
type Context struct {
	// start specifies start CHI from which normalization has started
	start *api.ClickHouseInstallation
	// chi specifies current CHI being normalized
	chi *api.ClickHouseInstallation
	// options specifies normalization options
	options *Options
}

// NewContext creates new Context
func NewContext(options *Options) *Context {
	return &Context{
		options: options,
	}
}
