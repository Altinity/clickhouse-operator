package normalizer

// Options specifies normalization options
type Options struct {
	// WithDefaultCluster specifies whether to insert default cluster in case no cluster specified
	WithDefaultCluster bool
	// DefaultUserAdditionalIPs specifies set of additional IPs applied to default user
	DefaultUserAdditionalIPs   []string
	DefaultUserInsertHostRegex bool
}

// NewOptions creates new Options
func NewOptions() *Options {
	return &Options{
		DefaultUserInsertHostRegex: true,
	}
}
