// +build tools

// This package imports things required by build scripts, to force `go mod` to see them as dependencies
package tools

import (
	_ "k8s.io/code-generator"
	_ "github.com/securego/gosec/v2/cmd/gosec"
)
