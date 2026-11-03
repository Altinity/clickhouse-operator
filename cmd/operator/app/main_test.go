// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package app

import (
	"os"
	"testing"

	"github.com/altinity/clickhouse-operator/pkg/chop"
)

// Several code paths under test resolve configuration through the global chop singleton:
// keeperPredicate() reaches chop.Config().IsNamespaceWatched(), and building a labeler runs
// NewDefaultConfig, which dereferences chop.Config(). Both nil-panic before chop.New.
//
// This lives in TestMain rather than an init() because the compiler allows exactly one TestMain
// per package, so package-wide setup cannot be written twice, and no test is left depending on
// setup that happens to live in a file someone may delete.
func TestMain(m *testing.M) {
	// chop.New reads the watch namespace from the environment, and both the informer scope and
	// IsNamespaceWatched are derived from it - so an exported WATCH_NAMESPACE(S) in a developer's
	// or CI shell silently changes what these tests assert. Clear it so the config is the default.
	os.Unsetenv("WATCH_NAMESPACE")
	os.Unsetenv("WATCH_NAMESPACES")

	chop.New(nil, nil, "")
	os.Exit(m.Run())
}
