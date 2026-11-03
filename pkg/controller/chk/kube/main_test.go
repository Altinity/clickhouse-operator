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

package kube

import (
	"os"
	"testing"

	"github.com/altinity/clickhouse-operator/pkg/chop"
)

// Building a labeler runs NewDefaultConfig, which dereferences chop.Config() - so ListForHost,
// which resolves label keys to build its selector, nil-panics before chop.New. In a TestMain
// because the compiler allows exactly one per package, so this setup cannot be duplicated and no
// test is left depending on setup that happens to live in a sibling file.
func TestMain(m *testing.M) {
	chop.New(nil, nil, "")
	os.Exit(m.Run())
}
