// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
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

package poller

import (
	"context"
)

type Functions struct {
	Get            func(context.Context) (any, error)
	IsDone         func(context.Context, any) bool
	ShouldContinue func(context.Context, any, error) bool
}

func (p *Functions) CallGet(c context.Context) (any, error) {
	if p == nil {
		return nil, nil
	}
	if p.Get == nil {
		return nil, nil
	}
	return p.Get(c)
}

func (p *Functions) CallIsDone(c context.Context, a any) bool {
	if p == nil {
		return false
	}
	if p.IsDone == nil {
		return false
	}
	return p.IsDone(c, a)
}

func (p *Functions) CallShouldContinue(c context.Context, a any, e error) bool {
	if p == nil {
		return false
	}
	if p.ShouldContinue == nil {
		return false
	}
	return p.ShouldContinue(c, a, e)
}

type BackgroundFunctions struct {
	F func(context.Context)
}
