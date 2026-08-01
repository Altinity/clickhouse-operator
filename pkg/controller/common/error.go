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

package common

import (
	"errors"
)

// ErrorCRUD specifies errors of the CRUD operations
type ErrorCRUD error

var (
	ErrCRUDAbort          ErrorCRUD = errors.New("crud error - should abort")
	ErrCRUDIgnore         ErrorCRUD = errors.New("crud error - should ignore")
	ErrCRUDRecreate       ErrorCRUD = errors.New("crud error - should recreate")
	ErrCRUDUnexpectedFlow ErrorCRUD = errors.New("crud error - unexpected flow")
	// ErrCRUDDeferred says the operation was intentionally postponed, not that it failed.
	// Kept distinct from ErrCRUDAbort so a deferred host does not starve its sibling shards:
	// callers carry on and surface it once at the end of the pass. It must still reach the
	// top as an error - a pass that swallowed it would advance the CR ancestor past a host
	// that was never rolled (losing the pending restart) and let clean() purge that host.
	ErrCRUDDeferred ErrorCRUD = errors.New("crud error - should defer")
)
