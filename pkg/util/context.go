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

package util

import (
	"context"
	"time"
)

// IsContextDone is a non-blocking one-word syntactic sugar to check whether context is done.
// Convenient to be used as
// if IsContextDone(ctx) {
//   ... do something ...
// }
func IsContextDone(ctx context.Context) bool {
	if ctx == nil {
		// In case there is no context, it can not be done
		return false
	}

	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// WaitContextDoneOrTimeout waits either for ctx to be done or specified timeout
// returns true in case ctx is Done or false in case timeout reached
func WaitContextDoneOrTimeout(ctx context.Context, timeout time.Duration) bool {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(timeout):
		return false
	}
}

// WaitContextDoneUntil waits either for ctx to be done or specified moment in time reached
// returns true in case ctx is Done or false in case specified time reached
func WaitContextDoneUntil(ctx context.Context, t time.Time) bool {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(time.Until(t)):
		return false
	}
}

// ContextError is a one-word syntactic sugar to check what error is reported by the context.
func ContextError(ctx context.Context) error {
	if ctx == nil {
		// In case there is no context, there is no context error
		return nil
	}

	return ctx.Err()
}

// ErrIsNotCanceled checks whether specified error is either not an error is is not a context.Canceled
func ErrIsNotCanceled(err error) bool {
	if err == nil {
		return false
	}

	if err == context.Canceled {
		return false
	}

	return true
}
