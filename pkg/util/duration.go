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

import "time"

// ReasonableDuration finds first duration > 0 or returns 0 in case no durations provided
func ReasonableDuration(durations ...time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	for _, duration := range durations {
		if duration > 0 {
			return duration
		}
	}
	return 0
}
