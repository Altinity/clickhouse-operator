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
	log "github.com/golang/glog"
	// log "k8s.io/klog"

	"time"
)

// Retry
func Retry(tries int, desc string, f func() error) error {
	var err error
	for try := 1; try <= tries; try++ {
		err = f()
		if err == nil {
			// All ok, no need to retry more
			if try > 1 {
				// Done, but after some retries, this is not 'clean'
				log.V(1).Infof("DONE attempt %d of %d: %s", try, tries, desc)
			}
			return nil
		}

		if try < tries {
			// Try failed, need to sleep and retry
			seconds := try * 5
			log.V(1).Infof("FAILED attempt %d of %d, sleep %d sec and retry: %s", try, tries, seconds, desc)
			select {
			case <-time.After(time.Duration(seconds) * time.Second):
			}
		} else if tries == 1 {
			// On single try do not put so much emotion. It just failed and user is not intended to retry
			log.V(1).Infof("FAILED single try. No retries will be made for %s", desc)
		} else {
			// On last try no need to wait more
			log.V(1).Infof("FAILED AND ABORT. All %d attempts: %s", tries, desc)
		}
	}

	return err
}
