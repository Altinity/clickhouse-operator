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

package clickhouse

import "strings"

// legacyAsyncMetricName is the name a key-value asynchronous metric element had before
// ClickHouse 26.8, or "" for a family that has none. It mirrors getLegacyAsynchronousMetricName
// in src/Common/AsynchronousMetrics.cpp. The table is historical: families added later never
// had a legacy name.
func legacyAsyncMetricName(metric, key string) string {
	if metric == "Temperature" {
		// Thermal zones have numeric keys appended as is, hardware monitors after an underscore.
		if isNumeric(key) {
			return metric + key
		}
		return metric + "_" + key
	}
	affix, ok := legacyAsyncMetricAffixes[metric]
	if !ok {
		return ""
	}
	return affix.prefix + key + affix.suffix
}

type legacyAffix struct {
	prefix string
	suffix string
}

var legacyAsyncMetricAffixes = buildLegacyAsyncMetricAffixes()

func buildLegacyAsyncMetricAffixes() map[string]legacyAffix {
	res := map[string]legacyAffix{}

	// The key was appended to the name: OSUserTimeCPU3
	for _, metric := range strings.Fields(`
		OSUserTimeCPU OSNiceTimeCPU OSSystemTimeCPU OSIdleTimeCPU OSIOWaitTimeCPU
		OSIrqTimeCPU OSSoftIrqTimeCPU OSStealTimeCPU OSGuestTimeCPU OSGuestNiceTimeCPU`) {
		res[metric] = legacyAffix{prefix: metric}
	}

	// The key was appended after an underscore: BlockReadBytes_sda
	for _, metric := range strings.Fields(`
		CPUFrequencyMHz
		BlockReadOps BlockWriteOps BlockDiscardOps
		BlockReadMerges BlockWriteMerges BlockDiscardMerges
		BlockReadBytes BlockWriteBytes BlockDiscardBytes
		BlockReadTime BlockWriteTime BlockDiscardTime
		BlockInFlightOps BlockActiveTime BlockQueueTime
		BlockActiveTimePerOp BlockQueueTimePerOp
		NetworkReceiveBytes NetworkReceivePackets NetworkReceiveErrors NetworkReceiveDrop
		NetworkSendBytes NetworkSendPackets NetworkSendErrors NetworkSendDrop
		DiskTotal DiskUsed DiskAvailable DiskUnreserved
		DiskPutObjectThrottlerRPS DiskPutObjectThrottlerAvailable
		DiskGetObjectThrottlerRPS DiskGetObjectThrottlerAvailable`) {
		res[metric] = legacyAffix{prefix: metric + "_"}
	}

	// The key was in the middle of the name: EDAC0_Correctable, AsyncLoggingTextLogQueueSize
	res["EDACCorrectable"] = legacyAffix{prefix: "EDAC", suffix: "_Correctable"}
	res["EDACUncorrectable"] = legacyAffix{prefix: "EDAC", suffix: "_Uncorrectable"}
	res["AsyncLoggingQueueSize"] = legacyAffix{prefix: "AsyncLogging", suffix: "QueueSize"}

	// The key was a prefix of the name: s3_diskDeadBlobsQueueEstimate
	res["DeadBlobsQueueEstimate"] = legacyAffix{suffix: "DeadBlobsQueueEstimate"}
	res["MissingBlobsQueueEstimate"] = legacyAffix{suffix: "MissingBlobsQueueEstimate"}

	return res
}

func isNumeric(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
