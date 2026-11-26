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

import "testing"

// The cases mirror src/Common/tests/gtest_async_metrics_key_values_mode.cpp of ClickHouse.
func TestLegacyAsyncMetricName(t *testing.T) {
	for _, c := range []struct{ metric, key, want string }{
		{"OSUserTimeCPU", "3", "OSUserTimeCPU3"},
		{"OSGuestNiceTimeCPU", "11", "OSGuestNiceTimeCPU11"},
		{"BlockReadBytes", "sda", "BlockReadBytes_sda"},
		{"BlockActiveTimePerOp", "nvme0n1", "BlockActiveTimePerOp_nvme0n1"},
		{"NetworkReceiveDrop", "eth0", "NetworkReceiveDrop_eth0"},
		{"CPUFrequencyMHz", "7", "CPUFrequencyMHz_7"},
		{"DiskTotal", "default", "DiskTotal_default"},
		{"DiskGetObjectThrottlerRPS", "s3", "DiskGetObjectThrottlerRPS_s3"},
		{"EDACCorrectable", "0", "EDAC0_Correctable"},
		{"EDACUncorrectable", "1", "EDAC1_Uncorrectable"},
		{"AsyncLoggingQueueSize", "TextLog", "AsyncLoggingTextLogQueueSize"},
		{"DeadBlobsQueueEstimate", "s3_disk", "s3_diskDeadBlobsQueueEstimate"},
		{"MissingBlobsQueueEstimate", "s3_disk", "s3_diskMissingBlobsQueueEstimate"},
		{"Temperature", "3", "Temperature3"},
		{"Temperature", "coretemp_Core_0", "Temperature_coretemp_Core_0"},
		{"Temperature", "0x_sensor", "Temperature_0x_sensor"},
		{"SomeFamilyAddedLater", "key", ""},
		{"Uptime", "", ""},
	} {
		if got := legacyAsyncMetricName(c.metric, c.key); got != c.want {
			t.Errorf("legacyAsyncMetricName(%q, %q) = %q, want %q", c.metric, c.key, got, c.want)
		}
	}
}

// TestLegacyAsyncMetricNamesCount guards against dropping a family while editing the table.
func TestLegacyAsyncMetricNamesCount(t *testing.T) {
	// 49 families in the table plus Temperature, handled separately
	if got := len(legacyAsyncMetricAffixes); got != 49 {
		t.Fatalf("expected 49 families, got %d", got)
	}
}
