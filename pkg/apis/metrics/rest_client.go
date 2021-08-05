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

package metrics

// InformMetricsExporterAboutWatchedCHI informs exporter about new watched CHI
func InformMetricsExporterAboutWatchedCHI(namespace, chiName string, hostnames []string) error {
	chi := &WatchedCHI{
		Namespace: namespace,
		Name:      chiName,
		Hostnames: hostnames,
	}
	return makeRESTCall(chi, "POST")
}

// InformMetricsExporterToDeleteWatchedCHI informs exporter to delete/forget watched CHI
func InformMetricsExporterToDeleteWatchedCHI(namespace, chiName string) error {
	chi := &WatchedCHI{
		Namespace: namespace,
		Name:      chiName,
		Hostnames: []string{},
	}
	return makeRESTCall(chi, "DELETE")
}
