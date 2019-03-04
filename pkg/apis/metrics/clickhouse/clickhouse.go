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

import (
	"fmt"
	"io/ioutil"
	"net/http"
	neturl "net/url"
	"strings"
	"time"
)

const (
	queryMetricsSQL = "SELECT metric, cast(value as Float64) AS value FROM system.asynchronous_metrics " +
		"UNION ALL SELECT metric, cast(value as Float64) AS value FROM system.metrics"
)

const (
	chQueryUrlPattern     = "http://%s:8123/"
	chQueryUrlParam       = "query"
	chQueryDefaultTimeout = 10 * time.Second
)

// QueryMetricsFromCH requests metrics data from the ClickHouse database using REST interface
// data is a concealed output
func QueryMetricsFromCH(data map[string]string, hostname string) error {
	url, err := neturl.Parse(fmt.Sprintf(chQueryUrlPattern, hostname))
	if err != nil {
		return err
	}
	encodeQuery(url, queryMetricsSQL)
	return httpCall(data, url.String())
}

// encodeQuery injects SQL command into url.URL query
func encodeQuery(url *neturl.URL, sql string) {
	query := url.Query()
	query.Set(chQueryUrlParam, sql)
	url.RawQuery = query.Encode()
}

// httpCall runs HTTP request using provided URL
func httpCall(results map[string]string, url string) (err error) {
	client := &http.Client{
		Timeout: time.Duration(chQueryDefaultTimeout),
	}
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}

	for _, line := range strings.Split(string(body), "\n") {
		pairs := strings.Split(line, "\t")
		if len(pairs) < 2 {
			continue
		}
		results[pairs[0]] = pairs[1]
	}

	return nil
}
