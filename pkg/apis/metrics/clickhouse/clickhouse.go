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
	"net/url"
	"strings"
	"time"
)

const (
	queryMetricsSQL = "SELECT metric, cast(value as Float64) AS value FROM system.asynchronous_metrics " +
		"UNION ALL SELECT metric, cast(value as Float64) AS value FROM system.metrics"
)

const (
	uriPattern     = "http://%s:8123/"
	qParam         = "query"
	defaultTimeout = 10 // seconds
)

// QueryDataFrom requests data from the ClickHouse database using REST interface
func QueryDataFrom(data map[string]string, hostname string) error {
	uri, err := url.Parse(fmt.Sprintf(uriPattern, hostname))
	if err != nil {
		return err
	}
	encodeQuery(uri, queryMetricsSQL)
	return httpCall(data, uri.String())
}

// encodeQuery injects SQL command into url.URL query
func encodeQuery(uri *url.URL, sql string) {
	query := uri.Query()
	query.Set(qParam, sql)
	uri.RawQuery = query.Encode()
}

// httpCall runs HTTP request using provided URL
func httpCall(results map[string]string, url string) (err error) {
	client := &http.Client{
		Timeout: time.Duration(defaultTimeout * time.Second),
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
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
