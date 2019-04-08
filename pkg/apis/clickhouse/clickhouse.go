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
	"github.com/golang/glog"
	"io/ioutil"
	"net/http"
	neturl "net/url"
	"strings"
	"time"
)

const (
	chQueryUrlPattern     = "http://%s:8123/"
	chQueryUrlParam       = "query"
	chQueryDefaultTimeout = 10 * time.Second
)

// Query runs given sql and writes results into data
func Query(data *[][]string, sql string, hostname string) error {
	url, err := neturl.Parse(fmt.Sprintf(chQueryUrlPattern, hostname))
	if err != nil {
		return err
	}
	encodeQuery(url, sql)
	httpCall(data, url.String())
	// glog.Infof("Loaded %d rows", len(*data))
	return nil
}

// encodeQuery injects SQL command into url.URL query
func encodeQuery(url *neturl.URL, sql string) {
	query := url.Query()
	query.Set(chQueryUrlParam, sql)
	url.RawQuery = query.Encode()
}

// httpCall runs HTTP request using provided URL
func httpCall(results *[][]string, url string) error {
	// glog.Infof("HTTP GET %s\n", url)
	client := &http.Client{
		Timeout: time.Duration(chQueryDefaultTimeout),
	}
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		glog.Error(err)
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		glog.Error(err)
		return err
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		glog.Error(err)
		return err
	}

	for _, line := range strings.Split(string(body), "\n") {
		rows := strings.Split(line, "\t")
		*results = append(*results, rows)
	}
	// glog.Infof("Loaded %d rows", len(*results))

	return nil
}
