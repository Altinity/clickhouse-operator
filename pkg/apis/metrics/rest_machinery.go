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

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

func makeRESTCall(chi *WatchedCHI, op string) error {
	url := "http://127.0.0.1:8888/chi"

	json, err := json.Marshal(chi)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(op, url, bytes.NewBuffer(json))
	if err != nil {
		return err
	}
	//req.SetBasicAuth(s.Username, s.Password)
	_, err = doRequest(req)

	return err
}

func doRequest(req *http.Request) ([]byte, error) {
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("NON 200 status code: %s", body)
	}

	return body, nil
}
