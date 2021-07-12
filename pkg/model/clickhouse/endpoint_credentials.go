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
	"strconv"
)

const (
	// http://user:password@host:8123/
	chDsnUrlPattern = "http://%s%s:%s/"

	usernameReplacer = "***"
	passwordReplacer = "***"

	dsnUsernamePasswordPairPattern             = "%s:%s@"
	dsnUsernamePasswordPairUsernameOnlyPattern = "%s@"
)

// EndpointCredentials specifies credentials to access specified endpoint
type EndpointCredentials struct {
	// External data
	hostname string
	username string
	password string
	port     int

	// Internal generated data
	dsn                  string
	dsnHiddenCredentials string
}

// NewEndpointCredentials creates new EndpointCredentials object
func NewEndpointCredentials(hostname, username, password string, port int) *EndpointCredentials {
	params := &EndpointCredentials{
		hostname: hostname,
		username: username,
		password: password,
		port:     port,
	}

	params.dsn = params.makeDSN(false)
	params.dsnHiddenCredentials = params.makeDSN(true)

	return params
}

// makeUsernamePassword makes "username:password" pair for connection
func (c *EndpointCredentials) makeUsernamePassword(hidden bool) string {

	// In case of hidden username+password pair we'd just return replacement
	if hidden {
		return fmt.Sprintf(dsnUsernamePasswordPairPattern, usernameReplacer, passwordReplacer)
	}

	// We may have neither username nor password
	if c.username == "" && c.password == "" {
		return ""
	}

	// Password may be omitted
	if c.password == "" {
		return fmt.Sprintf(dsnUsernamePasswordPairUsernameOnlyPattern, c.username)
	}

	// Expecting both username and password to be in place
	return fmt.Sprintf(dsnUsernamePasswordPairPattern, c.username, c.password)
}

// makeDSN makes ClickHouse DSN
func (c *EndpointCredentials) makeDSN(hideCredentials bool) string {
	return fmt.Sprintf(
		chDsnUrlPattern,
		c.makeUsernamePassword(hideCredentials),
		c.hostname,
		strconv.Itoa(c.port),
	)
}

// GetDSN gets DSN
func (c *EndpointCredentials) GetDSN() string {
	return c.dsn
}

// GetDSNWithHiddenCredentials gets DSN with hidden sensitive info
func (c *EndpointCredentials) GetDSNWithHiddenCredentials() string {
	return c.dsnHiddenCredentials
}
