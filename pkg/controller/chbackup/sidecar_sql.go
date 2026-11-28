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

package chbackup

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	chdriver "github.com/mailru/go-clickhouse/v2"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
)

type actionRow struct {
	Command string
	Start   string
	Finish  string
	Status  string
	Error   string
}

const (
	actionInProgress = "in progress"
	actionSuccess    = "success"
)

func (row *actionRow) running() bool {
	return (row.Finish == "") || strings.HasPrefix(row.Finish, "1970-01-01") || (row.Status == actionInProgress)
}

type backupRow struct {
	Name           string
	Location       string
	Required       string
	Desc           string
	DataSize       uint64
	ObjectDiskSize uint64
	MetadataSize   uint64
	RBACSize       uint64
	Size           uint64
	// Created is zero when the row tells no time.
	Created time.Time
}

func (row *backupRow) broken() bool {
	return strings.HasPrefix(row.Desc, "broken")
}

func (row *backupRow) holdsData() bool {
	return (row.DataSize != 0) || (row.ObjectDiskSize != 0)
}

func (row *backupRow) sized() bool {
	return (row.DataSize != 0) || (row.ObjectDiskSize != 0) || (row.MetadataSize != 0) || (row.RBACSize != 0)
}

type sidecars interface {
	// Now returns the ClickHouse time of the host in UTC, as "YYYY-MM-DD hh:mm:ss".
	Now(ctx context.Context, host string) (string, error)
	Push(ctx context.Context, host, command string, receiveTimeout time.Duration) error
	// Action returns the first row of the command that started at timeFrom or later, or nil.
	Action(ctx context.Context, host, command, timeFrom string) (*actionRow, error)
	Actions(ctx context.Context, host, timeFrom string) ([]actionRow, error)
	Busy(ctx context.Context, host string) (bool, error)
	Running(ctx context.Context, host string) ([]actionRow, error)
	List(ctx context.Context, host, location string) ([]backupRow, error)
	Ready(ctx context.Context, host string) error
}

var (
	errSidecarBusy        = errors.New("the sidecar runs another operation")
	errNoClickHouseAccess = errors.New("the operator configuration sets no ClickHouse username and password, " +
		"clickhouse.access.username and password or the Secret clickhouse.access.secret names: backup uses no default credentials")
)

// Such queries can list the remote storage, so the limit is long.
const sidecarQueryTimeout = 10 * time.Minute

// A synchronous delete answers only when done, so its limit is the run HTTP receive timeout plus sidecarPushMargin.
const (
	sidecarPushTimeout = 30 * time.Second
	sidecarPushMargin  = 30 * time.Second
)

type sqlSidecars struct {
	endpoint func(host string) model.HostEndpoint
}

func newSQLSidecars(endpoint func(host string) model.HostEndpoint) (*sqlSidecars, error) {
	if err := clickHouseAccess(); err != nil {
		return nil, err
	}
	return &sqlSidecars{endpoint: endpoint}, nil
}

// Default ClickHouse credentials do not count.
func clickHouseAccess() error {
	if !chop.Config().HasExplicitClickHouseAccess() {
		return errNoClickHouseAccess
	}
	return nil
}

func (s *sqlSidecars) connection(host string) *clickhouse.Connection {
	params := clickhouse.NewClusterConnectionParamsFromCHOpConfig(chop.Config())
	params.SetQueryTimeout(sidecarQueryTimeout)
	address := host
	if s.endpoint != nil {
		endpoint := s.endpoint(host)
		address = endpoint.Address
		params.OverlayClusterSecurityTLS(endpoint.TLS)
		switch params.Scheme {
		case api.ChSchemeAuto:
			switch {
			case endpoint.HTTPPort != 0:
				params.Scheme, params.Port = api.ChSchemeHTTP, endpoint.HTTPPort
			case endpoint.HTTPSPort != 0:
				params.Scheme, params.Port = api.ChSchemeHTTPS, endpoint.HTTPSPort
			}
		case api.ChSchemeHTTP:
			if endpoint.HTTPPort != 0 {
				params.Port = endpoint.HTTPPort
			}
		case api.ChSchemeHTTPS:
			if endpoint.HTTPSPort != 0 {
				params.Port = endpoint.HTTPSPort
			}
		}
	}
	return clickhouse.GetPooledDBConnection(params.NewEndpointConnectionParams(address))
}

var quoter = strings.NewReplacer(`\`, `\\`, `'`, `\'`)

func quote(value string) string {
	return "'" + quoter.Replace(value) + "'"
}

func (s *sqlSidecars) rows(ctx context.Context, host, query string) ([]map[string]string, error) {
	result, err := s.connection(host).QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	if result == nil || result.Rows == nil {
		return nil, nil
	}
	columns, err := result.Rows.Columns()
	if err != nil {
		return nil, err
	}
	var rows []map[string]string
	for result.Rows.Next() {
		values := make([]sql.NullString, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := result.Rows.Scan(pointers...); err != nil {
			return nil, err
		}
		row := map[string]string{}
		for i, column := range columns {
			row[column] = values[i].String
		}
		rows = append(rows, row)
	}
	return rows, result.Rows.Err()
}

func (s *sqlSidecars) Now(ctx context.Context, host string) (string, error) {
	rows, err := s.rows(ctx, host, "SELECT toString(now('UTC')) AS t")
	if (err != nil) || (len(rows) == 0) {
		return "", cmpErr(err, "no time")
	}
	return rows[0]["t"], nil
}

func pushQuery(command string, receiveTimeout time.Duration) string {
	timeout := strconv.FormatInt(int64(max(receiveTimeout, time.Second)/time.Second), 10)
	return "INSERT INTO system.backup_actions (command) SELECT " + quote(command) +
		" SETTINGS async_insert = 0, http_receive_timeout = " + timeout + ", http_send_timeout = " + timeout
}

func (s *sqlSidecars) Push(ctx context.Context, host, command string, receiveTimeout time.Duration) error {
	timeout := sidecarPushTimeout
	if synchronous(command) {
		timeout = max(receiveTimeout+sidecarPushMargin, sidecarPushTimeout)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	opts := clickhouse.NewQueryOptions().SetSilent(true)
	opts.SetQueryTimeout(timeout)
	err := s.connection(host).Exec(ctx, pushQuery(command, receiveTimeout), opts)
	if (err != nil) && busyError(err) {
		return fmt.Errorf("%w: %v", errSidecarBusy, err)
	}
	return err
}

// sidecarBusyText is the clickhouse-backup HTTP 500 text for ErrAPILocked.
const sidecarBusyText = "another operation is currently running"

// busyError matches the text only, because a status code alone could match digits of a backup name or date.
func busyError(err error) bool {
	return strings.Contains(err.Error(), sidecarBusyText)
}

// The sidecar returns start and finish in container local time without a zone.
// The operator sets TZ to UTC (see Altinity/clickhouse-backup#1687).
const actionColumns = "command, toString(start) AS start, toString(finish) AS finish, status, error"

func sinceUTC(timeFrom string) string {
	return "start >= " + quote(timeFrom)
}

func actionOf(row map[string]string) actionRow {
	return actionRow{Command: row["command"], Start: row["start"], Finish: row["finish"], Status: row["status"], Error: row["error"]}
}

func (s *sqlSidecars) Action(ctx context.Context, host, command, timeFrom string) (*actionRow, error) {
	rows, err := s.rows(ctx, host, "SELECT "+actionColumns+" FROM system.backup_actions WHERE command = "+quote(command)+
		" AND "+sinceUTC(timeFrom)+" ORDER BY start ASC LIMIT 1")
	if (err != nil) || (len(rows) == 0) {
		return nil, err
	}
	row := actionOf(rows[0])
	return &row, nil
}

func (s *sqlSidecars) Actions(ctx context.Context, host, timeFrom string) ([]actionRow, error) {
	return s.actions(ctx, host, sinceUTC(timeFrom))
}

func (s *sqlSidecars) actions(ctx context.Context, host, where string) ([]actionRow, error) {
	rows, err := s.rows(ctx, host, "SELECT "+actionColumns+" FROM system.backup_actions WHERE "+where+" ORDER BY start ASC")
	if err != nil {
		return nil, err
	}
	var actions []actionRow
	for _, row := range rows {
		actions = append(actions, actionOf(row))
	}
	return actions, nil
}

func (s *sqlSidecars) Busy(ctx context.Context, host string) (bool, error) {
	rows, err := s.rows(ctx, host, "SELECT count() AS n FROM system.backup_actions WHERE status = "+quote(actionInProgress))
	if (err != nil) || (len(rows) == 0) {
		return false, err
	}
	return rows[0]["n"] != "0", nil
}

func (s *sqlSidecars) Running(ctx context.Context, host string) ([]actionRow, error) {
	return s.actions(ctx, host, "status = "+quote(actionInProgress))
}

func (s *sqlSidecars) List(ctx context.Context, host, location string) ([]backupRow, error) {
	rows, err := s.rows(ctx, host, "SELECT *, toUnixTimestamp(created) AS created_unix FROM system.backup_list WHERE location = "+
		quote(location)+" ORDER BY created DESC")
	if err != nil {
		return nil, err
	}
	number := func(value string) uint64 {
		n, _ := strconv.ParseUint(value, 10, 64)
		return n
	}
	var backups []backupRow
	for _, row := range rows {
		var created time.Time
		if unix := number(row["created_unix"]); unix > 0 {
			created = time.Unix(int64(unix), 0).UTC()
		}
		backups = append(backups, backupRow{
			Name: row["name"], Location: row["location"], Required: row["required"], Desc: row["desc"],
			DataSize: number(row["data_size"]), ObjectDiskSize: number(row["object_disk_size"]),
			MetadataSize: number(row["metadata_size"]), RBACSize: number(row["rbac_size"]), Size: number(row["size"]),
			Created: created,
		})
	}
	return backups, nil
}

func (s *sqlSidecars) Ready(ctx context.Context, host string) error {
	rows, err := s.rows(ctx, host, "SELECT count() AS n FROM system.tables WHERE database = 'system' AND "+
		"name IN ('backup_actions', 'backup_list')")
	if err != nil {
		return fmt.Errorf("%s: ClickHouse does not answer: %w", host, err)
	}
	if (len(rows) == 0) || (rows[0]["n"] != "2") {
		return fmt.Errorf("%s: ClickHouse has no system.backup_actions or system.backup_list: the clickhouse-backup sidecar "+
			"creates them as it starts, with api.create_integration_tables", host)
	}
	if _, err := s.rows(ctx, host, "SELECT count() FROM system.backup_actions"); err != nil {
		return fmt.Errorf("%s: the clickhouse-backup sidecar does not answer system.backup_actions: %w", host, err)
	}
	return nil
}

var invalidResponseCodes = map[int]bool{
	6:   true, // CANNOT_PARSE_TEXT
	26:  true, // CANNOT_PARSE_QUOTED_STRING
	27:  true, // CANNOT_PARSE_INPUT_ASSERTION_FAILED
	38:  true, // CANNOT_PARSE_DATE
	41:  true, // CANNOT_PARSE_DATETIME
	72:  true, // CANNOT_PARSE_NUMBER
	117: true, // INCORRECT_DATA
}

var (
	clickHouseCodeRegexp = regexp.MustCompile(`(?i)(status\s+)?code:\s*(\d+)`)
	httpStatusRegexp     = regexp.MustCompile(`(?i)HTTP status code:\s*(\d+)`)
)

func describeQueryError(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	var details []string
	code := 0
	var driverErr *chdriver.Error
	if errors.As(err, &driverErr) {
		code = driverErr.Code
	} else {
		for _, match := range clickHouseCodeRegexp.FindAllStringSubmatch(text, -1) {
			if match[1] == "" {
				code, _ = strconv.Atoi(match[2])
				break
			}
		}
	}
	if code != 0 {
		details = append(details, "ClickHouse error code "+strconv.Itoa(code))
	}
	if match := httpStatusRegexp.FindStringSubmatch(text); match != nil {
		details = append(details, "HTTP status code "+match[1]+" from the sidecar")
	}
	lower := strings.ToLower(text)
	switch {
	case invalidResponseCodes[code] || strings.Contains(lower, "cannot parse") || strings.Contains(lower, "jsoneachrow"):
		details = append(details, "the response of the sidecar is not valid JSON")
	case (code == 0) && (errors.Is(err, context.DeadlineExceeded) || strings.Contains(lower, "connection refused") ||
		strings.Contains(lower, "no such host") || strings.Contains(lower, "i/o timeout") ||
		strings.Contains(lower, "transport failed") || strings.Contains(lower, "eof")):
		details = append(details, "ClickHouse does not answer")
	}
	if len(details) == 0 {
		return text
	}
	return strings.Join(details, ", ") + ": " + text
}

func cmpErr(err error, message string) error {
	if err != nil {
		return err
	}
	return errors.New(message)
}
