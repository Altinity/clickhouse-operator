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

// actionRow is a row of system.backup_actions: a command the sidecar ran or runs.
type actionRow struct {
	Command string
	Start   string
	Finish  string
	Status  string
	Error   string
}

// Statuses of a row of system.backup_actions.
const (
	actionInProgress = "in progress"
	actionSuccess    = "success"
)

// running reports whether the command of the row still runs: it has not finished, or the sidecar tells it in
// progress.
func (row *actionRow) running() bool {
	return (row.Finish == "") || strings.HasPrefix(row.Finish, "1970-01-01") || (row.Status == actionInProgress)
}

// backupRow is a row of system.backup_list: a backup a sidecar lists, local or remote.
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
}

// broken reports whether the sidecar lists the backup broken.
func (row *backupRow) broken() bool {
	return strings.HasPrefix(row.Desc, "broken")
}

// holdsData reports whether the backup holds data on its host.
func (row *backupRow) holdsData() bool {
	return (row.DataSize != 0) || (row.ObjectDiskSize != 0)
}

// sized reports whether the backup is of a size on its host: anything of it was uploaded.
func (row *backupRow) sized() bool {
	return (row.DataSize != 0) || (row.ObjectDiskSize != 0) || (row.MetadataSize != 0) || (row.RBACSize != 0)
}

// sidecars drives the clickhouse-backup sidecars of the hosts of a cluster through the SQL of their ClickHouse: the
// integration tables system.backup_actions and system.backup_list, which call the REST API of the sidecar in the pod.
type sidecars interface {
	// Now returns the time of the ClickHouse of the host, UTC, "YYYY-MM-DD hh:mm:ss"
	Now(ctx context.Context, host string) (string, error)
	// Push inserts a command into system.backup_actions of the host, with the http_receive_timeout and
	// http_send_timeout of the URL engine towards the sidecar
	Push(ctx context.Context, host, command string, receiveTimeout time.Duration) error
	// Action returns the first row of the command that started at timeFrom or later, nil for none
	Action(ctx context.Context, host, command, timeFrom string) (*actionRow, error)
	// Actions returns the rows of the commands that started at timeFrom or later, oldest first
	Actions(ctx context.Context, host, timeFrom string) ([]actionRow, error)
	// Busy reports whether the sidecar of the host runs a command
	Busy(ctx context.Context, host string) (bool, error)
	// Running returns the rows of the commands the sidecar of the host tells in progress
	Running(ctx context.Context, host string) ([]actionRow, error)
	// List returns the backups the sidecar of the host lists at the location, local or remote
	List(ctx context.Context, host, location string) ([]backupRow, error)
	// Ready reports, as an error, why the host cannot be driven: its ClickHouse does not answer, or lacks the
	// integration tables, or the sidecar does not answer them
	Ready(ctx context.Context, host string) error
}

// Errors of the sidecars.
var (
	// errSidecarBusy: the sidecar refused a command while it runs another one
	errSidecarBusy = errors.New("the sidecar runs another operation")
	// errNoClickHouseAccess: the operator configuration sets no ClickHouse credentials of its own
	errNoClickHouseAccess = errors.New("the operator configuration sets no ClickHouse username and password, " +
		"clickhouse.access.username and password or the Secret clickhouse.access.secret names: backup uses no default credentials")
)

// sidecarQueryTimeout bounds a query of the integration tables, which list the remote storage.
const sidecarQueryTimeout = 10 * time.Minute

// sidecarPushTimeout bounds the insert of an asynchronous command, which the sidecar answers at once. A synchronous
// one, a delete, which the sidecar answers once it is done, is bounded by the HTTP receive timeout of the run and
// sidecarPushMargin over it, which leaves ClickHouse the time to tell the timeout of the URL engine.
const (
	sidecarPushTimeout = 30 * time.Second
	sidecarPushMargin  = 30 * time.Second
)

// sqlSidecars drives the sidecars through the ClickHouse of their hosts with the credentials of the operator,
// clickhouse.access of its configuration.
type sqlSidecars struct {
	// endpoint returns how the operator reaches the ClickHouse of a host, nil for the host name and the operator's port
	endpoint func(host string) model.HostEndpoint
}

// newSQLSidecars returns the sidecars of the hosts as the operator reaches them, errNoClickHouseAccess when its
// configuration sets no credentials: the operator has no default ones for backup.
func newSQLSidecars(endpoint func(host string) model.HostEndpoint) (*sqlSidecars, error) {
	if err := clickHouseAccess(); err != nil {
		return nil, err
	}
	return &sqlSidecars{endpoint: endpoint}, nil
}

// clickHouseAccess tells why the operator cannot drive the sidecars, nil when it can: it needs a ClickHouse username and
// password its configuration sets itself, inline or by a Secret, never the defaults.
func clickHouseAccess() error {
	if !chop.Config().HasExplicitClickHouseAccess() {
		return errNoClickHouseAccess
	}
	return nil
}

// connection returns the connection to the ClickHouse of the host, as the operator connects for schema propagation: by
// its address, with the TLS settings of its cluster over those of the operator, and the scheme Auto resolved to the
// HTTP port of the host, else its HTTPS port.
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

// quote quotes a string literal of ClickHouse SQL.
func quote(value string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(value) + "'"
}

// rows runs a query and returns its rows as maps of the columns, every value as a string.
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

// pushQuery returns the insert of the command into system.backup_actions, with the timeouts of the URL engine towards
// the sidecar.
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

// sidecarBusyText is the text the /backup/actions endpoint of clickhouse-backup answers, with HTTP 500, for a command
// sent while the sidecar runs another operation (ErrAPILocked of clickhouse-backup).
const sidecarBusyText = "another operation is currently running"

// busyError reports whether an error of an insert into system.backup_actions tells that the sidecar runs another
// operation. It matches the text of clickhouse-backup alone: a bare status code would match the digits of a backup
// name or of a date the error echoes with the command.
func busyError(err error) bool {
	return strings.Contains(err.Error(), sidecarBusyText)
}

// actionColumns are the columns of a row of system.backup_actions. The sidecar tells start and finish as strings,
// YYYY-MM-DD hh:mm:ss in the local time of its container, without a zone, which the operator pins to UTC by TZ (see
// Altinity/clickhouse-backup#1687): they compare with timeFrom, now('UTC')
// of ClickHouse, as strings, as the Altinity usual backup workflow compares them.
const actionColumns = "command, toString(start) AS start, toString(finish) AS finish, status, error"

// sinceUTC is the condition of a row that started at the time, in UTC, or later.
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
	rows, err := s.rows(ctx, host, "SELECT "+actionColumns+" FROM system.backup_actions WHERE "+sinceUTC(timeFrom)+
		" ORDER BY start ASC")
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
	rows, err := s.rows(ctx, host, "SELECT "+actionColumns+" FROM system.backup_actions WHERE status = "+quote(actionInProgress)+
		" ORDER BY start ASC")
	if err != nil {
		return nil, err
	}
	var actions []actionRow
	for _, row := range rows {
		actions = append(actions, actionOf(row))
	}
	return actions, nil
}

func (s *sqlSidecars) List(ctx context.Context, host, location string) ([]backupRow, error) {
	rows, err := s.rows(ctx, host, "SELECT * FROM system.backup_list WHERE location = "+quote(location)+" ORDER BY created DESC")
	if err != nil {
		return nil, err
	}
	number := func(value string) uint64 {
		n, _ := strconv.ParseUint(value, 10, 64)
		return n
	}
	var backups []backupRow
	for _, row := range rows {
		backups = append(backups, backupRow{
			Name: row["name"], Location: row["location"], Required: row["required"], Desc: row["desc"],
			DataSize: number(row["data_size"]), ObjectDiskSize: number(row["object_disk_size"]),
			MetadataSize: number(row["metadata_size"]), RBACSize: number(row["rbac_size"]), Size: number(row["size"]),
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

// Error codes of ClickHouse that tell a response of the sidecar the URL engine cannot read as JSONEachRow.
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
	// clickHouseCodeRegexp finds the error code of ClickHouse in the text of an error: "Code: 86." of an exception,
	// "code: 86," as a driver tells it
	clickHouseCodeRegexp = regexp.MustCompile(`(?i)(status\s+)?code:\s*(\d+)`)
	// httpStatusRegexp finds the HTTP status code the URL engine reports of the sidecar
	httpStatusRegexp = regexp.MustCompile(`(?i)HTTP status code:\s*(\d+)`)
)

// describeQueryError tells what a failed query of the integration tables says: the error code of ClickHouse, the HTTP
// status code the URL engine reports of the sidecar, whether the response of the sidecar was no valid JSON, or that
// ClickHouse does not answer, followed by the error itself.
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

// cmpErr returns err, or an error of the message without one.
func cmpErr(err error, message string) error {
	if err != nil {
		return err
	}
	return errors.New(message)
}
