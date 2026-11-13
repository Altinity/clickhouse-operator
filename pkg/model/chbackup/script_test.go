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
	"cmp"
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
)

const (
	scriptTestAPIPort = "7171"
	scriptTestUser    = "backup-api"
	// Colon, quotes and a command substitution must all survive the Authorization header untouched
	scriptTestPassword = `p:a"s's$(touch /tmp/x)`
)

// stubBackup is a row of the clickhouse-backup 2.8.1 /backup/list response
type stubBackup struct {
	Name           string `json:"name"`
	Created        string `json:"created"`
	DataSize       uint64 `json:"data_size,omitempty"`
	ObjectDiskSize uint64 `json:"object_disk_size,omitempty"`
	MetadataSize   uint64 `json:"metadata_size"`
	RBACSize       uint64 `json:"rbac_size,omitempty"`
	Location       string `json:"location"`
	Required       string `json:"required"`
	Desc           string `json:"desc"`
}

// sizelessBackup is a backup list row as clickhouse-backup before 2.6.6 sends it, without sizes
type sizelessBackup struct {
	Name     string `json:"name"`
	Created  string `json:"created"`
	Location string `json:"location"`
	Required string `json:"required"`
	Desc     string `json:"desc"`
}

// stubHost is the state of one emulated clickhouse-backup sidecar
type stubHost struct {
	remote []stubBackup
	local  []stubBackup
	// tables lists the "db.table" names /backup/tables/all reports on top of the system tables
	tables []string
	// sizes maps a "db.table" name of tables to the size /backup/tables/all reports of it, 0 when absent
	sizes map[string]uint64
	// engines maps a "db.table" name of tables to the engine /backup/tables/all reports of it, MergeTree
	// when absent
	engines map[string]string
	// uuids maps a "db.table" name of tables to the UUID /backup/tables/all reports of it
	uuids map[string]string
	// rawTables are /backup/tables/all rows sent verbatim
	rawTables []string
	// busyPolls is the number of /backup/actions answers that report an operation in progress
	busyPolls int
	// lockedPosts is the number of POSTs answered 423 Locked, lockedActions that of the commands of the actions endpoint
	lockedPosts   int
	lockedActions int
	// unavailable is the number of requests answered 503, a negative value means every request
	unavailable int
	// unavailableAfterPost is the number of requests answered 503 once the host accepted a POST, a
	// negative value means every request
	unavailableAfterPost int
	// posted tells whether the host accepted a POST, or postsBeforePosted of them when set
	posted            bool
	posts             int
	postsBeforePosted int
	// hangUpPosts makes the host accept every POST and close the connection without answering
	hangUpPosts bool
	// emptyBackups makes every backup the host creates hold no data, as of tables that match nothing
	emptyBackups bool
	// sizelessList makes the host list its remote backups without sizes, as clickhouse-backup before 2.6.6 does
	sizelessList bool
	// objectDiskBackups makes every backup the host creates hold its data on an object disk alone
	objectDiskBackups bool
	// brokenBackups makes every backup the host creates list as broken, without sizes, as a copy
	// whose upload did not complete does
	brokenBackups bool
	// listsUnavailableAfterPost makes the host answer every remote backup list 503 once it accepted a
	// POST, as a sidecar that went away right after the backup
	listsUnavailableAfterPost bool
	// failSubmit, when set, is the error the host refuses every asynchronous operation with, HTTP 500
	failSubmit string
	// failPostsAfter is the number of POSTs hangUpPosts and proxyFailsPosts let through first
	failPostsAfter int
	// proxyFailsPosts makes a proxy in front of the host answer every POST 503, which tells nothing
	// about whether the host received it
	proxyFailsPosts bool
	// status, when set, answers every request with this HTTP status
	status int
	// failOp maps an asynchronous operation (create_remote, download, restore_remote) to the error it ends with
	failOp map[string]string
	// lostOps makes every status lookup find no operation, as after a sidecar restart
	lostOps bool
	// resume is the status an upload or download the sidecar resumes after that restart ends with,
	// "" when it resumes nothing
	resume string
	// completedBeforeRestart makes an operation lost to a restart complete before the restart
	completedBeforeRestart bool
	// resumed lists the actions the sidecar resumed after a restart
	resumed []*stubResumed
	// resumeDelay is the number of /backup/actions answers a resumed action takes to appear. Listing
	// backups meanwhile abandons it, as 2.8.1 resumes nothing while another action is in progress
	resumeDelay int
	// pending is the resumed action waiting for resumeDelay
	pending *stubResumed
	// restartAgainAfter makes the sidecar restart again once a resumed action has run that long,
	// forgetting it and resuming it anew
	restartAgainAfter time.Duration
	// deleteFail maps a backup name to the error its remote deletion fails with
	deleteFail map[string]string
	// leavesFailedUpload makes a create_remote that fails leave what clickhouse-backup 2.8.3 leaves of an upload
	// that fails: the local backup, and the remote one listed as broken
	leavesFailedUpload bool
	// noLocalOnDownload makes a download finish without a local copy
	noLocalOnDownload bool
	// backupTables lists the "db.table" names GET /backup/tables?remote_backup= reports of the backup
	backupTables []string
	// rawBackupTables are GET /backup/tables?remote_backup= rows sent verbatim
	rawBackupTables []string
	// backupTablesFail is the error GET /backup/tables?remote_backup= fails with
	backupTablesFail string
	// actions are the rows the actions log holds of the commands POST /backup/actions accepted
	actions []*stubAction
	// failAction is the error every command POST /backup/actions accepts ends with
	failAction string
	// failRestoreAction is the error every restore or restore_remote command of the actions endpoint ends with,
	// once restore_remote downloaded the backup, which leaves the downloaded copy behind
	failRestoreAction string
	// remoteListStatus, when set, answers every list of the remote backups with this HTTP status
	remoteListStatus int
	// paths maps a storage path other than the host's own to the backups there: a command of the actions
	// endpoint whose --env flags set the storage path reads the backups of that value of it, others read remote
	paths map[string][]stubBackup
	// schemaCopies holds the names of the local copies a download of the actions endpoint made of the
	// schema alone: a restore_remote takes such a copy for one it downloaded in part and restores no data,
	// which the stub fails
	schemaCopies map[string]bool
	// loseActions makes the sidecar restart right after it accepted a command, which it does not resume
	loseActions bool
	// restartAfter is the number of operations and actions the host completes before its sidecar
	// restarts, forgetting them
	restartAfter int
	// completed is the number of operations and actions the host completed
	completed int
	// epoch counts the restarts of the sidecar: it knows the operations and actions of its own alone
	epoch int
}

type stubOp struct {
	host   string
	kind   string
	name   string
	params url.Values
	polls  int
	done   bool
	epoch  int
	// fails is the error the operation ends with, as the host was when it accepted it
	fails string
}

// stubAction is a row of the actions log of a command POST /backup/actions accepted: it has a
// start time, and no operation id
type stubAction struct {
	command string
	start   string
	status  string
	err     string
	polls   int
	epoch   int
	// fails is the error the command ends with, as the host was when it accepted it
	fails string
}

// stubResumed is an upload or download clickhouse-backup 2.8.1 resumes after a restart: an action
// of its own, without an operation id, whose command ends with "--resumable=1 <name>"
type stubResumed struct {
	command string
	kind    string
	name    string
	polls   int
	final   string
	done    bool
	since   time.Time
}

// stubCall records one request received by the stub
type stubCall struct {
	host   string
	method string
	path   string
	query  url.Values
	// busy tells whether a /backup/actions answer reported an operation in progress
	busy bool
	// command is the command of a POST /backup/actions
	command string
}

func (c stubCall) String() string {
	return c.host + " " + c.method + " " + c.path
}

// sidecarStub emulates the clickhouse-backup 2.8.1 REST API of several hosts. The Job script reaches it
// as an HTTP proxy, so every host name of the script lands here and is told apart by the request URL.
type sidecarStub struct {
	t            *testing.T
	mu           sync.Mutex
	hosts        map[string]*stubHost
	ops          map[string]*stubOp
	calls        []stubCall
	authFailures int
	nextOp       int
	nextAction   int
	server       *httptest.Server
}

func newSidecarStub(t *testing.T, hosts map[string]*stubHost) *sidecarStub {
	t.Helper()
	requireOrchestrator(t)
	s := &sidecarStub{t: t, hosts: hosts, ops: map[string]*stubOp{}}
	s.server = httptest.NewServer(s)
	t.Cleanup(s.server.Close)
	return s
}

func writeRows[T any](w http.ResponseWriter, code int, rows ...T) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(code)
	for _, row := range rows {
		out, _ := json.Marshal(row)
		_, _ = fmt.Fprintln(w, string(out))
	}
}

func writeError(w http.ResponseWriter, code int, operation, err string) {
	writeRows(w, code, map[string]string{"status": "error", "operation": operation, "error": err})
}

func (s *sidecarStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	host, port, err := net.SplitHostPort(r.URL.Host)
	if (err != nil) || (port != scriptTestAPIPort) {
		s.t.Errorf("request is not a proxied call to the API port: %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	user, pass, ok := r.BasicAuth()
	if !ok || (user != scriptTestUser) || (pass != scriptTestPassword) {
		s.authFailures++
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = fmt.Fprintln(w, "401 Unauthorized")
		return
	}
	call := stubCall{host: host, method: r.Method, path: r.URL.Path, query: r.URL.Query()}
	if (r.Method == http.MethodPost) && (r.URL.Path == "/backup/actions") {
		call.command = s.actionCommand(r)
	}
	h, known := s.hosts[host]
	if !known {
		s.calls = append(s.calls, call)
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	if h.status != 0 {
		s.calls = append(s.calls, call)
		writeError(w, h.status, "", "rejected by the test")
		return
	}
	if h.unavailable != 0 {
		s.calls = append(s.calls, call)
		if h.unavailable > 0 {
			h.unavailable--
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if h.posted && (h.unavailableAfterPost != 0) {
		s.calls = append(s.calls, call)
		if h.unavailableAfterPost > 0 {
			h.unavailableAfterPost--
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if (r.Method == http.MethodGet) && (r.URL.Path == "/backup/actions") {
		call.busy = h.busyPolls > 0
	}
	s.calls = append(s.calls, call)

	segments := strings.Split(strings.TrimPrefix(r.URL.Path, "/backup/"), "/")
	switch r.Method {
	case http.MethodGet:
		s.serveGet(w, h, call, segments)
	case http.MethodPost:
		if (h.lockedActions > 0) && (segments[0] == "actions") {
			h.lockedActions--
			writeError(w, http.StatusInternalServerError, segments[0], "another operation is currently running")
			return
		}
		if h.lockedPosts > 0 {
			h.lockedPosts--
			// clickhouse-backup 2.8.1 refuses a command of the actions endpoint with 500, others with 423
			code := http.StatusLocked
			if segments[0] == "actions" {
				code = http.StatusInternalServerError
			}
			writeError(w, code, segments[0], "another operation is currently running")
			return
		}
		failing := h.posts >= h.failPostsAfter
		if h.proxyFailsPosts && failing {
			h.posts++
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		h.posts++
		h.posted = h.posts >= h.postsBeforePosted
		if h.hangUpPosts && failing {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				s.t.Errorf("cannot hang up on %s %s: %v", r.Method, r.URL, err)
				return
			}
			_ = conn.Close()
			return
		}
		s.servePost(w, h, call, segments)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *sidecarStub) serveGet(w http.ResponseWriter, h *stubHost, call stubCall, segments []string) {
	type actionRow struct {
		Command     string `json:"command"`
		Status      string `json:"status"`
		Start       string `json:"start,omitempty"`
		Error       string `json:"error,omitempty"`
		OperationID string `json:"operation_id,omitempty"`
	}
	// tableRow is a row of /backup/tables/all: clickhouse-backup 2.8.1 sends clickhouse.Table, which
	// has no JSON tags, with its fields in this order
	type tableRow struct {
		Database         string
		Name             string
		Engine           string
		DataPath         string
		DataPaths        []string
		UUID             string
		CreateTableQuery string
		TotalBytes       uint64
		StoragePolicy    string
		Skip             bool
		BackupType       string
	}
	switch {
	case segments[0] == "actions":
		h.resumePending()
		// Finished rows that merely mention "in progress" must not read as a running operation
		rows := []actionRow{
			{Command: `create_remote --tables="x\"status\":\"in progress\"" old`, Status: "success"},
			{Command: "download older", Status: "error", Error: `"status":"in progress"`},
		}
		if h.busyPolls > 0 {
			h.busyPolls--
			rows = append(rows, actionRow{Command: "create_remote other", Status: "in progress"})
		}
		for _, r := range h.resumed {
			row := actionRow{Command: r.command, Status: "in progress"}
			switch {
			case r.polls > 0:
				r.polls--
			case (h.restartAgainAfter > 0) && (r.final == "success"):
				// Still in progress when the sidecar restarts again
			case r.final == "success":
				row.Status = r.final
				if !r.done {
					completeStubOp(h, r.kind, r.name)
					r.done = true
				}
			default:
				row.Status, row.Error = r.final, "resumed "+r.command+" failed"
			}
			rows = append(rows, row)
		}
		for _, a := range h.actions {
			if a.epoch != h.epoch {
				continue
			}
			if a.status == "in progress" {
				if a.polls > 0 {
					a.polls--
				} else {
					a.status, a.err = "success", cmp.Or(h.failAction, a.fails)
					if fields := strings.Fields(a.command); (a.err == "") && (h.failRestoreAction != "") && ((fields[0] == "restore_remote") || (fields[0] == "restore")) {
						completeStubOp(h, "restore_remote", fields[len(fields)-1])
						a.err = h.failRestoreAction
					}
					if a.err != "" {
						a.status = "error"
					}
					if a.status == "success" {
						h.completeAction(strings.Fields(a.command))
						defer h.stepDone()
					}
				}
			}
			rows = append(rows, actionRow{Command: a.command, Status: a.status, Start: a.start, Error: a.err})
		}
		writeRows(w, http.StatusOK, rows...)
	case segments[0] == "version":
		writeRows(w, http.StatusOK, map[string]string{"version": "2.8.1"})
	case (segments[0] == "list") && (len(segments) == 2) && (segments[1] == "remote"):
		h.pending = nil
		if h.remoteListStatus != 0 {
			writeError(w, h.remoteListStatus, "list", "can't connect to remote storage")
			return
		}
		if h.posted && h.listsUnavailableAfterPost {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if h.sizelessList {
			rows := make([]sizelessBackup, 0, len(h.remote))
			for _, b := range h.remote {
				rows = append(rows, sizelessBackup{Name: b.Name, Created: b.Created, Location: b.Location, Required: b.Required, Desc: b.Desc})
			}
			writeRows(w, http.StatusOK, rows...)
			return
		}
		writeRows(w, http.StatusOK, h.remote...)
	case (segments[0] == "list") && (len(segments) == 2) && (segments[1] == "local"):
		h.pending = nil
		writeRows(w, http.StatusOK, h.local...)
	case (segments[0] == "tables") && (len(segments) == 1) && call.query.Has("remote_backup"):
		s.serveBackupTables(w, h, call.query.Get("table"))
	// clickhouse-backup 2.8.1 lists the same tables of a local copy as of the remote backup it copies
	case (segments[0] == "tables") && (len(segments) == 1) && call.query.Has("local_backup"):
		if name := call.query.Get("local_backup"); findStubBackup(h.local, name) < 0 {
			writeError(w, http.StatusInternalServerError, "tables", fmt.Sprintf("'%s' is not found on local storage", name))
			return
		}
		s.serveBackupTables(w, h, call.query.Get("table"))
	// The live tables a pattern matches, which takes a query of ClickHouse: the system ones are left out
	case (segments[0] == "tables") && (len(segments) == 1):
		writeRows[map[string]string](w, http.StatusOK)
	case (segments[0] == "tables") && (len(segments) == 2) && (segments[1] == "all"):
		w.WriteHeader(http.StatusOK)
		rows := []tableRow{
			{Database: "system", Name: "one", Engine: "SystemOne"},
			{Database: "INFORMATION_SCHEMA", Name: "tables", Engine: "View"},
			{Database: "information_schema", Name: "tables", Engine: "View"},
		}
		for _, table := range h.tables {
			db, name, _ := strings.Cut(table, ".")
			rows = append(rows, tableRow{
				Database: db,
				Name:     name,
				Engine:   cmp.Or(h.engines[table], "MergeTree"),
				UUID:     h.uuids[table],
				// A definition that mentions a size must not read as the size of the table
				CreateTableQuery: "CREATE TABLE " + table + ` (s String DEFAULT '"TotalBytes":99999999999999999999') ENGINE = MergeTree ORDER BY s`,
				TotalBytes:       h.sizes[table],
				DataPaths:        []string{},
				BackupType:       "full",
			})
		}
		for _, row := range rows {
			out, _ := json.Marshal(row)
			_, _ = fmt.Fprintln(w, string(out))
		}
		for _, row := range h.rawTables {
			_, _ = fmt.Fprintln(w, row)
		}
	case segments[0] == "status":
		s.serveStatus(w, h, call.query.Get("operationid"))
	default:
		writeError(w, http.StatusNotFound, "", "GET "+call.path+" 404 Not Found")
	}
}

func (s *sidecarStub) serveStatus(w http.ResponseWriter, h *stubHost, id string) {
	op, found := s.ops[id]
	if !found || h.lostOps || (op.epoch != h.epoch) {
		w.WriteHeader(http.StatusOK)
		return
	}
	row := map[string]string{
		// The command echoes user input, which must not confuse the status parser
		"command":      fmt.Sprintf("%s --tables=%q %s", op.kind, op.params.Get("table"), op.name),
		"status":       "in progress",
		"operation_id": id,
	}
	if op.polls > 0 {
		op.polls--
		writeRows(w, http.StatusOK, row)
		return
	}
	if failure, failed := h.failOp[op.kind]; failed || (op.fails != "") {
		if (op.kind == "create_remote") && h.leavesFailedUpload && !op.done {
			op.done = true
			h.local = append(h.local, local(op.name))
			h.remote = append(h.remote, broken(op.name))
		}
		row["status"] = "error"
		row["error"] = cmp.Or(failure, op.fails)
		writeRows(w, http.StatusOK, row)
		return
	}
	// The sidecar keeps the row of a finished operation, which a later lookup finds as it ended
	row["status"] = "success"
	if !op.done {
		op.done = true
		completeStubOp(h, op.kind, op.name, op.params.Get("diff-from-remote"))
		defer h.stepDone()
	}
	writeRows(w, http.StatusOK, row)
}

// stepDone counts an operation or action the host completed, and restarts the sidecar once it
// completed restartAfter of them
func (h *stubHost) stepDone() {
	h.completed++
	if h.completed == h.restartAfter {
		h.epoch++
	}
}

// serveBackupTables answers GET /backup/tables?remote_backup= with the tables of the backup that
// match the comma-separated patterns, as clickhouse-backup 2.8.1 filters them
func (s *sidecarStub) serveBackupTables(w http.ResponseWriter, h *stubHost, pattern string) {
	type backupTableRow struct {
		Database   string   `json:"database"`
		Table      string   `json:"table"`
		TotalBytes uint64   `json:"total_bytes"`
		Size       string   `json:"size"`
		Parts      int      `json:"parts"`
		Disks      []string `json:"disks"`
		Skip       bool     `json:"skip"`
	}
	if h.backupTablesFail != "" {
		writeError(w, http.StatusInternalServerError, "tables", h.backupTablesFail)
		return
	}
	var rows []backupTableRow
	for _, table := range h.backupTables {
		matched := pattern == ""
		for _, p := range strings.Split(pattern, ",") {
			if ok, _ := filepath.Match(strings.TrimSpace(p), table); ok {
				matched = true
			}
		}
		if matched {
			db, name, _ := strings.Cut(table, ".")
			rows = append(rows, backupTableRow{Database: db, Table: name, Size: "0B", Disks: []string{}})
		}
	}
	writeRows(w, http.StatusOK, rows...)
	for _, row := range h.rawBackupTables {
		_, _ = fmt.Fprintln(w, row)
	}
}

// actionCommand returns the command of a POST /backup/actions request: the "command" of its one
// JSON line, empty when the body is anything else
func (s *sidecarStub) actionCommand(r *http.Request) string {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return ""
	}
	var row struct {
		Command string `json:"command"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if (decoder.Decode(&row) != nil) || decoder.More() || (strings.Count(strings.TrimSpace(string(body)), "\n") > 0) {
		return ""
	}
	return row.Command
}

// completeStubOp applies what a successful operation of the given kind leaves behind
func completeStubOp(h *stubHost, kind, name string, base ...string) {
	switch kind {
	case "create_remote", "upload":
		backup := remote(name, strings.Join(base, ""))
		switch {
		case h.brokenBackups:
			backup = broken(name)
		case h.emptyBackups:
			backup.DataSize = 0
		case h.objectDiskBackups:
			backup.DataSize, backup.ObjectDiskSize = 0, stubDataSize
		}
		h.remote = append(h.remote, backup)
	case "download", "restore_remote":
		if i := findStubBackup(h.local, name); !h.noLocalOnDownload && (i >= 0) {
			h.local[i] = local(name)
		} else if !h.noLocalOnDownload {
			h.local = append(h.local, local(name))
		}
	}
}

// restart emulates a sidecar restart during an operation: its status is lost, and an interrupted
// upload or download is resumed under a new action, as clickhouse-backup 2.8.1 does
func (h *stubHost) restart(kind, name string, params url.Values) {
	if h.completedBeforeRestart {
		completeStubOp(h, kind, name, params.Get("diff-from-remote"))
		return
	}
	resumed := map[string]string{"create_remote": "upload", "download": "download", "restore_remote": "download"}[kind]
	if (h.resume == "") || (resumed == "") {
		return
	}
	command := resumed
	if base := params.Get("diff-from-remote"); base != "" {
		command += " --diff-from-remote=" + base
	}
	command += " --resumable=1 " + name
	action := &stubResumed{command: command, kind: resumed, name: name, polls: 1, final: h.resume}
	if h.resumeDelay > 0 {
		h.pending = action
		return
	}
	action.since = time.Now()
	h.resumed = append(h.resumed, action)
}

// stubResumeAgainDelay is the resumeDelay of the resume after a second restart
const stubResumeAgainDelay = 3

// resumePending lets a pending resumed action appear once its delay is over, and restarts the sidecar
// again once a resumed action ran restartAgainAfter
func (h *stubHost) resumePending() {
	if (h.restartAgainAfter > 0) && (len(h.resumed) > 0) && (time.Since(h.resumed[0].since) >= h.restartAgainAfter) {
		h.restartAgainAfter = 0
		h.pending, h.resumed = h.resumed[0], nil
		h.pending.polls = 1
		h.resumeDelay = stubResumeAgainDelay
	}
	if h.pending == nil {
		return
	}
	if h.resumeDelay > 0 {
		h.resumeDelay--
		return
	}
	h.pending.since = time.Now()
	h.resumed, h.pending = append(h.resumed, h.pending), nil
}

func (s *sidecarStub) servePost(w http.ResponseWriter, h *stubHost, call stubCall, segments []string) {
	startOp := func(kind, name string, code int) {
		if h.failSubmit != "" {
			writeError(w, http.StatusInternalServerError, kind, h.failSubmit)
			return
		}
		s.nextOp++
		id := fmt.Sprintf("op-%d", s.nextOp)
		op := &stubOp{host: call.host, kind: kind, name: name, params: call.query, polls: 1, epoch: h.epoch}
		// clickhouse-backup 2.8.1 downloads no backup it holds a copy of, and restores none it holds no copy of
		switch {
		// An incomplete copy, listed broken, is no copy: its download resumes
		case (kind == "download") && (findStubBackup(h.local, name) >= 0) && !strings.HasPrefix(h.local[findStubBackup(h.local, name)].Desc, "broken"):
			op.fails = fmt.Sprintf("'%s' already exists", name)
		case (kind == "download") && (findStubBackup(h.remote, name) < 0):
			op.fails = fmt.Sprintf("'%s' is not found on remote storage", name)
		case (kind == "restore") && (findStubBackup(h.local, name) < 0):
			op.fails = fmt.Sprintf("'%s' is not found on local storage", name)
		}
		s.ops[id] = op
		if h.lostOps {
			h.restart(kind, name, call.query)
		}
		writeRows(w, code, map[string]string{"status": "acknowledged", "operation": kind, "backup_name": name, "operation_id": id})
	}
	switch {
	case segments[0] == "actions":
		// clickhouse-backup 2.8.1 takes the flags of a command after its name alone
		fields := strings.Fields(call.command)
		if (len(fields) < 2) || ((fields[0] != "restore_remote") && (fields[0] != "download") && (fields[0] != "restore") &&
			(fields[0] != "create_remote")) {
			writeError(w, http.StatusBadRequest, call.command, "unknown command")
			return
		}
		s.nextAction++
		h.actions = append(h.actions, &stubAction{
			command: call.command,
			start:   fmt.Sprintf("2026-01-01 00:%02d:%02d", s.nextAction/60, s.nextAction%60),
			status:  "in progress",
			polls:   1,
			epoch:   h.epoch,
			fails:   h.actionFailure(fields),
		})
		if h.loseActions {
			h.epoch++
			// A create_remote whose upload the sidecar resumes, without its --env flags
			if fields[0] == "create_remote" {
				params := url.Values{}
				for _, field := range fields {
					if base, found := strings.CutPrefix(field, "--diff-from-remote="); found {
						params.Set("diff-from-remote", base)
					}
				}
				h.restart("create_remote", fields[len(fields)-1], params)
			}
		}
		writeRows(w, http.StatusOK, map[string]string{"status": "acknowledged", "operation": call.command})
	case segments[0] == "create_remote":
		startOp("create_remote", call.query.Get("name"), http.StatusCreated)
	case (segments[0] == "download") && (len(segments) == 2):
		startOp("download", segments[1], http.StatusOK)
	case (segments[0] == "restore_remote") && (len(segments) == 2):
		startOp("restore_remote", segments[1], http.StatusOK)
	case (segments[0] == "restore") && (len(segments) == 2):
		startOp("restore", segments[1], http.StatusOK)
	case (segments[0] == "delete") && (len(segments) == 3):
		list := &h.remote
		if segments[1] == "local" {
			list = &h.local
		}
		name := segments[2]
		if failure, failed := h.deleteFail[name]; failed && (segments[1] == "remote") {
			writeError(w, http.StatusInternalServerError, "delete", failure)
			return
		}
		i := findStubBackup(*list, name)
		if i < 0 {
			writeError(w, http.StatusInternalServerError, "delete", fmt.Sprintf("'%s' is not found on %s storage", name, segments[1]))
			return
		}
		*list = append((*list)[:i], (*list)[i+1:]...)
		if segments[1] == "local" {
			delete(h.schemaCopies, name)
		}
		writeRows(w, http.StatusOK, map[string]string{"status": "success", "operation": "delete", "backup_name": name, "location": segments[1]})
	default:
		writeError(w, http.StatusNotFound, "", "POST "+call.path+" 404 Not Found")
	}
}

// readBackups returns the backups at the storage path a command of the actions endpoint reads: the
// value its --env flags set the storage path to, or the host's own
func (h *stubHost) readBackups(fields []string) []stubBackup {
	for _, field := range fields {
		setting, found := strings.CutPrefix(field, "--env=")
		key, value, _ := strings.Cut(setting, "=")
		if found && strings.HasSuffix(key, "_PATH") && !strings.HasSuffix(key, "_OBJECT_DISK_PATH") {
			return h.paths[value]
		}
	}
	return h.remote
}

// actionFailure returns the error a command of the actions endpoint ends with on the host, empty when
// it succeeds: a backup missing at the storage path it reads, a download of a backup the host holds a
// local copy of, or a restore that would take the copy a download made of the schema alone for its own
func (h *stubHost) actionFailure(fields []string) string {
	name := fields[len(fields)-1]
	switch {
	case fields[0] == "create_remote":
		return h.failOp["create_remote"]
	case (fields[0] == "restore") && (findStubBackup(h.local, name) < 0):
		return fmt.Sprintf("'%s' is not found on local storage", name)
	case (fields[0] == "restore") && h.schemaCopies[name] && slices.Contains(fields, "--data"):
		return fmt.Sprintf("the local copy of %s holds the schema alone: no data was restored", name)
	case fields[0] == "restore":
		return ""
	case findStubBackup(h.readBackups(fields), name) < 0:
		return fmt.Sprintf("'%s' is not found on remote storage", name)
	case (fields[0] == "download") && (findStubBackup(h.local, name) >= 0):
		return fmt.Sprintf("'%s' already exists", name)
	case (fields[0] == "restore_remote") && h.schemaCopies[name]:
		return fmt.Sprintf("the local copy of %s holds the schema alone: no data was restored", name)
	}
	return ""
}

// completeAction applies what a command of the actions endpoint that succeeded leaves behind: a
// download, a local copy of the backup at the storage path it read, which lists the size of the access
// entities only when it downloaded them; a restore, the copy it downloaded
func (h *stubHost) completeAction(fields []string) {
	name := fields[len(fields)-1]
	if fields[0] == "create_remote" {
		base := ""
		for _, field := range fields {
			if value, found := strings.CutPrefix(field, "--diff-from-remote="); found {
				base = value
			}
		}
		completeStubOp(h, "create_remote", name, base)
		return
	}
	if fields[0] != "download" {
		completeStubOp(h, fields[0], name)
		return
	}
	backups := h.readBackups(fields)
	backup := local(name)
	backup.MetadataSize = backups[findStubBackup(backups, name)].MetadataSize
	backup.Required = backups[findStubBackup(backups, name)].Required
	if slices.Contains(fields, "--rbac") {
		backup.RBACSize = backups[findStubBackup(backups, name)].RBACSize
	}
	h.local = append(h.local, backup)
	if slices.Contains(fields, "--schema") {
		if h.schemaCopies == nil {
			h.schemaCopies = map[string]bool{}
		}
		h.schemaCopies[name] = true
	}
}

func findStubBackup(list []stubBackup, name string) int {
	for i := range list {
		if list[i].Name == name {
			return i
		}
	}
	return -1
}

// run executes orchestratorScript under bash with only the given environment on top of the API settings
func (s *sidecarStub) run(env map[string]string) (int, string) {
	s.t.Helper()
	return s.runScript(orchestratorScript, env)
}

// runScript executes script, a variant of orchestratorScript, as run does
func (s *sidecarStub) runScript(script string, env map[string]string) (int, string) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	vars := map[string]string{
		"PATH":          os.Getenv("PATH"),
		"http_proxy":    s.server.URL,
		envAPIPort:      scriptTestAPIPort,
		envAPIUsername:  scriptTestUser,
		envAPIPassword:  scriptTestPassword,
		envPollInterval: "0.01",
	}
	for name, value := range env {
		vars[name] = value
	}
	// The script runs from a file, as the Job runs it: as an argument it would exceed the length Linux allows one,
	// MAX_ARG_STRLEN
	cmd := scriptCommand(ctx, s.t, script)
	for name, value := range vars {
		cmd.Env = append(cmd.Env, name+"="+value)
	}
	out, err := cmd.CombinedOutput()
	require.NoError(s.t, ctx.Err(), "the Job script did not finish:\n%s", out)
	code := 0
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		code = exitErr.ExitCode()
	} else {
		require.NoError(s.t, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Zero(s.t, s.authFailures, "requests without the API credentials:\n%s", out)
	return code, string(out)
}

// significant lists the calls without the idle and status polls
func (s *sidecarStub) significant() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var calls []string
	for _, c := range s.calls {
		if (c.method == http.MethodGet) && ((c.path == "/backup/actions") || (c.path == "/backup/status")) {
			continue
		}
		calls = append(calls, c.String())
	}
	return calls
}

// matching returns the calls of the given method whose path starts with prefix
func (s *sidecarStub) matching(method, prefix string) []stubCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	var calls []stubCall
	for _, c := range s.calls {
		if (c.method == method) && strings.HasPrefix(c.path, prefix) {
			calls = append(calls, c)
		}
	}
	return calls
}

func (s *sidecarStub) countCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *sidecarStub) remoteNames(host string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var names []string
	for _, b := range s.hosts[host].remote {
		names = append(names, b.Name)
	}
	return names
}

// Sizes of the backups the stub lists: one that holds data, and the metadata every backup has
const (
	stubDataSize     = 1024
	stubMetadataSize = 128
)

// withoutData returns the backup as a host lists it whose tables hold no data
func withoutData(backup stubBackup) stubBackup {
	backup.DataSize = 0
	return backup
}

func broken(name string) stubBackup {
	return stubBackup{Name: name, Location: "remote", Desc: "broken (can't stat metadata.json)"}
}

func remote(name, required string) stubBackup {
	return stubBackup{Name: name, DataSize: stubDataSize, MetadataSize: stubMetadataSize, Location: "remote", Required: required, Desc: "tar, regular"}
}

func local(name string) stubBackup {
	return stubBackup{Name: name, Location: "local", Desc: "regular"}
}

func TestJobScriptSyntax(t *testing.T) {
	bashOnly(t)
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not available")
	}
	cmd := exec.Command("bash", "-n")
	cmd.Stdin = strings.NewReader(orchestratorScript)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
}

// declaredScriptEnv are the variables of the environment of a Job the orchestrator scripts read
var declaredScriptEnv = []string{
	envOperation, envAPIPort, envAPIUsername, envAPIPassword, envPollInterval, envTerminationMessagePath,
	envCluster, envBackupName, envBackupNamePrefix, envBackupHosts, envRetentionHosts,
	envSchemaOnly, envTables, envPartitions, envDiffFromRemote, envNoDataMessage,
	envSchedules, envScheduleTimezone, envCronJobName, envJobName, envRestoreInProgress, envBackupTransport,
	envBackupPodWaitSeconds, envEventObjectAPIVersion, envEventObjectKind, envEventObjectName, envEventObjectUID,
	envPodNamespace, envPodName,
	envVerifyHosts, envSchemaHosts, envDataHosts, envGuardHosts, envOverwrite, envValidateTopology,
	envRestoreRBAC, envRBACNotRestoredMessage, envUnchangedExitCode, envReplicated, envMaxTableSizeToDrop,
	envPathClusters, envPathSelf, envPathInstallation, envPathNamespace, envPathReplicaSelection,
	envDefaultPath, envDefaultObjectDiskPath, envSourceInstallation, envSourceNamespace, envSkipTables, envSkipTableEngines,
	envDatabaseMapping, envBackupFallbackHosts, envHostsUsedMessage, envBackupNaming, envShardNumbers, envNamingInstallation,
	envNamingNamespace, envKeepBackups, envPreviousSchedules, envHeldBackupsMessage, envConfigOverrides,
}

func TestJobScriptReadsDeclaredEnv(t *testing.T) {
	bashOnly(t)
	for _, name := range declaredScriptEnv {
		require.Regexp(t, regexp.MustCompile(`\$\{!?`+name+`\b|\b(parse_hosts|is_true|check_name|default_path "\$\{index\}") `+name+`\b`), orchestratorScript, name)
	}
	for _, operation := range []string{operationBackup, operationVerify, operationRestore} {
		require.Contains(t, orchestratorScript, "\n\t"+operation+") do_"+operation+" ;;")
	}
	require.NotRegexp(t, `\bKEEP_LAST\b`, orchestratorScript, "KEEP_LAST is no longer read")
	for _, transport := range []string{backupTransportSidecar, backupTransportPod} {
		require.Regexp(t, `\s`+transport+`\) `, orchestratorScript)
	}
}

// The storage path check reads the settings of every cluster under the names pathClusterEnv gives
// them, the operator's builder names them by: the script knows exactly the keys PathCheckKeys lists,
// and the replica selections of the API.
func TestJobScriptReadsThePathCheckEnv(t *testing.T) {
	bashOnly(t)
	keys := regexp.MustCompile(`(?m)^PATH_CHECK_KEYS=\(([^)]*)\)$`).FindStringSubmatch(orchestratorScript)
	require.NotNil(t, keys, "the script lists its keys in PATH_CHECK_KEYS")
	require.Equal(t, PathCheckKeys, strings.Fields(keys[1]))
	require.Equal(t, "REMOTE_STORAGE", PathCheckKeys[0])
	for _, prefix := range []string{"S3", "GCS", "AZBLOB", "COS", "FTP", "SFTP"} {
		require.Contains(t, PathCheckKeys, prefix+"_PATH")
		require.Contains(t, PathCheckKeys, prefix+"_OBJECT_DISK_PATH")
		require.Contains(t, orchestratorScript, ") REPLY="+prefix+" ;;")
	}

	require.Equal(t, "PATH_12_S3_PATH", pathClusterEnv(12, "S3_PATH"))
	require.Contains(t, orchestratorScript, `local name="`+pathEnvPrefix+`$1_$2"`)
	require.Contains(t, orchestratorScript, `local own="`+pathEnvPrefix+`$1_$2"`)
	for _, name := range []string{pathEnvCluster, pathEnvShards, pathEnvUnverifiable} {
		require.Contains(t, orchestratorScript, `path_setting "${index}" `+name+"\n")
	}
	require.Contains(t, orchestratorScript, `"" | `+string(api.ReplicaSelectionFirstPerShard)+` | `+string(api.ReplicaSelectionAllReplicas)+`) ;;`)
	require.Contains(t, orchestratorScript, `${selection} == `+string(api.ReplicaSelectionAllReplicas)+` `)
}

func TestJobScriptBackup(t *testing.T) {
	s := newSidecarStub(t, map[string]*stubHost{
		"chi-a-0-0": {remote: []stubBackup{remote("base", "")}},
		"chi-a-1-0": {remote: []stubBackup{remote("base", "")}},
	})
	code, out := s.run(map[string]string{
		envOperation:      operationBackup,
		envBackupName:     "bk-1",
		envBackupHosts:    "chi-a-0-0 chi-a-1-0",
		envSchemaOnly:     "true",
		envTables:         "db.*",
		envPartitions:     "202601,202602",
		envDiffFromRemote: "base",
	})
	require.Zero(t, code, out)
	require.Equal(t, []string{
		"chi-a-0-0 GET /backup/list/remote",
		"chi-a-1-0 GET /backup/list/remote",
		"chi-a-0-0 GET /backup/list/remote",
		"chi-a-0-0 POST /backup/create_remote",
		"chi-a-1-0 GET /backup/list/remote",
		"chi-a-1-0 POST /backup/create_remote",
	}, s.significant())
	for _, c := range s.matching(http.MethodPost, "/backup/create_remote") {
		require.Equal(t, url.Values{
			"name":             {"bk-1"},
			"schema":           {""},
			"table":            {"db.*"},
			"partitions":       {"202601,202602"},
			"diff-from-remote": {"base"},
		}, c.query)
	}
	require.Equal(t, []string{"base", "bk-1"}, s.remoteNames("chi-a-0-0"))
	require.Equal(t, []string{"base", "bk-1"}, s.remoteNames("chi-a-1-0"))
}

// A backup of one replica per shard runs on the preferred replica of every shard, backup.preferredReplica, as the
// Altinity Cloud Manager backs up its backupReplica: a preferred replica whose ClickHouse does not answer leaves the
// backup of its shard to the first replica, whose storage path is the shard's too.
func TestJobScriptBacksUpThePreferredReplica(t *testing.T) {
	s := newSidecarStub(t, map[string]*stubHost{"a1": {}, "b1": {status: http.StatusInternalServerError}, "a0": {}, "b0": {}})
	termination := filepath.Join(t.TempDir(), "termination-log")
	code, out := s.run(map[string]string{envOperation: operationBackup, envBackupName: "bk-1",
		envBackupHosts: "a1 b1", envBackupFallbackHosts: "a0 b0", envTerminationMessagePath: termination,
		envHostsUsedMessage: HostsUsedMessage})
	require.Zero(t, code, out)
	require.Contains(t, out, "WARNING: b1: the preferred replica does not answer (curl exit 0, HTTP 500), backing up b0 instead")
	// The operator records the replicas the backup was made on, as the termination message tells them
	message, err := os.ReadFile(termination)
	require.NoError(t, err)
	pod := &core.Pod{Status: core.PodStatus{ContainerStatuses: []core.ContainerStatus{{Name: jobContainerName,
		State: core.ContainerState{Terminated: &core.ContainerStateTerminated{Message: string(message)}}}}}}
	require.Equal(t, []string{"a1", "b0"}, JobHostsUsed(pod), string(message))
	require.False(t, JobBackupHoldsNoData(pod))
	require.Equal(t, []string{"bk-1"}, s.remoteNames("a1"))
	require.Equal(t, []string{"bk-1"}, s.remoteNames("b0"))
	require.Empty(t, s.remoteNames("a0"))
	for _, c := range s.matching(http.MethodGet, "/backup/tables") {
		require.Equal(t, url.Values{"table": {"system.one"}}, c.query)
	}

	// Every preferred replica answers: the termination message tells nothing
	s = newSidecarStub(t, map[string]*stubHost{"a1": {}, "b1": {}, "a0": {}, "b0": {}})
	termination = filepath.Join(t.TempDir(), "termination-log")
	code, out = s.run(map[string]string{envOperation: operationBackup, envBackupName: "bk-1",
		envBackupHosts: "a1 b1", envBackupFallbackHosts: "a0 b0", envTerminationMessagePath: termination,
		envHostsUsedMessage: HostsUsedMessage})
	require.Zero(t, code, out)
	require.NoFileExists(t, termination)

	// Fallback hosts of another count are refused
	s = newSidecarStub(t, map[string]*stubHost{"a1": {}})
	code, out = s.run(map[string]string{envOperation: operationBackup, envBackupName: "bk-1",
		envBackupHosts: "a1", envBackupFallbackHosts: "a0 b0", envUnchangedExitCode: "3"})
	require.Equal(t, 3, code, out)
	require.Contains(t, out, "BACKUP_FALLBACK_HOSTS must name a host for every host of BACKUP_HOSTS")
}

func TestJobScriptBackupWithoutOptions(t *testing.T) {
	s := newSidecarStub(t, map[string]*stubHost{"chi-a-0-0": {}})
	code, out := s.run(map[string]string{
		envOperation:   operationBackup,
		envBackupName:  "bk-1",
		envBackupHosts: "chi-a-0-0",
		envSchemaOnly:  "false",
	})
	require.Zero(t, code, out)
	creates := s.matching(http.MethodPost, "/backup/create_remote")
	require.Len(t, creates, 1)
	require.Equal(t, url.Values{"name": {"bk-1"}}, creates[0].query)
}

func TestJobScriptBackupScheduleName(t *testing.T) {
	s := newSidecarStub(t, map[string]*stubHost{"chi-a-0-0": {}})
	code, out := s.run(map[string]string{
		envOperation:        operationBackup,
		envBackupNamePrefix: "nightly.db",
		envBackupHosts:      "chi-a-0-0",
	})
	require.Zero(t, code, out)
	creates := s.matching(http.MethodPost, "/backup/create_remote")
	require.Len(t, creates, 1)
	require.Regexp(t, `^nightly\.db-[0-9]{14}$`, creates[0].query.Get("name"))
}

func TestJobScriptBackupRefusals(t *testing.T) {
	tests := []struct {
		name    string
		hosts   map[string]*stubHost
		env     map[string]string
		output  string
		creates int
	}{
		{
			name:   "name already in the storage of a later host",
			hosts:  map[string]*stubHost{"h0": {}, "h1": {remote: []stubBackup{remote("bk-1", "")}}},
			env:    map[string]string{envBackupName: "bk-1", envBackupHosts: "h0 h1"},
			output: "backup bk-1 already exists in the storage of h1",
		},
		{
			name:   "incremental base missing on a later host",
			hosts:  map[string]*stubHost{"h0": {remote: []stubBackup{remote("base", "")}}, "h1": {}},
			env:    map[string]string{envBackupName: "bk-1", envBackupHosts: "h0 h1", envDiffFromRemote: "base"},
			output: "base backup base is missing in the storage of h1",
		},
		{
			name:   "a host listed twice",
			hosts:  map[string]*stubHost{"h0": {}},
			env:    map[string]string{envBackupName: "bk-1", envBackupHosts: "h0 h0"},
			output: "BACKUP_HOSTS lists h0 twice: hosts of one backup must not share a storage path",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSidecarStub(t, tt.hosts)
			tt.env[envOperation] = operationBackup
			code, out := s.run(tt.env)
			require.Equal(t, 1, code, out)
			require.Contains(t, out, tt.output)
			require.Len(t, s.matching(http.MethodPost, "/"), tt.creates)
		})
	}
}

func TestJobScriptEmptyHostList(t *testing.T) {
	tests := map[string]map[string]string{
		"backup":            {envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: " "},
		"verify":            {envOperation: operationVerify, envBackupName: "bk-1", envVerifyHosts: ""},
		"restore schema":    {envOperation: operationRestore, envBackupName: "bk-1", envSchemaHosts: "", envDataHosts: "h0"},
		"restore data":      {envOperation: operationRestore, envBackupName: "bk-1", envSchemaHosts: "h0", envDataHosts: ""},
		"restore data only": {envOperation: operationRestore, envBackupName: "bk-1", envSchemaHosts: "h0", envSchemaOnly: "false"},
	}
	for name, env := range tests {
		t.Run(name, func(t *testing.T) {
			s := newSidecarStub(t, map[string]*stubHost{"h0": {}})
			code, out := s.run(env)
			require.Equal(t, 1, code, out)
			require.Contains(t, out, "is empty: no target hosts")
			require.Zero(t, s.countCalls())
		})
	}
}

func TestJobScriptRejectsInvalidEnv(t *testing.T) {
	base := map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0"}
	tests := map[string]map[string]string{
		"unknown operation":     {envOperation: "drop"},
		"no operation":          {envOperation: ""},
		"port":                  {envAPIPort: "7171/x"},
		"poll interval":         {envPollInterval: "1;x"},
		"no password":           {envAPIPassword: ""},
		"boolean":               {envSchemaOnly: "yes please"},
		"schedules":             {envSchedules: "period=daily keep=1"},
		"a schedule time":       {envSchedules: "period=daily time=5:00 keep=1"},
		"restore in progress":   {envRestoreInProgress: "maybe"},
		"a time zone":           {envBackupName: "", envBackupNamePrefix: "daily", envScheduleTimezone: "Mars/Olympus"},
		"a transport":           {envBackupTransport: "carrier-pigeon"},
		"a pod wait":            {envBackupTransport: backupTransportPod, envBackupPodWaitSeconds: "soon"},
		"no backup name":        {envBackupName: ""},
		"consecutive dots":      {envBackupName: "a..b"},
		"name with a slash":     {envBackupName: "a/b"},
		"name with a space":     {envBackupName: "a b"},
		"prefix":                {envBackupName: "", envBackupNamePrefix: "-x"},
		"diff from remote":      {envDiffFromRemote: "../x"},
		"host with a semicolon": {envBackupHosts: "h0;id"},
		"host with a slash":     {envBackupHosts: "h0/x"},
		"host with a port":      {envBackupHosts: "h0:1"},
	}
	for name, override := range tests {
		t.Run(name, func(t *testing.T) {
			s := newSidecarStub(t, map[string]*stubHost{"h0": {}})
			env := map[string]string{}
			for k, v := range base {
				env[k] = v
			}
			for k, v := range override {
				env[k] = v
			}
			code, out := s.run(env)
			require.Equal(t, 1, code, out)
			require.Contains(t, out, "ERROR:")
			require.Zero(t, s.countCalls())
		})
	}
}

func TestJobScriptQueuesWhileSidecarBusy(t *testing.T) {
	s := newSidecarStub(t, map[string]*stubHost{"h0": {busyPolls: 3, lockedPosts: 2}})
	code, out := s.run(map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0"})
	require.Zero(t, code, out)
	require.Contains(t, out, "another operation is in progress, waiting")
	require.Contains(t, out, "sidecar busy, queueing create_remote")
	require.Len(t, s.matching(http.MethodPost, "/backup/create_remote"), 3, "two 423 answers, then accepted")

	s.mu.Lock()
	defer s.mu.Unlock()
	busy, idlePolls := false, 0
	for _, c := range s.calls {
		switch {
		case c.path == "/backup/actions":
			busy = c.busy
			if !c.busy {
				idlePolls++
			}
		case c.method == http.MethodPost:
			require.False(t, busy, "submitted while the sidecar reported a running operation")
		}
	}
	require.Equal(t, 3, idlePolls, "every submission waits for an idle answer")
}

func TestJobScriptIgnoresFinishedActions(t *testing.T) {
	s := newSidecarStub(t, map[string]*stubHost{"h0": {}})
	code, out := s.run(map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0"})
	require.Zero(t, code, out)
	require.Len(t, s.matching(http.MethodGet, "/backup/actions"), 1)
}

func TestJobScriptRetriesUnreachableSidecar(t *testing.T) {
	s := newSidecarStub(t, map[string]*stubHost{"h0": {unavailable: 2}})
	code, out := s.run(map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0"})
	require.Zero(t, code, out)
	require.Contains(t, out, "sidecar unreachable")
	require.Equal(t, []string{"bk-1"}, s.remoteNames("h0"))
}

// scriptWithTimeout returns script with the timeout variable name set to seconds
func scriptWithTimeout(t *testing.T, script, name string, seconds int) string {
	t.Helper()
	setting := regexp.MustCompile(`(?m)^` + name + `=[0-9]+$`)
	require.Len(t, setting.FindAllString(script, -1), 1, name)
	return setting.ReplaceAllString(script, fmt.Sprintf("%s=%d", name, seconds))
}

// scriptGivingUpAfter returns orchestratorScript with the time it waits for an unreachable sidecar before
// anything has changed set to seconds
func scriptGivingUpAfter(t *testing.T, seconds int) string {
	t.Helper()
	return scriptWithTimeout(t, orchestratorScript, "UNREACHABLE_TIMEOUT", seconds)
}

// While nothing has changed, a sidecar that does not answer, such as one of a stopped host, fails
// the Job rather than holding it, and every other operation on the installation, until its deadline.
func TestJobScriptGivesUpOnAnUnreachableSidecar(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"backup":  {envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0 down"},
		"verify":  {envOperation: operationVerify, envBackupName: "bk-1", envVerifyHosts: "down"},
		"restore": restoreEnv(map[string]string{envSchemaHosts: "s0", envDataHosts: "s0 down", envOverwrite: "true"}),
	} {
		t.Run(name, func(t *testing.T) {
			hosts := restoreHosts()
			hosts["h0"] = &stubHost{}
			hosts["down"] = &stubHost{unavailable: -1}
			s := newSidecarStub(t, hosts)
			code, out := s.runScript(scriptGivingUpAfter(t, 0), env)
			require.Equal(t, 1, code, out)
			require.Contains(t, out, "down: sidecar unreachable for")
			require.Contains(t, out, "nothing was changed")
			require.Empty(t, s.matching(http.MethodPost, "/"))
		})
	}
}

// Once a sidecar accepted a change, giving up would leave a partial backup or restore behind, so a
// sidecar that stops answering, as one restarting does, is waited for.
func TestJobScriptWaitsForAnUnreachableSidecarAfterAChange(t *testing.T) {
	s := newSidecarStub(t, map[string]*stubHost{"h0": {unavailableAfterPost: 3}})
	code, out := s.runScript(scriptGivingUpAfter(t, 0), map[string]string{
		envOperation:   operationBackup,
		envBackupName:  "bk-1",
		envBackupHosts: "h0",
	})
	require.Zero(t, code, out)
	require.Contains(t, out, "sidecar unreachable")
	require.Equal(t, []string{"bk-1"}, s.remoteNames("h0"))
}

// A sidecar that stays away after a change, as one of a host scaled away or stopped mid-operation
// does, fails the Job after a longer wait instead of holding every other operation on the
// installation until the Job deadline.
func TestJobScriptGivesUpOnASidecarGoneAfterAChange(t *testing.T) {
	for name, tc := range map[string]struct {
		env  map[string]string
		gone string
		// posts is the number of POSTs the host accepts before it is gone
		posts int
	}{
		"backup": {
			env:  map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0 h1"},
			gone: "h1",
		},
		// The download of the backup changes nothing, the data restore after it does
		"restore": {
			env:   restoreEnv(nil),
			gone:  "d1",
			posts: 2,
		},
	} {
		t.Run(name, func(t *testing.T) {
			hosts := restoreHosts()
			hosts["h0"], hosts["h1"] = &stubHost{}, &stubHost{}
			hosts[tc.gone].unavailableAfterPost = -1
			hosts[tc.gone].postsBeforePosted = tc.posts
			s := newSidecarStub(t, hosts)
			script := scriptWithTimeout(t, scriptGivingUpAfter(t, 0), "MUTATED_UNREACHABLE_TIMEOUT", 0)
			code, out := s.runScript(script, tc.env)
			require.Equal(t, 1, code, out)
			require.Contains(t, out, tc.gone+": sidecar unreachable for")
			require.Contains(t, out, "the "+tc.env[envOperation]+" is partially applied")
		})
	}
}

func TestJobScriptOperationFailure(t *testing.T) {
	t.Run("operation error", func(t *testing.T) {
		s := newSidecarStub(t, map[string]*stubHost{
			"h0": {failOp: map[string]string{"create_remote": "can't upload: disk full"}},
			"h1": {},
		})
		code, out := s.run(map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0 h1"})
		require.Equal(t, 1, code, out)
		require.Contains(t, out, "ERROR: backup bk-1 failed on h0: h0: create_remote bk-1 ended with status 'error': can't upload: disk full")
		// Every host makes its part at once: the others finish all the same
		require.Len(t, s.matching(http.MethodPost, "/backup/create_remote"), 2)
		require.Equal(t, []string{"bk-1"}, s.remoteNames("h1"))
	})
	t.Run("errors of several hosts", func(t *testing.T) {
		s := newSidecarStub(t, map[string]*stubHost{
			"h0": {failOp: map[string]string{"create_remote": "disk full"}},
			"h1": {},
			"h2": {failOp: map[string]string{"create_remote": "no access"}},
		})
		code, out := s.run(map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0 h1 h2"})
		require.Equal(t, 1, code, out)
		require.Contains(t, out, "ERROR: backup bk-1 failed on h0, h2: h0: create_remote bk-1 ended with status 'error': disk full; "+
			"h2: create_remote bk-1 ended with status 'error': no access")
		require.Equal(t, []string{"bk-1"}, s.remoteNames("h1"))
	})
	t.Run("a submission that fails", func(t *testing.T) {
		s := newSidecarStub(t, map[string]*stubHost{"h0": {}, "h1": {}, "h2": {}})
		s.hosts["h1"].failSubmit = "can't create: no such table"
		code, out := s.run(map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0 h1 h2"})
		require.Equal(t, 1, code, out)
		require.Contains(t, out, "ERROR: backup bk-1 failed on h1: h1: create_remote rejected (HTTP 500): can't create: no such table")
		// No host after it gets the backup submitted, those before it finish theirs; what the refused one may have
		// left is deleted
		require.Equal(t, []string{"h0 POST /backup/create_remote", "h1 POST /backup/create_remote",
			"h1 POST /backup/delete/local/bk-1", "h1 POST /backup/delete/remote/bk-1"}, func() []string {
			var calls []string
			for _, c := range s.matching(http.MethodPost, "/") {
				calls = append(calls, c.String())
			}
			return calls
		}())
		require.Equal(t, []string{"bk-1"}, s.remoteNames("h0"))
	})
	t.Run("user input echoed in the status command", func(t *testing.T) {
		s := newSidecarStub(t, map[string]*stubHost{"h0": {failOp: map[string]string{"create_remote": "no tables for backup"}}})
		code, out := s.run(map[string]string{
			envOperation:   operationBackup,
			envBackupName:  "bk-1",
			envBackupHosts: "h0",
			envTables:      `x","status":"success`,
		})
		require.Equal(t, 1, code, out)
		require.Contains(t, out, "no tables for backup")
	})
}

// clickhouse-backup deletes the local backup of a create_remote whose create fails, but not that of one whose upload
// fails, nor the part of the remote backup it uploaded, which lists as broken: the Job deletes both on the host it
// failed on, a copy that is not there being no failure, and fails all the same.
func TestJobScriptDeletesWhatAFailedBackupLeft(t *testing.T) {
	s := newSidecarStub(t, map[string]*stubHost{
		"h0": {failOp: map[string]string{"create_remote": "can't upload: connection reset"}, leavesFailedUpload: true},
		"h1": {failOp: map[string]string{"create_remote": "can't create: no tables"}},
		"h2": {},
	})
	code, out := s.run(map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0 h1 h2"})
	require.Equal(t, 1, code, out)
	require.Contains(t, out, "ERROR: backup bk-1 failed on h0, h1: ")
	require.Contains(t, out, "h0: the local copy of the failed backup bk-1 deleted")
	require.Contains(t, out, "h0: the remote copy of the failed backup bk-1 deleted")
	require.Contains(t, out, "h1: no remote copy of the failed backup bk-1 to delete")
	require.Empty(t, s.hosts["h0"].local)
	require.Empty(t, s.remoteNames("h0"))
	require.Equal(t, []string{"bk-1"}, s.remoteNames("h2"), "a host that made its part keeps it")
	for _, c := range s.matching(http.MethodPost, "/backup/delete/") {
		require.NotEqual(t, "h2", c.host)
	}

	// A copy that cannot be deleted is left with a warning
	s = newSidecarStub(t, map[string]*stubHost{"h0": {failOp: map[string]string{"create_remote": "can't upload"}, leavesFailedUpload: true,
		deleteFail: map[string]string{"bk-1": "access denied"}}})
	code, out = s.run(map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0"})
	require.Equal(t, 1, code, out)
	require.Contains(t, out, "WARNING: h0: the remote copy of the failed backup bk-1 is left: ")
	require.Contains(t, out, "ERROR: backup bk-1 failed on h0: h0: create_remote bk-1 ended with status 'error': can't upload")
}

// scheduledEnv returns the environment of a scheduled backup of the series "daily" on hosts, which are its retention
// hosts too, of the slot due at the RFC 3339 time slot, with the schedule entries schedules
func scheduledEnv(hosts, schedules, slot string) map[string]string {
	return map[string]string{
		envOperation:        operationBackup,
		envBackupNamePrefix: "daily",
		envBackupHosts:      hosts,
		envRetentionHosts:   hosts,
		envSchedules:        schedules,
		envCronJobName:      testCronJobName,
		envJobName:          slotJobName(slot),
	}
}

// testCronJobName is the CronJob of the scheduled runs of the tests
const testCronJobName = "daily-main-backup"

// slotJobName returns the name the CronJob controller gives the Job of testCronJobName of the slot due at slot, unix
// seconds or RFC 3339: "<CronJob>-<minutes since the epoch>"
func slotJobName(slot string) string {
	seconds, err := strconv.ParseInt(slot, 10, 64)
	if err != nil {
		due, err := time.Parse(time.RFC3339, slot)
		if err != nil {
			panic(err)
		}
		seconds = due.Unix()
	}
	return fmt.Sprintf("%s-%d", testCronJobName, seconds/60)
}

// dailyKeeping returns a schedule of one daily entry at midnight that keeps keep backups
func dailyKeeping(keep int) string {
	return "period=daily time=00:00 keep=" + strconv.Itoa(keep)
}

// Retention after a scheduled backup, as the cleanup of remote backups of the Altinity Cloud Manager: every entry of the
// schedule keeps its newest backups of its slots, the newest backup and the one just made are kept in any case, a
// backup made at once from the schedule is kept while it is no older than the oldest scheduled backup kept, and the rest
// of them goes, newest first: an incremental before its base, which clickhouse-backup rebases its dependents on. A name
// of another form is never touched, and a failure only warns.
func TestJobScriptRetention(t *testing.T) {
	backups := func() []stubBackup {
		return []stubBackup{
			remote("daily-20200101000000", ""),
			remote("daily-20200102000000", ""),
			remote("daily-20200103000000", "daily-20200101000000"),
			remote("daily-20200103T120000", ""),
			remote("daily-x-20200101000000", ""),
			remote("dailyx-20200101000000", ""),
			remote("daily-20200101-000000", ""),
			remote("manual-1", "daily-20200102000000"),
			remote("daily-20200104000000", ""),
			remote("daily-20200104T120000", "daily-20200104000000"),
		}
	}
	s := newSidecarStub(t, map[string]*stubHost{
		"h0": {remote: backups()},
		"h1": {remote: backups(), deleteFail: map[string]string{"daily-20200101000000": "access denied"}},
	})
	env := scheduledEnv("h0 h1", dailyKeeping(2), "2020-01-05T00:00:00Z")
	env[envRetentionHosts] = "h0 h1 bad;host"
	code, out := s.run(env)
	require.Zero(t, code, "retention failures never fail the backup:\n%s", out)
	creates := s.matching(http.MethodPost, "/backup/create_remote")
	require.Len(t, creates, 2)
	made := creates[0].query.Get("name")
	require.Equal(t, "daily-20200105000000", made)
	require.Contains(t, out, "access denied")
	require.Contains(t, out, "retention skipped an invalid host name")

	var deletes []string
	for _, c := range s.matching(http.MethodPost, "/backup/delete/") {
		deletes = append(deletes, c.host+" "+c.path)
	}
	obsolete := []string{"daily-20200103T120000", "daily-20200103000000", "daily-20200102000000", "daily-20200101000000"}
	var want []string
	for _, host := range []string{"h0", "h1"} {
		for _, name := range obsolete {
			want = append(want, host+" /backup/delete/remote/"+name)
		}
	}
	require.Equal(t, want, deletes, "newest first, an incremental before its base, which is not protected")
	require.Equal(t, []string{
		"daily-x-20200101000000", "dailyx-20200101000000", "daily-20200101-000000", "manual-1", "daily-20200104000000",
		"daily-20200104T120000", made,
	}, s.remoteNames("h0"))
	require.Equal(t, []string{
		"daily-20200101000000", "daily-x-20200101000000", "dailyx-20200101000000", "daily-20200101-000000", "manual-1",
		"daily-20200104000000", "daily-20200104T120000", made,
	}, s.remoteNames("h1"))
}

// The backup list comes from the storage, which anybody with access to it can fill: a row without a name never fails the
// backup the run has made, nor keeps anything.
func TestJobScriptRetentionToleratesARowWithoutName(t *testing.T) {
	s := newSidecarStub(t, map[string]*stubHost{"h0": {remote: []stubBackup{
		remote("daily-20200101000000", ""), remote("daily-20200102000000", ""), remote("", "daily-20200101000000"),
	}}})
	code, out := s.run(scheduledEnv("h0", dailyKeeping(1), "2020-01-03T00:00:00Z"))
	require.Zero(t, code, out)
	made := s.matching(http.MethodPost, "/backup/create_remote")[0].query.Get("name")
	require.Equal(t, []string{"", made}, s.remoteNames("h0"), out)
}

// emptyBackup returns a backup listed complete with a size of 0: failed, or in progress, or of nothing at all
func emptyBackup(name string) stubBackup {
	return stubBackup{Name: name, Location: "remote", Desc: "tar, regular"}
}

// A backup listed with a size of 0, or broken, is neither counted nor deleted: it is failed, or in progress, an upload
// in progress listing as broken until it completes. A backup that holds no data still has a size, as does every backup
// of a sidecar that lists no sizes.
func TestJobScriptRetentionCountsOnlyBackupsOfASize(t *testing.T) {
	huge := func(name string) stubBackup {
		backup := remote(name, "")
		backup.DataSize = 1 << 63
		return backup
	}
	for name, tc := range map[string]struct {
		host stubHost
		left []string
	}{
		"backups without data": {
			host: stubHost{emptyBackups: true, remote: []stubBackup{
				withoutData(remote("daily-20200101000000", "")), withoutData(remote("daily-20200102000000", "")),
				withoutData(remote("daily-20200103000000", "")),
			}},
			left: []string{"daily-20200103000000"},
		},
		"a backup of size 0": {
			host: stubHost{remote: []stubBackup{
				remote("daily-20200101000000", ""), remote("daily-20200102000000", ""), remote("daily-20200103000000", ""),
				emptyBackup("daily-20200104000000"),
			}},
			left: []string{"daily-20200103000000", "daily-20200104000000"},
		},
		"the newest backup of size 0": {
			host: stubHost{emptyBackups: true, remote: []stubBackup{
				remote("daily-20200101000000", ""), remote("daily-20200102000000", ""), emptyBackup("daily-29990101000000"),
			}},
			left: []string{"daily-20200102000000", "daily-29990101000000"},
		},
		// A broken backup is an upload that failed, whatever its size: it goes once a complete one is newer
		"broken backups": {
			host: stubHost{remote: []stubBackup{
				broken("daily-20200101000000"), remote("daily-20200102000000", ""), remote("daily-20200103000000", ""),
				broken("daily-20200104000000"),
			}},
			left: []string{"daily-20200103000000"},
		},
		"data beyond what bash arithmetic holds": {
			host: stubHost{remote: []stubBackup{remote("daily-20200101000000", ""), huge("daily-20200102000000"), remote("daily-20200103000000", "")}},
			left: []string{"daily-20200103000000"},
		},
		"a sidecar that lists no sizes": {
			host: stubHost{sizelessList: true, remote: []stubBackup{
				emptyBackup("daily-20200101000000"), emptyBackup("daily-20200102000000"), emptyBackup("daily-20200103000000"),
			}},
			left: []string{"daily-20200103000000"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			host := tc.host
			s := newSidecarStub(t, map[string]*stubHost{"h0": &host})
			code, out := s.run(scheduledEnv("h0", dailyKeeping(2), "2020-01-05T00:00:00Z"))
			require.Zero(t, code, out)
			made := s.matching(http.MethodPost, "/backup/create_remote")[0].query.Get("name")
			require.Equal(t, append(tc.left, made), s.remoteNames("h0"), out)
		})
	}
}

// A run that failed on one host leaves complete copies on the others, which cannot be restored on their own: a backup
// counts, and goes, only when every host lists it complete and of a size.
func TestJobScriptRetentionCountsOnlyBackupsCompleteEverywhere(t *testing.T) {
	for name, tc := range map[string]struct {
		h0, h1       []stubBackup
		left0, left1 []string
	}{
		// An incomplete backup goes from the hosts that hold it once a complete one is newer, as the Altinity Cloud
		// Manager deletes every backup of a size
		"a leftover of a failed run": {
			h0: []stubBackup{remote("daily-20200101000000", ""), remote("daily-20200103000000", "")},
			h1: []stubBackup{remote("daily-20200101000000", "")},
		},
		"backups from before a host was added": {
			h0: []stubBackup{remote("daily-20200101000000", ""), remote("daily-20200102000000", "")},
		},
		"a broken copy": {
			h0: []stubBackup{remote("daily-20200101000000", ""), remote("daily-20200102000000", "")},
			h1: []stubBackup{remote("daily-20200101000000", ""), broken("daily-20200102000000")},
		},
		"a backup of size 0 on one host, missing on the other": {
			h0:    []stubBackup{emptyBackup("daily-20200103000000")},
			left0: []string{"daily-20200103000000"},
		},
		"a broken leftover of size 0 on every host": {
			h0: []stubBackup{broken("daily-20200103000000")},
			h1: []stubBackup{broken("daily-20200103000000")},
		},
		"a copy of size 0 on one host": {
			h0:    []stubBackup{emptyBackup("daily-20200101000000"), remote("daily-20200102000000", "")},
			h1:    []stubBackup{remote("daily-20200101000000", ""), remote("daily-20200102000000", "")},
			left0: []string{"daily-20200101000000"},
			left1: []string{"daily-20200101000000"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := newSidecarStub(t, map[string]*stubHost{"h0": {remote: tc.h0}, "h1": {remote: tc.h1}})
			code, out := s.run(scheduledEnv("h0 h1", dailyKeeping(1), "2020-01-05T00:00:00Z"))
			require.Zero(t, code, out)
			made := s.matching(http.MethodPost, "/backup/create_remote")[0].query.Get("name")
			require.Equal(t, append(tc.left0, made), s.remoteNames("h0"), out)
			require.Equal(t, append(tc.left1, made), s.remoteNames("h1"), out)
		})
	}
}

// Every entry keeps the newest backups of its own slots: a daily one those at its time, a weekly one those of its day of
// the week (0 is Sunday), a monthly one those of its day of the month, the 31st being the last day of a shorter month.
// A backup of no slot, made with the prefix by hand, goes.
func TestJobScriptRetentionKeepsTheBackupsOfEveryEntry(t *testing.T) {
	var backups []stubBackup
	for day := time.Date(2026, time.January, 20, 0, 0, 0, 0, time.UTC); day.Before(time.Date(2026, time.March, 3, 0, 0, 0, 0, time.UTC)); day = day.AddDate(0, 0, 1) {
		backups = append(backups, remote("daily-"+day.Format("20060102150405"), ""))
	}
	backups = append(backups, remote("daily-20260215120000", ""))
	for name, tc := range map[string]struct {
		schedules string
		slot      string
		backups   []stubBackup
		left      []string
	}{
		"daily, weekly and monthly": {
			schedules: "period=daily time=00:00 keep=2;period=weekly day=0 time=00:00 keep=2;period=monthly day=31 time=00:00 keep=2",
			slot:      "2026-03-03T00:00:00Z",
			backups:   backups,
			left: []string{"daily-20260131000000", "daily-20260222000000", "daily-20260228000000", "daily-20260301000000",
				"daily-20260302000000", "daily-20260303000000"},
		},
		"hourly and every 6 hours": {
			schedules: "period=hour time=00:30 keep=3; period=6hours time=02:00 keep=2 ;",
			slot:      "1767313800",
			backups: func() []stubBackup {
				var hourly []stubBackup
				for hour := 0; hour < 24; hour++ {
					for _, minute := range []int{0, 30} {
						hourly = append(hourly, remote(fmt.Sprintf("daily-20260101%02d%02d00", hour, minute), ""))
					}
				}
				return hourly
			}(),
			left: []string{"daily-20260101140000", "daily-20260101200000", "daily-20260101223000", "daily-20260101233000",
				"daily-20260102003000"},
		},
		"slots in the time zone of the schedule": {
			schedules: "period=daily time=02:00 keep=2",
			slot:      "2026-01-03T01:00:00Z",
			backups:   []stubBackup{remote("daily-20260101020000", ""), remote("daily-20260102020000", ""), remote("daily-20260102010000", "")},
			left:      []string{"daily-20260102020000", "daily-20260103020000"},
		},
		"a name of no valid time": {
			schedules: dailyKeeping(1),
			slot:      "2026-01-03T00:00:00Z",
			backups:   []stubBackup{remote("daily-20261399000000", ""), remote("daily-20260101000000", "")},
			left:      []string{"daily-20260103000000", "daily-20261399000000"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := newSidecarStub(t, map[string]*stubHost{"h0": {remote: slices.Clone(tc.backups)}})
			env := scheduledEnv("h0", tc.schedules, tc.slot)
			if strings.Contains(name, "time zone") {
				env[envScheduleTimezone] = "Europe/Berlin"
			}
			code, out := s.run(env)
			require.Zero(t, code, out)
			left := s.remoteNames("h0")
			slices.Sort(left)
			require.Equal(t, tc.left, left, out)
		})
	}
}

// Every host keeps and deletes the same backups, which takes knowing what each of them holds: a
// retention host that does not answer skips retention on every host, and is not retried.
func TestJobScriptRetentionWithAnUnreachableHost(t *testing.T) {
	s := newSidecarStub(t, map[string]*stubHost{
		"h0":   {remote: []stubBackup{remote("daily-20200101000000", ""), remote("daily-20200102000000", "")}},
		"down": {unavailable: -1},
	})
	env := scheduledEnv("h0", dailyKeeping(1), "2020-01-03T00:00:00Z")
	env[envRetentionHosts] = "h0 down"
	code, out := s.run(env)
	require.Zero(t, code, "retention failures never fail the backup:\n%s", out)
	made := s.matching(http.MethodPost, "/backup/create_remote")[0].query.Get("name")
	require.Contains(t, out, "retention skipped: down cannot be listed")
	require.Equal(t, []string{"daily-20200101000000", "daily-20200102000000", made}, s.remoteNames("h0"))
	require.Empty(t, s.matching(http.MethodPost, "/backup/delete/"))
	downCalls := 0
	for _, c := range s.matching(http.MethodGet, "/") {
		if c.host == "down" {
			downCalls++
		}
	}
	require.Equal(t, 1, downCalls, "an unreachable retention host is not retried")
}

// A backup that holds no data - of tables a pattern matches none of, or of an installation recreated
// empty - succeeds, and tells so in the termination message its Job environment names, which the
// pod keeps after the Job and its log are gone. The backup holds data when any host's copy does. A
// host that cannot be listed, a sidecar that lists no sizes, or a copy listed broken, which tells no
// size, leaves that unknown, and a schema-only backup never holds data: none of them tells anything.
// The backup is made by then, so a host that does not answer is not waited for.
func TestJobScriptTellsABackupWithoutData(t *testing.T) {
	script := scriptWithTimeout(t, orchestratorScript, "MUTATED_UNREACHABLE_TIMEOUT", 1)
	job := BuildBackupJob(testBackup("b1"), testInstallation(), testTopology(), testOrchestrator())
	declared := scriptEnv(job)
	require.Equal(t, NoDataMessage, declared[envNoDataMessage], "the Job environment names the message")
	hosts := strings.Fields(declared[envBackupHosts])
	require.Len(t, hosts, 2)
	for name, tc := range map[string]struct {
		h0, h1     stubHost
		schemaOnly bool
		told       bool
		logged     string
	}{
		"no host's copy holds data": {
			h0:     stubHost{emptyBackups: true},
			h1:     stubHost{emptyBackups: true},
			told:   true,
			logged: "WARNING: " + declared[envBackupName] + " holds no data\n",
		},
		"one host's copy holds data": {
			h0: stubHost{emptyBackups: true},
		},
		"data on an object disk": {
			h0: stubHost{objectDiskBackups: true},
			h1: stubHost{objectDiskBackups: true},
		},
		"a sidecar that lists no sizes": {
			h0: stubHost{emptyBackups: true, sizelessList: true},
			h1: stubHost{emptyBackups: true, sizelessList: true},
		},
		"a copy listed broken on one host": {
			h0: stubHost{emptyBackups: true},
			h1: stubHost{brokenBackups: true},
		},
		"a host that cannot be listed": {
			h0:     stubHost{emptyBackups: true},
			h1:     stubHost{emptyBackups: true, listsUnavailableAfterPost: true},
			logged: "whether " + declared[envBackupName] + " holds data is unknown: " + hosts[1] + " cannot be listed",
		},
		"a schema-only backup": {
			h0:         stubHost{emptyBackups: true},
			h1:         stubHost{emptyBackups: true},
			schemaOnly: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			h0, h1 := tc.h0, tc.h1
			s := newSidecarStub(t, map[string]*stubHost{hosts[0]: &h0, hosts[1]: &h1})
			env := scriptEnv(job)
			env[envSchemaOnly] = strconv.FormatBool(tc.schemaOnly)
			path := filepath.Join(t.TempDir(), "termination-log")
			env[envTerminationMessagePath] = path
			code, out := s.runScript(script, env)
			require.Zero(t, code, "a backup without data succeeds:\n%s", out)
			require.Contains(t, out, tc.logged)
			message, err := os.ReadFile(path)
			if !tc.told {
				require.True(t, os.IsNotExist(err), "nothing is told:\n%s", out)
				require.NotContains(t, out, "holds no data")
				return
			}
			require.NoError(t, err, out)
			require.Equal(t, NoDataMessage+"\n", string(message))
		})
	}
}

func TestJobScriptRetentionKeepsNewBackup(t *testing.T) {
	// Clock skew: a scheduled backup that looks newer than the one just made
	s := newSidecarStub(t, map[string]*stubHost{
		"h0": {remote: []stubBackup{remote("daily-29991231235959", ""), remote("daily-20200101000000", "")}},
	})
	code, out := s.run(scheduledEnv("h0", dailyKeeping(1), "2026-01-02T00:00:00Z"))
	require.Zero(t, code, out)
	made := s.matching(http.MethodPost, "/backup/create_remote")[0].query.Get("name")
	require.Equal(t, []string{"daily-29991231235959", made}, s.remoteNames("h0"))
}

// Retention runs after a scheduled backup alone, and not while a restore of the cluster runs.
func TestJobScriptRetentionDisabled(t *testing.T) {
	for name, override := range map[string]map[string]string{
		"no schedule":           {envSchedules: ""},
		"a restore in progress": {envRestoreInProgress: "true"},
	} {
		t.Run(name, func(t *testing.T) {
			s := newSidecarStub(t, map[string]*stubHost{"h0": {remote: []stubBackup{remote("daily-20200101000000", "")}}})
			code, out := s.run(withEnv(scheduledEnv("h0", dailyKeeping(1), "2020-01-03T00:00:00Z"), override))
			require.Zero(t, code, out)
			require.Empty(t, s.matching(http.MethodPost, "/backup/delete/"))
			require.Len(t, s.remoteNames("h0"), 2)
			if override[envRestoreInProgress] != "" {
				require.Contains(t, out, "retention skipped: a restore of the cluster is in progress")
			}
		})
	}
}

// The heaviest entry whose slot the run is decides whether its backup is incremental: from the newest backup of the
// series every host holds complete, until the chain of incremental backups that ends with it is as long as the entry
// keeps; then the backup is full again. A base the run names itself wins.
func TestJobScriptMakesIncrementalBackups(t *testing.T) {
	chain := func(days ...int) []stubBackup {
		var backups []stubBackup
		for i, day := range days {
			required := ""
			if i > 0 && days[i-1] > 0 {
				required = fmt.Sprintf("daily-2020010%d000000", days[i-1])
			}
			if day > 0 {
				backups = append(backups, remote(fmt.Sprintf("daily-2020010%d000000", day), required))
			}
		}
		return backups
	}
	const incremental = "period=hour time=00:00 keep=24 incremental=true;period=daily time=00:00 keep=3 incremental=true"
	for name, tc := range map[string]struct {
		h0, h1    []stubBackup
		schedules string
		slot      string
		env       map[string]string
		base      string
	}{
		"after a full backup":                {h0: chain(1), h1: chain(1), base: "daily-20200101000000"},
		"a chain shorter than keep":          {h0: chain(1, 2, 3), h1: chain(1, 2, 3), base: "daily-20200103000000"},
		"a chain of keep":                    {h0: chain(1, 2, 3, 4), h1: chain(1, 2, 3, 4)},
		"a newest backup one host lacks":     {h0: chain(1, 2, 3, 4), h1: chain(1, 2, 3), base: "daily-20200103000000"},
		"a broken newest backup":             {h0: append(chain(1), broken("daily-20200102000000")), h1: chain(1, 2), base: "daily-20200101000000"},
		"no backup of the series":            {h0: []stubBackup{remote("other-20200101000000", "")}},
		"a slot of a lighter entry":          {h0: chain(1, 2, 3, 4), h1: chain(1, 2, 3, 4), slot: "2020-01-09T05:00:00Z", base: "daily-20200104000000"},
		"a winner that is not incremental":   {h0: chain(1), h1: chain(1), schedules: "period=hour keep=24 incremental=true;period=daily time=00:00 keep=3"},
		"a slot of no entry":                 {h0: chain(1), h1: chain(1), slot: "2020-01-09T05:30:00Z"},
		"a base the run names":               {h0: chain(1, 2), h1: chain(1, 2), env: map[string]string{envDiffFromRemote: "daily-20200101000000"}, base: "daily-20200101000000"},
		"the slot as unix seconds":           {h0: chain(1), h1: chain(1), slot: "1578528000", base: "daily-20200101000000"},
		"the slot as RFC 3339 with a offset": {h0: chain(1), h1: chain(1), slot: "2020-01-09T01:00:00+01:00", base: "daily-20200101000000"},
	} {
		t.Run(name, func(t *testing.T) {
			s := newSidecarStub(t, map[string]*stubHost{"h0": {remote: tc.h0}, "h1": {remote: tc.h1}})
			env := withEnv(map[string]string{
				envOperation:        operationBackup,
				envBackupNamePrefix: "daily",
				envBackupHosts:      "h0 h1",
				envSchedules:        cmp.Or(tc.schedules, incremental),
				envCronJobName:      testCronJobName,
				envJobName:          slotJobName(cmp.Or(tc.slot, "2020-01-09T00:00:00Z")),
			}, tc.env)
			code, out := s.run(env)
			require.Zero(t, code, out)
			creates := s.matching(http.MethodPost, "/backup/create_remote")
			require.Len(t, creates, 2)
			for _, c := range creates {
				require.Equal(t, tc.base, c.query.Get("diff-from-remote"), out)
			}
		})
	}
}

// A backup that runs a schedule now has the entries of the schedule: it is incremental as the first incremental entry
// says, as the Altinity Cloud Manager makes a backup by hand, from the newest backup of the series, and retention runs
// after it, keeping it while it is recent.
func TestJobScriptRunsAScheduleNow(t *testing.T) {
	s := newSidecarStub(t, map[string]*stubHost{"h0": {remote: []stubBackup{
		remote("daily-20200101000000", ""), remote("daily-20200102000000", "daily-20200101000000"), remote("daily-20200103000000", ""),
	}}})
	code, out := s.run(map[string]string{envOperation: operationBackup, envBackupName: "daily-20200103T120000",
		envBackupNamePrefix: "daily", envBackupHosts: "h0", envRetentionHosts: "h0",
		envSchedules: "period=weekly time=00:00 day=0 keep=1;period=daily time=00:00 keep=2 incremental=true"})
	require.Zero(t, code, out)
	require.Contains(t, out, "runs the schedule now, incremental as its daily entry")
	creates := s.matching(http.MethodPost, "/backup/create_remote")
	require.Len(t, creates, 1)
	require.Equal(t, "daily-20200103000000", creates[0].query.Get("diff-from-remote"))
	require.Equal(t, []string{"daily-20200102000000", "daily-20200103000000", "daily-20200103T120000"}, s.remoteNames("h0"),
		"retention ran after it")

	// The next scheduled backup is incremental from it, the newest backup, as the Altinity Cloud Manager makes a backup
	// incremental from the newest backup of the cluster, whether made by hand or on schedule
	code, out = s.run(scheduledEnv("h0", "period=weekly time=00:00 day=0 keep=1;period=daily time=00:00 keep=2 incremental=true",
		"2020-01-04T00:00:00Z"))
	require.Zero(t, code, out)
	creates = s.matching(http.MethodPost, "/backup/create_remote")
	require.Len(t, creates, 2)
	require.Equal(t, "daily-20200104000000", creates[1].query.Get("name"))
	require.Equal(t, "daily-20200103T120000", creates[1].query.Get("diff-from-remote"))

	// Without an incremental entry it is full
	s = newSidecarStub(t, map[string]*stubHost{"h0": {remote: []stubBackup{remote("daily-20200103000000", "")}}})
	code, out = s.run(map[string]string{envOperation: operationBackup, envBackupName: "daily-20200103T120000",
		envBackupNamePrefix: "daily", envBackupHosts: "h0", envRetentionHosts: "h0", envSchedules: "period=daily time=00:00 keep=2"})
	require.Zero(t, code, out)
	require.Empty(t, s.matching(http.MethodPost, "/backup/create_remote")[0].query.Get("diff-from-remote"))
}

func TestJobScriptVerify(t *testing.T) {
	s := newSidecarStub(t, map[string]*stubHost{
		"h0": {remote: []stubBackup{remote("bk-1", "")}, local: []stubBackup{local("bk-1")}},
		"h1": {remote: []stubBackup{remote("bk-1", "")}},
	})
	code, out := s.run(map[string]string{envOperation: operationVerify, envBackupName: "bk-1", envVerifyHosts: "h0 h1"})
	require.Zero(t, code, out)
	require.Equal(t, []string{
		"h0 GET /backup/list/local",
		"h0 POST /backup/delete/local/bk-1",
		"h0 POST /backup/download/bk-1",
		"h0 GET /backup/list/local",
		"h0 POST /backup/delete/local/bk-1",
		"h1 GET /backup/list/local",
		"h1 POST /backup/download/bk-1",
		"h1 GET /backup/list/local",
		"h1 POST /backup/delete/local/bk-1",
	}, s.significant())
	for _, c := range s.matching(http.MethodPost, "/backup/delete/local/") {
		require.Equal(t, "true", c.query.Get("force"))
	}
}

func TestJobScriptVerifyFailure(t *testing.T) {
	t.Run("download fails", func(t *testing.T) {
		s := newSidecarStub(t, map[string]*stubHost{"h0": {failOp: map[string]string{"download": "'bk-1' is not found on remote storage"}}})
		code, out := s.run(map[string]string{envOperation: operationVerify, envBackupName: "bk-1", envVerifyHosts: "h0"})
		require.Equal(t, 1, code, out)
		require.Contains(t, out, "'bk-1' is not found on remote storage")
	})
	t.Run("no local copy after the download", func(t *testing.T) {
		s := newSidecarStub(t, map[string]*stubHost{"h0": {noLocalOnDownload: true, remote: []stubBackup{remote("bk-1", "")}}})
		code, out := s.run(map[string]string{envOperation: operationVerify, envBackupName: "bk-1", envVerifyHosts: "h0"})
		require.Equal(t, 1, code, out)
		require.Contains(t, out, "bk-1 is not present locally after the download")
	})
}

// requirePreflightFirst asserts that no POST precedes any preflight request
func requirePreflightFirst(t *testing.T, s *sidecarStub) {
	t.Helper()
	posted := false
	for _, c := range s.significant() {
		preflight := strings.HasSuffix(c, " GET /backup/version") || strings.HasSuffix(c, " GET /backup/list/remote") ||
			strings.HasSuffix(c, " GET /backup/tables/all") || strings.HasSuffix(c, " GET /backup/tables")
		if strings.Contains(c, " POST ") {
			posted = true
		}
		require.False(t, preflight && posted, "preflight request %q after a destructive one", c)
	}
}

func restoreHosts() map[string]*stubHost {
	return map[string]*stubHost{
		"s0": {remote: []stubBackup{remote("bk-1", "")}},
		"d1": {remote: []stubBackup{remote("bk-1", "")}},
	}
}

// restoreTestCluster is the cluster the restores of the tests target
const restoreTestCluster = "main"

// testMaxTableSizeToDrop is the max_table_size_to_drop of the cluster the restores of the tests target,
// the ClickHouse default
const testMaxTableSizeToDrop = 50000000000

func restoreEnv(overrides map[string]string) map[string]string {
	env := map[string]string{
		envOperation:          operationRestore,
		envCluster:            restoreTestCluster,
		envBackupName:         "bk-1",
		envSchemaHosts:        "s0",
		envDataHosts:          "s0 d1",
		envSchemaOnly:         "false",
		envOverwrite:          "false",
		envValidateTopology:   "true",
		envReplicated:         "true",
		envMaxTableSizeToDrop: strconv.Itoa(testMaxTableSizeToDrop),
	}
	for k, v := range overrides {
		env[k] = v
	}
	return env
}

// An overwrite of a cluster whose shards have several replicas, which drops a replicated table, first
// checks, before anything changes, that the backup of every shard holds the same tables, and that no
// host but the first schema host holds a table it drops larger than max_table_size_to_drop. It then
// drops and recreates the tables of the backup ON CLUSTER from the first schema host, through the one
// command the script runs on the actions endpoint, restores the schema on every schema host on its own
// and the data on the data hosts.
func TestJobScriptRestore(t *testing.T) {
	hosts := withReplicatedTable(replicaRestoreHosts())
	hosts["a1"].sizes = map[string]uint64{"db1.t1": testMaxTableSizeToDrop}
	hosts["a1"].local = []stubBackup{local("bk-1")}
	s := newSidecarStub(t, hosts)
	code, out := s.run(replicaRestoreEnv(map[string]string{envOverwrite: "true"}))
	require.Zero(t, code, out)
	require.Equal(t, []string{
		"a0 GET /backup/version", "a1 GET /backup/version", "b0 GET /backup/version", "b1 GET /backup/version",
		"a0 GET /backup/list/remote", "a1 GET /backup/list/remote", "b0 GET /backup/list/remote", "b1 GET /backup/list/remote",
		// The tables of the backup of the first replica of every shard
		"a0 GET /backup/tables", "b0 GET /backup/tables",
		// The engines and sizes of the tables of every replica
		"a0 GET /backup/tables/all", "a1 GET /backup/tables/all", "b0 GET /backup/tables/all", "b1 GET /backup/tables/all",
		"a0 GET /backup/list/local", "a1 GET /backup/list/local", "b0 GET /backup/list/local", "b1 GET /backup/list/local",
		// Every host downloads the backup at once before anything is dropped, but a1, which holds a copy already
		"a0 POST /backup/download/bk-1", "b0 POST /backup/download/bk-1", "b1 POST /backup/download/bk-1",
		"a0 POST /backup/actions",
		"a0 POST /backup/restore/bk-1", "a1 POST /backup/restore/bk-1",
		"b0 POST /backup/restore/bk-1", "b1 POST /backup/restore/bk-1",
		"a0 POST /backup/restore/bk-1", "b0 POST /backup/restore/bk-1",
		// Only the copies the restore downloaded are dropped, a1 had its own before
		"a0 GET /backup/list/local", "a0 POST /backup/delete/local/bk-1",
		"b0 GET /backup/list/local", "b0 POST /backup/delete/local/bk-1",
		"b1 GET /backup/list/local", "b1 POST /backup/delete/local/bk-1",
	}, s.significant())
	for _, listing := range s.matching(http.MethodGet, "/backup/tables") {
		if listing.path == "/backup/tables" {
			require.Equal(t, url.Values{"remote_backup": {"bk-1"}}, listing.query)
		}
	}
	actions := s.matching(http.MethodPost, "/backup/actions")
	require.Equal(t, "restore --schema --rm --env=RESTORE_SCHEMA_ON_CLUSTER="+restoreTestCluster+" bk-1", actions[0].command)
	require.Equal(t, []string{
		// The data to the hosts that restore it, the schema alone to the others
		"a0 download hardlink_exists_files=", "b0 download hardlink_exists_files=", "b1 download schema=",
		"a0 restore --schema --rm --env=RESTORE_SCHEMA_ON_CLUSTER=" + restoreTestCluster + " bk-1",
		"a0 rm= schema=", "a1 rm= schema=", "b0 rm= schema=", "b1 rm= schema=", "a0 data=", "b0 data=",
	}, s.restoreSteps())
}

// withReplicatedTable gives every host the live table db1.t1 of a Replicated engine, which the backup
// holds too: an overwrite of their cluster drops it ON CLUSTER
func withReplicatedTable(hosts map[string]*stubHost) map[string]*stubHost {
	for _, host := range hosts {
		host.tables = append(host.tables, "db1.t1")
		host.engines = map[string]string{"db1.t1": "ReplicatedMergeTree"}
		host.backupTables = []string{"db1.t1"}
	}
	return hosts
}

// An overwrite drops the tables of the backup ON CLUSTER only where a replica could clone their old
// parts back from another replica of its shard: when a shard has several replicas, and the restore drops
// a table of a Replicated engine on any of them - one of the backup the pattern takes, one of a database
// it drops as a whole, or the inner table of a materialized view it drops. Otherwise every replica drops
// and recreates the tables of the backup on its own, which needs none of the checks of the drop ON
// CLUSTER. Whether the installation configures ZooKeeper only tells what to do when the engines of the
// tables cannot be told: a configuration file of its own may bring ZooKeeper, so a replicated table runs
// the drop ON CLUSTER anyway, while unknown engines without ZooKeeper fail the restore, changing nothing. A
// cluster of one replica in every shard needs no table of the backup listed for that.
func TestJobScriptDropsOnClusterOnlyWhatReplicates(t *testing.T) {
	replicatedEverywhere := func(table, engine string) func(map[string]*stubHost) {
		return func(hosts map[string]*stubHost) {
			for _, host := range hosts {
				host.tables = append(host.tables, table)
				host.engines = map[string]string{table: engine}
				host.backupTables = []string{"db1.t1"}
			}
		}
	}
	for name, tc := range map[string]struct {
		env       map[string]string
		mutate    func(map[string]*stubHost)
		onCluster bool
		logged    string
		refused   string
		// unlisted tells that no table of the backup is listed
		unlisted bool
	}{
		"one replica in every shard": {
			env:      map[string]string{envSchemaHosts: "a0 b0", envGuardHosts: "a0 b0"},
			mutate:   func(h map[string]*stubHost) { withReplicatedTable(h) },
			logged:   "restore bk-1: every shard of cluster main has one replica, which drops the tables of the backup on its own",
			unlisted: true,
		},
		"a replicated table the restore drops": {
			mutate:    func(h map[string]*stubHost) { withReplicatedTable(h) },
			onCluster: true,
		},
		"a replicated table the restore drops, though the installation configures no ZooKeeper": {
			env:       map[string]string{envReplicated: "false"},
			mutate:    func(h map[string]*stubHost) { withReplicatedTable(h) },
			onCluster: true,
			logged:    "cluster main holds the replicated table a0:db1.t1, which the restore drops ON CLUSTER though the installation configures no ZooKeeper for it",
		},
		"no replicated table": {
			mutate: replicatedEverywhere("db1.t1", "MergeTree"),
			logged: "restore bk-1: the restore drops no replicated table, every replica drops the tables of the backup on its own",
		},
		"a replicated table the restore keeps": {
			mutate: replicatedEverywhere("db9.t1", "ReplicatedMergeTree"),
			logged: "the restore drops no replicated table",
		},
		"no table": {
			mutate: func(h map[string]*stubHost) {},
			logged: "the restore drops no replicated table",
		},
		"a replicated table of a database the restore drops as a whole": {
			env: map[string]string{envTables: "db1.*"},
			mutate: func(h map[string]*stubHost) {
				replicatedEverywhere("db1.t1", "MergeTree")(h)
				h["a1"].tables = append(h["a1"].tables, "db1.new")
				h["a1"].engines["db1.new"] = "ReplicatedSummingMergeTree"
			},
			onCluster: true,
		},
		"the replicated inner table of a view the restore drops": {
			env: map[string]string{envTables: "db1.mv"},
			mutate: func(h map[string]*stubHost) {
				for _, host := range h {
					host.tables = []string{"db1.mv", "db1..inner_id.6c3b2a8e-0e4f-4d55-9a8e-6f1d0c2b7a11"}
					host.engines = map[string]string{"db1.mv": "MaterializedView", "db1..inner_id.6c3b2a8e-0e4f-4d55-9a8e-6f1d0c2b7a11": "ReplicatedMergeTree"}
					host.uuids = map[string]string{"db1.mv": "6c3b2a8e-0e4f-4d55-9a8e-6f1d0c2b7a11"}
					host.backupTables = []string{"db1.mv", "db1..inner_id.6c3b2a8e-0e4f-4d55-9a8e-6f1d0c2b7a11"}
				}
			},
			onCluster: true,
		},
		"a table whose engine cannot be told": {
			env: map[string]string{envMaxTableSizeToDrop: "0"},
			mutate: func(h map[string]*stubHost) {
				replicatedEverywhere("db1.t1", "MergeTree")(h)
				h["a1"].tables, h["a1"].rawTables = nil, []string{`{"Database":"db1","Name":"t1"}`}
			},
			onCluster: true,
		},
		"a table whose engine cannot be told, without ZooKeeper": {
			env: map[string]string{envMaxTableSizeToDrop: "0", envReplicated: "false"},
			mutate: func(h map[string]*stubHost) {
				replicatedEverywhere("db1.t1", "MergeTree")(h)
				h["a1"].tables, h["a1"].rawTables = nil, []string{`{"Database":"db1","Name":"t1"}`}
			},
			refused: "refusing to overwrite: whether the restore drops a replicated table of cluster main is unknown, as a table of a host or of the backup does not read as one",
		},
	} {
		t.Run(name, func(t *testing.T) {
			hosts := replicaRestoreHosts()
			tc.mutate(hosts)
			s := newSidecarStub(t, hosts)
			code, out := s.run(replicaRestoreEnv(withEnv(map[string]string{envOverwrite: "true", envUnchangedExitCode: "3"}, tc.env)))
			if tc.refused != "" {
				require.Equal(t, 3, code, out)
				require.Contains(t, out, "ERROR: "+tc.refused)
				require.Empty(t, s.matching(http.MethodPost, "/"), "nothing changes before the check passed")
				return
			}
			require.Zero(t, code, out)
			require.Contains(t, out, tc.logged)
			requirePreflightFirst(t, s)
			require.Equal(t, tc.unlisted, !slices.ContainsFunc(s.matching(http.MethodGet, "/backup/tables"), func(c stubCall) bool {
				return c.path == "/backup/tables"
			}))
			steps := slices.DeleteFunc(s.restoreSteps(), func(step string) bool { return strings.Contains(step, " download ") })
			require.NotEmpty(t, steps)
			if tc.onCluster {
				require.True(t, strings.HasPrefix(steps[0], "a0 restore --schema --rm --env=RESTORE_SCHEMA_ON_CLUSTER=main "), out)
				require.Len(t, s.matching(http.MethodPost, "/backup/actions"), 1)
				return
			}
			require.Empty(t, s.matching(http.MethodPost, "/backup/actions"), out)
			for _, step := range steps {
				require.NotContains(t, step, "--schema")
			}
		})
	}
}

// As the Altinity Cloud Manager restores, every host downloads the backup before anything is dropped, all at once: the
// data to the hosts that restore it, every file already on their disks hard-linked, the schema alone to the others. A
// host that cannot download it - short of disk space, say - fails the restore before it changes anything: the other
// hosts finish their downloads, which are deleted again.
func TestJobScriptRestoreDownloadsEverywhereFirst(t *testing.T) {
	hosts := replicaRestoreHosts()
	hosts["b0"].failOp = map[string]string{"download": "not enough space on disk default"}
	s := newSidecarStub(t, hosts)
	code, out := s.run(replicaRestoreEnv(map[string]string{envUnchangedExitCode: "3", envTables: "db1.*"}))
	require.Equal(t, 3, code, out)
	require.Contains(t, out, "ERROR: download of bk-1 failed on b0: b0: download bk-1 ended with status 'error': not enough space on disk default")
	require.Equal(t, []string{
		"a0 download hardlink_exists_files= table=db1.*", "a1 download schema= table=db1.*",
		"b0 download hardlink_exists_files= table=db1.*", "b1 download schema= table=db1.*",
	}, s.restoreSteps(), "nothing is restored")
	for _, host := range []string{"a0", "a1", "b1"} {
		require.Empty(t, hosts[host].local, "the copy %s downloaded is deleted:\n%s", host, out)
	}
}

// A host that holds an incomplete local copy of the backup, as a download that failed or was stopped leaves it,
// downloads it again, which clickhouse-backup resumes, rather than restoring from it; a restore whose download fails
// leaves that copy for the next one to resume, and deletes only the copies it started itself.
func TestJobScriptRestoreResumesAnIncompleteLocalCopy(t *testing.T) {
	incomplete := stubBackup{Name: "bk-1", Location: "local", Desc: "broken (can't stat metadata.json)"}
	t.Run("restored", func(t *testing.T) {
		hosts := replicaRestoreHosts()
		hosts["a0"].local = []stubBackup{incomplete}
		s := newSidecarStub(t, hosts)
		code, out := s.run(replicaRestoreEnv(nil))
		require.Zero(t, code, out)
		require.Contains(t, out, "a0: holds an incomplete local copy of bk-1, whose download the restore resumes")
		require.Contains(t, s.restoreSteps(), "a0 download hardlink_exists_files=")
		require.Empty(t, hosts["a0"].local, "the resumed copy is deleted after the restore, as any it downloaded")
	})
	t.Run("download failed", func(t *testing.T) {
		hosts := replicaRestoreHosts()
		hosts["a0"].local = []stubBackup{incomplete}
		hosts["b0"].failOp = map[string]string{"download": "not enough space on disk default"}
		hosts["a0"].failOp = map[string]string{"download": "connection reset"}
		s := newSidecarStub(t, hosts)
		code, out := s.run(replicaRestoreEnv(map[string]string{envUnchangedExitCode: "3"}))
		require.Equal(t, 3, code, out)
		require.Equal(t, []stubBackup{incomplete}, hosts["a0"].local, "the incomplete copy is kept to resume:\n%s", out)
		for _, host := range []string{"a1", "b1"} {
			require.Empty(t, hosts[host].local, "the copy %s downloaded is deleted:\n%s", host, out)
		}
		for _, request := range s.matching(http.MethodPost, "/backup/delete/local/bk-1") {
			require.NotEqual(t, "a0", request.host)
		}
	})
}

// Without overwrite the guard has proven that nothing of the backup exists, so every schema host
// drops what it has of the backup before it recreates it: a table that came through the replication
// of a Replicated database would fail its CREATE otherwise. Nothing runs ON CLUSTER.
func TestJobScriptRestoreWithoutOverwrite(t *testing.T) {
	s := newSidecarStub(t, restoreHosts())
	code, out := s.run(restoreEnv(nil))
	require.Zero(t, code, "system databases are not user tables:\n%s", out)
	requirePreflightFirst(t, s)
	require.Len(t, s.matching(http.MethodGet, "/backup/tables/all"), 2)
	require.Empty(t, s.matching(http.MethodPost, "/backup/actions"))
	restores := s.matching(http.MethodPost, "/backup/restore/")
	require.Len(t, restores, 3)
	require.Equal(t, url.Values{"schema": {""}, "rm": {""}}, restores[0].query)
}

func TestJobScriptRestoreSchemaOnly(t *testing.T) {
	s := newSidecarStub(t, restoreHosts())
	code, out := s.run(restoreEnv(map[string]string{envSchemaOnly: "true", envDataHosts: ""}))
	require.Zero(t, code, out)
	// The schema alone is downloaded
	require.Equal(t, []string{"s0 download schema=", "s0 rm= schema="}, s.restoreSteps())
}

func TestJobScriptRestorePreflightRefusals(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(map[string]*stubHost)
		env    map[string]string
		output string
		// checked lists the hosts whose tables must have been inspected before the refusal
		checked []string
	}{
		{
			name:    "user table on the last data host",
			mutate:  func(h map[string]*stubHost) { h["d1"].tables = []string{"db1.t1"} },
			output:  "refusing to restore over existing tables (d1:db1.t1)",
			checked: []string{"s0", "d1"},
		},
		{
			name:    "unparsable table row fails closed",
			mutate:  func(h map[string]*stubHost) { h["s0"].rawTables = []string{`{"database":"db1","name":"t1"}`} },
			output:  "refusing to restore over existing tables (s0:?)",
			checked: []string{"s0", "d1"},
		},
		{
			name:   "backup missing on a data host",
			mutate: func(h map[string]*stubHost) { h["d1"].remote = nil },
			env:    map[string]string{envOverwrite: "true"},
			output: "backup bk-1 is missing in the storage of d1",
		},
		{
			name: "broken backup",
			mutate: func(h map[string]*stubHost) {
				h["d1"].remote = []stubBackup{{Name: "bk-1", Location: "remote", Desc: "broken (can't stat metadata.json)"}}
			},
			output: "backup bk-1 is broken (can't stat metadata.json) in the storage of d1",
		},
		{
			name:   "sidecar of a host answers an error",
			mutate: func(h map[string]*stubHost) { h["d1"].status = http.StatusUnauthorized },
			env:    map[string]string{envOverwrite: "true"},
			output: "preflight failed: sidecar of d1 answered HTTP 401",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hosts := restoreHosts()
			tt.mutate(hosts)
			s := newSidecarStub(t, hosts)
			code, out := s.run(restoreEnv(tt.env))
			require.Equal(t, 1, code, out)
			require.Contains(t, out, tt.output)
			require.Empty(t, s.matching(http.MethodPost, "/"), "nothing destructive may happen before the preflight passed")
			var checked []string
			for _, c := range s.matching(http.MethodGet, "/backup/tables/all") {
				checked = append(checked, c.host)
			}
			require.Equal(t, tt.checked, checked)
		})
	}
}

// The schema restore drops the tables of the backup on every replica, so the overwrite guard looks at
// every replica of the restored cluster, whatever host lists the Job has: a replica that still holds
// a table refuses the restore.
func TestJobScriptRestoreGuardsEveryReplica(t *testing.T) {
	chr, topology := testRestore("r1"), testTopology()
	job := BuildRestoreJob(chr, testInstallation(), topology, testOrchestrator())
	hosts := map[string]*stubHost{}
	for _, host := range topology.allReplicas() {
		hosts[host] = &stubHost{remote: []stubBackup{remote(chr.Spec.BackupName, "")}}
	}
	hosts["chi-demo-main-0-1"].tables = []string{"db1.t1"}
	s := newSidecarStub(t, hosts)
	code, out := s.run(scriptEnv(job))
	require.Equal(t, UnchangedExitCode, code, out)
	require.Contains(t, out, "refusing to restore over existing tables (chi-demo-main-0-1:db1.t1)")
	require.Empty(t, s.matching(http.MethodPost, "/"), "nothing destructive may happen before the guard passed")
}

func TestJobScriptRestoreWithoutTopologyValidation(t *testing.T) {
	s := newSidecarStub(t, restoreHosts())
	code, out := s.run(restoreEnv(map[string]string{envValidateTopology: "false"}))
	require.Zero(t, code, out)
	require.Empty(t, s.matching(http.MethodGet, "/backup/list/remote"))
}

func TestJobScriptInjection(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "pwned")
	payload := `$(touch ` + marker + `)` + "`touch " + marker + "`"

	t.Run("backup name", func(t *testing.T) {
		s := newSidecarStub(t, map[string]*stubHost{"h0": {}})
		code, out := s.run(map[string]string{envOperation: operationBackup, envBackupName: "x" + payload, envBackupHosts: "h0"})
		require.Equal(t, 1, code, out)
		require.Zero(t, s.countCalls())
	})
	t.Run("host list", func(t *testing.T) {
		s := newSidecarStub(t, map[string]*stubHost{"h0": {}})
		code, out := s.run(map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0;" + payload})
		require.Equal(t, 1, code, out)
		require.Zero(t, s.countCalls())
	})
	t.Run("query values", func(t *testing.T) {
		tables := `db.*&name=evil&schema"` + payload
		partitions := `p1,p2&rm` + payload
		s := newSidecarStub(t, map[string]*stubHost{"h0": {}})
		code, out := s.run(map[string]string{
			envOperation:   operationBackup,
			envBackupName:  "bk-1",
			envBackupHosts: "h0",
			envTables:      tables,
			envPartitions:  partitions,
		})
		require.Zero(t, code, out)
		creates := s.matching(http.MethodPost, "/backup/create_remote")
		require.Len(t, creates, 1)
		require.Equal(t, url.Values{"name": {"bk-1"}, "table": {tables}, "partitions": {partitions}}, creates[0].query)
	})
	t.Run("values of the command of an overwrite", func(t *testing.T) {
		for name, override := range map[string]string{envCluster: "main" + payload, envTables: "db1.*" + payload} {
			s := newSidecarStub(t, restoreHosts())
			code, out := s.run(restoreEnv(map[string]string{envOverwrite: "true", name: override}))
			require.Equal(t, 1, code, out)
			require.Zero(t, s.countCalls())
		}
	})
	t.Run("values of the path check", func(t *testing.T) {
		s := newSidecarStub(t, map[string]*stubHost{"h0": {}})
		env := pathCheckTestEnv(pathCluster{name: "a" + payload, shards: "0 " + payload, settings: storageSettings("s3",
			"S3_BUCKET", payload, "S3_PATH", "{namespace}/{installation}/{shard}/"+payload, "S3_OBJECT_DISK_PATH", "{"+payload+"}")})
		env[envPathInstallation], env[envPathNamespace] = payload, payload
		code, out := s.run(withEnv(env, map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0"}))
		require.Zero(t, code, out)
	})
	_, err := os.Stat(marker)
	require.True(t, os.IsNotExist(err), "an injected command ran")
}

// The reason a Job failed has to outlive its log: the script writes it, with what made the failing
// step fail, to the termination message of its container, which the pod keeps in its status.
func TestJobScriptWritesWhyItFailed(t *testing.T) {
	for name, tc := range map[string]struct {
		hosts   map[string]*stubHost
		env     map[string]string
		message string
	}{
		"operation error": {
			hosts:   map[string]*stubHost{"h0": {failOp: map[string]string{"create_remote": "can't upload: disk full"}}},
			env:     map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0"},
			message: "backup bk-1 failed on h0: h0: create_remote bk-1 ended with status 'error': can't upload: disk full\n",
		},
		"preflight refusal": {
			hosts:   restoreHosts(),
			env:     restoreEnv(map[string]string{envBackupName: "bk-2", envOverwrite: "true"}),
			message: "preflight failed: backup bk-2 is missing in the storage of s0\n",
		},
		"invalid environment": {
			hosts:   map[string]*stubHost{},
			env:     map[string]string{envOperation: "drop"},
			message: "OPERATION must be one of backup, verify, restore\n",
		},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "termination-log")
			s := newSidecarStub(t, tc.hosts)
			tc.env[envTerminationMessagePath] = path
			code, out := s.run(tc.env)
			require.Equal(t, 1, code, out)
			message, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, tc.message, string(message))
		})
	}

	t.Run("success writes nothing", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "termination-log")
		s := newSidecarStub(t, map[string]*stubHost{"h0": {}})
		code, out := s.run(map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0", envTerminationMessagePath: path})
		require.Zero(t, code, out)
		_, err := os.Stat(path)
		require.True(t, os.IsNotExist(err))
	})

	t.Run("an unwritable path does not hide the failure", func(t *testing.T) {
		s := newSidecarStub(t, map[string]*stubHost{"h0": {failOp: map[string]string{"create_remote": "disk full"}}})
		code, out := s.run(map[string]string{
			envOperation:              operationBackup,
			envBackupName:             "bk-1",
			envBackupHosts:            "h0",
			envTerminationMessagePath: filepath.Join(t.TempDir(), "missing", "termination-log"),
		})
		require.Equal(t, 1, code, out)
		require.Contains(t, out, "ERROR: backup bk-1 failed on h0: h0: create_remote bk-1 ended with status 'error': disk full")
	})
}

// The exit code tells the operator whether a failed Job changed anything: only a Job that failed
// before any sidecar may have accepted a change exits with the code the Job environment names, a
// restore that exits so leaves nothing behind that scheduled backups must be held for.
func TestJobScriptTellsWhetherItChangedAnything(t *testing.T) {
	declared := scriptEnv(BuildRestoreJob(testRestore("r1"), testInstallation(), testTopology(), testOrchestrator()))[envUnchangedExitCode]
	unchanged, err := strconv.Atoi(declared)
	require.NoError(t, err, "the Job environment names the exit code")
	for name, tc := range map[string]struct {
		mutate func(map[string]*stubHost)
		env    map[string]string
		script func(t *testing.T) string
		code   int
	}{
		"refused by the overwrite guard": {
			mutate: func(h map[string]*stubHost) { h["d1"].tables = []string{"db1.t1"} },
			code:   unchanged,
		},
		"backup missing in the storage": {
			env:  map[string]string{envBackupName: "bk-2"},
			code: unchanged,
		},
		"sidecar unreachable before any change": {
			mutate: func(h map[string]*stubHost) { h["d1"].unavailable = -1 },
			env:    map[string]string{envOverwrite: "true"},
			script: func(t *testing.T) string { return scriptGivingUpAfter(t, 0) },
			code:   unchanged,
		},
		"a restore that failed after it started": {
			mutate: func(h map[string]*stubHost) { h["d1"].failOp = map[string]string{"restore": "disk full"} },
			code:   1,
		},
		// The download of the backup to every host comes first, and changes nothing
		"a download that failed": {
			mutate: func(h map[string]*stubHost) { h["d1"].failOp = map[string]string{"download": "not enough space"} },
			code:   unchanged,
		},
		"a download the sidecar may have accepted": {
			mutate: func(h map[string]*stubHost) { h["s0"].hangUpPosts = true },
			code:   unchanged,
		},
		"a request the sidecar may have accepted": {
			mutate: func(h map[string]*stubHost) { h["s0"].hangUpPosts, h["s0"].failPostsAfter = true, 1 },
			code:   1,
		},
		"a request a proxy failed after passing it on": {
			mutate: func(h map[string]*stubHost) { h["s0"].proxyFailsPosts, h["s0"].failPostsAfter = true, 1 },
			script: func(t *testing.T) string { return scriptGivingUpAfter(t, 0) },
			code:   1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			hosts := restoreHosts()
			if tc.mutate != nil {
				tc.mutate(hosts)
			}
			env := restoreEnv(tc.env)
			env[envUnchangedExitCode] = declared
			script := orchestratorScript
			if tc.script != nil {
				script = tc.script(t)
			}
			s := newSidecarStub(t, hosts)
			code, out := s.runScript(script, env)
			require.Equal(t, tc.code, code, out)
		})
	}
}

// A change a proxy failed may have reached the sidecar: it is never submitted again, which would run
// the restore, or make the backup, twice.
func TestJobScriptNeverResubmitsAChangeAProxyFailed(t *testing.T) {
	hosts := restoreHosts()
	hosts["s0"].proxyFailsPosts, hosts["s0"].failPostsAfter = true, 1
	s := newSidecarStub(t, hosts)
	// Long enough for a retry to happen, if the script retried
	code, out := s.runScript(scriptGivingUpAfter(t, 1), restoreEnv(nil))
	require.Equal(t, 1, code, out)
	// The downloads, and the schema restore the proxy failed
	require.Len(t, s.matching(http.MethodPost, "/"), 3, out)
	require.Len(t, s.matching(http.MethodPost, "/backup/restore/"), 1, out)
}

// scheduledRun returns a run of the schedule's CronJob as the CronJob controller creates it for the
// slot due at the given time, with the Job name labels the API server gives its pods.
func scheduledRun(chbs *api.ClickHouseBackupSchedule, due time.Time) *batch.Job {
	cronJob := BuildBackupCronJob(chbs, testInstallation(), testTopology(), testOrchestrator(), false)
	job := &batch.Job{Spec: cronJob.Spec.JobTemplate.Spec}
	job.Name = fmt.Sprintf("%s-%d", cronJob.Name, due.Unix()/60)
	job.Spec.Template.Labels = map[string]string{LegacyJobNameLabel: job.Name, batch.JobNameLabel: job.Name}
	return job
}

// scriptEnv returns the environment the Job script of a Job sees: the plain values. The poll interval and the
// termination message path are left to the test.
func scriptEnv(job *batch.Job) map[string]string {
	values := map[string]string{}
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		if e.ValueFrom == nil {
			values[e.Name] = e.Value
		}
	}
	delete(values, envPollInterval)
	delete(values, envTerminationMessagePath)
	values[envJobName] = job.Spec.Template.Labels[LegacyJobNameLabel]
	return values
}

// A scheduled backup is named after the wall time of its slot in the time zone of the schedule: the CronJob controller
// names the Job of a run after the minute it was due, which the script reads from the Job name label of its pod, so
// every run of one slot makes the same backup, and the operator derives its name the same way. A Job named otherwise
// takes the current minute.
func TestJobScriptNamesScheduledBackupsAfterTheirSlot(t *testing.T) {
	due := time.Date(2023, time.November, 14, 22, 14, 0, 0, time.UTC)
	for name, tc := range map[string]struct {
		zone    string
		jobName func(cronJob string) string
		backup  string
	}{
		"a run of the CronJob":                     {backup: "-20231114221400"},
		"a run in the zone of the schedule":        {zone: "Europe/Berlin", backup: "-20231114231400"},
		"a run in UTC by name":                     {zone: "UTC", backup: "-20231114221400"},
		"a Job created by hand from the CronJob":   {jobName: func(string) string { return "nightly-backup-manual" }},
		"a pod without the Job name label":         {jobName: func(string) string { return "" }},
		"a slot past the ten digits":               {jobName: func(cronJob string) string { return cronJob + "-28333334000" }},
		"a slot past the four-digit years":         {jobName: func(cronJob string) string { return cronJob + "-9999999999" }},
		"a Job of another CronJob of that pattern": {jobName: func(string) string { return "other-backup-28333334" }},
	} {
		t.Run(name, func(t *testing.T) {
			chbs := testSchedule("nightly")
			chbs.Spec.TimeZone = tc.zone
			run := scheduledRun(chbs, due)
			env := scriptEnv(run)
			env[envScheduleTimezone] = tc.zone
			if tc.jobName != nil {
				env[envJobName] = tc.jobName(env[envCronJobName])
			}
			s := newSidecarStub(t, map[string]*stubHost{"chi-demo-main-0-0": {}, "chi-demo-main-1-0": {}})
			code, out := s.run(env)
			require.Zero(t, code, out)
			made := s.matching(http.MethodPost, "/backup/create_remote")[0].query.Get("name")
			prefix := env[envBackupNamePrefix]
			if tc.backup != "" {
				require.Equal(t, prefix+tc.backup, made)
				if tc.zone == "" {
					require.Equal(t, ScheduledBackupName(run), made, "the operator derives the same name")
				}
				return
			}
			require.Regexp(t, `^`+regexp.QuoteMeta(prefix)+`-[0-9]{12}00$`, made, "a run named otherwise takes the current minute")
			require.NotEqual(t, prefix+"-20231114221400", made)
		})
	}
}

// The longest prefix the API admits gives a name of the format the operator derives, within the backup name limit,
// which the script admits back as the name of the backup to restore.
func TestJobScriptScheduledBackupNameFitsTheBackupNameLimit(t *testing.T) {
	prefix := strings.Repeat("p", api.BackupNamePrefixMaxLength)
	s := newSidecarStub(t, map[string]*stubHost{"h0": {}})
	code, out := s.run(map[string]string{
		envOperation: operationBackup, envBackupNamePrefix: prefix, envBackupHosts: "h0",
		envCronJobName: testCronJobName, envJobName: slotJobName("2026-01-02T03:04:00Z"),
	})
	require.Zero(t, code, out)
	made := s.matching(http.MethodPost, "/backup/create_remote")[0].query.Get("name")
	require.Len(t, made, api.BackupNamePrefixMaxLength+api.BackupNameScheduleSuffixLength)
	require.LessOrEqual(t, len(made), api.BackupNameMaxLength)
	timestamp, found := strings.CutPrefix(made, prefix+"-")
	require.True(t, found, made)
	_, err := time.Parse(scheduleTimestampLayout, timestamp)
	require.NoError(t, err, made)

	s = newSidecarStub(t, map[string]*stubHost{"h0": {remote: []stubBackup{remote(made, "")}}})
	code, out = s.run(map[string]string{
		envOperation: operationRestore, envBackupName: made,
		envSchemaHosts: "h0", envDataHosts: "h0", envGuardHosts: "h0",
	})
	require.Zero(t, code, out)

	s = newSidecarStub(t, map[string]*stubHost{"h0": {}})
	code, out = s.run(map[string]string{
		envOperation: operationBackup, envBackupNamePrefix: prefix + "pp", envBackupHosts: "h0",
	})
	require.Equal(t, 1, code, out)
	require.Contains(t, out, "BACKUP_NAME_PREFIX is too long for a backup name")
	require.Zero(t, s.countCalls())
}

func lostOpEnv(operation string) map[string]string {
	if operation == operationVerify {
		return map[string]string{envOperation: operationVerify, envBackupName: "bk-1", envVerifyHosts: "h0"}
	}
	return map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0", envDiffFromRemote: "base"}
}

// A restarted sidecar forgets the operation the script polls, and resumes an interrupted upload or
// download by itself, under a new action. The script follows that action instead of failing a
// backup that completes; when nothing tells the outcome, it says that the backup may still complete.
func TestJobScriptFollowsAnOperationResumedAfterARestart(t *testing.T) {
	for name, tc := range map[string]struct {
		operation string
		host      stubHost
		// resumeTimeout is how long the script waits for a resumed action, 0 when the test does not
		// depend on it
		resumeTimeout int
		code          int
		output        string
	}{
		"resumed upload succeeds": {
			operation: operationBackup,
			host:      stubHost{lostOps: true, resume: "success"},
			output:    "h0: the resumed upload of bk-1 succeeded",
		},
		"resumed upload fails": {
			operation: operationBackup,
			host:      stubHost{lostOps: true, resume: "error"},
			code:      1,
			output:    "backup bk-1 failed on h0: h0: the resumed upload of bk-1 ended with status 'error': resumed upload --diff-from-remote=base --resumable=1 bk-1 failed",
		},
		"completed before the restart": {
			operation: operationBackup,
			host:      stubHost{lostOps: true, completedBeforeRestart: true},
			output:    "h0: create_remote bk-1 completed before the sidecar restarted",
		},
		"nothing resumed": {
			operation: operationBackup,
			host:      stubHost{lostOps: true},
			code:      1,
			output:    "the outcome is unknown: the remote backup bk-1 may still complete",
		},
		"resumed download of a verification succeeds": {
			operation: operationVerify,
			host:      stubHost{lostOps: true, resume: "success"},
			output:    "h0: the resumed download of bk-1 succeeded",
		},
		"resumed upload that starts late": {
			operation:     operationBackup,
			host:          stubHost{lostOps: true, resume: "success", resumeDelay: 5},
			resumeTimeout: 10,
			output:        "h0: the resumed upload of bk-1 succeeded",
		},
		"resumed upload interrupted by another restart": {
			operation:     operationBackup,
			host:          stubHost{lostOps: true, resume: "success", restartAgainAfter: 3 * time.Second},
			resumeTimeout: 2,
			output:        "h0: the resumed upload of bk-1 succeeded",
		},
	} {
		t.Run(name, func(t *testing.T) {
			host := tc.host
			host.remote = []stubBackup{remote("base", "")}
			if tc.operation == operationVerify {
				host.remote = append(host.remote, remote("bk-1", ""))
			}
			s := newSidecarStub(t, map[string]*stubHost{"h0": &host})
			code, out := s.runScript(scriptWithTimeout(t, orchestratorScript, "RESUME_TIMEOUT", tc.resumeTimeout), lostOpEnv(tc.operation))
			require.Equal(t, tc.code, code, out)
			require.Contains(t, out, tc.output)
			if (tc.code == 0) && (tc.operation == operationBackup) {
				require.Contains(t, s.remoteNames("h0"), "bk-1")
			}
		})
	}

	// clickhouse-backup resumes only the download of an interrupted restore, never the restore
	t.Run("restore", func(t *testing.T) {
		hosts := restoreHosts()
		hosts["s0"].lostOps, hosts["s0"].resume = true, "success"
		s := newSidecarStub(t, hosts)
		code, out := s.run(restoreEnv(map[string]string{envOverwrite: "true"}))
		require.Equal(t, 1, code, out)
		require.Contains(t, out, "schema restore of bk-1 failed on s0: s0: the sidecar restarted during restore schema bk-1, which it does not resume: the restore is partially applied")
	})
}

// withEnv returns a copy of env with overrides applied
func withEnv(env, overrides map[string]string) map[string]string {
	merged := map[string]string{}
	for k, v := range env {
		merged[k] = v
	}
	for k, v := range overrides {
		merged[k] = v
	}
	return merged
}

// replicaRestoreHosts returns the sidecars of a cluster of two shards of two replicas each - a0 and a1
// replicate shard 0, b0 and b1 shard 1 - each of which lists backup bk-1
func replicaRestoreHosts() map[string]*stubHost {
	hosts := map[string]*stubHost{}
	for _, host := range []string{"a0", "a1", "b0", "b1"} {
		hosts[host] = &stubHost{remote: []stubBackup{remote("bk-1", "")}}
	}
	return hosts
}

// replicaRestoreEnv returns the environment of a restore of that cluster: the schema on every
// replica, the data on the first replica of each shard
func replicaRestoreEnv(overrides map[string]string) map[string]string {
	return restoreEnv(withEnv(map[string]string{
		envSchemaHosts: "a0 a1 b0 b1",
		envDataHosts:   "a0 b0",
		envGuardHosts:  "a0 a1 b0 b1",
	}, overrides))
}

// restoreSteps lists the restore requests the hosts received, in order: an action as its command, a
// typed request as its query
func (s *sidecarStub) restoreSteps() []string {
	var steps []string
	for _, c := range s.matching(http.MethodPost, "/backup/") {
		switch {
		case c.path == "/backup/actions":
			steps = append(steps, c.host+" "+c.command)
		case strings.HasPrefix(c.path, "/backup/restore_remote/"), strings.HasPrefix(c.path, "/backup/restore/"),
			strings.HasPrefix(c.path, "/backup/download/"):
			var params []string
			for key, values := range c.query {
				for _, value := range values {
					params = append(params, key+"="+value)
				}
			}
			slices.Sort(params)
			if strings.HasPrefix(c.path, "/backup/download/") {
				params = append([]string{"download"}, params...)
			}
			steps = append(steps, c.host+" "+strings.Join(params, " "))
		}
	}
	return steps
}

// Every replica restores the schema on its own, with nothing ON CLUSTER, which would have
// clickhouse-backup rewrite a Distributed table that names another cluster. The data is restored on
// the first replica of each shard, which the other replicas replicate it from.
func TestJobScriptRestoresTheSchemaOnEveryReplica(t *testing.T) {
	s := newSidecarStub(t, replicaRestoreHosts())
	code, out := s.run(replicaRestoreEnv(nil))
	require.Zero(t, code, out)
	require.Equal(t, []string{
		"a0 download hardlink_exists_files=", "a1 download schema=", "b0 download hardlink_exists_files=", "b1 download schema=",
		"a0 rm= schema=", "a1 rm= schema=", "b0 rm= schema=", "b1 rm= schema=",
		"a0 data=", "b0 data=",
	}, s.restoreSteps())
}

// An overwrite has to empty the replication of every replicated table on every replica before any
// replica recreates its tables, or a recreated replica clones the old parts back from another: it
// drops and recreates the tables of the backup ON CLUSTER from the first schema host first, through
// the one command the script runs on the actions endpoint. Every replica then restores the schema on
// its own, which undoes what the ON CLUSTER DDL rewrote, and the data follows. A table pattern narrows
// every step.
func TestJobScriptOverwriteDropsTheBackupOnClusterFirst(t *testing.T) {
	s := newSidecarStub(t, withReplicatedTable(replicaRestoreHosts()))
	code, out := s.run(replicaRestoreEnv(map[string]string{envOverwrite: "true", envTables: "db1.*,db2.t?"}))
	require.Zero(t, code, out)
	require.Equal(t, []string{
		"a0 download hardlink_exists_files= table=db1.*,db2.t?", "a1 download schema= table=db1.*,db2.t?",
		"b0 download hardlink_exists_files= table=db1.*,db2.t?", "b1 download schema= table=db1.*,db2.t?",
		"a0 restore --schema --rm --env=RESTORE_SCHEMA_ON_CLUSTER=main --tables=db1.*,db2.t? bk-1",
		"a0 rm= schema= table=db1.*,db2.t?", "a1 rm= schema= table=db1.*,db2.t?",
		"b0 rm= schema= table=db1.*,db2.t?", "b1 rm= schema= table=db1.*,db2.t?",
		"a0 data= table=db1.*,db2.t?", "b0 data= table=db1.*,db2.t?",
	}, s.restoreSteps())
	require.Contains(t, out, "a0: restore schema on cluster main bk-1 succeeded")
}

// The answer to a command of the actions endpoint names no operation: the script follows the row of
// the actions log that started after it submitted the command, not a row an earlier run of the same
// command left behind.
func TestJobScriptFollowsTheRowOfItsOwnAction(t *testing.T) {
	hosts := withReplicatedTable(replicaRestoreHosts())
	command := "restore_remote --schema --rm --env=RESTORE_SCHEMA_ON_CLUSTER=main bk-1"
	hosts["a0"].actions = []*stubAction{
		{command: command, start: "2025-12-31 23:59:58", status: "error", err: "an earlier run failed"},
		{command: command, start: "2025-12-31 23:59:59", status: "success"},
	}
	s := newSidecarStub(t, hosts)
	code, out := s.run(replicaRestoreEnv(map[string]string{envOverwrite: "true"}))
	require.Zero(t, code, out)
	require.Len(t, s.matching(http.MethodPost, "/backup/actions"), 1)
	require.Len(t, s.matching(http.MethodPost, "/backup/restore/"), 6)
}

// A sidecar that runs another operation refuses a command of the actions endpoint with HTTP 500 and
// the error it refuses the other requests with HTTP 423 with: the command was not accepted, so it is
// submitted again once the sidecar is idle.
func TestJobScriptQueuesAnActionWhileTheSidecarIsLocked(t *testing.T) {
	hosts := withReplicatedTable(replicaRestoreHosts())
	hosts["a0"].lockedActions = 2
	s := newSidecarStub(t, hosts)
	code, out := s.run(replicaRestoreEnv(map[string]string{envOverwrite: "true"}))
	require.Zero(t, code, out)
	require.Contains(t, out, "a0: sidecar busy, queueing restore schema on cluster main")
	require.Len(t, s.matching(http.MethodPost, "/backup/actions"), 3, "two refusals, then accepted")
	require.Len(t, s.matching(http.MethodPost, "/backup/restore/"), 6)
}

// The drop ON CLUSTER changes every replica: when it fails, or the sidecar restarts during it and
// forgets it, no other step runs, and the restore is partially applied.
func TestJobScriptStopsWhenTheDropOnClusterFails(t *testing.T) {
	for name, tc := range map[string]struct {
		host   stubHost
		output string
	}{
		"the action fails": {
			host:   stubHost{failAction: "code: 47, unknown identifier"},
			output: "schema restore of bk-1 on cluster main failed on a0: a0: restore schema on cluster main bk-1 ended with status 'error': code: 47, unknown identifier",
		},
		"the sidecar restarts during the action": {
			host:   stubHost{loseActions: true},
			output: "a0: the sidecar restarted during restore schema on cluster main bk-1, which it does not resume: the restore is partially applied",
		},
	} {
		t.Run(name, func(t *testing.T) {
			host := tc.host
			hosts := replicaRestoreHosts()
			host.remote = hosts["a0"].remote
			hosts["a0"] = &host
			withReplicatedTable(hosts)
			s := newSidecarStub(t, hosts)
			code, out := s.run(replicaRestoreEnv(map[string]string{envOverwrite: "true", envUnchangedExitCode: "3"}))
			require.Equal(t, 1, code, out)
			require.Contains(t, out, tc.output)
			require.Empty(t, s.matching(http.MethodPost, "/backup/restore/"))
		})
	}
}

// The drop ON CLUSTER drops and recreates on every shard the tables of the backup of the first schema
// host's shard: a table only another shard's backup holds would keep its replication, which clones its
// old parts back, and one only the first shard's backup holds would be made on shards that never had
// it. An overwrite whose backup holds other tables the pattern takes in the storage of the first
// replica of one shard than in that of another fails before anything changes; so does one whose tables
// cannot be told.
func TestJobScriptRefusesAnOverwriteWhoseShardsHoldOtherTables(t *testing.T) {
	for name, tc := range map[string]struct {
		tables string
		a0, b0 []string
		mutate func(*stubHost)
		// refused is the reason the Job fails for, empty when it runs
		refused string
	}{
		"the same tables": {
			a0: []string{"db1.t1", "db2.t1"}, b0: []string{"db2.t1", "db1.t1"},
		},
		"a table only another shard's backup holds": {
			a0: []string{"db1.t1"}, b0: []string{"db1.t1", "db1.t9", "db0.t0"},
			refused: "refusing to overwrite: the tables of bk-1 differ between the shards of cluster main (db0.t0 only on b0, db1.t9 only on b0): the drop ON CLUSTER drops and recreates on every shard the tables of the backup of a0",
		},
		"a table only the first shard's backup holds": {
			a0: []string{"db1.t1", "db1.t2"}, b0: []string{"db1.t1"},
			refused: "(db1.t2 not on b0)",
		},
		"other tables the pattern leaves out": {
			tables: "db1.t1", a0: []string{"db1.t1", "db1.t2"}, b0: []string{"db1.t1", "db9.t1"},
		},
		"a table that does not read as one": {
			a0: []string{"db1.t1"}, b0: []string{"db1.t1"},
			mutate:  func(h *stubHost) { h.rawBackupTables = []string{`{"Database":"db1","Name":"t2"}`} },
			refused: "refusing to overwrite: b0 lists a table of bk-1 that does not read as one",
		},
		"tables that cannot be listed": {
			a0: []string{"db1.t1"}, b0: []string{"db1.t1"},
			mutate:  func(h *stubHost) { h.backupTablesFail = "can't connect to remote storage" },
			refused: "preflight failed: cannot list the tables of bk-1 in the storage of b0 (HTTP 500): can't connect to remote storage",
		},
	} {
		t.Run(name, func(t *testing.T) {
			hosts := withReplicatedTable(replicaRestoreHosts())
			hosts["a0"].backupTables, hosts["b0"].backupTables = tc.a0, tc.b0
			if tc.mutate != nil {
				tc.mutate(hosts["b0"])
			}
			s := newSidecarStub(t, hosts)
			code, out := s.run(replicaRestoreEnv(map[string]string{envOverwrite: "true", envTables: tc.tables, envUnchangedExitCode: "3"}))
			var listed []string
			for _, c := range s.matching(http.MethodGet, "/backup/tables") {
				if c.path == "/backup/tables" {
					listed = append(listed, c.host)
				}
			}
			require.Equal(t, []string{"a0", "b0"}, listed, "the tables of the backup of the first replica of every shard")
			if tc.refused == "" {
				require.Zero(t, code, out)
				requirePreflightFirst(t, s)
				return
			}
			require.Equal(t, 3, code, out)
			require.Contains(t, out, "ERROR: ")
			require.Contains(t, out, tc.refused)
			require.Empty(t, s.matching(http.MethodPost, "/"), "nothing changes before the check passed")
		})
	}
}

// ClickHouse drops a table larger than max_table_size_to_drop only when a flag file on its own host
// forces it, which clickhouse-backup writes on the host that runs the drop ON CLUSTER alone: on any
// other host the drop would fail half way, the smaller tables already dropped everywhere. An overwrite
// that would drop such a table on another host - one of the backup the pattern takes, or any of a
// database it drops as a whole, of which the backup may list no table when the pattern takes it -
// fails before anything changes, and tells how to lift the limit; the sizes are those
// /backup/tables/all lists, compared without bash arithmetic, and a table whose size cannot be told
// counts as too large. A limit of 0 lifts it.
func TestJobScriptRefusesAnOverwriteWhoseDropOnClusterExceedsTheSizeLimit(t *testing.T) {
	larger := uint64(testMaxTableSizeToDrop + 1)
	// sized gives the host the table of the given size
	sized := func(host, table string, size uint64) func(map[string]*stubHost) {
		return func(h map[string]*stubHost) {
			h[host].tables = append(h[host].tables, table)
			h[host].sizes = map[string]uint64{table: size}
		}
	}
	for name, tc := range map[string]struct {
		env    map[string]string
		mutate func(map[string]*stubHost)
		// refused names the tables the refusal names, or why it refuses, empty when the restore runs
		refused string
	}{
		"a larger table on another replica": {
			mutate:  sized("a1", "db1.t2", larger),
			refused: "(a1:db1.t2 (50000000001 bytes))",
		},
		"larger tables on another shard": {
			mutate: func(h map[string]*stubHost) {
				h["b1"].tables = append(h["b1"].tables, "db1.t2")
				h["b1"].sizes = map[string]uint64{"db1.t1": larger, "db1.t2": larger}
			},
			refused: "(b1:db1.t1 (50000000001 bytes), b1:db1.t2 (50000000001 bytes))",
		},
		"a larger table on the host that drops it":                  {mutate: sized("a0", "db1.t2", larger)},
		"a table at the limit":                                      {mutate: sized("a1", "db1.t2", testMaxTableSizeToDrop)},
		"a larger table the restore keeps":                          {env: map[string]string{envTables: "db1.t1"}, mutate: sized("a1", "db1.t2", larger)},
		"a larger table of a database the backup lacks":             {mutate: sized("a1", "db9.t1", larger)},
		"a larger table of a database the restore drops as a whole": {env: map[string]string{envTables: "db1.*"}, mutate: sized("a1", "db1.new", larger), refused: "(a1:db1.new (50000000001 bytes))"},
		"a size bash arithmetic wraps":                              {mutate: sized("a1", "db1.t2", 18446744073709551615), refused: "(a1:db1.t2 (18446744073709551615 bytes))"},
		"a table of unknown size": {
			mutate: func(h map[string]*stubHost) {
				h["a1"].rawTables = []string{`{"Database":"db1","Name":"t2","Engine":"MergeTree"}`}
			},
			refused: "(a1:db1.t2 (of unknown size))",
		},
		"a table that does not read as one": {
			mutate:  func(h map[string]*stubHost) { h["b0"].rawTables = []string{`{"database":"db1","name":"t1"}`} },
			refused: "refusing to overwrite: b0 lists a table that does not read as one",
		},
		"a larger table of a database the restore drops as a whole, of which the backup lists no table": {
			env:     map[string]string{envTables: "db1.t1,db2.*"},
			mutate:  sized("a1", "db2.new", larger),
			refused: "(a1:db2.new (50000000001 bytes))",
		},
		"no limit": {env: map[string]string{envMaxTableSizeToDrop: "0"}, mutate: sized("a1", "db1.t2", larger)},
	} {
		t.Run(name, func(t *testing.T) {
			hosts := withReplicatedTable(replicaRestoreHosts())
			for _, host := range hosts {
				host.backupTables = []string{"db1.t1", "db1.t2"}
			}
			tc.mutate(hosts)
			s := newSidecarStub(t, hosts)
			code, out := s.run(replicaRestoreEnv(withEnv(map[string]string{envOverwrite: "true", envUnchangedExitCode: "3"}, tc.env)))
			var listed []string
			for _, c := range s.matching(http.MethodGet, "/backup/tables/all") {
				listed = append(listed, c.host)
			}
			require.Equal(t, []string{"a0", "a1", "b0", "b1"}, listed)
			if tc.refused == "" {
				require.Zero(t, code, out)
				require.Len(t, s.matching(http.MethodPost, "/backup/actions"), 1, "the drop ON CLUSTER runs")
				return
			}
			require.Equal(t, 3, code, out)
			require.Contains(t, out, "ERROR: refusing to overwrite: ")
			require.Contains(t, out, tc.refused)
			if !strings.Contains(tc.refused, "does not read") {
				require.Contains(t, out, "the drop ON CLUSTER from a0 would stop at tables larger than max_table_size_to_drop (50000000000 bytes) of cluster main on other hosts")
				require.Contains(t, out, "set max_table_size_to_drop to 0 wherever a limit is set (the smallest non-zero limit applies), or drop these tables by hand, and restore again")
			}
			require.Empty(t, s.matching(http.MethodPost, "/"), "nothing changes before the check passed")
		})
	}
}

// clickhouse-backup drops a database the restore takes as a whole with one DROP DATABASE, for which it
// writes the one flag file that forces the drop of a table larger than max_table_size_to_drop: ClickHouse
// takes the flag at the first such table of the database and stops at the second, the databases before
// it already dropped. An overwrite fails before anything changes when a host that drops the databases on
// its own - the one that runs the drop ON CLUSTER, or every host when each drops the tables of the backup
// on its own - holds two such tables of one database the restore drops as a whole: without a pattern one
// of which the backup of any shard holds a table, with one any it takes as a whole, as the backup may hold
// it without a table, or any database once a table of the backup does not read as one. A table of unknown
// size counts as larger, and a row of the host that does not read as a table, larger or of unknown size,
// counts in every database of the host, as its name does not tell its database; the refusal then tells
// that the tables may be larger. Tables the restore drops one by one, each forced by a flag of its own,
// and those of a database it keeps do not count, nor does any with a limit of 0. Without a pattern, the
// tables of the backup are listed for this only when a database could stop the drop.
func TestJobScriptRefusesAnOverwriteThatStopsInADatabaseDroppedAsAWhole(t *testing.T) {
	larger := uint64(testMaxTableSizeToDrop + 1)
	// sized gives the host the tables of the given size
	sized := func(host string, size uint64, tables ...string) func(map[string]*stubHost) {
		return func(h map[string]*stubHost) {
			if h[host].sizes == nil {
				h[host].sizes = map[string]uint64{}
			}
			for _, table := range tables {
				h[host].tables = append(h[host].tables, table)
				h[host].sizes[table] = size
			}
		}
	}
	// unreadable gives the host a row of /backup/tables/all of each given size that does not read as a
	// table, as its name holds a character the JSON escapes, or one of unknown size without any size
	unreadable := func(host string, sizes ...uint64) func(map[string]*stubHost) {
		return func(h map[string]*stubHost) {
			const row = `{"Database":"db9","Name":"a\u0026b","Engine":"MergeTree"`
			if len(sizes) == 0 {
				h[host].rawTables = append(h[host].rawTables, row+"}")
			}
			for _, size := range sizes {
				h[host].rawTables = append(h[host].rawTables, fmt.Sprintf(`%s,"TotalBytes":%d}`, row, size))
			}
		}
	}
	both := func(mutations ...func(map[string]*stubHost)) func(map[string]*stubHost) {
		return func(h map[string]*stubHost) {
			for _, mutate := range mutations {
				mutate(h)
			}
		}
	}
	for name, tc := range map[string]struct {
		// oneReplica restores a cluster of one replica in every shard, a0 and b0, instead of one of two
		// replicas in every shard that holds a replicated table, which a0 drops ON CLUSTER
		oneReplica bool
		env        map[string]string
		mutate     func(map[string]*stubHost)
		// local tells that every replica drops the tables of the backup on its own
		local bool
		// refused names the tables the refusal names, empty when the restore runs
		refused string
		// uncertain tells that some of them may be no larger, or of another database
		uncertain bool
		// listed tells whether the tables of the backup were listed, of a cluster of one replica in every shard
		listed bool
	}{
		"two larger tables of a database on the host that runs the drop ON CLUSTER": {
			mutate:  sized("a0", larger, "db1.big1", "db1.big2"),
			refused: "a0:db1.big1 (50000000001 bytes), a0:db1.big2 (50000000001 bytes)",
		},
		"one larger table of a database there": {
			mutate: sized("a0", larger, "db1.big1"),
		},
		"two larger tables of a database the backup lacks there": {
			mutate: sized("a0", larger, "db9.big1", "db9.big2"),
		},
		"two larger tables of a database the pattern takes as a whole there": {
			env:     map[string]string{envTables: "db1.*"},
			mutate:  sized("a0", larger, "db1.big1", "db1.big2"),
			refused: "a0:db1.big1 (50000000001 bytes), a0:db1.big2 (50000000001 bytes)",
		},
		"two larger tables of a database the pattern takes as a whole, of which the backup lists no table": {
			env:     map[string]string{envTables: "db1.t1,db2.*"},
			mutate:  sized("a0", larger, "db2.big1", "db2.big2"),
			refused: "a0:db2.big1 (50000000001 bytes), a0:db2.big2 (50000000001 bytes)",
		},
		"two larger tables the pattern takes one by one there": {
			env: map[string]string{envTables: "db1.t1,db1.big1,db1.big2"},
			mutate: both(sized("a0", larger, "db1.big1", "db1.big2"), func(h map[string]*stubHost) {
				for _, host := range h {
					host.backupTables = []string{"db1.t1", "db1.big1", "db1.big2"}
				}
			}),
		},
		"two tables of unknown size there": {
			mutate: func(h map[string]*stubHost) {
				h["a0"].rawTables = []string{`{"Database":"db1","Name":"u1","Engine":"MergeTree"}`, `{"Database":"db1","Name":"u2","Engine":"MergeTree"}`}
			},
			refused:   "a0:db1.u1 (of unknown size), a0:db1.u2 (of unknown size)",
			uncertain: true,
		},
		"a larger table and a row of unknown size that does not read as a table there": {
			mutate:    both(sized("a0", larger, "db1.big1"), unreadable("a0")),
			refused:   "a0:? (a table whose name cannot be read, of unknown size), a0:db1.big1 (50000000001 bytes)",
			uncertain: true,
		},
		"a larger table and a larger row that does not read as a table there": {
			mutate:    both(sized("a0", larger, "db1.big1"), unreadable("a0", larger)),
			refused:   "a0:? (a table whose name cannot be read, 50000000001 bytes), a0:db1.big1 (50000000001 bytes)",
			uncertain: true,
		},
		"a larger table and rows that do not read as a table, within the limit, there": {
			mutate: both(sized("a0", larger, "db1.big1"), unreadable("a0", testMaxTableSizeToDrop, 1)),
		},
		// clickhouse-backup forces the drop of any one table there
		"a row that does not read as a table there": {
			mutate: unreadable("a0"),
		},
		"two larger tables of a database there, without a limit": {
			env:    map[string]string{envMaxTableSizeToDrop: "0"},
			mutate: sized("a0", larger, "db1.big1", "db1.big2"),
		},
		"two larger tables of a database on another replica, when every replica drops the backup on its own": {
			mutate: both(sized("a1", larger, "db1.big1", "db1.big2"), func(h map[string]*stubHost) {
				for _, host := range h {
					host.engines = nil
				}
			}),
			local:   true,
			refused: "a1:db1.big1 (50000000001 bytes), a1:db1.big2 (50000000001 bytes)",
		},
		"two larger tables of a database on a host of a cluster of one replica in every shard": {
			oneReplica: true,
			mutate:     sized("b0", larger, "db1.big1", "db1.big2"),
			refused:    "b0:db1.big1 (50000000001 bytes), b0:db1.big2 (50000000001 bytes)",
			listed:     true,
		},
		"larger tables of other databases on a host of a cluster of one replica in every shard": {
			oneReplica: true,
			mutate:     sized("b0", larger, "db1.big1", "db2.big2", "db3.big3"),
		},
		"two larger tables of a database the backup lacks on a host of a cluster of one replica in every shard": {
			oneReplica: true,
			mutate:     sized("b0", larger, "db9.big1", "db9.big2"),
			listed:     true,
		},
		"two larger tables of a database the backup may hold, as a table of the backup does not read as one, on a host of a cluster of one replica in every shard": {
			oneReplica: true,
			mutate: both(sized("b0", larger, "db9.big1", "db9.big2"), func(h map[string]*stubHost) {
				h["a0"].rawBackupTables = append(h["a0"].rawBackupTables, `{"Database":"db1","Name":"t2"}`)
			}),
			refused: "b0:db9.big1 (50000000001 bytes), b0:db9.big2 (50000000001 bytes)",
			listed:  true,
		},
		"two rows that do not read as a table, within the limit, on a host of a cluster of one replica in every shard": {
			oneReplica: true,
			mutate:     unreadable("b0", 1, 1),
		},
		"two larger rows that do not read as a table on a host of a cluster of one replica in every shard": {
			oneReplica: true,
			mutate:     unreadable("b0", larger, larger),
			refused:    "b0:? (a table whose name cannot be read, 50000000001 bytes), b0:? (a table whose name cannot be read, 50000000001 bytes)",
			uncertain:  true,
			listed:     true,
		},
		"two larger tables of a database the pattern takes as a whole on a host of a cluster of one replica in every shard": {
			oneReplica: true,
			env:        map[string]string{envTables: "db1.*"},
			mutate:     sized("b0", larger, "db1.big1", "db1.big2"),
			refused:    "b0:db1.big1 (50000000001 bytes), b0:db1.big2 (50000000001 bytes)",
		},
		"two larger tables of a database on a host of a cluster of one replica in every shard, without a limit": {
			oneReplica: true,
			env:        map[string]string{envMaxTableSizeToDrop: "0"},
			mutate:     sized("b0", larger, "db1.big1", "db1.big2"),
		},
		"two larger tables of a database on another replica, when every replica drops the backup on its own, without a limit": {
			env: map[string]string{envMaxTableSizeToDrop: "0"},
			mutate: both(sized("a1", larger, "db1.big1", "db1.big2"), func(h map[string]*stubHost) {
				for _, host := range h {
					host.engines = nil
				}
			}),
			local: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			hosts := withReplicatedTable(replicaRestoreHosts())
			env := map[string]string{envOverwrite: "true", envUnchangedExitCode: "3"}
			if tc.oneReplica {
				delete(hosts, "a1")
				delete(hosts, "b1")
				env = withEnv(env, map[string]string{envSchemaHosts: "a0 b0", envDataHosts: "a0 b0", envGuardHosts: "a0 b0"})
			}
			tc.mutate(hosts)
			s := newSidecarStub(t, hosts)
			code, out := s.run(replicaRestoreEnv(withEnv(env, tc.env)))
			if tc.oneReplica {
				require.Equal(t, tc.listed, slices.ContainsFunc(s.matching(http.MethodGet, "/backup/tables"), func(c stubCall) bool {
					return c.path == "/backup/tables"
				}), out)
			}
			if tc.refused == "" {
				require.Zero(t, code, out)
				requirePreflightFirst(t, s)
				actions := 1
				if tc.oneReplica || tc.local {
					actions = 0
				}
				require.Len(t, s.matching(http.MethodPost, "/backup/actions"), actions, "the drop ON CLUSTER runs where it has to")
				require.NotEmpty(t, s.matching(http.MethodPost, "/backup/restore/"))
				return
			}
			require.Equal(t, 3, code, out)
			stop := "larger than"
			if tc.uncertain {
				stop = "that is, or may be, larger than"
			}
			require.Contains(t, out, "ERROR: refusing to overwrite: a host of cluster main that drops a database as a whole would stop at the second table of it "+stop+" max_table_size_to_drop (50000000000 bytes) ("+tc.refused+"), as clickhouse-backup forces the drop of only one such table of each database it drops as a whole; set max_table_size_to_drop to 0 wherever a limit is set (the smallest non-zero limit applies), or drop these tables by hand, and restore again")
			require.Empty(t, s.matching(http.MethodPost, "/"), "nothing changes before the check passed")
		})
	}
}

// The settings of the drops of an overwrite come from the operator: whether the cluster has ZooKeeper
// must be a boolean, and the limit of the size of a table it drops a number of bytes, also when every
// shard has one replica, or the Job fails before any request.
func TestJobScriptRejectsAnInvalidOverwriteEnv(t *testing.T) {
	oneReplica := map[string]string{envSchemaHosts: "a0 b0", envDataHosts: "a0 b0", envGuardHosts: "a0 b0"}
	for name, override := range map[string]map[string]string{
		"replicated":                   {envReplicated: "yes please"},
		"a size limit that is no size": {envMaxTableSizeToDrop: "50GB"},
		"a negative size limit":        {envMaxTableSizeToDrop: "-1"},
		"no size limit":                {envMaxTableSizeToDrop: ""},
		"a size limit that is no size, of a cluster of one replica in every shard": withEnv(oneReplica, map[string]string{envMaxTableSizeToDrop: "50GB"}),
	} {
		t.Run(name, func(t *testing.T) {
			s := newSidecarStub(t, replicaRestoreHosts())
			code, out := s.run(replicaRestoreEnv(withEnv(map[string]string{envOverwrite: "true", envUnchangedExitCode: "3"}, override)))
			require.Equal(t, 3, code, out)
			require.Contains(t, out, "ERROR:")
			require.Zero(t, s.countCalls())
		})
	}
}

// The Job runs in an Alpine image, whose regex library refuses a repetition count above 255, the least
// RE_DUP_MAX POSIX allows: a regex with a larger one fails every match, as if every value were invalid.
func TestJobScriptRegexesArePortable(t *testing.T) {
	bashOnly(t)
	for _, count := range regexp.MustCompile(`\{([0-9]*),?([0-9]*)\}`).FindAllStringSubmatch(orchestratorScript, -1) {
		for _, bound := range count[1:] {
			if bound == "" {
				continue
			}
			n, err := strconv.Atoi(bound)
			require.NoError(t, err)
			require.LessOrEqual(t, n, 255, "repetition count %s", count[0])
		}
	}
}

// Every value of the command the script runs through the actions endpoint is a single token, as is
// every value a restore takes: a cluster name or a table pattern that could split that command, or
// start a flag of it, fails the restore before any request.
func TestJobScriptRefusesRestoreValuesThatAreNoToken(t *testing.T) {
	for name, override := range map[string]map[string]string{
		"no cluster":               {envCluster: ""},
		"cluster with a blank":     {envCluster: "main --rbac"},
		"cluster with a quote":     {envCluster: `main'`},
		"cluster with a semicolon": {envCluster: "main;id"},
		"cluster starting a flag":  {envCluster: "-main"},
		"tables with a blank":      {envTables: "db1.* --env=CLICKHOUSE_RESTART_COMMAND=exec:id"},
		"tables with a quote":      {envTables: `db1.*"`},
		"tables with a backslash":  {envTables: `db1.\*`},
		"tables starting a flag":   {envTables: "-db1.*"},
		"tables with a newline":    {envTables: "db1.*\n--rbac"},
		"tables with a bracket":    {envTables: "db1.[a-z]"},
		"tables too long":          {envTables: strings.Repeat("t", 1025)},
		"partitions with a blank":  {envPartitions: "2026 --rbac"},
		"partitions with a quote":  {envPartitions: "('a')"},
		"an empty partition":       {envPartitions: "2026,,2027"},
		"skipped tables of a flag": {envSkipTables: "-db.*"},
		"skipped tables too long":  {envSkipTables: strings.Repeat("t", 1025)},
		"an engine with a blank":   {envSkipTableEngines: "Kafka --rbac"},
		"an engine with a digit":   {envSkipTableEngines: "1Kafka"},
		"a mapping with a blank":   {envDatabaseMapping: "a:b --rbac"},
		"a mapping without target": {envDatabaseMapping: "a:"},
		"a mapping of a dot":       {envDatabaseMapping: "a.b:c"},
		"a mapping to itself":      {envDatabaseMapping: "a:a"},
		"a mapping of one twice":   {envDatabaseMapping: "a:b,a:c"},
		"a mapping to one twice":   {envDatabaseMapping: "a:c,b:c"},
		"a chain of mappings":      {envDatabaseMapping: "a:b,b:c"},
	} {
		t.Run(name, func(t *testing.T) {
			s := newSidecarStub(t, restoreHosts())
			env := withEnv(restoreEnv(map[string]string{envOverwrite: "true", envUnchangedExitCode: "3"}), override)
			code, out := s.run(env)
			require.Equal(t, 3, code, out)
			require.Contains(t, out, "ERROR:")
			require.Zero(t, s.countCalls())
		})
	}
}

// A restore narrowed as the Altinity Cloud Manager narrows one - partitions of the data, tables and table engines left
// out - sets clickhouse-backup settings of its own, which a typed request takes none of: every step runs as a command of
// the actions endpoint, the download and the data restore with the partitions, every command with the --env flags of
// the tables and engines left out, on top of the system databases clickhouse-backup leaves out by default, and, as the
// Altinity Cloud Manager restores partitions, without the check of the replicas before the parts are attached. A
// database mapping restores the schema and the data into other databases.
func TestJobScriptRestoresWithTheFiltersOfTheAltinityCloudManager(t *testing.T) {
	hosts := restoreHosts()
	// A restore of partitions attaches them into the tables as they are, which the guard of a restore with the schema
	// would refuse
	for _, host := range hosts {
		host.tables = []string{"db2.t1"}
		host.backupTables = []string{"db1.t1"}
	}
	s := newSidecarStub(t, hosts)
	code, out := s.run(restoreEnv(map[string]string{
		envTables: "db1.*", envPartitions: "202601,202602", envSkipTables: "db1.tmp*", envSkipTableEngines: "Kafka,RabbitMQ",
		envDatabaseMapping: "db1:db2",
	}))
	require.Zero(t, code, out)
	env := " --env=CLICKHOUSE_SKIP_TABLES=system.*,INFORMATION_SCHEMA.*,information_schema.*,_temporary_and_external_tables.*,db1.tmp*" +
		" --env=CLICKHOUSE_SKIP_TABLE_ENGINES=Kafka,RabbitMQ --env=CLICKHOUSE_CHECK_REPLICAS_BEFORE_ATTACH=0 bk-1"
	require.Equal(t, []string{
		"s0 download --hardlink-exists-files --tables=db1.* --partitions=202601,202602" + env,
		"d1 download --hardlink-exists-files --tables=db1.* --partitions=202601,202602" + env,
		"s0 restore --data --tables=db1.* --partitions=202601,202602 --restore-database-mapping=db1:db2" + env,
		"d1 restore --data --tables=db1.* --partitions=202601,202602 --restore-database-mapping=db1:db2" + env,
	}, s.restoreSteps())
	for _, host := range []string{"s0", "d1"} {
		require.Empty(t, s.hosts[host].local, "the downloaded copies are dropped")
	}
	require.Contains(t, out, "restore bk-1: restores the partitions 202601,202602 into the tables as they are, without the schema")

	// Without the schema step, a restore of partitions leaves the replicas that restore no data alone, and has no
	// overwrite: nothing runs ON CLUSTER
	replicas := withReplicatedTable(replicaRestoreHosts())
	s = newSidecarStub(t, replicas)
	code, out = s.run(replicaRestoreEnv(map[string]string{envPartitions: "202601", envOverwrite: "true"}))
	require.Zero(t, code, out)
	partitionEnv := " --env=CLICKHOUSE_CHECK_REPLICAS_BEFORE_ATTACH=0 bk-1"
	require.Equal(t, []string{
		"a0 download --hardlink-exists-files --partitions=202601" + partitionEnv,
		"b0 download --hardlink-exists-files --partitions=202601" + partitionEnv,
		"a0 restore --data --partitions=202601" + partitionEnv,
		"b0 restore --data --partitions=202601" + partitionEnv,
	}, s.restoreSteps())
	for _, call := range s.calls {
		require.NotContains(t, []string{"a1", "b1"}, call.host, "%s %s", call.method, call.path)
	}

	// Partitions take data, and no schema to restore the access entities with
	for env, refused := range map[string]string{envSchemaOnly: "PARTITIONS restore data, which SCHEMA_ONLY leaves out",
		envRestoreRBAC: "PARTITIONS restore no schema, which RESTORE_RBAC restores the access entities with"} {
		s = newSidecarStub(t, restoreHosts())
		code, out = s.run(restoreEnv(map[string]string{envPartitions: "202601", env: "true", envUnchangedExitCode: "3"}))
		require.Equal(t, 3, code, out)
		require.Contains(t, out, "ERROR: "+refused)
	}

	// A database mapping alone takes no setting: typed requests
	s = newSidecarStub(t, restoreHosts())
	code, out = s.run(restoreEnv(map[string]string{envDatabaseMapping: "db1:db2,db3:db4"}))
	require.Zero(t, code, out)
	require.Equal(t, []string{
		"s0 download hardlink_exists_files=", "d1 download hardlink_exists_files=",
		"s0 restore_database_mapping=db1:db2,db3:db4 rm= schema=",
		"s0 data= restore_database_mapping=db1:db2,db3:db4", "d1 data= restore_database_mapping=db1:db2,db3:db4",
	}, s.restoreSteps())
}

// The overwrite guard looks where a database mapping restores the tables: a table of the database the backup's is
// restored into refuses the restore, one of the database of the backup, which the restore leaves alone, does not.
func TestJobScriptRestoreGuardChecksTheMappedDatabases(t *testing.T) {
	for name, tc := range map[string]struct {
		tables  string
		live    string
		refused bool
	}{
		"a table of the database restored into":              {tables: "db1.t1", live: "db2.t1", refused: true},
		"the table of the backup in its own database":        {tables: "db1.t1", live: "db1.t1"},
		"any table of the database restored into as a whole": {tables: "db1.*", live: "db2.other", refused: true},
		"a table of another database":                        {tables: "db1.*", live: "db3.t1"},
		"any table without a pattern, but the mapped source": {live: "db1.t1"},
		"any table without a pattern":                        {live: "db3.t1", refused: true},
	} {
		t.Run(name, func(t *testing.T) {
			hosts := restoreHosts()
			for _, host := range hosts {
				host.backupTables = []string{"db1.t1"}
			}
			hosts["d1"].tables = []string{tc.live}
			s := newSidecarStub(t, hosts)
			code, out := s.run(restoreEnv(map[string]string{envTables: tc.tables, envDatabaseMapping: "db1:db2",
				envUnchangedExitCode: "3"}))
			if tc.refused {
				require.Equal(t, 3, code, out)
				require.Contains(t, out, "refusing to restore over existing tables (d1:"+tc.live+")")
				require.Empty(t, s.matching(http.MethodPost, "/"))
				return
			}
			require.Zero(t, code, out)
		})
	}
}

// Without overwrite the guard refuses a restore that would drop a user table: one of the backup that
// the table pattern matches, which the schema restore drops and recreates, or any table of a database
// the schema restore drops as a whole - every database of the backup without a pattern, one a
// pattern names as "db.*" or matches with the dot that ends a database name. The tables of the
// backup come from the storage of every restored host, and only when a user table exists. A
// table the restore leaves alone does not refuse it, and the guard fails closed on a table it cannot
// read.
func TestJobScriptRestoreGuardRefusesWhatTheRestoreDrops(t *testing.T) {
	for name, tc := range map[string]struct {
		tables string
		live   []string
		backup []string
		raw    []string
		fail   string
		// refused is the list of tables the refusal names, empty when the restore runs
		refused string
		// listed tells whether the tables of the backup were listed
		listed bool
	}{
		"a table the pattern leaves out": {
			tables: "db1.t1", live: []string{"db1.t2", "db9.t1"}, backup: []string{"db1.t1", "db1.t2"}, listed: true,
		},
		"a table the backup lacks": {
			tables: "db1.t*", live: []string{"db1.t9"}, backup: []string{"db1.t1"}, listed: true,
		},
		"a table of the backup the pattern matches": {
			tables: "db2.x,db1.t*", live: []string{"db9.t1", "db1.t1"}, backup: []string{"db1.t1"}, refused: "(s0:db1.t1)", listed: true,
		},
		"a table of a database restored as a whole": {
			tables: "db1.*", live: []string{"db1.new"}, backup: []string{"db1.t1"}, refused: "(s0:db1.new)", listed: true,
		},
		"a table of a database a pattern matches with its dot": {
			tables: "db*", live: []string{"db1x.t"}, backup: []string{"db1.t1"}, refused: "(s0:db1x.t)", listed: true,
		},
		"any table without a pattern": {
			live: []string{"db9.t1"}, backup: []string{"db1.t1"}, refused: "(s0:db9.t1)",
		},
		"a table of the backup that does not read as one": {
			tables: "db1.t1", live: []string{"db9.t1"}, raw: []string{`{"Database":"db1","Name":"t1"}`}, refused: "(s0:db9.t1)", listed: true,
		},
		"no user table": {
			tables: "db1.t1",
		},
	} {
		t.Run(name, func(t *testing.T) {
			hosts := restoreHosts()
			hosts["s0"].tables = tc.live
			for _, host := range hosts {
				host.backupTables, host.rawBackupTables, host.backupTablesFail = tc.backup, tc.raw, tc.fail
			}
			s := newSidecarStub(t, hosts)
			code, out := s.run(restoreEnv(map[string]string{envTables: tc.tables, envUnchangedExitCode: "3"}))
			listings := s.matching(http.MethodGet, "/backup/tables")
			var listed []string
			for _, c := range listings {
				if c.path == "/backup/tables" {
					listed = append(listed, c.host)
					require.Equal(t, url.Values{"remote_backup": {"bk-1"}, "table": {tc.tables}}, c.query)
				}
			}
			if tc.listed {
				require.Equal(t, []string{"s0", "d1"}, listed, "the tables of the backup in every restored host's storage")
			} else {
				require.Empty(t, listed)
			}
			if tc.refused == "" {
				require.Zero(t, code, out)
				requirePreflightFirst(t, s)
				return
			}
			require.Equal(t, 3, code, out)
			require.Contains(t, out, "refusing to restore over existing tables "+tc.refused+"; set overwrite to replace them")
			require.Empty(t, s.matching(http.MethodPost, "/"))
		})
	}

	t.Run("the tables of the backup cannot be listed", func(t *testing.T) {
		hosts := restoreHosts()
		hosts["s0"].tables = []string{"db9.t1"}
		hosts["d1"].backupTablesFail = "can't connect to remote storage"
		s := newSidecarStub(t, hosts)
		code, out := s.run(restoreEnv(map[string]string{envTables: "db1.t1", envUnchangedExitCode: "3"}))
		require.Equal(t, 3, code, out)
		require.Contains(t, out, "preflight failed: cannot list the tables of bk-1 in the storage of d1 (HTTP 500): can't connect to remote storage")
		require.Empty(t, s.matching(http.MethodPost, "/"))
	})
}

// Access entities are stored apart on every host, and a backup holds those of the replica it was
// made on: they are restored on every schema host, once its schema and the data are restored.
func TestJobScriptRestoresAccessEntitiesOnEveryReplica(t *testing.T) {
	hosts := replicaRestoreHosts()
	hosts["a0"].remote[0].RBACSize = 1
	s := newSidecarStub(t, hosts)
	path := filepath.Join(t.TempDir(), "termination-log")
	code, out := s.run(replicaRestoreEnv(map[string]string{
		envRestoreRBAC:            "true",
		envTables:                 "db1.*",
		envRBACNotRestoredMessage: RBACNotRestoredMessage,
		envTerminationMessagePath: path,
	}))
	require.Zero(t, code, out)
	require.Equal(t, []string{
		"a0 download hardlink_exists_files= table=db1.*", "a1 download schema= table=db1.*",
		"b0 download hardlink_exists_files= table=db1.*", "b1 download schema= table=db1.*",
		// The access entities come with the schema of every replica, before the data, as the Altinity Cloud Manager
		// restores them
		"a0 rbac= rm= schema= table=db1.*", "a1 rbac= rm= schema= table=db1.*", "b0 rbac= rm= schema= table=db1.*",
		"b1 rbac= rm= schema= table=db1.*",
		"a0 data= table=db1.*", "b0 data= table=db1.*",
	}, s.restoreSteps())
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err), "a restore of the access entities tells nothing:\n%s", out)
}

// A restore that leaves out the access entities its backup holds succeeds, and tells so in the
// termination message its Job environment names, which the operator reports on the restore. The
// backup holds them when any host's copy lists a size of them; without topology validation no copy is
// listed, which leaves that unknown.
func TestJobScriptTellsAccessEntitiesItDidNotRestore(t *testing.T) {
	for name, tc := range map[string]struct {
		rbacSize uint64
		env      map[string]string
		told     bool
		logged   string
	}{
		"held by a copy":      {rbacSize: 1, told: true, logged: "WARNING: bk-1 holds access entities - users, roles, grants - which this restore did not restore"},
		"held by no copy":     {},
		"restored":            {rbacSize: 1, env: map[string]string{envRestoreRBAC: "true"}},
		"copies never listed": {rbacSize: 1, env: map[string]string{envValidateTopology: "false"}, logged: "whether bk-1 holds access entities is unknown"},
	} {
		t.Run(name, func(t *testing.T) {
			hosts := restoreHosts()
			hosts["d1"].remote[0].RBACSize = tc.rbacSize
			s := newSidecarStub(t, hosts)
			path := filepath.Join(t.TempDir(), "termination-log")
			code, out := s.run(restoreEnv(withEnv(map[string]string{
				envRBACNotRestoredMessage: RBACNotRestoredMessage,
				envTerminationMessagePath: path,
			}, tc.env)))
			require.Zero(t, code, out)
			require.Contains(t, out, tc.logged)
			message, err := os.ReadFile(path)
			if !tc.told {
				require.True(t, os.IsNotExist(err), "nothing is told:\n%s", out)
				return
			}
			require.NoError(t, err, out)
			require.Equal(t, RBACNotRestoredMessage+"\n", string(message))
		})
	}
}

// A sidecar that restarts between two steps of a restore forgets the steps it ran, and may come back
// with another environment - another storage path above all. Before each step on a host the
// script asks its sidecar for the last step it ran there, and stops when the sidecar no longer knows
// it: the restore is partially applied, never continued.
func TestJobScriptStopsWhenASidecarRestartsBetweenSteps(t *testing.T) {
	downloads := []string{
		"a0 download hardlink_exists_files=", "a1 download schema=", "b0 download hardlink_exists_files=", "b1 download schema=",
	}
	for name, tc := range map[string]struct {
		host string
		// after is the number of steps the host runs before its sidecar restarts, 1 when unset
		after     int
		unchanged bool
		env       map[string]string
		steps     []string
	}{
		// The downloads change nothing
		"after its download": {
			host:      "a0",
			steps:     downloads,
			unchanged: true,
		},
		"after its schema restore": {
			host:  "a0",
			after: 2,
			steps: append(slices.Clone(downloads), "a0 rm= schema=", "a1 rm= schema=", "b0 rm= schema=", "b1 rm= schema="),
		},
		"after the drop on cluster": {
			host:  "a0",
			after: 2,
			env:   map[string]string{envOverwrite: "true"},
			steps: append(slices.Clone(downloads), "a0 restore --schema --rm --env=RESTORE_SCHEMA_ON_CLUSTER=main bk-1"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			hosts := replicaRestoreHosts()
			if tc.env[envOverwrite] == "true" {
				withReplicatedTable(hosts)
			}
			hosts[tc.host].restartAfter = max(tc.after, 1)
			s := newSidecarStub(t, hosts)
			code, out := s.run(replicaRestoreEnv(withEnv(map[string]string{envUnchangedExitCode: "3"}, tc.env)))
			if tc.unchanged {
				require.Equal(t, 3, code, out)
				require.Contains(t, out, tc.host+": the sidecar restarted after the last step of this restore on it, which it no longer knows; nothing was changed")
			} else {
				require.Equal(t, 1, code, out)
				require.Contains(t, out, tc.host+": the sidecar restarted after the last step of this restore on it, which it no longer knows: the restore is partially applied")
			}
			require.Equal(t, tc.steps, s.restoreSteps())
		})
	}
}

// testPathNamespace is the namespace of the installation of the storage path check of the tests, the
// value of its {namespace} macro
const testPathNamespace = "demo-ns"

// pathCluster is a backup-enabled cluster as the storage path check sees it
type pathCluster struct {
	name   string
	shards string
	// settings are of PathCheckKeys, or one of the cluster's own default paths
	settings map[string]string
}

// pathCheckTestEnv returns the environment of the storage path check of a Job that targets the first of
// clusters, in an installation named demo of the namespace testPathNamespace, whose sidecars set the
// paths the operator configuration gives when the storage settings leave them unset
func pathCheckTestEnv(clusters ...pathCluster) map[string]string {
	env := map[string]string{
		envPathSelf:              "0",
		envPathInstallation:      "demo",
		envPathNamespace:         testPathNamespace,
		envDefaultPath:           chop.Config().Backup.StoragePath,
		envDefaultObjectDiskPath: chop.Config().Backup.StorageObjectDiskPath,
	}
	var indexes []string
	for i, c := range clusters {
		indexes = append(indexes, strconv.Itoa(i))
		env[pathClusterEnv(i, pathEnvCluster)] = c.name
		env[pathClusterEnv(i, pathEnvShards)] = c.shards
		for key, value := range c.settings {
			env[pathClusterEnv(i, key)] = value
		}
	}
	env[envPathClusters] = strings.Join(indexes, " ")
	return env
}

// storageSettings returns the settings of a cluster that keeps its backups in a storage of the
// given kind, with the given settings on top
func storageSettings(storage string, settings ...string) map[string]string {
	values := map[string]string{"REMOTE_STORAGE": storage}
	for i := 0; i+1 < len(settings); i += 2 {
		values[settings[i]] = settings[i+1]
	}
	return values
}

// Before a backup or a restore changes anything, the script checks that every shard of the target
// cluster has a storage path of its own: one that no other shard of any backup-enabled cluster of the
// installation shares in the same storage, as clickhouse-backup expands, trims and joins it,
// the default paths the sidecar sets for unset ones included, the root of the storage too - and likewise
// for object disk data. A storage is told apart by its settings as clickhouse-backup takes them, the
// defaults it gives unset ones included. A backup of the first replica of each shard must not have
// {replica} in its path, as every replica restores its schema from its own path; a backup of every
// replica must have it. A path proven shared fails the Job before any request, changing nothing;
// another cluster whose paths the check cannot know - its sidecar configured by a file, a macro of
// unknown value, settings the operator cannot tell - is left out of it with a warning. So is the target
// cluster, when it is the only backup-enabled one; beside others it fails the Job instead, as their
// Jobs of one backup run at the same time and could make it at one path, and a restore could take the
// data of another.
func TestJobScriptChecksTheStoragePaths(t *testing.T) {
	s3 := func(settings ...string) map[string]string {
		return storageSettings("s3", append([]string{"S3_BUCKET", "backups"}, settings...)...)
	}
	// ownDefault is the storage path shard 0 of cluster a keeps its backups at by default
	ownDefault := strings.NewReplacer("{installation}", "demo", "{namespace}", testPathNamespace, "{cluster}", "a", "{shard}", "0").
		Replace(chop.Config().Backup.StoragePath)
	for name, tc := range map[string]struct {
		clusters  []pathCluster
		env       map[string]string
		operation string
		// refused is the reason the Job fails for, empty when it runs
		refused string
		warned  string
	}{
		"the default paths": {
			clusters: []pathCluster{{name: "a", shards: "0 1", settings: s3()}, {name: "b", shards: "0 1", settings: s3()}},
		},
		"one path for two clusters": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "backup/{shard}")}, {name: "b", shards: "0", settings: s3("S3_PATH", "backup/{shard}")}},
			refused:  "the storage path 'backup/0' of shard 0 of cluster b is that of shard 0 of cluster a too, in the same s3 storage",
		},
		"one path in another bucket": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "backup/{shard}")}, {name: "b", shards: "0", settings: s3("S3_PATH", "backup/{shard}", "S3_BUCKET", "other")}},
		},
		"one path behind another endpoint": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "x")}, {name: "b", shards: "0", settings: s3("S3_PATH", "x", "S3_ENDPOINT", "https://minio:9000")}},
		},
		"an endpoint set empty and one unset": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "x")}, {name: "b", shards: "0", settings: s3("S3_PATH", "x", "S3_ENDPOINT", "")}},
			refused:  "the storage path 'x' of shard 0 of cluster b is that of shard 0 of cluster a too",
		},
		"a path without the shard": {
			clusters: []pathCluster{{name: "a", shards: "0 1", settings: s3("S3_PATH", "backup/{cluster}")}},
			refused:  "the storage path 'backup/a' of shard 1 of cluster a is that of shard 0 of cluster a too",
		},
		"paths that differ in slashes and blanks": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", " /backup/{shard}/")}, {name: "b", shards: "0", settings: s3("S3_PATH", "backup/{shard}")}},
			refused:  "the storage path 'backup/0' of shard 0 of cluster b is that of shard 0 of cluster a too",
		},
		"an azblob endpoint suffix unset and one set to its default": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: storageSettings("azblob", "AZBLOB_ACCOUNT_NAME", "acc", "AZBLOB_CONTAINER", "one", "AZBLOB_PATH", "x")},
				{name: "b", shards: "0", settings: storageSettings("azblob", "AZBLOB_ACCOUNT_NAME", "acc", "AZBLOB_CONTAINER", "one", "AZBLOB_PATH", "x", "AZBLOB_ENDPOINT_SUFFIX", "core.windows.net")},
			},
			refused: "the storage path 'x' of shard 0 of cluster b is that of shard 0 of cluster a too, in the same azblob storage",
		},
		"an sftp port unset and one set to its default": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: storageSettings("sftp", "SFTP_ADDRESS", "sftp", "SFTP_PATH", "x")},
				{name: "b", shards: "0", settings: storageSettings("sftp", "SFTP_ADDRESS", "sftp", "SFTP_PORT", "22", "SFTP_PATH", "x")},
			},
			refused: "the storage path 'x' of shard 0 of cluster b is that of shard 0 of cluster a too, in the same sftp storage",
		},
		"paths that differ in empty and dot elements": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "backup//./{shard}")}, {name: "b", shards: "0", settings: s3("S3_PATH", "x/../backup/{shard}/.")}},
			refused:  "the storage path 'backup/0' of shard 0 of cluster b is that of shard 0 of cluster a too",
		},
		"object disk paths that differ in empty elements": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "od//{shard}")}, {name: "b", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "od/{shard}")}},
			refused:  "the object disk path 'od/0' of shard 0 of cluster b is that of shard 0 of cluster a too",
		},
		"absolute ftp paths that differ in a parent element": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: storageSettings("ftp", "FTP_ADDRESS", "ftp:21", "FTP_PATH", "/../backup/{shard}")},
				{name: "b", shards: "0", settings: storageSettings("ftp", "FTP_ADDRESS", "ftp:21", "FTP_PATH", "/backup/{shard}")},
			},
			refused: "the storage path '/backup/0' of shard 0 of cluster b is that of shard 0 of cluster a too",
		},
		"an absolute and a relative ftp path": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: storageSettings("ftp", "FTP_ADDRESS", "ftp:21", "FTP_PATH", "/backup/{shard}/")},
				{name: "b", shards: "0", settings: storageSettings("ftp", "FTP_ADDRESS", "ftp:21", "FTP_PATH", "backup/{shard}")},
			},
		},
		"one installation macro expanded": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "{installation}/{shard}")}, {name: "b", shards: "0", settings: s3("S3_PATH", "demo/0")}},
			refused:  "the storage path 'demo/0' of shard 0 of cluster b is that of shard 0 of cluster a too",
		},
		"one namespace macro expanded": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "od/{namespace}/{shard}")},
				{name: "b", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "od/"+testPathNamespace+"/0")},
			},
			refused: "the object disk path 'od/" + testPathNamespace + "/0' of shard 0 of cluster b is that of shard 0 of cluster a too",
		},
		"paths of other namespaces": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "{namespace}/{shard}")}, {name: "b", shards: "0", settings: s3("S3_PATH", "other/0")}},
		},
		"a path set empty": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "")}, {name: "b", shards: "0", settings: s3("S3_PATH", "")}},
			refused:  "the storage path '.' of shard 0 of cluster b is that of shard 0 of cluster a too",
		},
		"unset paths without a default": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3()}, {name: "b", shards: "0", settings: s3()}},
			env:      map[string]string{envDefaultPath: ""},
			refused:  "the storage path '.' of shard 0 of cluster b is that of shard 0 of cluster a too",
		},
		"a cluster whose sidecar sets no default": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: s3()},
				{name: "b", shards: "0", settings: s3("S3_PATH", ownDefault, "DEFAULT_PATH", "")},
			},
			refused: "the storage path '" + ownDefault + "' of shard 0 of cluster b is that of shard 0 of cluster a too",
		},
		"two clusters whose sidecars set no default": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: s3("DEFAULT_PATH", "")},
				{name: "b", shards: "0", settings: s3("DEFAULT_PATH", "")},
			},
			refused: "the storage path '.' of shard 0 of cluster b is that of shard 0 of cluster a too",
		},
		"one object disk path for two clusters": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "od/{shard}")}, {name: "b", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "od/{shard}")}},
			refused:  "the object disk path 'od/0' of shard 0 of cluster b is that of shard 0 of cluster a too",
		},
		"object disk paths set empty": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "")}, {name: "b", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "")}},
		},
		// Unlike one that trims to nothing, an object disk path that cleans to the root is the root
		"object disk paths at the root": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "od/..")}, {name: "b", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "/./")}},
			refused:  "the object disk path '.' of shard 0 of cluster b is that of shard 0 of cluster a too",
		},
		// A relative path keeps the parent elements above its start, which are keys of their own
		"a relative path above the root and the one it climbs to": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "../backup/{shard}")}, {name: "b", shards: "0", settings: s3("S3_PATH", "backup/{shard}")}},
		},
		"a relative path two levels above the root and the one it climbs to": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "../../backup/{shard}")}, {name: "b", shards: "0", settings: s3("S3_PATH", "backup/{shard}")}},
		},
		"relative paths that climb above the root alike": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "../backup/{shard}")}, {name: "b", shards: "0", settings: s3("S3_PATH", "x/../../backup/{shard}/")}},
			refused:  "the storage path '../backup/0' of shard 0 of cluster b is that of shard 0 of cluster a too",
		},
		"the replica in a backup of the first replica of each shard": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "backup/{shard}/{replica}")}},
			refused:  "the storage path 'backup/{shard}/{replica}' of cluster a holds {replica}",
		},
		"a backup of every replica without the replica": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3()}},
			env:      map[string]string{envPathReplicaSelection: string(api.ReplicaSelectionAllReplicas)},
			refused:  "the storage path '" + chop.Config().Backup.StoragePath + "' of cluster a lacks {replica}",
		},
		"a backup of every replica": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "backup/{replica}")}, {name: "b", shards: "0", settings: s3("S3_PATH", "backup/{replica}")}},
			env:      map[string]string{envPathReplicaSelection: string(api.ReplicaSelectionAllReplicas)},
		},
		"two other clusters that share a path": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: s3()},
				{name: "b", shards: "0", settings: s3("S3_PATH", "x")},
				{name: "c", shards: "0", settings: s3("S3_PATH", "x")},
			},
		},
		"azblob containers": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: storageSettings("azblob", "AZBLOB_ACCOUNT_NAME", "acc", "AZBLOB_CONTAINER", "one", "AZBLOB_PATH", "x")},
				{name: "b", shards: "0", settings: storageSettings("azblob", "AZBLOB_ACCOUNT_NAME", "acc", "AZBLOB_CONTAINER", "two", "AZBLOB_PATH", "x")},
			},
		},
		"one azblob container": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: storageSettings("azblob", "AZBLOB_ACCOUNT_NAME", "acc", "AZBLOB_CONTAINER", "one", "AZBLOB_PATH", "x")},
				{name: "b", shards: "0", settings: storageSettings("azblob", "AZBLOB_ACCOUNT_NAME", "acc", "AZBLOB_CONTAINER", "one", "AZBLOB_PATH", "x")},
			},
			refused: "the storage path 'x' of shard 0 of cluster b is that of shard 0 of cluster a too, in the same azblob storage",
		},
		"sftp servers on other ports": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: storageSettings("sftp", "SFTP_ADDRESS", "sftp", "SFTP_PORT", "22", "SFTP_PATH", "x")},
				{name: "b", shards: "0", settings: storageSettings("sftp", "SFTP_ADDRESS", "sftp", "SFTP_PORT", "2222", "SFTP_PATH", "x")},
			},
		},
		"another cluster that sets no storage": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "x")}, {name: "b", shards: "0", settings: map[string]string{"S3_PATH": "x"}}},
			warned:   "cluster b is left out of the storage path check: its configuration does not set remote_storage",
		},
		"the target cluster that sets no storage": {
			clusters: []pathCluster{{name: "a", shards: "0 1", settings: map[string]string{"S3_PATH": "x"}}},
			warned:   "the storage path check is skipped: its configuration does not set remote_storage",
		},
		"custom storage": {
			clusters: []pathCluster{{name: "a", shards: "0 1", settings: storageSettings("custom")}},
			warned:   "the storage path check is skipped: it keeps its backups in custom storage",
		},
		"the target cluster that sets no storage, beside another cluster": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: map[string]string{"S3_PATH": "x"}}, {name: "b", shards: "0", settings: s3()}},
			refused:  "the storage path check cannot tell whether cluster a shares a storage path with another of the 2 backup-enabled clusters of the installation, which would mix their backups and restore one with the data of the other: its configuration does not set remote_storage",
		},
		"custom storage, beside another cluster": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: storageSettings("custom")}, {name: "b", shards: "0", settings: s3()}},
			refused:  "the storage path check cannot tell whether cluster a shares a storage path with another of the 2 backup-enabled clusters of the installation, which would mix their backups and restore one with the data of the other: it keeps its backups in custom storage",
		},
		"a macro of unknown value in the target cluster's path, beside another cluster": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "x/{layer}")}, {name: "b", shards: "0", settings: s3()}},
			refused:  "the storage path check cannot tell whether cluster a shares a storage path with another of the 2 backup-enabled clusters of the installation, which would mix their backups and restore one with the data of the other: the path 'x/{layer}' of cluster a holds {layer}",
		},
		"a restore of the target cluster that sets no storage, beside another cluster": {
			operation: operationRestore,
			clusters:  []pathCluster{{name: "a", shards: "0", settings: map[string]string{}}, {name: "b", shards: "0", settings: s3()}},
			refused:   "the storage path check cannot tell whether cluster a shares a storage path with another of the 2 backup-enabled clusters of the installation",
		},
		"the target cluster's settings unverifiable": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "x", pathEnvUnverifiable, "its pod template takes env from envFrom")}},
			warned:   "the storage path check is skipped: the operator cannot tell the sidecar settings of cluster a: its pod template takes env from envFrom",
		},
		"the target cluster's settings unverifiable, beside another cluster": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: s3(pathEnvUnverifiable, "a volume is mounted at /etc/clickhouse-backup\nof its sidecar")},
				{name: "b", shards: "0", settings: s3("S3_PATH", "y")},
			},
			refused: "the storage path check cannot tell whether cluster a shares a storage path with another of the 2 backup-enabled clusters of the installation, which would mix their backups and restore one with the data of the other: the operator cannot tell the sidecar settings of cluster a: a volume is mounted at /etc/clickhouse-backup of its sidecar",
		},
		"another cluster's settings unverifiable": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: s3("S3_PATH", "x")},
				{name: "b", shards: "0", settings: s3("S3_PATH", "x", pathEnvUnverifiable, "its hosts disagree on the default paths")},
			},
			warned: "cluster b is left out of the storage path check: the operator cannot tell the sidecar settings of cluster b: its hosts disagree on the default paths",
		},
		"a macro of unknown value in the target cluster's path": {
			clusters: []pathCluster{{name: "a", shards: "0 1", settings: s3("S3_PATH", "x/{all-sharded-shard}")}},
			warned:   "the storage path check is skipped: the path 'x/{all-sharded-shard}' of cluster a holds {all-sharded-shard}",
		},
		"a macro of unknown value in another cluster's path": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "x")}, {name: "b", shards: "0", settings: s3("S3_PATH", "x", "S3_OBJECT_DISK_PATH", "{layer}")}},
			warned:   "cluster b is left out of the storage path check: its path '{layer}' holds {layer}",
		},
		"a restore from one path for two clusters": {
			operation: operationRestore,
			clusters:  []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "x")}, {name: "b", shards: "0", settings: s3("S3_PATH", "x")}},
			refused:   "the storage path 'x' of shard 0 of cluster b is that of shard 0 of cluster a too",
		},
		// A backup of every replica is restored from the path of each replica
		"a restore from a path with the replica": {
			operation: operationRestore,
			clusters:  []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "backup/{shard}/{replica}")}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			hosts := map[string]*stubHost{"h0": {remote: []stubBackup{remote("bk-1", "")}}}
			env := map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0"}
			if tc.operation == operationRestore {
				env = restoreEnv(map[string]string{envSchemaHosts: "h0", envDataHosts: "h0", envGuardHosts: "h0"})
			}
			env = withEnv(withEnv(env, pathCheckTestEnv(tc.clusters...)), tc.env)
			env[envUnchangedExitCode] = "3"
			if tc.operation == operationRestore {
				env[envBackupName] = "bk-1"
			} else {
				env[envBackupName] = "bk-2"
			}
			s := newSidecarStub(t, hosts)
			code, out := s.run(env)
			if tc.refused != "" {
				require.Equal(t, 3, code, out)
				require.Contains(t, out, "ERROR: "+tc.refused)
				require.Zero(t, s.countCalls(), "nothing is asked of a sidecar before the check passed")
				return
			}
			require.Zero(t, code, out)
			require.Contains(t, out, tc.warned)
			if tc.warned == "" {
				require.NotContains(t, out, "WARNING:")
				require.Contains(t, out, "storage path check: every shard of cluster "+tc.clusters[0].name+" has a path of its own")
			}
		})
	}
}

// The region of an S3 storage tells no storage from another, as a bucket name is global: the Job the
// operator builds for a cluster whose sidecars keep their backups at the path of those of another
// cluster, in a bucket of the same name, fails whatever region either sets, or leaves to the AWS SDK of
// the sidecar by setting it empty.
func TestJobScriptTellsNoStorageByItsRegion(t *testing.T) {
	var templates []api.PodTemplate
	cluster := func(name string, region ...string) *api.Cluster {
		env := []core.EnvVar{
			{Name: "REMOTE_STORAGE", Value: "s3"},
			{Name: "S3_BUCKET", Value: "backups"},
			{Name: "S3_PATH", Value: "backup/{shard}"},
		}
		for _, value := range region {
			env = append(env, core.EnvVar{Name: "S3_REGION", Value: value})
		}
		template := name + "-" + strconv.Itoa(len(templates))
		templates = append(templates, api.PodTemplate{Name: template, Spec: core.PodSpec{Containers: []core.Container{
			{Name: config.ClickHouseContainerName, Image: "clickhouse/clickhouse-server"},
			{Name: config.ClickHouseBackupContainerName, Env: env},
		}}})
		cluster := layoutCluster(name, 1, 1, &api.ClusterBackup{
			Enabled:         types.NewStringBool(true),
			PodTemplate:     types.NewString(template),
			ConfigSecretRef: &core.LocalObjectReference{Name: "backup-config"},
		})
		cluster.Templates = &api.TemplatesList{PodTemplate: template}
		return cluster
	}
	for name, clusters := range map[string][]*api.Cluster{
		"other regions":                    {cluster("a", "us-east-1"), cluster("b", "eu-west-1")},
		"a region set empty and one unset": {cluster("a", ""), cluster("b")},
	} {
		t.Run(name, func(t *testing.T) {
			chi := newCHI("demo", clusters...)
			chi.Spec.Templates = &api.Templates{PodTemplates: templates}
			_, stored := normalize(t, chi)
			topology, err := NewTopology(stored, "")
			require.NoError(t, err)
			target, err := topology.ForCluster("a")
			require.NoError(t, err)
			env := scriptEnv(BuildBackupJob(testBackup("b1"), stored, target, testOrchestrator()))
			hosts := map[string]*stubHost{}
			for _, host := range strings.Fields(env[envBackupHosts]) {
				hosts[host] = &stubHost{}
			}
			s := newSidecarStub(t, hosts)
			code, out := s.run(env)
			require.Equal(t, UnchangedExitCode, code, out)
			require.Contains(t, out, "ERROR: the storage path 'backup/0' of shard 0 of cluster b is that of shard 0 of cluster a too, in the same s3 storage")
			require.Zero(t, s.countCalls(), "nothing is asked of a sidecar before the check passed")
		})
	}
}

// The environment of the storage path check comes from the operator: an index that is none, a target
// cluster missing from them, or a replica selection the API does not know fails the Job before any
// request.
func TestJobScriptRejectsAnInvalidPathCheckEnv(t *testing.T) {
	for name, override := range map[string]map[string]string{
		"an index that is none":    {envPathClusters: "0 x"},
		"a target cluster missing": {envPathSelf: "1"},
		"no target cluster":        {envPathSelf: ""},
		"a replica selection":      {envPathReplicaSelection: "EveryOther"},
	} {
		t.Run(name, func(t *testing.T) {
			s := newSidecarStub(t, map[string]*stubHost{"h0": {}})
			env := withEnv(pathCheckTestEnv(pathCluster{name: "a", shards: "0", settings: storageSettings("s3")}), override)
			env = withEnv(env, map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0"})
			code, out := s.run(env)
			require.Equal(t, 1, code, out)
			require.Contains(t, out, "ERROR:")
			require.Zero(t, s.countCalls())
		})
	}
}

// A replica restores its schema from its own path: with {replica} in it, that holds a backup of every
// replica, while one of the first replica of each shard is missing there. The preflight then fails,
// changing nothing, and tells why.
func TestJobScriptTellsWhyABackupIsMissingAtAReplicaPath(t *testing.T) {
	hosts := replicaRestoreHosts()
	hosts["a1"].remote = nil
	s := newSidecarStub(t, hosts)
	env := withEnv(replicaRestoreEnv(map[string]string{envUnchangedExitCode: "3"}),
		pathCheckTestEnv(pathCluster{name: "main", shards: "0 1", settings: storageSettings("s3", "S3_PATH", "backup/{shard}/{replica}")}))
	code, out := s.run(env)
	require.Equal(t, 3, code, out)
	require.Contains(t, out, "preflight failed: backup bk-1 is missing in the storage of a1: the storage path 'backup/{shard}/{replica}' holds {replica}, which gives every replica a path of its own, and the backup was made on the first replica of each shard only")
	require.Empty(t, s.matching(http.MethodPost, "/"))
}

// The storage paths of the cluster the restores from another installation of the tests target, and those
// of the installation src of the namespace src-ns, which they read the backup at
const (
	testOwnPath              = "backup/{installation}/{cluster}/{shard}"
	testOwnObjectDiskPath    = "object_disks/{namespace}/{installation}/{shard}"
	testSourcePath           = "backup/src/{cluster}/{shard}"
	testSourceObjectDiskPath = "object_disks/src-ns/src/{shard}"
)

// atSourcePath moves the backups of every host from its own storage path to path, as when another
// installation made them, unless path is the own storage path testOwnPath, which a restore from another
// installation reads when the object disk paths alone tell the installations apart
func atSourcePath(hosts map[string]*stubHost, path string) map[string]*stubHost {
	for _, host := range hosts {
		host.paths = map[string][]stubBackup{path: host.remote}
		if path != testOwnPath {
			host.remote = nil
		}
	}
	return hosts
}

// sourceRestoreEnv returns the environment of a restore of restoreEnv from the installation src of the
// namespace src-ns, of a cluster that keeps its backups in S3 at testOwnPath and testOwnObjectDiskPath
func sourceRestoreEnv(overrides map[string]string) map[string]string {
	env := withEnv(restoreEnv(nil), pathCheckTestEnv(pathCluster{name: restoreTestCluster, shards: "0 1", settings: storageSettings("s3",
		"S3_BUCKET", "backups", "S3_PATH", testOwnPath, "S3_OBJECT_DISK_PATH", testOwnObjectDiskPath)}))
	env[envSourceInstallation] = "src"
	env[envSourceNamespace] = "src-ns"
	env[envUnchangedExitCode] = "3"
	return withEnv(env, overrides)
}

// sourceEnvFlags returns the --env flags of the commands of a restore that read the backup at the
// storage path and object disk path given
func sourceEnvFlags(path, objectDiskPath string) string {
	return " --env=S3_PATH=" + path + " --env=S3_OBJECT_DISK_PATH=" + objectDiskPath
}

// A restore that names another installation, or another namespace, reads the backup at the storage
// paths of its cluster with the values of that installation for {installation} and {namespace}, and
// every other macro kept for the sidecar to expand. A typed request takes no setting in place of the
// sidecar's own, and lists the backups at the sidecar's paths alone, so the restore runs only commands
// of the actions endpoint, each with the --env flags of the storage path and of the object disk path. Its
// preflight downloads the schema of the backup from there to every restored host, which changes nothing:
// the backup is missing where that fails, and the copy tells whether the backup is complete and holds
// access entities, and lists its tables. The copies are deleted before the restore steps, as
// restore_remote would take such a copy for one it downloaded in part and restore no data; a host that
// holds a local copy of the backup beforehand refuses the restore for the same reason.
func TestJobScriptRestoresFromTheStoragePathsOfAnotherInstallation(t *testing.T) {
	for name, tc := range map[string]struct {
		env map[string]string
		// path and objectDiskPath are the paths the restore reads the backup at
		path, objectDiskPath string
		// steps are the commands the hosts run after the downloads of the preflight
		steps []string
	}{
		"of another installation in another namespace": {
			path: testSourcePath, objectDiskPath: testSourceObjectDiskPath,
		},
		"of another installation": {
			env:  map[string]string{envSourceNamespace: ""},
			path: testSourcePath, objectDiskPath: "object_disks/{namespace}/src/{shard}",
		},
		"of an installation of the same name in another namespace": {
			env:  map[string]string{envSourceInstallation: ""},
			path: testOwnPath, objectDiskPath: "object_disks/src-ns/{installation}/{shard}",
		},
		"naming its own installation, in another namespace": {
			env:  map[string]string{envSourceInstallation: "demo"},
			path: testOwnPath, objectDiskPath: "object_disks/src-ns/{installation}/{shard}",
		},
		"with a table pattern and the access entities": {
			env:  map[string]string{envTables: "db1.*", envRestoreRBAC: "true"},
			path: testSourcePath, objectDiskPath: testSourceObjectDiskPath,
			steps: []string{
				"s0 download --hardlink-exists-files --tables=db1.*", "d1 download --hardlink-exists-files --tables=db1.*",
				"s0 restore --schema --rm --rbac --tables=db1.*",
				"s0 restore --data --tables=db1.*", "d1 restore --data --tables=db1.*",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			hosts := atSourcePath(restoreHosts(), tc.path)
			hosts["d1"].paths[tc.path][0].RBACSize = 1
			hosts["s0"].tables = []string{"db9.t1"}
			// The own paths hold a backup of another name, which the restore leaves alone
			hosts["s0"].remote = append(hosts["s0"].remote, remote("bk-0", ""))
			s := newSidecarStub(t, hosts)
			path := filepath.Join(t.TempDir(), "termination-log")
			code, out := s.run(sourceRestoreEnv(withEnv(map[string]string{
				envTables:                 "db1.t1",
				envRBACNotRestoredMessage: RBACNotRestoredMessage,
				envTerminationMessagePath: path,
			}, tc.env)))
			require.Zero(t, code, out)
			require.Contains(t, out, "restore bk-1: reading the backup at the storage path '"+
				strings.NewReplacer("{installation}", "demo", "{cluster}", restoreTestCluster).Replace(tc.path)+"' of the installation it names")
			steps := tc.steps
			if steps == nil {
				steps = []string{
					// Every host downloads the backup first, with its data
					"s0 download --hardlink-exists-files --tables=db1.t1", "d1 download --hardlink-exists-files --tables=db1.t1",
					"s0 restore --schema --rm --tables=db1.t1", "s0 restore --data --tables=db1.t1", "d1 restore --data --tables=db1.t1",
				}
			}
			calls := []string{
				"s0 GET /backup/version", "d1 GET /backup/version",
				// No host holds a local copy of the backup, which the restore would take for its own, nor a
				// backup of its name at the own storage path, which clickhouse-backup would take for it
				"s0 GET /backup/list/local", "s0 GET /backup/list/remote", "d1 GET /backup/list/local", "d1 GET /backup/list/remote",
			}
			if tc.path == testOwnPath {
				// The backup the restore reads at the own storage path is the backup of its name there
				calls = []string{"s0 GET /backup/version", "d1 GET /backup/version", "s0 GET /backup/list/local", "d1 GET /backup/list/local"}
			}
			calls = append(calls,
				// The schema downloaded from the source paths, listed for whether it is complete
				"s0 POST /backup/actions", "s0 GET /backup/list/local",
				"d1 POST /backup/actions", "d1 GET /backup/list/local",
				"s0 GET /backup/tables/all", "d1 GET /backup/tables/all",
				// The tables of the backup, as the downloaded copies list them
				"s0 GET /backup/tables", "d1 GET /backup/tables",
				// The copies go before the restore steps
				"s0 POST /backup/delete/local/bk-1", "d1 POST /backup/delete/local/bk-1",
			)
			for _, step := range steps {
				host, _, _ := strings.Cut(step, " ")
				calls = append(calls, host+" POST /backup/actions")
			}
			calls = append(calls,
				"s0 GET /backup/list/local", "s0 POST /backup/delete/local/bk-1",
				"d1 GET /backup/list/local", "d1 POST /backup/delete/local/bk-1",
			)
			require.Equal(t, calls, s.significant())
			for _, listing := range s.matching(http.MethodGet, "/backup/tables") {
				if listing.path == "/backup/tables" {
					require.Equal(t, url.Values{"local_backup": {"bk-1"}, "table": {cmp.Or(tc.env[envTables], "db1.t1")}}, listing.query)
				}
			}
			flags := sourceEnvFlags(tc.path, tc.objectDiskPath)
			commands := []string{"s0 download --schema --rbac" + flags + " bk-1", "d1 download --schema --rbac" + flags + " bk-1"}
			for _, step := range steps {
				commands = append(commands, step+flags+" bk-1")
			}
			require.Equal(t, commands, s.restoreSteps())
			ownLists := 2
			if tc.path == testOwnPath {
				ownLists = 0
			}
			require.Len(t, s.matching(http.MethodGet, "/backup/list/remote"), ownLists, "each restored host lists its own storage path once, for a backup of the name of the one it restores")
			for _, host := range []string{"s0", "d1"} {
				require.Empty(t, hosts[host].local, "the downloaded copies are dropped")
			}
			message, err := os.ReadFile(path)
			if tc.env[envRestoreRBAC] == "true" {
				require.True(t, os.IsNotExist(err), out)
				return
			}
			require.NoError(t, err, "the downloaded schema tells the access entities of the backup:\n%s", out)
			require.Equal(t, RBACNotRestoredMessage+"\n", string(message))
		})
	}
}

// An overwrite that drops the tables of the backup ON CLUSTER reads them, as every step, at the paths of
// the installation the restore names: the preflight lists the tables of the backup of the first replica
// of every shard in the copy of the schema it downloaded, and the drop ON CLUSTER runs with the --env
// flags of those paths.
func TestJobScriptDropsOnClusterFromTheStoragePathsOfAnotherInstallation(t *testing.T) {
	hosts := atSourcePath(withReplicatedTable(replicaRestoreHosts()), testSourcePath)
	s := newSidecarStub(t, hosts)
	code, out := s.run(withEnv(sourceRestoreEnv(nil), replicaRestoreEnv(map[string]string{envOverwrite: "true"})))
	require.Zero(t, code, out)
	flags := sourceEnvFlags(testSourcePath, testSourceObjectDiskPath)
	require.Equal(t, []string{
		"a0 download --schema --rbac" + flags + " bk-1", "a1 download --schema --rbac" + flags + " bk-1",
		"b0 download --schema --rbac" + flags + " bk-1", "b1 download --schema --rbac" + flags + " bk-1",
		"a0 download --hardlink-exists-files" + flags + " bk-1", "a1 download --schema" + flags + " bk-1",
		"b0 download --hardlink-exists-files" + flags + " bk-1", "b1 download --schema" + flags + " bk-1",
		"a0 restore --schema --rm --env=RESTORE_SCHEMA_ON_CLUSTER=main" + flags + " bk-1",
		"a0 restore --schema --rm" + flags + " bk-1", "a1 restore --schema --rm" + flags + " bk-1",
		"b0 restore --schema --rm" + flags + " bk-1", "b1 restore --schema --rm" + flags + " bk-1",
		"a0 restore --data" + flags + " bk-1", "b0 restore --data" + flags + " bk-1",
	}, s.restoreSteps())
	var listed []string
	for _, c := range s.matching(http.MethodGet, "/backup/tables") {
		if c.path == "/backup/tables" {
			listed = append(listed, c.host)
			require.Equal(t, url.Values{"local_backup": {"bk-1"}}, c.query)
		}
	}
	require.Equal(t, []string{"a0", "b0"}, listed, "the tables of the backup of the first replica of every shard")
	var deleted []string
	for _, c := range s.significant() {
		if strings.HasSuffix(c, " POST /backup/delete/local/bk-1") {
			deleted = append(deleted, c)
		}
		if c == "a0 POST /backup/actions" && len(deleted) > 0 {
			break
		}
	}
	require.Equal(t, []string{
		"a0 POST /backup/delete/local/bk-1", "a1 POST /backup/delete/local/bk-1",
		"b0 POST /backup/delete/local/bk-1", "b1 POST /backup/delete/local/bk-1",
	}, deleted, "every downloaded copy is gone before the drop ON CLUSTER")
	require.Empty(t, s.matching(http.MethodPost, "/backup/restore/"))
}

// A restore from another installation fails before it changes anything, with the exit code that tells
// so, when a name it takes for {installation} or {namespace} is no Kubernetes name, when the storage
// path check cannot tell the paths of its cluster - a configuration file of the sidecar, a storage
// of unknown paths, a macro of unknown value - when the path it would set holds a character a command
// cannot carry, when a host holds a local copy of the backup, which restore_remote would take for the
// backup, or when the backup is missing at the source paths of a host. The copies of the schema the
// preflight downloaded are deleted again then.
func TestJobScriptRefusesARestoreFromAnotherInstallationBeforeItChangesAnything(t *testing.T) {
	s3 := func(settings ...string) map[string]string {
		return storageSettings("s3", append([]string{"S3_BUCKET", "backups"}, settings...)...)
	}
	cluster := func(settings map[string]string) map[string]string {
		return pathCheckTestEnv(pathCluster{name: restoreTestCluster, shards: "0 1", settings: settings})
	}
	unknown := "ERROR: a restore from another installation needs the storage paths of the sidecars, which it reads the backup at with the {installation} and {namespace} of that installation, and the storage path check cannot tell them: "
	for name, tc := range map[string]struct {
		env    map[string]string
		mutate func(map[string]*stubHost)
		// refused is the reason the Job fails for
		refused string
		// requests tells that the Job asked the sidecars before it failed
		requests bool
	}{
		"an installation name with a capital": {
			env:     map[string]string{envSourceInstallation: "Src"},
			refused: "ERROR: SOURCE_INSTALLATION is not a valid installation name",
		},
		"an installation name with a slash": {
			env:     map[string]string{envSourceInstallation: "src/x"},
			refused: "ERROR: SOURCE_INSTALLATION is not a valid installation name",
		},
		"an installation name with a dot": {
			env:     map[string]string{envSourceInstallation: "src.x"},
			refused: "ERROR: SOURCE_INSTALLATION is not a valid installation name",
		},
		"an installation name too long": {
			env:     map[string]string{envSourceInstallation: strings.Repeat("s", api.BackupSourceInstallationMaxLength+1)},
			refused: "ERROR: SOURCE_INSTALLATION is not a valid installation name",
		},
		"a namespace starting a flag": {
			env:     map[string]string{envSourceNamespace: "-ns"},
			refused: "ERROR: SOURCE_NAMESPACE is not a valid namespace",
		},
		"a namespace with a blank": {
			env:     map[string]string{envSourceNamespace: "ns --rbac"},
			refused: "ERROR: SOURCE_NAMESPACE is not a valid namespace",
		},
		"a namespace too long": {
			env:     map[string]string{envSourceNamespace: strings.Repeat("n", api.BackupSourceNamespaceMaxLength+1)},
			refused: "ERROR: SOURCE_NAMESPACE is not a valid namespace",
		},
		"no storage path check": {
			env:     map[string]string{envPathClusters: ""},
			refused: unknown + "PATH_CLUSTERS names no cluster",
		},
		"a storage of unknown paths": {
			env:     cluster(storageSettings("custom")),
			refused: unknown + "it keeps its backups in custom storage",
		},
		"a macro of unknown value": {
			env:     cluster(s3("S3_PATH", "backup/{installation}/{layer}/{shard}")),
			refused: unknown + "the path 'backup/{installation}/{layer}/{shard}' of cluster main holds {layer}",
		},
		"a character a command cannot carry": {
			env:     cluster(s3("S3_PATH", "backup/{installation}/a+b/{shard}")),
			refused: "ERROR: the storage path 'backup/src/a+b/{shard}' of the installation restore bk-1 reads the backup of cannot be set in a command of the restore",
		},
		"a blank in the object disk path": {
			env:     cluster(s3("S3_OBJECT_DISK_PATH", "od/{installation}/x y/{shard}")),
			refused: "ERROR: the storage path 'od/src/x y/{shard}' of the installation restore bk-1 reads the backup of cannot be set in a command of the restore",
		},
		"a storage path at the root of the storage": {
			env: pathCheckTestEnv(pathCluster{name: restoreTestCluster, shards: "0", settings: s3("S3_PATH", "",
				"S3_OBJECT_DISK_PATH", "od/{installation}/{shard}")}),
			refused: "ERROR: the storage path '' of the installation restore bk-1 reads the backup of cannot be set in a command of the restore",
		},
		"a local copy of the backup on a host": {
			mutate:   func(h map[string]*stubHost) { h["d1"].local = []stubBackup{local("bk-1")} },
			refused:  "ERROR: preflight failed: d1 holds a local copy of bk-1, which a restore from the storage paths of another installation would restore in place of the backup there; delete it (clickhouse-backup delete local bk-1) and restore again",
			requests: true,
		},
		"a local copy of the backup on a host, without topology validation": {
			env:      map[string]string{envValidateTopology: "false"},
			mutate:   func(h map[string]*stubHost) { h["d1"].local = []stubBackup{local("bk-1")} },
			refused:  "ERROR: preflight failed: d1 holds a local copy of bk-1",
			requests: true,
		},
		"a backup of the same name at the own storage path of a host": {
			mutate:   func(h map[string]*stubHost) { h["d1"].remote = []stubBackup{remote("bk-1", "")} },
			refused:  "ERROR: preflight failed: d1 holds a backup bk-1 at its own storage path too, which clickhouse-backup would take for the one at the storage path 'backup/src/main/{shard}', as it tells the backups of every path apart by name alone: restore a backup of another name, or delete that one first\n",
			requests: true,
		},
		"a broken backup of the same name at the own storage path of a host": {
			mutate:   func(h map[string]*stubHost) { h["d1"].remote = []stubBackup{broken("bk-1")} },
			refused:  "ERROR: preflight failed: d1 holds a backup bk-1 at its own storage path too",
			requests: true,
		},
		"a backup of the same name at the own storage path of a host, without topology validation": {
			env:      map[string]string{envValidateTopology: "false"},
			mutate:   func(h map[string]*stubHost) { h["d1"].remote = []stubBackup{remote("bk-1", "")} },
			refused:  "ERROR: preflight failed: d1 holds a backup bk-1 at its own storage path too",
			requests: true,
		},
		"the backup the restored one is incremental from at the own storage path of a host": {
			mutate: func(h map[string]*stubHost) {
				h["d1"].paths[testSourcePath][findStubBackup(h["d1"].paths[testSourcePath], "bk-1")].Required = "bk-0"
				h["d1"].remote = []stubBackup{remote("bk-0", "")}
			},
			refused:  "ERROR: preflight failed: backup bk-1 is incremental from bk-0, and d1 holds a backup bk-0 at its own storage path too, which clickhouse-backup would take for the one at the storage path 'backup/src/main/{shard}', as it tells the backups of every path apart by name alone: delete that one first\n",
			requests: true,
		},
		"the backup missing at the source paths of a host": {
			mutate:   func(h map[string]*stubHost) { h["d1"].paths = nil },
			refused:  "ERROR: preflight failed: backup bk-1 is missing in the storage path 'backup/src/main/{shard}' of d1, or cannot be read there: d1: download bk-1 ended with status 'error': 'bk-1' is not found on remote storage",
			requests: true,
		},
		"the backup missing at source paths that hold {replica}": {
			env:      cluster(s3("S3_PATH", "backup/{installation}/{cluster}/{shard}/{replica}")),
			refused:  "or cannot be read there: s0: download bk-1 ended with status 'error': 'bk-1' is not found on remote storage; the storage path holds {replica}, the host name, which holds the name of the installation, so the hosts of installations of different names have different paths\n",
			requests: true,
		},
		"the backup missing at source paths whose object disk path alone holds {replica}": {
			env:      cluster(s3("S3_PATH", testOwnPath, "S3_OBJECT_DISK_PATH", "od/{installation}/{shard}/{replica}")),
			mutate:   func(h map[string]*stubHost) { h["d1"].paths = nil },
			refused:  "or cannot be read there: d1: download bk-1 ended with status 'error': 'bk-1' is not found on remote storage\n",
			requests: true,
		},
		"an object disk path the installations share": {
			env:     cluster(s3("S3_PATH", testOwnPath, "S3_OBJECT_DISK_PATH", "od/{cluster}/{shard}")),
			refused: "ERROR: the object disk path 'od/{cluster}/{shard}' of cluster main holds no {installation} or {namespace} that tells the installation restore bk-1 names apart from this one, while the storage path does: deleting the copy the restore downloads would delete the object disk data of the backup; give the object disk path the macros of the storage path\n",
		},
		"the own storage path of a host that cannot be listed": {
			mutate:   func(h map[string]*stubHost) { h["d1"].remoteListStatus = http.StatusInternalServerError },
			refused:  "ERROR: preflight failed on d1: d1: cannot list remote backups (HTTP 500): ",
			requests: true,
		},
		"the sidecar of a host restarting during the download": {
			mutate:   func(h map[string]*stubHost) { h["d1"].loseActions = true },
			refused:  "ERROR: preflight failed: backup bk-1 is missing in the storage path 'backup/src/main/{shard}' of d1, or cannot be read there: d1: the sidecar restarted during download bk-1\n",
			requests: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			hosts := atSourcePath(restoreHosts(), testSourcePath)
			if tc.mutate != nil {
				tc.mutate(hosts)
			}
			s := newSidecarStub(t, hosts)
			code, out := s.run(withEnv(sourceRestoreEnv(nil), tc.env))
			require.Equal(t, 3, code, out)
			require.Contains(t, out, tc.refused)
			if !tc.requests {
				require.Zero(t, s.countCalls(), "nothing is asked of a sidecar before the check passed")
				return
			}
			for _, c := range s.matching(http.MethodPost, "/") {
				require.True(t, (c.path == "/backup/delete/local/bk-1") || strings.HasPrefix(c.command, "download --schema --rbac "), "%s %s", c, c.command)
			}
			require.Empty(t, hosts["s0"].local, "the copies the preflight downloaded go with the failed Job")
		})
	}
}

// A restore step from the paths of another installation that fails leaves the copy of the backup it
// downloaded, which would refuse the next restore from the same paths: the Job that fails deletes the
// copies on every restored host.
func TestJobScriptDropsTheCopiesOfAFailedRestoreFromAnotherInstallation(t *testing.T) {
	hosts := atSourcePath(restoreHosts(), testSourcePath)
	hosts["d1"].failRestoreAction = "no space left on device"
	s := newSidecarStub(t, hosts)
	code, out := s.run(sourceRestoreEnv(nil))
	require.Equal(t, 1, code, out)
	require.Contains(t, out, "ERROR: data restore of bk-1 failed on d1")
	for _, host := range []string{"s0", "d1"} {
		require.Empty(t, hosts[host].local, "the copy of %s goes with the failed Job:\n%s", host, out)
	}
	require.NotContains(t, out, "left on local disk")
}

// A restore reads its own storage paths as any other restore does, with typed requests, when the
// installation it names changes none of them: they hold neither {installation} nor {namespace} with
// another value, or it names its own installation, which takes no knowing of its paths. It warns that
// the paths are its own.
func TestJobScriptRestoresFromItsOwnPathsWhenTheyTellNoOtherInstallation(t *testing.T) {
	s3 := func(settings ...string) map[string]string {
		return storageSettings("s3", append([]string{"S3_BUCKET", "backups"}, settings...)...)
	}
	cluster := func(settings map[string]string) map[string]string {
		return pathCheckTestEnv(pathCluster{name: restoreTestCluster, shards: "0 1", settings: settings})
	}
	for name, tc := range map[string]struct {
		env    map[string]string
		warned string
	}{
		"paths without the macros": {
			env:    cluster(s3("S3_PATH", "backup/{cluster}/{shard}", "S3_OBJECT_DISK_PATH", "od/{cluster}/{shard}")),
			warned: "WARNING: the storage paths of cluster main hold no {installation} or {namespace} that tells the installation restore bk-1 names apart from this one, so it reads them as they are: 'backup/{cluster}/{shard}' and the object disk path 'od/{cluster}/{shard}'",
		},
		"another namespace of paths without it": {
			env: withEnv(cluster(s3("S3_PATH", "backup/{installation}/{cluster}/{shard}", "S3_OBJECT_DISK_PATH", "")),
				map[string]string{envSourceInstallation: ""}),
			warned: "WARNING: the storage paths of cluster main hold no {installation} or {namespace} that tells the installation restore bk-1 names apart from this one, so it reads them as they are: 'backup/{installation}/{cluster}/{shard}'\n",
		},
		"its own installation": {
			env:    map[string]string{envPathClusters: "", envSourceInstallation: "demo", envSourceNamespace: testPathNamespace},
			warned: "WARNING: restore bk-1 names its own installation as the one it restores the backup of, which it reads at its own storage paths",
		},
		"its own installation name": {
			env:    map[string]string{envPathClusters: "", envSourceInstallation: "demo", envSourceNamespace: ""},
			warned: "WARNING: restore bk-1 names its own installation",
		},
		"no installation": {
			env: map[string]string{envSourceInstallation: "", envSourceNamespace: ""},
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := newSidecarStub(t, restoreHosts())
			code, out := s.run(withEnv(sourceRestoreEnv(nil), tc.env))
			require.Zero(t, code, out)
			if tc.warned == "" {
				require.NotContains(t, out, "WARNING:")
			}
			require.Contains(t, out, tc.warned)
			require.Empty(t, s.matching(http.MethodPost, "/backup/actions"))
			require.Equal(t, []string{"s0 download hardlink_exists_files=", "d1 download hardlink_exists_files=", "s0 rm= schema=", "s0 data=", "d1 data="},
				s.restoreSteps())
			require.Len(t, s.matching(http.MethodGet, "/backup/list/remote"), 2)
		})
	}
}

// A backup missing at the storage path of a host, which holds {installation} or {namespace}, may be one
// another installation made at a path of its own: the preflight that fails tells how to restore it.
func TestJobScriptTellsWhereABackupOfAnotherInstallationIs(t *testing.T) {
	for name, tc := range map[string]struct {
		path    string
		message string
	}{
		"a path of every installation": {
			path:    "backup/{installation}/{cluster}/{shard}",
			message: "preflight failed: backup bk-1 is missing in the storage of d1; the storage path 'backup/{installation}/{cluster}/{shard}' holds {installation}, which keeps apart the backups of installations of another name: a backup made by one of them is found by naming that installation in the restore (sourceInstallation)\n",
		},
		"a path of every installation and namespace": {
			path:    "backup/{namespace}/{installation}/{cluster}/{shard}",
			message: "preflight failed: backup bk-1 is missing in the storage of d1; the storage path 'backup/{namespace}/{installation}/{cluster}/{shard}' holds {installation} and {namespace}, which keeps apart the backups of installations of another name or namespace: a backup made by one of them is found by naming that installation in the restore (sourceInstallation, sourceNamespace)\n",
		},
		"a path of every namespace": {
			path:    "backup/{namespace}/{cluster}/{shard}",
			message: "preflight failed: backup bk-1 is missing in the storage of d1; the storage path 'backup/{namespace}/{cluster}/{shard}' holds {namespace}, which keeps apart the backups of installations of another namespace: a backup made by one of them is found by naming that installation in the restore (sourceNamespace)\n",
		},
		"a path of every replica": {
			path:    "backup/{installation}/{shard}/{replica}",
			message: "the backup was made on the first replica of each shard only; the storage path 'backup/{installation}/{shard}/{replica}' holds {installation}, which keeps",
		},
		"a path shared by the installations": {
			path:    "backup/{cluster}/{shard}",
			message: "preflight failed: backup bk-1 is missing in the storage of d1\n",
		},
	} {
		t.Run(name, func(t *testing.T) {
			hosts := restoreHosts()
			hosts["d1"].remote = nil
			s := newSidecarStub(t, hosts)
			path := filepath.Join(t.TempDir(), "termination-log")
			env := sourceRestoreEnv(map[string]string{envSourceInstallation: "", envSourceNamespace: "", envTerminationMessagePath: path})
			env[pathClusterEnv(0, "S3_PATH")] = tc.path
			code, out := s.run(env)
			require.Equal(t, 3, code, out)
			message, err := os.ReadFile(path)
			require.NoError(t, err)
			if strings.HasSuffix(tc.message, "\n") {
				require.Equal(t, tc.message, string(message))
			} else {
				require.Contains(t, string(message), tc.message)
			}
			require.Empty(t, s.matching(http.MethodPost, "/"))
		})
	}
}

// Without topology validation a restore from another installation does not require the backup on every
// host, which no download is needed for then. A preflight that lists the tables of the backup downloads
// the schema to the hosts it lists them on, which the restore deletes before its steps; a local copy of
// the backup on any restored host refuses the restore all the same.
func TestJobScriptRestoresFromAnotherInstallationWithoutTopologyValidation(t *testing.T) {
	flags := sourceEnvFlags(testSourcePath, testSourceObjectDiskPath)
	for name, tc := range map[string]struct {
		env   map[string]string
		live  []string
		calls []string
		// base, when set, is the backup the restored one is incremental from, which the own storage path of
		// every host holds too: no download tells it, so the restore cannot refuse it
		base string
	}{
		"no table of the backup listed": {
			calls: []string{
				"s0 GET /backup/version", "d1 GET /backup/version",
				"s0 GET /backup/list/local", "s0 GET /backup/list/remote", "d1 GET /backup/list/local", "d1 GET /backup/list/remote",
				"s0 GET /backup/tables/all", "d1 GET /backup/tables/all",
				"s0 POST /backup/actions", "d1 POST /backup/actions", "s0 POST /backup/actions", "s0 POST /backup/actions", "d1 POST /backup/actions",
				"s0 GET /backup/list/local", "s0 POST /backup/delete/local/bk-1",
				"d1 GET /backup/list/local", "d1 POST /backup/delete/local/bk-1",
			},
		},
		"the backup it is incremental from at the own storage paths": {
			base: "bk-0",
			calls: []string{
				"s0 GET /backup/version", "d1 GET /backup/version",
				"s0 GET /backup/list/local", "s0 GET /backup/list/remote", "d1 GET /backup/list/local", "d1 GET /backup/list/remote",
				"s0 GET /backup/tables/all", "d1 GET /backup/tables/all",
				"s0 POST /backup/actions", "d1 POST /backup/actions", "s0 POST /backup/actions", "s0 POST /backup/actions", "d1 POST /backup/actions",
				"s0 GET /backup/list/local", "s0 POST /backup/delete/local/bk-1",
				"d1 GET /backup/list/local", "d1 POST /backup/delete/local/bk-1",
			},
		},
		"the tables of the backup listed": {
			env:  map[string]string{envTables: "db1.t1"},
			live: []string{"db9.t1"},
			calls: []string{
				"s0 GET /backup/version", "d1 GET /backup/version",
				"s0 GET /backup/list/local", "s0 GET /backup/list/remote", "d1 GET /backup/list/local", "d1 GET /backup/list/remote",
				"s0 GET /backup/tables/all", "d1 GET /backup/tables/all",
				"s0 POST /backup/actions", "s0 GET /backup/tables", "d1 POST /backup/actions", "d1 GET /backup/tables",
				"s0 POST /backup/delete/local/bk-1", "d1 POST /backup/delete/local/bk-1",
				"s0 POST /backup/actions", "d1 POST /backup/actions", "s0 POST /backup/actions", "s0 POST /backup/actions", "d1 POST /backup/actions",
				"s0 GET /backup/list/local", "s0 POST /backup/delete/local/bk-1",
				"d1 GET /backup/list/local", "d1 POST /backup/delete/local/bk-1",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			hosts := atSourcePath(restoreHosts(), testSourcePath)
			hosts["s0"].tables = tc.live
			if tc.base != "" {
				for _, host := range hosts {
					for i := range host.paths[testSourcePath] {
						host.paths[testSourcePath][i].Required = tc.base
					}
					host.remote = []stubBackup{remote(tc.base, "")}
				}
			}
			s := newSidecarStub(t, hosts)
			code, out := s.run(sourceRestoreEnv(withEnv(map[string]string{envValidateTopology: "false"}, tc.env)))
			require.Zero(t, code, out)
			require.Equal(t, tc.calls, s.significant())
			for _, step := range s.restoreSteps() {
				require.True(t, strings.HasSuffix(step, flags+" bk-1"), step)
			}
		})
	}
}

// orchestratorScriptDir returns the directory of the orchestrator script in the repository: the backup folder of the
// operator configuration, generated from deploy/builder/templates-config/backup
func orchestratorScriptDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "config", "backup")
}

// orchestratorScript is the orchestrator script of the repository, which the tests run
var orchestratorScript string

func init() {
	script, err := ReadOrchestratorScript(orchestratorScriptDir())
	if testedOrchestrator == orchestratorPython {
		script, err = readScript(filepath.Join(pythonOrchestratorScriptDir(), OrchestratorPythonScriptFile))
	}
	if err != nil {
		panic(err)
	}
	orchestratorScript = script
}

func TestReadOrchestratorScript(t *testing.T) {
	bashOnly(t)
	script, err := ReadOrchestratorScript(orchestratorScriptDir())
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(script, "# backup-orchestrator.sh "))
	_, err = ReadOrchestratorScript(t.TempDir())
	require.Error(t, err)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, OrchestratorScriptFile), []byte(" \n"), 0o600))
	_, err = ReadOrchestratorScript(dir)
	require.Error(t, err)
}

// The Job runs the script as a file: its main runs then, as under bash -c.
func TestJobScriptRunsAsAFile(t *testing.T) {
	requireOrchestrator(t)
	path := filepath.Join(orchestratorScriptDir(), OrchestratorScriptFile)
	termination := filepath.Join(t.TempDir(), "termination-log")
	cmd := exec.Command("bash", path)
	if testedOrchestrator == orchestratorPython {
		cmd = exec.Command("python3", filepath.Join(pythonOrchestratorScriptDir(), OrchestratorPythonScriptFile))
	}
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), envOperation + "=drop", envAPIPort + "=7171", envAPIUsername + "=u",
		envAPIPassword + "=p", envTerminationMessagePath + "=" + termination, envUnchangedExitCode + "=3"}
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, string(out))
	require.Equal(t, 3, exitErr.ExitCode(), string(out))
	message, err := os.ReadFile(termination)
	require.NoError(t, err)
	require.Equal(t, "OPERATION must be one of backup, verify, restore\n", string(message))
}

// sourced runs snippet in bash after sourcing the script, which runs nothing then but leaves its functions, with only
// the given environment on top of PATH, and returns its exit code and output.
func sourced(t *testing.T, env map[string]string, snippet string) (int, string) {
	t.Helper()
	bashOnly(t)
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not available")
	}
	path := filepath.Join(t.TempDir(), "backup-orchestrator.sh")
	require.NoError(t, os.WriteFile(path, []byte(orchestratorScript), 0o600))
	cmd := exec.Command("bash", "-c", "source \"$1\"\nshift\n"+snippet, "bash", path)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	for name, value := range env {
		cmd.Env = append(cmd.Env, name+"="+value)
	}
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), string(out)
	}
	require.NoError(t, err)
	return 0, string(out)
}

// sourcedOutput runs snippet as sourced does and requires it to succeed, returning its output
func sourcedOutput(t *testing.T, env map[string]string, snippet string) string {
	t.Helper()
	code, out := sourced(t, env, snippet)
	require.Zero(t, code, out)
	return out
}

// Sourced, the script runs nothing: no request, no output, no exit.
func TestJobScriptSourcedRunsNothing(t *testing.T) {
	require.Equal(t, "sourced\n", snippetOutput(t, map[string]string{envOperation: "drop"}, "echo sourced", "print('sourced')"))
}

// The slot rules of the script are those of the scheduler: an hour entry matches the minute of its time, a 6hours
// entry that minute every 6 hours from its hour, a daily entry its time, a weekly entry its day of the week too, 0
// being Sunday, a monthly entry its day of the month too, clamped to the last day of a shorter month.
func TestJobScriptSlotMatches(t *testing.T) {
	for _, tc := range []struct {
		entry string
		tag   string
		match bool
	}{
		{"period=minute keep=1", "20260101031759", true},
		{"period=minute time=07:15 keep=1", "20261231235900", true},
		{"period=hour time=07:15 keep=1", "20260101031500", true},
		{"period=hour time=07:15 keep=1", "20260101031600", false},
		{"period=hour keep=1", "20260101090059", true},
		{"period=6hours time=08:30 keep=1", "20260101023000", true},
		{"period=6hours time=08:30 keep=1", "20260101203000", true},
		{"period=6hours time=08:30 keep=1", "20260101033000", false},
		{"period=6hours time=08:30 keep=1", "20260101080000", false},
		{"period=daily time=23:45 keep=1", "20260505234500", true},
		{"period=daily time=23:45 keep=1", "20260505224500", false},
		// 2026-11-15 is a Sunday
		{"period=weekly day=0 time=01:00 keep=1", "20261115010000", true},
		{"period=weekly day=0 time=01:00 keep=1", "20261116010000", false},
		{"period=weekly day=6 time=01:00 keep=1", "20261114010000", true},
		{"period=weekly day=4 time=00:00 keep=1", "19700101000000", true},
		{"period=weekly day=1 time=00:00 keep=1", "20000228000000", true},
		{"period=monthly day=31 time=00:00 keep=1", "20260131000000", true},
		{"period=monthly day=31 time=00:00 keep=1", "20260228000000", true},
		{"period=monthly day=31 time=00:00 keep=1", "20280229000000", true},
		{"period=monthly day=31 time=00:00 keep=1", "20280228000000", false},
		{"period=monthly day=31 time=00:00 keep=1", "20260430000000", true},
		{"period=monthly day=31 time=00:00 keep=1", "20260429000000", false},
		{"period=monthly day=29 time=00:00 keep=1", "21000228000000", true},
		{"period=monthly day=15 time=00:00 keep=1", "20260215000000", true},
		{"period=monthly day=15 time=00:00 keep=1", "20260228000000", false},
	} {
		out := snippetOutput(t, map[string]string{envSchedules: tc.entry, "TAG": tc.tag},
			`parse_schedules; if slot_matches 0 "${TAG}"; then echo yes; else echo no; fi`, pySlotMatches)
		require.Equal(t, map[bool]string{true: "yes\n", false: "no\n"}[tc.match], out, "%s at %s", tc.entry, tc.tag)
	}
}

// int32Ref returns a reference to value
func int32Ref(value int32) *int32 {
	return &value
}

// The script and the operator agree on the entries a slot is one of, and on the one it is above all: the script
// decides from it whether the backup is incremental, the operator records it on the schedule.
func TestJobScriptMatchesTheEntriesAsTheOperator(t *testing.T) {
	schedules := [][]api.BackupScheduleEntry{
		{
			{Period: api.BackupPeriodMinute, Keep: 60},
			{Period: api.BackupPeriodHour, Time: "05:30", Keep: 24},
			{Period: api.BackupPeriodDaily, Time: "05:30", Keep: 7},
		},
		{
			{Period: api.BackupPeriodHour, Time: "05:30", Keep: 24},
			{Period: api.BackupPeriod6Hours, Time: "05:30", Keep: 4},
			{Period: api.BackupPeriodDaily, Time: "05:30", Keep: 7},
			{Period: api.BackupPeriodWeekly, Time: "05:30", Day: int32Ref(0), Keep: 4},
			{Period: api.BackupPeriodMonthly, Time: "05:30", Day: int32Ref(31), Keep: 12},
		},
		{
			{Period: api.BackupPeriodDaily, Time: "23:00", Keep: 7},
			{Period: api.BackupPeriodMonthly, Time: "23:00", Day: int32Ref(30), Keep: 3},
		},
	}
	for i, entries := range schedules {
		env := EntriesEnv(entries)
		var tags []string
		var want []string
		for at := time.Date(2028, time.February, 25, 0, 30, 0, 0, time.UTC); at.Before(time.Date(2028, time.March, 2, 0, 0, 0, 0, time.UTC)); at = at.Add(30 * time.Minute) {
			tags = append(tags, at.Format(scheduleTimestampLayout))
			winner := ""
			if w := Winner(MatchingEntries(entries, at)); w != nil {
				winner = string(w.Period)
			}
			want = append(want, winner)
		}
		out := snippetOutput(t, map[string]string{envSchedules: env, "TAGS": strings.Join(tags, " ")}, `
			parse_schedules
			for tag in ${TAGS}; do
				winner_entry "${tag}"
				printf '%s\n' "${WINNER:+${SCHED_PERIOD[WINNER]}}"
			done`, `
S.sched = parse_schedules(env('SCHEDULES'))
for tag in env('TAGS').split():
    winner_entry(tag)
    print(S.sched[S.winner]['period'] if S.winner is not None else '')`)
		require.Equal(t, want, strings.Split(strings.TrimSuffix(out, "\n"), "\n"), "schedule %d: %s", i, env)
	}
}

// The heaviest entry of a slot wins: minute < hour < 6hours < daily < weekly < monthly.
func TestJobScriptPicksTheHeaviestEntry(t *testing.T) {
	const schedules = "period=minute keep=60;period=hour time=00:30 keep=24;period=6hours time=00:30 keep=4;" +
		"period=daily time=06:30 keep=7;period=weekly day=0 time=06:30 keep=4;period=monthly day=15 time=06:30 keep=12"
	for tag, want := range map[string]string{
		"20261112071700": "minute 0", "20261112073000": "hour 0 1", "20261112003000": "6hours 0 1 2",
		"20261112063000": "daily 0 1 2 3", "20261122063000": "weekly 0 1 2 3 4", "20261215063000": "monthly 0 1 2 3 5",
	} {
		out := snippetOutput(t, map[string]string{envSchedules: schedules, "TAG": tag},
			`parse_schedules; winner_entry "${TAG}"; echo "${SCHED_PERIOD[WINNER]} ${MATCHED[*]}"`, `
S.sched = parse_schedules(env('SCHEDULES'))
winner_entry(env('TAG'))
print(S.sched[S.winner]['period'], *S.matched)`)
		require.Equal(t, want+"\n", out, tag)
	}
}

func TestJobScriptParsesSchedules(t *testing.T) {
	out := snippetOutput(t, map[string]string{
		envSchedules: " period=daily time=05:00 keep=7;period=weekly day=0 time=05:07 keep=4; ;period=hour keep=24 incremental=true;period=6hours time=13:15 keep=2;period=monthly day=31 time=05:00 keep=12 incremental=false;period=minute time=07:07 keep=5",
	}, `parse_schedules
		for i in "${!SCHED_PERIOD[@]}"; do
			echo "${SCHED_PERIOD[i]} ${SCHED_HOUR[i]} ${SCHED_MINUTE[i]} ${SCHED_DAY[i]:--} ${SCHED_KEEP[i]} ${SCHED_INCREMENTAL[i]}"
		done`, `
for e in parse_schedules(env('SCHEDULES')):
    print(e['period'], e['hour'], e['minute'], '-' if e['day'] is None else e['day'], e['keep'], str(e['incremental']).lower())`)
	require.Equal(t, "daily 5 0 - 7 false\nweekly 5 7 0 4 false\nhour 0 0 - 24 true\n6hours 13 15 - 2 false\nmonthly 5 0 31 12 false\nminute 0 0 - 5 false\n", out)
	for _, invalid := range []string{
		"period=yearly keep=1",
		"period=minute keep=1 day=1",
		"period=minute time=7:07 keep=1",
		"period=daily time=00:00",
		"period=daily time=00:00 keep=0",
		"period=daily keep=1",
		"period=daily time=24:00 keep=1",
		"period=daily time=1:00 keep=1",
		"period=daily time=01:00 day=1 keep=1",
		"period=weekly time=01:00 keep=1",
		"period=weekly time=01:00 day=7 keep=1",
		"period=monthly time=01:00 day=0 keep=1",
		"period=monthly time=01:00 day=32 keep=1",
		"period=daily time=01:00 keep=1 incremental=yes",
		"period=daily time=01:00 keep=1 colour=red",
		"period=daily time=01:00 keep",
		"period=daily time=01:00 keep=1;period=daily time=01:00 keep=2",
		"period=daily time=01:00 keep=1\nperiod=hour keep=1",
	} {
		code, out := snippet(t, map[string]string{envSchedules: invalid}, "parse_schedules", "parse_schedules(env('SCHEDULES'))")
		require.Equal(t, 1, code, "%q: %s", invalid, out)
		require.Contains(t, out, "ERROR: SCHEDULES", invalid)
	}
}

// The slot of a run is the minute its Job name ends with, after the name of its CronJob, in the time zone of the
// schedule; a Job named otherwise, or a slot past the four-digit years, takes the current minute.
func TestJobScriptReadsTheSlotTime(t *testing.T) {
	for _, tc := range []struct {
		cronJob, job, zone, want string
	}{
		{"nightly", "nightly-0", "", "19700101000000"},
		{"nightly", "nightly-28333334", "", "20231114221400"},
		{"nightly", "nightly-0028333334", "", "20231114221400"},
		{"nightly", "nightly-28333334", "UTC", "20231114221400"},
		{"nightly", "nightly-28333334", "America/New_York", "20231114171400"},
		{"nightly", "nightly-28333334", "Asia/Kolkata", "20231115034400"},
		{"nightly", "nightly-x28333334", "", ""},
		{"nightly", "nightly-283333340000", "", ""},
		{"nightly", "nightly-9999999999", "", ""},
		{"nightly", "other-28333334", "", ""},
		{"nightly", "", "", ""},
		{"", "nightly-28333334", "", ""},
	} {
		out := snippetOutput(t, map[string]string{envCronJobName: tc.cronJob, envJobName: tc.job, envScheduleTimezone: tc.zone},
			`check_timezone; schedule_slot; echo "${SLOT_TAG}"`, "check_timezone(env('SCHEDULE_TIMEZONE')); schedule_slot(); print(S.slot_tag)")
		if tc.want == "" {
			require.Regexp(t, `^[0-9]{12}00\n$`, out, "%+v", tc)
			require.NotEqual(t, "20231114221400\n", out)
			continue
		}
		require.Equal(t, tc.want+"\n", out, "%+v", tc)
	}
	for _, zone := range []string{"Mars/Olympus", "../etc/passwd", "Europe/../UTC", "a b"} {
		code, out := snippet(t, map[string]string{envScheduleTimezone: zone}, "check_timezone", "check_timezone(env('SCHEDULE_TIMEZONE'))")
		require.Equal(t, 1, code, "%q: %s", zone, out)
		require.Contains(t, out, "ERROR: SCHEDULE_TIMEZONE is not a time zone of the tzdata of the image")
	}
}

func TestJobScriptTellsValidTags(t *testing.T) {
	for tag, valid := range map[string]bool{
		"20260101000000": true, "20240229235959": true, "20230229000000": false, "20261301000000": false,
		"20260100000000": false, "20260101240000": false, "20260101006000": false, "2026010100000": false,
		"202601010000000": false, "2026010100000x": false, "": false,
	} {
		out := snippetOutput(t, map[string]string{"TAG": tag}, `if tag_valid "${TAG}"; then echo yes; else echo no; fi`,
			"print('yes' if tag_valid(env('TAG')) else 'no')")
		require.Equal(t, map[bool]string{true: "yes\n", false: "no\n"}[valid], out, tag)
	}
}

// retention_plan decides as the cleanup of remote backups of the Altinity Cloud Manager: what it keeps, and what it
// deletes, newest first.
func TestJobScriptRetentionPlan(t *testing.T) {
	all := func(names ...string) []string { return names }
	for name, tc := range map[string]struct {
		schedules string
		names     []string
		// incomplete and empty name the backups not every host lists complete, and those listed of size 0;
		// sizeless those no host lists a size above 0 of, broken those a host lists broken
		incomplete, empty, sizeless, broken []string
		made                                string
		keep, obsolete                      []string
	}{
		"the newest of a daily entry": {
			schedules: "period=daily time=00:00 keep=2",
			names:     all("d-20200101000000", "d-20200104000000", "d-20200102000000", "d-20200103000000"),
			made:      "d-20200104000000",
			keep:      all("d-20200103000000", "d-20200104000000"),
			obsolete:  all("d-20200102000000", "d-20200101000000"),
		},
		"a backup kept by several entries": {
			schedules: "period=weekly day=0 time=00:00 keep=2;period=daily time=00:00 keep=2",
			// 2020-01-05 and 2020-01-12 are Sundays
			names:    all("d-20200105000000", "d-20200111000000", "d-20200112000000", "d-20200113000000", "d-20200110000000"),
			keep:     all("d-20200105000000", "d-20200112000000", "d-20200113000000"),
			obsolete: all("d-20200111000000", "d-20200110000000"),
		},
		"backups made at once from the schedule": {
			schedules: "period=daily time=00:00 keep=2",
			names:     all("d-20200101000000", "d-20200102000000", "d-20200103000000", "d-20200101T120000", "d-20200102T120000", "d-20200103T120000"),
			keep:      all("d-20200102000000", "d-20200102T120000", "d-20200103000000", "d-20200103T120000"),
			obsolete:  all("d-20200101T120000", "d-20200101000000"),
		},
		"the newest backup made at once": {
			schedules: "period=daily time=00:00 keep=1",
			names:     all("d-20200101000000", "d-20200102000000", "d-20200102T120000"),
			keep:      all("d-20200102000000", "d-20200102T120000"),
			obsolete:  all("d-20200101000000"),
		},
		"backups made at once without a scheduled one": {
			schedules: "period=daily time=00:00 keep=1",
			names:     all("d-20200101T120000", "d-20200102T120000"),
			keep:      all("d-20200101T120000", "d-20200102T120000"),
		},
		"backups of no slot": {
			schedules: "period=daily time=00:00 keep=5",
			names:     all("d-20200101000000", "d-20200101060000", "d-20200102000000"),
			keep:      all("d-20200101000000", "d-20200102000000"),
			obsolete:  all("d-20200101060000"),
		},
		"incomplete, empty and invalid backups": {
			schedules:  "period=daily time=00:00 keep=1",
			names:      all("d-20200101000000", "d-20200102000000", "d-20200103000000", "d-20200104000000", "d-20201399000000"),
			incomplete: all("d-20200104000000"),
			empty:      all("d-20200101000000"),
			sizeless:   all("d-20200101000000"),
			keep:       all("d-20200103000000"),
			obsolete:   all("d-20200102000000"),
		},
		// An incomplete backup never counts, and goes once a complete one is newer, newest first among the others
		"incomplete backups older than the newest complete one": {
			schedules:  "period=daily time=00:00 keep=2",
			names:      all("d-20200101000000", "d-20200102000000", "d-20200103000000", "d-20200104000000", "d-20200102T120000"),
			incomplete: all("d-20200102000000", "d-20200102T120000"),
			keep:       all("d-20200103000000", "d-20200104000000"),
			obsolete:   all("d-20200102T120000", "d-20200102000000", "d-20200101000000"),
		},
		"incomplete backups the newest complete one keeps for the count": {
			schedules:  "period=daily time=00:00 keep=3",
			names:      all("d-20200101000000", "d-20200102000000", "d-20200103000000", "d-20200104000000"),
			incomplete: all("d-20200103000000"),
			keep:       all("d-20200101000000", "d-20200102000000", "d-20200104000000"),
			obsolete:   all("d-20200103000000"),
		},
		"an incomplete backup of size 0": {
			schedules:  "period=daily time=00:00 keep=1",
			names:      all("d-20200101000000", "d-20200102000000"),
			incomplete: all("d-20200101000000"),
			sizeless:   all("d-20200101000000"),
			keep:       all("d-20200102000000"),
		},
		"a broken backup of size 0": {
			schedules:  "period=daily time=00:00 keep=1",
			names:      all("d-20200101000000", "d-20200102000000", "d-20200103000000"),
			incomplete: all("d-20200101000000", "d-20200103000000"),
			sizeless:   all("d-20200101000000", "d-20200103000000"),
			broken:     all("d-20200101000000", "d-20200103000000"),
			keep:       all("d-20200102000000"),
			obsolete:   all("d-20200101000000"),
		},
		"an incomplete backup newer than every complete one": {
			schedules:  "period=daily time=00:00 keep=1",
			names:      all("d-20200101000000", "d-20200102000000", "d-20200102T120000"),
			incomplete: all("d-20200102000000", "d-20200102T120000"),
			keep:       all("d-20200101000000"),
			obsolete:   all(),
		},
		"incomplete backups alone": {
			schedules:  "period=daily time=00:00 keep=1",
			names:      all("d-20200101000000", "d-20200102000000"),
			incomplete: all("d-20200101000000", "d-20200102000000"),
		},
		"the run just made, incomplete": {
			schedules:  "period=daily time=00:00 keep=1",
			names:      all("d-20200101000000", "d-20200102000000", "d-29990101000000"),
			incomplete: all("d-20200101000000"),
			made:       "d-20200101000000",
			keep:       all("d-29990101000000"),
			obsolete:   all("d-20200102000000"),
		},
		"the run just made, past newer ones": {
			schedules: "period=daily time=00:00 keep=1",
			names:     all("d-20200101000000", "d-20200102000000", "d-29990101000000"),
			made:      "d-20200101000000",
			keep:      all("d-20200101000000", "d-29990101000000"),
			obsolete:  all("d-20200102000000"),
		},
		"no backups": {schedules: "period=daily time=00:00 keep=1"},
	} {
		t.Run(name, func(t *testing.T) {
			out := snippetOutput(t, map[string]string{
				envSchedules: tc.schedules, envBackupNamePrefix: "d", envBackupName: tc.made,
				"ALL": strings.Join(tc.names, " "), "INCOMPLETE": strings.Join(tc.incomplete, " "), "EMPTIES": strings.Join(tc.empty, " "),
				"SIZELESS": strings.Join(tc.sizeless, " "), "BROKENS": strings.Join(tc.broken, " "),
			}, `
				parse_schedules
				read -r -a NAMES <<<"${ALL}"
				declare -A COMPLETE=() EMPTY=() SIZED=()
				for name in "${NAMES[@]}"; do COMPLETE[${name}]=1; SIZED[${name}]=1; done
				for name in ${INCOMPLETE}; do unset "COMPLETE[${name}]"; done
				for name in ${EMPTIES}; do EMPTY[${name}]=1; done
				for name in ${SIZELESS}; do unset "SIZED[${name}]"; done
				for name in ${BROKENS}; do BROKEN[${name}]=1; done
				retention_plan
				echo "keep: $(for name in "${!KEEP[@]}"; do echo "${name}"; done | sort | tr '\n' ' ')"
				echo "obsolete: ${OBSOLETE[*]}"`, pyRetention+`
obsolete, _, keep = plan()
print('keep: ' + ''.join(name + ' ' for name in sorted(keep)))
print('obsolete: ' + ' '.join(obsolete))`)
			keep := slices.Clone(tc.keep)
			if tc.made != "" && !slices.Contains(keep, tc.made) && !slices.Contains(tc.incomplete, tc.made) {
				keep = append(keep, tc.made)
			}
			slices.Sort(keep)
			want := "keep: " + strings.Join(keep, " ")
			if len(keep) > 0 {
				want += " "
			}
			require.Equal(t, want+"\nobsolete: "+strings.Join(tc.obsolete, " ")+"\n", out)
		})
	}
}

// The day by day simulations of Altinity/acm#814, a backup at midnight every day and one made at once at noon every 10
// days, retention after every scheduled backup, leave what the pull request shows: every entry's backups, and the
// backups made at once in its window. Which backups are incremental does not change what retention keeps.
func TestJobScriptRetentionSimulation(t *testing.T) {
	for name, tc := range map[string]struct {
		schedules string
		days      int
		left      []string
	}{
		"4 weekly + 7 daily incremental, after 90 days": {
			schedules: "period=weekly day=0 time=00:00 keep=4;period=daily time=00:00 keep=7 incremental=true",
			days:      90,
			left: []string{
				"20260308000000", "20260315000000", "20260317T120000", "20260322000000", "20260325000000", "20260326000000",
				"20260327000000", "20260327T120000", "20260328000000", "20260329000000", "20260330000000", "20260331000000",
			},
		},
		"1 monthly + 2 weekly + 3 daily incremental, after 120 days": {
			schedules: "period=monthly day=1 time=00:00 keep=1;period=weekly day=0 time=00:00 keep=2;period=daily time=00:00 keep=3 incremental=true",
			days:      120,
			left: []string{
				"20260401000000", "20260406T120000", "20260416T120000", "20260419000000", "20260426000000", "20260426T120000",
				"20260428000000", "20260429000000", "20260430000000",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC).Unix()
			out := snippetOutput(t, map[string]string{
				envSchedules: tc.schedules, envBackupNamePrefix: "d", "DAYS": strconv.Itoa(tc.days), "START": strconv.FormatInt(start, 10),
			}, `
				parse_schedules
				declare -A HELD=() COMPLETE=() EMPTY=()
				for ((day = 0; day < DAYS; day++)); do
					TZ=UTC0 printf -v date '%(%Y%m%d)T' "$((START + day * 86400))"
					BACKUP_NAME=d-${date}000000
					HELD[${BACKUP_NAME}]=1
					NAMES=("${!HELD[@]}")
					COMPLETE=()
					for name in "${NAMES[@]}"; do COMPLETE[${name}]=1; done
					retention_plan
					for name in "${OBSOLETE[@]}"; do unset "HELD[${name}]"; done
					# A backup made at once, at noon, after the scheduled one
					if ((day % 10 == 5)); then HELD[d-${date}T120000]=1; fi
				done
				printf '%s\n' "${!HELD[@]}" | sed 's/^d-//' | sort`, `
S.sched = parse_schedules(env('SCHEDULES'))
held = set()
for day in range(int(env('DAYS'))):
    date = datetime.fromtimestamp(int(env('START')) + day * 86400, timezone.utc).strftime('%Y%m%d')
    S.env['BACKUP_NAME'] = 'd-' + date + '000000'
    held.add(S.env['BACKUP_NAME'])
    obsolete, _, _ = retention_plan(sorted(held), {'complete': set(held), 'empty': set(), 'sized': set(), 'broken': set()})
    held -= set(obsolete)
    if day % 10 == 5:
        held.add('d-' + date + 'T120000')
print('\n'.join(sorted(name[2:] for name in held)))`)
			require.Equal(t, strings.Join(tc.left, "\n")+"\n", out)
		})
	}
}

// incremental_base makes a backup incremental from the newest backup of the series every host holds, until the chain
// of incremental backups that ends with it is as long as the entry keeps. A backup made at once from the schedule is
// a base as a scheduled one, ordered by the time it is named after, as the Altinity Cloud Manager makes a backup
// incremental from the newest backup of the cluster.
func TestJobScriptIncrementalBase(t *testing.T) {
	const (
		d1 = "d-20260101000000"
		d2 = "d-20260102000000"
		d3 = "d-20260103000000"
		// Made at once from the schedule, at noon of the first day: after d1, before d2
		r1 = "d-20260101T120000"
	)
	for name, tc := range map[string]struct {
		series string
		keep   string
		base   string
		chain  string
	}{
		"no backup":                  {series: "", keep: "3", base: "", chain: "0"},
		"after a full backup":        {series: "h0 " + d1 + "=;h1 " + d1 + "=", keep: "3", base: d1, chain: "0"},
		"a chain shorter than keep":  {series: "h0 " + d1 + "=;h0 " + d2 + "=" + d1 + ";h1 " + d1 + "=;h1 " + d2 + "=" + d1, keep: "2", base: d2, chain: "1"},
		"a chain of keep":            {series: "h0 " + d1 + "=;h0 " + d2 + "=" + d1 + ";h0 " + d3 + "=" + d2 + ";h1 " + d1 + "=;h1 " + d2 + "=" + d1 + ";h1 " + d3 + "=" + d2, keep: "2", base: "", chain: "2"},
		"a newest backup one lacks":  {series: "h0 " + d1 + "=;h0 " + d2 + "=" + d1 + ";h0 " + d3 + "=" + d2 + ";h1 " + d1 + "=;h1 " + d2 + "=" + d1, keep: "2", base: d2, chain: "1"},
		"the longest chain of hosts": {series: "h0 " + d1 + "=;h0 " + d2 + "=;h1 " + d1 + "=;h1 " + d2 + "=" + d1, keep: "1", base: "", chain: "1"},
		"another host's backups":     {series: "h0 " + d1 + "=;h1 " + d1 + "=;h9 " + d2 + "=" + d1, keep: "1", base: d1, chain: "0"},
		"a host without the series":  {series: "h0 " + d1 + "=;h9 " + d1 + "=", keep: "1", base: "", chain: "0"},
		"a backup made at once, newest": {series: "h0 " + d1 + "=;h0 " + r1 + "=" + d1 + ";h1 " + d1 + "=;h1 " + r1 + "=" + d1,
			keep: "3", base: r1, chain: "1"},
		"a backup made at once, older than a scheduled one": {series: "h0 " + d1 + "=;h0 " + r1 + "=" + d1 + ";h0 " + d2 + "=" + r1 +
			";h1 " + d1 + "=;h1 " + r1 + "=" + d1 + ";h1 " + d2 + "=" + r1, keep: "3", base: d2, chain: "2"},
		"a backup made at once in a chain of keep": {series: "h0 " + d1 + "=;h0 " + r1 + "=" + d1 + ";h0 " + d2 + "=" + r1 +
			";h1 " + d1 + "=;h1 " + r1 + "=" + d1 + ";h1 " + d2 + "=" + r1, keep: "2", base: "", chain: "2"},
	} {
		t.Run(name, func(t *testing.T) {
			out := snippetOutput(t, map[string]string{"SERIES": tc.series, "KEEP": tc.keep, envBackupNamePrefix: "d"}, `
				declare -A SERIES_REQUIRED=()
				IFS=';' read -r -a entries <<<"${SERIES}"
				for entry in "${entries[@]}"; do
					SERIES_REQUIRED["${entry%%=*}"]=${entry#*=}
				done
				incremental_base "${KEEP}" h0 h1
				echo "${REPLY} ${CHAIN}"`, `
for entry in filter(None, env('SERIES').split(';')):
    key, required = entry.split('=', 1)
    S.series_required[tuple(key.split(' ', 1))] = required
print(*incremental_base(int(env('KEEP')), ['h0', 'h1']))`)
			require.Equal(t, tc.base+" "+tc.chain+"\n", out)
		})
	}
}

// Only a request whose connection was never established is safe to submit again: it cannot have reached the API.
func TestJobScriptRetriesWhatNeverConnected(t *testing.T) {
	for _, tc := range []struct {
		method, exit, connected, code string
		retry                         bool
	}{
		{"POST", "7", "", "000", true},
		{"POST", "6", "", "000", true},
		{"POST", "28", "", "000", true},
		{"POST", "28", "1", "000", false},
		{"POST", "52", "1", "000", false},
		{"POST", "0", "1", "503", false},
		{"GET", "28", "1", "000", true},
		{"GET", "0", "1", "502", true},
		{"GET", "0", "1", "500", false},
	} {
		out := snippetOutput(t, map[string]string{"METHOD": tc.method, "EXIT": tc.exit, "CONNECTED": tc.connected, "CODE": tc.code}, `
			CURL_EXIT=${EXIT} CURL_CONNECTED=${CONNECTED} HTTP_CODE=${CODE}
			if unreachable "${METHOD}"; then echo retry; else echo no; fi`, `
S.curl_exit, S.curl_connected, S.http_code = int(env('EXIT')), bool(env('CONNECTED')), env('CODE')
print('retry' if unreachable(env('METHOD')) else 'no')`)
		require.Equal(t, map[bool]string{true: "retry\n", false: "no\n"}[tc.retry], out, "%+v", tc)
	}
}

// json_escape escapes as encoding/json reads it back, whatever the message holds.
func TestJobScriptEscapesJSON(t *testing.T) {
	for _, value := range []string{
		"", "plain", `quote " and backslash \`, "new\nline\r\ttab", "bell\a esc\x1b del\x7f", "ünïcødé ✓", `A`, "{}[],:",
	} {
		out := sourcedOutput(t, map[string]string{"VALUE": value}, `json_escape "${VALUE}"; printf '"%s"' "${REPLY}"`)
		var decoded string
		require.NoError(t, json.Unmarshal([]byte(out), &decoded), "%q: %s", value, out)
		require.Equal(t, value, decoded)
	}
}

// eventTestEnv is the environment of the object the script posts Events on
func eventTestEnv() map[string]string {
	return map[string]string{
		envEventObjectAPIVersion: "clickhouse.altinity.com/v1",
		envEventObjectKind:       "ClickHouseBackup",
		envEventObjectName:       "b1",
		envEventObjectUID:        "0d9c7e4c-1b5e-4f4e-9d1e-6f2c1b9a7e11",
		envPodNamespace:          "ns",
		envPodName:               "b1-main-backup-x",
	}
}

func TestJobScriptBuildsEvents(t *testing.T) {
	message := "h0: create_remote \"bk-1\" failed:\n\tdisk full " + strings.Repeat("x", 2000)
	out := snippetOutput(t, withEnv(eventTestEnv(), map[string]string{"MESSAGE": message}),
		`event_body Warning OrchestratorHostFailed "${MESSAGE}" 2026-01-02T03:04:05Z; printf '%s' "${REPLY}"`,
		`print(event_body('Warning', 'OrchestratorHostFailed', env('MESSAGE'), '2026-01-02T03:04:05Z'), end='')`)
	var event map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &event), out)
	require.Equal(t, map[string]any{
		"apiVersion": "v1",
		"kind":       "Event",
		"metadata":   map[string]any{"generateName": "b1.", "namespace": "ns"},
		"involvedObject": map[string]any{
			"apiVersion": "clickhouse.altinity.com/v1", "kind": "ClickHouseBackup", "name": "b1", "namespace": "ns",
			"uid": "0d9c7e4c-1b5e-4f4e-9d1e-6f2c1b9a7e11",
		},
		"reason":             "OrchestratorHostFailed",
		"message":            message[:1024],
		"type":               "Warning",
		"source":             map[string]any{"component": "clickhouse-backup-orchestrator"},
		"firstTimestamp":     "2026-01-02T03:04:05Z",
		"lastTimestamp":      "2026-01-02T03:04:05Z",
		"count":              float64(1),
		"reportingComponent": "clickhouse-backup-orchestrator",
		"reportingInstance":  "b1-main-backup-x",
	}, event)
}

// scriptWith returns script with the setting name, a line of its own, set to value
func scriptWith(t *testing.T, script, name, value string) string {
	t.Helper()
	setting := regexp.MustCompile(`(?m)^` + name + `=.*$`)
	require.Len(t, setting.FindAllString(script, -1), 1, name)
	if testedOrchestrator == orchestratorPython {
		value = strconv.Quote(value)
	}
	return setting.ReplaceAllLiteralString(script, name+"="+value)
}

// kubeStub emulates the events endpoint of the Kubernetes API over TLS, and the service account of the pod
type kubeStub struct {
	mu     sync.Mutex
	events []map[string]any
	posts  int
	status int
	// script is orchestratorScript reading the service account at dir
	script string
	env    map[string]string
}

const kubeStubToken = "orchestrator-token"

func newKubeStub(t *testing.T, status int) *kubeStub {
	t.Helper()
	k := &kubeStub{status: status}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		k.mu.Lock()
		defer k.mu.Unlock()
		if (r.Method != http.MethodPost) || (r.URL.Path != "/api/v1/namespaces/ns/events") ||
			(r.Header.Get("Authorization") != "Bearer "+kubeStubToken) || (r.Header.Get("Content-Type") != "application/json") {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		k.posts++
		var event map[string]any
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Errorf("an event that is no JSON: %v", err)
		}
		if k.status < 300 {
			k.events = append(k.events, event)
		}
		w.WriteHeader(k.status)
	}))
	t.Cleanup(server.Close)
	dir := t.TempDir()
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ca.crt"), cert, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "token"), []byte(kubeStubToken), 0o600))
	host, port, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "https://"))
	require.NoError(t, err)
	k.script = scriptWith(t, orchestratorScript, "SERVICE_ACCOUNT_DIR", dir)
	k.env = withEnv(eventTestEnv(), map[string]string{"KUBERNETES_SERVICE_HOST": host, "KUBERNETES_SERVICE_PORT": port})
	return k
}

func (k *kubeStub) reasons() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	var reasons []string
	for _, event := range k.events {
		reasons = append(reasons, event["reason"].(string))
	}
	return reasons
}

// The run posts an Event on the object it works for at every step, with the token of its service account.
func TestJobScriptPostsEvents(t *testing.T) {
	k := newKubeStub(t, http.StatusCreated)
	s := newSidecarStub(t, map[string]*stubHost{"h0": {}, "h1": {}})
	code, out := s.runScript(k.script, withEnv(k.env, map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0 h1"}))
	require.Zero(t, code, out)
	require.Equal(t, []string{"OrchestratorBackupStarted", "OrchestratorCommand", "OrchestratorCommand",
		"OrchestratorCommandFinished", "OrchestratorHostStepSucceeded", "OrchestratorCommandFinished", "OrchestratorHostStepSucceeded",
		"OrchestratorBackupCompleted"}, k.reasons(), out)
	require.Equal(t, "Normal", k.events[0]["type"])
	require.Equal(t, "b1", k.events[0]["involvedObject"].(map[string]any)["name"])

	k = newKubeStub(t, http.StatusCreated)
	s = newSidecarStub(t, map[string]*stubHost{"h0": {failOp: map[string]string{"create_remote": "disk full"}}})
	code, out = s.runScript(k.script, withEnv(k.env, map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0"}))
	require.Equal(t, 1, code, out)
	require.Equal(t, []string{"OrchestratorBackupStarted", "OrchestratorCommand", "OrchestratorCommandFinished",
		"OrchestratorHostFailed", "OrchestratorBackupFailed"}, k.reasons(), out)
	require.Equal(t, "Warning", k.events[4]["type"])
	require.Contains(t, k.events[4]["message"], "disk full")

	k = newKubeStub(t, http.StatusCreated)
	s = newSidecarStub(t, map[string]*stubHost{"h0": {remote: []stubBackup{remote("daily-20200101000000", ""), remote("daily-20200102000000", "")}}})
	code, out = s.runScript(k.script, withEnv(k.env, scheduledEnv("h0", dailyKeeping(1), "2020-01-03T00:00:00Z")))
	require.Zero(t, code, out)
	require.Equal(t, 4, slices.Index(k.reasons(), "OrchestratorBackupCompleted"))
	require.Equal(t, []string{"OrchestratorCommand", "OrchestratorRetentionDeleted", "OrchestratorCommand", "OrchestratorRetentionDeleted"},
		k.reasons()[5:], out)
	require.Equal(t, []string{
		"h0: create_remote: POST h0:7171/backup/create_remote?name=daily-20200103000000",
		"h0: delete remote daily-20200102000000: POST h0:7171/backup/delete/remote/daily-20200102000000",
		"h0: delete remote daily-20200101000000: POST h0:7171/backup/delete/remote/daily-20200101000000",
	}, k.messages("OrchestratorCommand"), out)
}

func (k *kubeStub) messages(reason string) []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	var messages []string
	for _, event := range k.events {
		if event["reason"] == reason {
			messages = append(messages, event["message"].(string))
		}
	}
	return messages
}

// Every request that starts or changes something shows in an Event as it is sent: the method, the host and port, the
// path and the query string as curl encodes it, the command of the actions endpoint; and once the operation ends, the
// command the sidecar tells it ran. No Event shows the credentials of the API.
func TestJobScriptPostsTheCommandsItRuns(t *testing.T) {
	k := newKubeStub(t, http.StatusCreated)
	s := newSidecarStub(t, map[string]*stubHost{"h0": {remote: []stubBackup{remote("base", "")}}})
	code, out := s.runScript(k.script, withEnv(k.env, map[string]string{envOperation: operationBackup, envBackupName: "bk-1",
		envBackupHosts: "h0", envTables: "db.t*,x?y", envPartitions: "('a b'),2026", envDiffFromRemote: "base"}))
	require.Zero(t, code, out)
	// The query string is as curl encodes it, which tells the values apart however its version spells the escapes
	commands := k.messages("OrchestratorCommand")
	require.Len(t, commands, 1, out)
	sent, found := strings.CutPrefix(commands[0], "h0: create_remote: POST ")
	require.True(t, found, commands[0])
	request, err := url.Parse("http://" + sent)
	require.NoError(t, err)
	require.Equal(t, "h0:7171", request.Host)
	require.Equal(t, "/backup/create_remote", request.Path)
	require.Equal(t, "name=bk-1&table=db.t*,x?y&partitions=('a b'),2026&diff-from-remote=base", func() string {
		var pairs []string
		for _, pair := range strings.Split(request.RawQuery, "&") {
			key, value, _ := strings.Cut(pair, "=")
			value, err := url.QueryUnescape(value)
			require.NoError(t, err)
			pairs = append(pairs, key+"="+value)
		}
		return strings.Join(pairs, "&")
	}())
	require.Equal(t, []string{
		`h0: create_remote bk-1 (operation op-1) ended with status success: create_remote --tables="db.t*,x?y" bk-1`,
	}, k.messages("OrchestratorCommandFinished"), out)

	k = newKubeStub(t, http.StatusCreated)
	s = newSidecarStub(t, map[string]*stubHost{"s0": {remote: []stubBackup{remote("bk-1", "")}}, "d1": {remote: []stubBackup{remote("bk-1", "")}}})
	code, out = s.runScript(k.script, withEnv(k.env, restoreEnv(map[string]string{envTables: "db.*"})))
	require.Zero(t, code, out)
	require.NotEmpty(t, k.messages("OrchestratorCommand"), out)
	require.NotEmpty(t, k.messages("OrchestratorCommandFinished"), out)
	for _, event := range k.events {
		message := event["message"].(string)
		require.NotContains(t, message, scriptTestPassword)
		require.NotContains(t, message, "Authorization")
		require.NotContains(t, message, "Basic ")
	}

	// A command of the actions endpoint shows with its body
	s = newSidecarStub(t, map[string]*stubHost{"h0": {remote: []stubBackup{remote("bk-1", "")}}})
	out = snippetOutput(t, map[string]string{"http_proxy": s.server.URL, envAPIPort: scriptTestAPIPort,
		envAPIUsername: scriptTestUser, envAPIPassword: scriptTestPassword, envPollInterval: "0.01"}, `
		event() { printf 'EVENT %s %s\n' "$2" "$3"; }
		log() { :; }
		API_PORT=${API_PORT} POLL_INTERVAL=${POLL_INTERVAL}
		AUTH=$(printf '%s:%s' "${API_USERNAME}" "${API_PASSWORD}" | base64 | tr -d '\n')
		submit h0 download /backup/actions --data-binary '{"command":"download --schema bk-1"}'`, `
def event(type_, reason, message): print('EVENT', reason, message)
def log(message, stream=None): pass
S.auth = base64.b64encode((env('API_USERNAME') + ':' + env('API_PASSWORD')).encode()).decode()
submit('h0', 'download', '/backup/actions', body='{"command":"download --schema bk-1"}')`)
	require.Equal(t, `EVENT OrchestratorCommand h0: download: POST h0:7171/backup/actions {"command":"download --schema bk-1"}`+"\n", out)
}

// An Event message is cut to the bytes the Kubernetes API takes, never inside a character.
func TestJobScriptCutsEventMessages(t *testing.T) {
	for _, tc := range []struct {
		value string
		max   int
		want  string
	}{
		{"abc", 3, "abc"},
		{"abcd", 3, "abc"},
		{"aü", 2, "a"},
		{"aü", 3, "aü"},
		{"a✓", 3, "a"},
		{"ab✓", 3, "ab"},
		{"a😀", 4, "a"},
		{"a😀", 5, "a😀"},
		{"üü", 3, "ü"},
	} {
		out := snippetOutput(t, map[string]string{"VALUE": tc.value, "MAX": strconv.Itoa(tc.max), "LANG": "C.UTF-8"},
			`truncate_bytes "${VALUE}" "${MAX}"; printf '%s' "${REPLY}"`, "print(truncate_bytes(env('VALUE'), int(env('MAX'))), end='')")
		require.Equal(t, tc.want, out, "%q %d", tc.value, tc.max)
	}
	out := snippetOutput(t, withEnv(eventTestEnv(), map[string]string{"MESSAGE": strings.Repeat("ü", 600), "LANG": "C.UTF-8"}),
		`event_body Normal OrchestratorCommand "${MESSAGE}" 2026-01-02T03:04:05Z; printf '%s' "${REPLY}"`,
		`print(event_body('Normal', 'OrchestratorCommand', env('MESSAGE'), '2026-01-02T03:04:05Z'), end='')`)
	var event map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &event), out)
	require.Equal(t, strings.Repeat("ü", 512), event["message"])
}

// An Event that cannot be posted never fails the run, nor holds it: after a few failures in a row none is tried; and a
// run without the object or the token of an Event posts none.
func TestJobScriptRunsWithoutEvents(t *testing.T) {
	k := newKubeStub(t, http.StatusInternalServerError)
	s := newSidecarStub(t, map[string]*stubHost{"h0": {}, "h1": {}})
	code, out := s.runScript(k.script, withEnv(k.env, map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0 h1"}))
	require.Zero(t, code, out)
	require.Equal(t, 3, k.posts)
	require.Contains(t, out, "WARNING: cannot post the event OrchestratorBackupStarted: 500")
	require.Equal(t, []string{"bk-1"}, s.remoteNames("h1"))

	for name, override := range map[string]map[string]string{
		"no object":    {envEventObjectUID: ""},
		"no namespace": {envPodNamespace: ""},
	} {
		t.Run(name, func(t *testing.T) {
			k := newKubeStub(t, http.StatusCreated)
			s := newSidecarStub(t, map[string]*stubHost{"h0": {}})
			code, out := s.runScript(k.script, withEnv(withEnv(k.env, override), map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0"}))
			require.Zero(t, code, out)
			require.Zero(t, k.posts)
		})
	}
	k = newKubeStub(t, http.StatusCreated)
	s = newSidecarStub(t, map[string]*stubHost{"h0": {}})
	code, out = s.runScript(scriptWith(t, orchestratorScript, "SERVICE_ACCOUNT_DIR", t.TempDir()),
		withEnv(k.env, map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0"}))
	require.Zero(t, code, out)
	require.Zero(t, k.posts, "no token, no events")
}

// With the pod transport the host lists name the backup Pods of the shards, which start with the operation: the run
// waits until each answers, and fails, changing nothing, when one does not in time.
func TestJobScriptWaitsForTheBackupPods(t *testing.T) {
	s := newSidecarStub(t, map[string]*stubHost{"bk-0": {unavailable: 3}, "bk-1": {}})
	code, out := s.run(map[string]string{envOperation: operationBackup, envBackupName: "b", envBackupHosts: "bk-0 bk-1",
		envBackupTransport: backupTransportPod})
	require.Zero(t, code, out)
	require.Contains(t, out, "bk-0: waiting for the backup Pod to answer")
	require.Equal(t, []string{"b"}, s.remoteNames("bk-1"))

	for name, env := range map[string]map[string]string{
		"backup":  {envOperation: operationBackup, envBackupName: "b", envBackupHosts: "bk-0 bk-1"},
		"verify":  {envOperation: operationVerify, envBackupName: "b", envVerifyHosts: "bk-1"},
		"restore": restoreEnv(map[string]string{envSchemaHosts: "bk-0", envDataHosts: "bk-0 bk-1"}),
	} {
		t.Run(name, func(t *testing.T) {
			s := newSidecarStub(t, map[string]*stubHost{"bk-0": {}, "bk-1": {unavailable: -1}})
			code, out := s.run(withEnv(env, map[string]string{envBackupTransport: backupTransportPod, envBackupPodWaitSeconds: "0",
				envUnchangedExitCode: "3"}))
			require.Equal(t, 3, code, out)
			require.Contains(t, out, "ERROR: the backup Pod bk-1 does not answer after ")
			require.Empty(t, s.matching(http.MethodPost, "/"))
		})
	}
}

// orchestratorScriptMaxBytes bounds the orchestrator script. dev/generate_helm_chart.sh passes the data of the
// ConfigMap that ships it, the script indented as YAML, to yq in a single environment variable, and Linux refuses
// any single argument or environment string longer than MAX_ARG_STRLEN, 128 KiB: the chart then fails to build,
// on Linux alone. The bound leaves room for the indentation and a margin; macOS has no such limit, so this test
// catches it there.
const orchestratorScriptMaxBytes = 110000

func TestJobScriptFitsTheLinuxArgumentLimit(t *testing.T) {
	require.LessOrEqual(t, len(orchestratorScript), orchestratorScriptMaxBytes,
		"the orchestrator script must stay within %d bytes, as Linux passes the ConfigMap data that holds it in one "+
			"environment string of at most 128 KiB: condense it", orchestratorScriptMaxBytes)
	lines := strings.Count(orchestratorScript, "\n") + 1
	require.Less(t, len(orchestratorScript)+2*lines, 120000, "the script as the data of its ConfigMap, indented by YAML")
}
