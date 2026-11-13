# backup-orchestrator.py is the Python orchestrator of a backup Job, the twin of backup-orchestrator.sh: the same
# environment, the same REST calls in the same order, the same Events, termination message lines and exit codes. It
# drives the clickhouse-backup REST API of one cluster: through the sidecar of every pod (BACKUP_TRANSPORT=sidecar) or
# the backup Pod of every shard (pod), and backs up, verifies, restores and prunes. Every input arrives through the
# environment (see script.go); values reach the API as URL-encoded parameters, or commands of the actions endpoint as
# tokens validated against the *_RE regexes. Standard library only, Python 3.10 or newer. Imported, it runs nothing.
# See docs/backup.md for what it does and why, and backup-orchestrator.sh for the comments of each step.
import base64
import contextlib
import http.client
import json
import os
import re
import socket
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

LB = '{'  # a literal brace: the Helm chart renders this file as a template, which reads two as an action
NAME_RE = r'^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'
HOST_RE = r'^[A-Za-z0-9]([A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$'
NUMBER_RE = r'^[0-9]+$'
TABLES_RE = r'^[A-Za-z0-9_.*?,][A-Za-z0-9_.*?,-]*$'
TABLES_MAX_LENGTH = 1024
PARTITIONS_RE = r'^[A-Za-z0-9_-]+(,[A-Za-z0-9_-]+)*$'
ENGINES_RE = r'^[A-Za-z][A-Za-z0-9_]*(,[A-Za-z][A-Za-z0-9_]*)*$'
DATABASE_MAPPING_RE = r'^[A-Za-z0-9_][A-Za-z0-9_-]*:[A-Za-z0-9_][A-Za-z0-9_-]*(,[A-Za-z0-9_][A-Za-z0-9_-]*:[A-Za-z0-9_][A-Za-z0-9_-]*)*$'
FILTER_MAX_LENGTH = 16384
DEFAULT_SKIP_TABLES = 'system.*,INFORMATION_SCHEMA.*,information_schema.*,_temporary_and_external_tables.*'
INDEX_RE = r'^(0|[1-9][0-9]{0,3})$'
SOURCE_NAME_RE = r'^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'
SOURCE_INSTALLATION_MAX_LENGTH = 253
SOURCE_NAMESPACE_MAX_LENGTH = 63
SOURCE_PATH_RE = r'^[A-Za-z0-9_./{}-]+$'
TAG_RE = r'^[0-9]{14}$'
NAMING_RE = r'^([A-Za-z0-9._-]|\{[A-Za-z]+\})+$'
NAMING_MAX_LENGTH = 128
NAMING_TAG_RE = r'^[0-9]{8}T?[0-9]{6}$'
NAMING_SLOT_TOLERANCE = 1800
SLOT_RE = r'^[0-9]{1,10}$'
SCHEDULE_TIME_RE = r'^([01][0-9]|2[0-3]):([0-5][0-9])$'
KEEP_RE = r'^[1-9][0-9]{0,5}$'
ZONE_RE = r'^[A-Za-z0-9_+-]+(/[A-Za-z0-9_+-]+)*$'
INTERVAL_RE = r'^[0-9]+([.][0-9]+)?$'
EXIT_CODE_RE = r'^[1-9][0-9]?$'
IN_PROGRESS = '"status":"in progress"'
TABLE_ROW_RE = r'^\{"Database":"([^"\\]*)","Name":"([^"\\]*)"'
BACKUP_TABLE_ROW_RE = r'^\{"database":"([^"\\]*)","table":"([^"\\]*)"'
SYSTEM_DATABASES = {'system', 'INFORMATION_SCHEMA', 'information_schema', '_temporary_and_external_tables'}
LOCKED_ERROR = 'another operation is currently running'
SIZE_LIMIT_ADVICE = ('set max_table_size_to_drop to 0 wherever a limit is set (the smallest non-zero limit applies), '
					 'or drop these tables by hand, and restore again')
PATH_CHECK_KEYS = [
	'REMOTE_STORAGE',
	'S3_BUCKET', 'S3_ENDPOINT', 'S3_PATH', 'S3_OBJECT_DISK_PATH',
	'GCS_BUCKET', 'GCS_ENDPOINT', 'GCS_PATH', 'GCS_OBJECT_DISK_PATH',
	'AZBLOB_ACCOUNT_NAME', 'AZBLOB_CONTAINER', 'AZBLOB_ENDPOINT_SUFFIX', 'AZBLOB_PATH', 'AZBLOB_OBJECT_DISK_PATH',
	'COS_URL', 'COS_PATH', 'COS_OBJECT_DISK_PATH',
	'FTP_ADDRESS', 'FTP_PATH', 'FTP_OBJECT_DISK_PATH',
	'SFTP_ADDRESS', 'SFTP_PORT', 'SFTP_PATH', 'SFTP_OBJECT_DISK_PATH',
]
CONFIG_OVERRIDE_RE = r'^[A-Z][A-Z0-9_]*=[A-Za-z0-9_.,:/@+=-]+$'
CONFIG_OVERRIDE_KEY_MAX_LENGTH = 64
CONFIG_OVERRIDE_VALUE_MAX_LENGTH = 256
CONFIG_OVERRIDES_MAX_ITEMS = 16
CONFIG_OVERRIDE_FORBIDDEN = ' API_LISTEN API_SECURE API_ALLOW_PARALLEL RESTORE_SCHEMA_ON_CLUSTER CLICKHOUSE_RESTART_COMMAND RESTORE_DATABASE_MAPPING RESTORE_TABLE_MAPPING BACKUPS_TO_KEEP_REMOTE REBASE_DURING_DELETE REBASE_BEFORE_REMOVE_OLD_REMOTE API_USERNAME API_PASSWORD CLICKHOUSE_USERNAME CLICKHOUSE_PASSWORD CLICKHOUSE_BACKUP_CONFIG CLICKHOUSE_HOST CLICKHOUSE_PORT CLICKHOUSE_SECURE CLICKHOUSE_SKIP_VERIFY BACKUPS_TO_KEEP_LOCAL ALLOW_EMPTY_BACKUPS API_CREATE_INTEGRATION_TABLES '
CONFIG_OVERRIDE_SENSITIVE = ['PASSWORD', 'SECRET', 'ACCESS_KEY', 'SECRET_KEY', 'ACCOUNT_KEY', 'SSE_KEY', 'SSE_CUSTOMER_KEY',
							 'ENCRYPTION_KEY', 'TOKEN', 'CREDENTIAL', 'PRIVATE_KEY', 'SAS', 'CONNECTION_STRING']
HELD_REPORT_MAX = 24
# The timeouts, in seconds, each a line of its own, which tests set
REQUEST_TIMEOUT=600
DELETE_TIMEOUT=3600
UNREACHABLE_TIMEOUT=600
MUTATED_UNREACHABLE_TIMEOUT=3600
RESUME_TIMEOUT=300
DEFAULT_BACKUP_POD_WAIT_SECONDS = 600
SERVICE_ACCOUNT_DIR="/var/run/secrets/kubernetes.io/serviceaccount"
EVENT_TIMEOUT = 5
EVENT_MAX_FAILURES = 3
EVENT_SOURCE_COMPONENT = 'clickhouse-backup-orchestrator'
EVENT_MESSAGE_MAX_LENGTH = 1024
PERIOD_WEIGHT = {'minute': 1, 'hour': 2, '6hours': 3, 'daily': 4, 'weekly': 5, 'monthly': 6}
STORAGE_PREFIX = {'s3': 'S3', 'gcs': 'GCS', 'azblob': 'AZBLOB', 'cos': 'COS', 'ftp': 'FTP', 'sftp': 'SFTP'}
CNTRL = re.compile(r'[\x00-\x1f\x7f]')

def rx(pattern, value):
	"""Matches value against a regex of the bash script, its $ the end of the value, as bash takes it."""
	return re.search(pattern[:-1] + r'\Z' if pattern.endswith('$') else pattern, value)

class State:
	"""What the bash script keeps in global variables: empty strings, lists, dicts, flags and numbers to start with."""

	def __init__(self):
		self.env, self.start, self.lst = dict(os.environ), time.monotonic(), Listing()
		for names, value in (
				('slot_tag backup_key naming_prefix last_warning self_path self_object_disk_path self_storage_prefix '
				 'self_cluster_name self_paths_unknown source_env source_path_shown source_hint step_name action_env '
				 'create_flags create_env http_body request_url backup_tables_unreadable', ''),
				('sched matched naming_parts source_copies no_local_copy failed_hosts failures step_failed_hosts '
				 'preferred_hosts step_args data_downloads create_params shard_tables_differ live live_unreadable', list),
				('shard_of host_backup last_op last_action_start last_action_command own_backups op_id action_command '
				 'action_before db_source db_target base_of series_required backup_tables backup_dbs', dict),
				('events_disabled backup_name_given mutated read_only no_step best_effort source_own_path on_cluster '
				 'restore_rbac_with_schema', False),
				('event_failures slot_tolerance curl_exit', 0), ('winner naming_cache', None),
				('restore_kind create_kind', 'op')):
			for name in names.split():
				setattr(self, name, value() if callable(value) else value)
		self.http_code, self.curl_connected = '000', True

class Listing:
	"""A backup list of a host, as list_backups reads it: a dict of the fields of every row."""

	def __init__(self, rows=None):
		self.rows = rows or []

	def find(self, name):
		for row in self.rows:
			if row['name'] == name:
				return row
		return None

S = State()

def env(name, default=''):
	value = S.env.get(name)
	return default if value is None or value == '' else value

def seconds():
	return int(time.monotonic() - S.start)

def sleep():
	time.sleep(float(S.env['POLL_INTERVAL']))

@contextlib.contextmanager
def scoped(**values):
	"""Sets attributes of the state for a block, as the bash script sets a local variable its callees see."""
	saved = {name: getattr(S, name) for name in values}
	for name, value in values.items():
		setattr(S, name, value)
	try:
		yield
	finally:
		for name, value in saved.items():
			setattr(S, name, value)

def log(message, stream=None):
	print(datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ') + ' ' + message, file=stream or sys.stdout, flush=True)

def warn(message):
	S.last_warning = message
	log('WARNING: ' + message, sys.stderr)

def advise(message):
	log('WARNING: ' + message, sys.stderr)

def write_message(message, mode):
	path = env('TERMINATION_MESSAGE_PATH')
	if path:
		with contextlib.suppress(OSError):
			with open(path, mode, encoding='utf-8') as out:
				out.write(message + '\n')

def termination_message(message):
	write_message(message, 'w')

def report_message(message):
	write_message(message, 'a')

def die(message):
	log('ERROR: ' + message, sys.stderr)
	termination_message(message)
	event('Warning', 'Orchestrator' + operation_title() + 'Failed', message)
	if S.source_copies:
		drop_source_copies()
	code = env('UNCHANGED_EXIT_CODE')
	sys.exit(int(code) if not S.mutated and rx(EXIT_CODE_RE, code) else 1)

def fail(message):
	die(message + (': ' + S.last_warning if S.last_warning else ''))

def operation_title():
	return {'backup': 'Backup', 'verify': 'Verify', 'restore': 'Restore'}.get(env('OPERATION'), '')

def truncate_bytes(value, maximum):
	return value.encode('utf-8')[:maximum].decode('utf-8', 'ignore')

def event_body(type_, reason, message, at):
	name = env('EVENT_OBJECT_NAME')
	namespace = env('POD_NAMESPACE')
	component = EVENT_SOURCE_COMPONENT
	return json.dumps({
		'apiVersion': 'v1', 'kind': 'Event',
		'metadata': {'generateName': name[:200] + '.', 'namespace': namespace},
		'involvedObject': {'apiVersion': env('EVENT_OBJECT_API_VERSION'), 'kind': env('EVENT_OBJECT_KIND'), 'name': name,
						   'namespace': namespace, 'uid': env('EVENT_OBJECT_UID')},
		'reason': reason, 'message': truncate_bytes(message, EVENT_MESSAGE_MAX_LENGTH), 'type': type_,
		'source': {'component': component}, 'firstTimestamp': at, 'lastTimestamp': at, 'count': 1,
		'reportingComponent': component, 'reportingInstance': env('POD_NAME', env('HOSTNAME')),
	}, ensure_ascii=False, separators=(',', ':'))

def event(type_, reason, message):
	"""Posts a Kubernetes Event on the object EVENT_OBJECT_* names, with the token of the service account of the pod."""
	host = env('KUBERNETES_SERVICE_HOST')
	if S.events_disabled or not all(env(name) for name in ('EVENT_OBJECT_KIND', 'EVENT_OBJECT_NAME', 'EVENT_OBJECT_UID',
														   'EVENT_OBJECT_API_VERSION', 'POD_NAMESPACE')) \
			or not host or not os.access(SERVICE_ACCOUNT_DIR + '/token', os.R_OK):
		return
	if ':' in host:
		host = '[' + host + ']'
	at = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
	try:
		with open(SERVICE_ACCOUNT_DIR + '/token', encoding='utf-8') as source:
			token = source.read().strip('\n')
	except OSError:
		token = ''
	url = f"https://{host}:{env('KUBERNETES_SERVICE_PORT', '443')}/api/v1/namespaces/{env('POD_NAMESPACE')}/events"
	req = urllib.request.Request(url, data=event_body(type_, reason, message, at).encode('utf-8'), method='POST',
								 headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
	try:
		context = ssl.create_default_context(cafile=SERVICE_ACCOUNT_DIR + '/ca.crt')
		opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=context))
		with opener.open(req, timeout=EVENT_TIMEOUT) as response:
			code = str(response.status)
	except urllib.error.HTTPError as error:
		code = str(error.code)
	except (OSError, ValueError, http.client.HTTPException) as error:
		code = f'000curl: ({curl_error(error)[0]}) {error}'
	if code.startswith('2') and len(code) == 3:
		S.event_failures = 0
		return
	S.event_failures += 1
	log(f'WARNING: cannot post the event {reason}: {code}', sys.stderr)
	if S.event_failures >= EVENT_MAX_FAILURES:
		S.events_disabled = True
		log(f'WARNING: no more events are posted after {S.event_failures} failures in a row', sys.stderr)

def json_get(field, text):
	"""The raw, still escaped, value of the first string or number field of a JSON object, '' without one."""
	match = re.search(r'"' + re.escape(field) + r'":"(([^"\\]|\\.)*)"', text) or \
		re.search(r'"' + re.escape(field) + r'":([0-9]+)', text)
	return match.group(1) if match else ''

def is_true(name):
	value = env(name).lower()
	if value in ('true', '1'):
		return True
	if value in ('false', '0', ''):
		return False
	die(name + ' must be true or false')

def check_name(name):
	value = env(name)
	if not rx(NAME_RE, value) or '..' in value:
		die(name + ' is not a valid backup name')

def check_cluster():
	if not rx(NAME_RE, env('CLUSTER')):
		die('CLUSTER is not a valid cluster name')

def json_string(value):
	if '"' in value or '\\' in value or CNTRL.search(value):
		die('a command value holds a character that JSON escapes')
	return '"' + value + '"'

def parse_hosts(name, optional=False):
	hosts = env(name).split()
	if not hosts and not optional:
		die(name + ' is empty: no target hosts')
	for host in hosts:
		if not rx(HOST_RE, host):
			die(name + ' contains an invalid host name')
	return hosts

def curl_error(error):
	"""The exit code curl fails with for the error of a request, and whether the connection was established."""
	reason = getattr(error, 'reason', error)
	if isinstance(reason, socket.gaierror):
		return 6, False
	if isinstance(reason, ConnectionRefusedError):
		return 7, False
	if isinstance(error, urllib.error.URLError) and isinstance(reason, (socket.timeout, TimeoutError)):
		return 28, False
	if isinstance(reason, (socket.timeout, TimeoutError)):
		return 28, True
	if isinstance(reason, http.client.RemoteDisconnected):
		return 52, True
	if isinstance(reason, ssl.SSLError):
		return 35, True
	return 56, True

OPENER = urllib.request.build_opener()

def request(method, host, path, params=None, body=None, timeout=None):
	"""Performs one API call: HTTP_CODE, CURL_EXIT and HTTP_BODY tell how it went, as curl does in the bash script."""
	S.http_code, S.curl_exit, S.curl_connected = '000', 0, True
	url = f"{host}:{S.env['API_PORT']}{path}"
	if params:
		url += '?' + '&'.join(key + '=' + urllib.parse.quote(value, safe='') for key, value in params)
	S.request_url = url
	headers = {'Authorization': 'Basic ' + S.auth}
	data = None
	if body is not None:
		data = body.encode('utf-8')
		headers['Content-Type'] = 'application/x-www-form-urlencoded'
	req = urllib.request.Request('http://' + url, data=data, method=method, headers=headers)
	try:
		with OPENER.open(req, timeout=timeout or REQUEST_TIMEOUT) as response:
			S.http_code, S.http_body = f'{response.status:03d}', response.read().decode('utf-8', 'replace')
	except urllib.error.HTTPError as error:
		S.http_code = f'{error.code:03d}'
		try:
			S.http_body = error.read().decode('utf-8', 'replace')
		except (OSError, http.client.HTTPException):
			S.http_body = ''
	except (OSError, ValueError, http.client.HTTPException) as error:
		S.curl_exit, S.curl_connected = curl_error(error)
		S.http_code, S.http_body = '000', f'curl: ({S.curl_exit}) {error}'

def unreachable(method):
	if S.curl_exit == 0:
		return method == 'GET' and proxy_failed()
	if S.curl_exit in (6, 7):
		return True
	return method == 'GET' or not S.curl_connected

def proxy_failed():
	return S.http_code in ('502', '503', '504')

def call(method, host, path, params=None, body=None, timeout=None):
	"""Performs an API call, retrying while the sidecar is unreachable."""
	last, since = '', seconds()
	while True:
		request(method, host, path, params, body, timeout)
		if S.best_effort or not unreachable(method):
			return
		if not S.mutated and seconds() - since >= UNREACHABLE_TIMEOUT:
			die(f'{host}: sidecar unreachable for {seconds() - since}s (curl exit {S.curl_exit}, HTTP {S.http_code}), '
			    'nothing was changed')
		if S.mutated and seconds() - since >= MUTATED_UNREACHABLE_TIMEOUT:
			die(f"{host}: sidecar unreachable for {seconds() - since}s (curl exit {S.curl_exit}, HTTP {S.http_code}), "
			    f"the {env('OPERATION')} is partially applied")
		if last != f'{S.curl_exit}/{S.http_code}':
			last = f'{S.curl_exit}/{S.http_code}'
			log(f'{host}: sidecar unreachable (curl exit {S.curl_exit}, HTTP {S.http_code}), retrying')
		sleep()

def error_of():
	return json_get('error', S.http_body) or S.http_body

def wait_idle(host):
	announced = False
	while True:
		call('GET', host, '/backup/actions')
		if S.http_code != '200':
			warn(f'{host}: cannot read sidecar actions (HTTP {S.http_code}): {error_of()}')
			return False
		if IN_PROGRESS not in S.http_body:
			return True
		if not announced:
			announced = True
			log(host + ': another operation is in progress, waiting')
		sleep()

def changed():
	if not S.read_only:
		S.mutated = True

def command_event(host, what, note, body):
	message = f"{host}: {what}: POST {S.request_url}{(' ' + body if body is not None else '')}{(' (' + note + ')' if note else '')}"
	log(message)
	event('Normal', 'OrchestratorCommand', message)

def finished_event(host, what, op, status):
	command = json_get('command', S.http_body).replace('\\"', '"').replace('\\\\', '\\')
	event('Normal', 'OrchestratorCommandFinished', f"{host}: {what} (operation {op}) ended with status {status or 'unknown'}{(': ' + command if command else '')}")

def submit(host, what, path, params=None, body=None, timeout=None):
	"""POSTs a request once the sidecar is idle, queueing while it is busy."""
	while True:
		if not wait_idle(host):
			return False
		call('POST', host, path, params, body, timeout)
		if S.curl_exit != 0:
			if not unreachable('POST'):
				changed()
				command_event(host, what, f'sent, no answer: curl exit {S.curl_exit}', body)
			warn(f'{host}: {what} request failed: {S.http_body}')
			return False
		if proxy_failed():
			changed()
			command_event(host, what, 'sent, a proxy answered HTTP ' + S.http_code, body)
			warn(f'{host}: {what} failed (HTTP {S.http_code}): {error_of()}')
			return False
		if S.http_code.startswith('2'):
			changed()
			command_event(host, what, '', body)
			return True
		if S.http_code in ('409', '423'):
			log(f'{host}: sidecar busy, queueing {what}')
		elif error_of() != LOCKED_ERROR:
			warn(f'{host}: {what} rejected (HTTP {S.http_code}): {error_of()}')
			return False
		else:
			log(f'{host}: sidecar busy, queueing {what}')
		sleep()

def remember_step(host, op='', start='', command=''):
	S.last_op[host], S.last_action_start[host], S.last_action_command[host] = op, start, command

def sidecar_kept(host):
	"""Whether the sidecar of HOST still knows the last step this Job ran on it."""
	if S.last_op.get(host):
		call('GET', host, '/backup/status', [('operationid', S.last_op[host])])
		if S.http_code != '200':
			warn(f'{host}: cannot read the status of the last step (HTTP {S.http_code}): {error_of()}')
			return False
		if json_get('status', S.http_body) == 'success':
			return True
	elif S.last_action_command.get(host):
		rows = action_rows(host, S.last_action_command[host])
		if rows is None:
			return False
		if any(start == S.last_action_start[host] and status == 'success' for start, status, _ in rows):
			return True
	else:
		return True
	if not S.mutated:
		warn(f"{host}: the sidecar restarted after the last step of this {env('OPERATION')} on it, which it "
		     "no longer knows; nothing was changed")
		return False
	warn(f"{host}: the sidecar restarted after the last step of this {env('OPERATION')} on it, which it no longer "
	     f"knows: the {env('OPERATION')} is partially applied")
	return False

def action_rows(host, command):
	"""The (start, status, error) of the rows of the actions log of HOST that ran COMMAND, oldest first, or None."""
	call('GET', host, '/backup/actions')
	if S.http_code != '200':
		warn(f'{host}: cannot read sidecar actions (HTTP {S.http_code}): {error_of()}')
		return None
	return [(json_get('start', line), json_get('status', line), json_get('error', line))
			for line in S.http_body.split('\n') if json_get('command', line) == command]

def run_action(host, what, name, command):
	if run_steps('action', host, what, name, command):
		return True
	if not S.no_step:
		event('Warning', 'OrchestratorHostFailed', f"{host}: {what} {name} failed{(': ' + S.last_warning if S.last_warning else '')}")
	return False

def run_steps(kind, host, what, name, *args):
	if not START[kind](host, what, name, *args):
		return False
	while True:
		sleep()
		rc = POLL[kind](host, what, name)
		if rc != 2:
			return rc == 0

def action_start(host, what, name, command):
	log(f'{host}: {what} {name}')
	if not sidecar_kept(host):
		return False
	body = '{"command":' + json_string(command) + '}'
	rows = action_rows(host, command)
	if rows is None:
		return False
	S.action_command[host] = command
	S.action_before[host] = [start for start, _, _ in rows]
	if not submit(host, what, '/backup/actions', body=body):
		return False
	if json_get('status', S.http_body) != 'acknowledged':
		warn(f'{host}: {what} was not acknowledged: {S.http_body}')
		return False
	return True

def action_poll(host, what, name):
	"""Reads once the row of the action action_start submitted: 0 once it succeeded, 2 while it runs, 1 if it failed."""
	command = S.action_command[host]
	rows = action_rows(host, command)
	if rows is None:
		return 1
	start = status = error = ''
	for row in reversed(rows):
		if row[1] == 'in progress' or row[0] not in S.action_before[host]:
			start, status, error = row
			break
	if status == 'in progress':
		return 2
	if status == 'success':
		log(f'{host}: {what} {name} succeeded')
		if not S.no_step:
			remember_step(host, '', start, command)
			event('Normal', 'OrchestratorHostStepSucceeded', f'{host}: {what} {name} succeeded')
		return 0
	if status == '':
		if S.no_step:
			warn(f'{host}: the sidecar restarted during {what} {name}')
			return 1
		if what == 'create_remote':
			if not lost_op(host, what, name):
				return 1
			remember_step(host)
			message = ((f'{host}: the sidecar restarted during the upload of {name}, which it resumed without '
			            f'the config overrides{S.create_env}: the parts uploaded after the restart are stored '
			            'with the settings of the sidecar'))
			advise(message)
			event('Warning', 'OrchestratorOverridesNotResumed', message)
			return 0
		warn(f"{host}: the sidecar restarted during {what} {name}, which it does not resume: the {env('OPERATION')} "
		     "is partially applied")
		return 1
	warn(f'{host}: {what} {name} ended with status \'{status}\': {error}')
	return 1

def run_op(host, what, name, path, params=None):
	if run_steps('op', host, what, name, path, params):
		return True
	event('Warning', 'OrchestratorHostFailed', f"{host}: {what} {name} failed{(': ' + S.last_warning if S.last_warning else '')}")
	return False

def op_start(host, what, name, path, params=None):
	log(f'{host}: {what} {name}')
	if not sidecar_kept(host):
		return False
	if not submit(host, what, path, params):
		return False
	if json_get('backup_name', S.http_body) != name:
		warn(f'{host}: {what} acknowledged a different backup: {S.http_body}')
		return False
	op = json_get('operation_id', S.http_body)
	if not op:
		warn(f'{host}: {what} returned no operation id: {S.http_body}')
		return False
	S.op_id[host] = op
	return True

def op_poll(host, what, name):
	op = S.op_id[host]
	call('GET', host, '/backup/status', [('operationid', op)])
	if S.http_code != '200':
		warn(f'{host}: cannot read status of {what} (HTTP {S.http_code}): {error_of()}')
		return 1
	status = json_get('status', S.http_body)
	if status and status != 'in progress':
		finished_event(host, f'{what} {name}', op, status)
	if status == 'in progress':
		return 2
	if status == 'success':
		log(f'{host}: {what} {name} succeeded')
		remember_step(host, op)
		event('Normal', 'OrchestratorHostStepSucceeded', f'{host}: {what} {name} succeeded')
		return 0
	if status == '':
		if not lost_op(host, what, name):
			return 1
		remember_step(host)
		return 0
	warn(f'{host}: {what} {name} ended with status \'{status}\': {error_of()}')
	return 1

START = {'op': op_start, 'action': action_start}
POLL = {'op': op_poll, 'action': action_poll}

def run_all(kind, hosts, check, what, name, *args):
	"""Runs a step on every host at once: submitted to one host after the other, each once CHECK passed for it - CHECK
	may set the arguments and the name of the step there -, then polled on every host until it runs on none."""
	S.failed_hosts, S.failures, S.step_failed_hosts = [], [], []
	pending, names = [], {}
	for host in hosts:
		S.last_warning = ''
		S.step_args, S.step_name = list(args), name
		if check and not check(host):
			step_failed(kind, host, what, S.step_name)
			break
		names[host] = S.step_name
		if not START[kind](host, what, S.step_name, *S.step_args):
			step_failed(kind, host, what, S.step_name)
			S.step_failed_hosts.append(host)
			break
		pending.append(host)
	while pending:
		sleep()
		running = []
		for host in pending:
			S.last_warning = ''
			rc = POLL[kind](host, what, names[host])
			if rc == 2:
				running.append(host)
			elif rc != 0:
				step_failed(kind, host, what, names[host])
				S.step_failed_hosts.append(host)
		pending = running
	return not S.failed_hosts

def step_failed(kind, host, what, name):
	S.failed_hosts.append(host)
	S.failures.append(S.last_warning or f'{host}: {what} {name} failed')
	if kind == 'op' or not S.no_step:
		event('Warning', 'OrchestratorHostFailed', f"{host}: {what} {name} failed{(': ' + S.last_warning if S.last_warning else '')}")

def fail_all(message):
	die(f"{message} failed on {', '.join(S.failed_hosts)}: {'; '.join(S.failures)}")

def resumed_action(host, command, name):
	"""The status and the error of the last COMMAND of NAME the sidecar resumed after a restart, or None."""
	call('GET', host, '/backup/actions')
	if S.http_code != '200':
		warn(f'{host}: cannot read sidecar actions (HTTP {S.http_code}): {error_of()}')
		return None
	status = error = ''
	for line in S.http_body.split('\n'):
		ran = json_get('command', line)
		if ran.startswith(command + ' ') and ran.endswith('--resumable=1 ' + name) and \
				len(ran) >= len(command) + 1 + len('--resumable=1 ' + name):
			status, error = json_get('status', line), json_get('error', line)
	return status, error

def lost_op(host, what, name):
	"""Settles an operation a restarted sidecar no longer knows: an upload or download it resumes is waited for."""
	if what == 'create_remote':
		resumed, where, outcome = 'upload', 'remote', f'the remote backup {name} may still complete'
	elif what == 'download':
		resumed, where, outcome = 'download', 'local', f'the download of {name} may still complete'
	else:
		warn(f"{host}: the sidecar restarted during {what} {name}, which it does not resume: the {env('OPERATION')} "
		     "is partially applied")
		return False
	since = seconds()
	log(f'{host}: the sidecar restarted during {what} {name}, waiting for it to resume the {resumed}')
	while True:
		result = resumed_action(host, resumed, name)
		if result is None:
			return False
		status, error = result
		if status == 'in progress':
			since = seconds()
		elif status == 'success':
			log(f'{host}: the resumed {resumed} of {name} succeeded')
			return True
		elif status == '':
			if seconds() - since >= RESUME_TIMEOUT:
				if has_usable_backup(host, where, name):
					log(f'{host}: {what} {name} completed before the sidecar restarted')
					return True
				warn(f'{host}: the sidecar restarted during {what} {name} and resumed nothing within {RESUME_TIMEOUT}s, '
				     f'the outcome is unknown: {outcome}')
				return False
		else:
			warn(f'{host}: the resumed {resumed} of {name} ended with status \'{status}\': {error}')
			return False
		sleep()

def list_backups(host, where):
	"""The backup list of HOST, local or remote, kept as the last list, or None."""
	S.lst = Listing()
	call('GET', host, '/backup/list/' + where)
	if S.http_code != '200':
		warn(f'{host}: cannot list {where} backups (HTTP {S.http_code}): {error_of()}')
		return None
	S.lst = Listing([{field: json_get(field, line) for field in
					  ('name', 'required', 'desc', 'data_size', 'object_disk_size', 'metadata_size', 'rbac_size')}
					 for line in S.http_body.split('\n') if line])
	return S.lst

def find_backup(name):
	return S.lst.find(name)

def has_usable_backup(host, where, name):
	if list_backups(host, where) is None:
		return False
	row = find_backup(name)
	return row is not None and not row['desc'].startswith('broken')

def delete_local(host, name):
	if not submit(host, 'delete local ' + name, '/backup/delete/local/' + name, [('force', 'true')]):
		return False
	log(f'{host}: local copy of {name} deleted')
	return True

def holds_data(row):
	return row['metadata_size'] == '' or (row['data_size'] or '0') != '0' or (row['object_disk_size'] or '0') != '0'

def list_all(key_of, hosts):
	"""Lists the backups of every host and classifies their keys: COMPLETE on every host, WITH_DATA, EMPTY, SIZED,
	BROKEN and ALL, and UNLISTED, the hosts that cannot be listed."""
	sets = {name: set() for name in ('complete', 'with_data', 'empty', 'sized', 'broken', 'all')}
	unlisted, first = [], True
	for host in hosts:
		if not rx(HOST_RE, host):
			continue
		if list_backups(host, 'remote') is None:
			unlisted.append(host)
			continue
		listed = set()
		for row in S.lst.rows:
			key = key_of(host, row['name']) if row['name'] else None
			if key is None:
				continue
			sets['all'].add(key)
			sizes = [row['data_size'] or '0', row['object_disk_size'] or '0', row['metadata_size'], row['rbac_size'] or '0']
			if row['metadata_size'] and any(size != '0' for size in sizes):
				sets['sized'].add(key)
			if row['desc'].startswith('broken'):
				sets['broken'].add(key)
				continue
			listed.add(key)
			if holds_data(row):
				sets['with_data'].add(key)
			if row['metadata_size'] and all(size == '0' for size in sizes):
				sets['empty'].add(key)
		sets['complete'] = listed if first else listed & sets['complete']
		first = False
	return sets, unlisted

def retention_pattern():
	return '^' + env('BACKUP_NAME_PREFIX').replace('.', '[.]') + '-[0-9]{8}T?[0-9]{6}$'

def retention_plan(names, sets):
	"""Which backups retention deletes, newest first, holds and keeps, as the cleanup of the Altinity Cloud Manager
	decides."""
	made = S.backup_key or env('BACKUP_NAME')
	ordered, incomplete, obsolete, held = [], [], [], []
	for name in names:
		key = key_time(name)[0]
		if not tag_valid(key):
			continue
		if name not in sets['complete']:
			if (name in sets['sized'] or name in sets['broken']) and name != made:
				incomplete.append((key, name))
		elif name not in sets['empty']:
			ordered.append((key, name))
	ordered.sort(reverse=True)
	newest = ordered[0][0] if ordered else ''
	keep = rules_keep(S.sched, ordered)
	keep |= {name for _, name in ordered if name == made}
	keep |= pinned_keys()
	held_keys_ = held_keys(ordered)
	for key, name in ordered:
		if name in keep:
			continue
		if name in held_keys_:
			held.append(name)
		else:
			obsolete.append((key, name))
	obsolete += [entry for entry in incomplete if newest and entry[0] < newest]
	return [name for _, name in sorted(obsolete, reverse=True)], held, keep

def rules_keep(sched, ordered):
	"""The keys of the backups, newest first, the schedule entries keep: the newest, the newest keep of the slots of
	every entry, and the backups made at once no older than the oldest of those."""
	keep, manual, counts, oldest = set(), [], [0] * len(sched), ''
	for index, (key, name) in enumerate(ordered):
		if index == 0:
			keep.add(name)
		if key_time(name)[1]:
			manual.append((key, name))
			continue
		for i, entry in enumerate(sched):
			if slot_matches(entry, key):
				counts[i] += 1
				if counts[i] <= entry['keep']:
					keep.add(name)
					oldest = key
	keep |= {name for key, name in manual if not oldest or not key < oldest}
	return keep

def pinned_keys():
	value = env('KEEP_BACKUPS')
	if CNTRL.search(value):
		die('KEEP_BACKUPS must be one line')
	prefix = env('BACKUP_NAME_PREFIX') if naming_holds('prefix') else ''
	pinned = set()
	for item in value.split():
		if not S.naming_parts or rx(NAMING_TAG_RE, item):
			pinned.add(item)
		else:
			parsed = naming_parse(item, prefix)
			if parsed:
				pinned.add(parsed[2])
	return pinned

def held_keys(ordered):
	"""The keys of the backups retention holds for the changes of the schedule PREVIOUS_SCHEDULES tells."""
	value, held = env('PREVIOUS_SCHEDULES'), set()
	if not value:
		return held
	if CNTRL.search(value):
		die('PREVIOUS_SCHEDULES must be one line')
	for record in value.split('#'):
		match = rx(r'^([0-9]{1,12})\|([^|]*)\|([^|]+)$', record)
		if not match:
			die('PREVIOUS_SCHEDULES must list <epoch>|<time zone>|<entries> records separated by #')
		check_timezone(match.group(2))
		change = slot_tag(int(match.group(1)), match.group(2))
		subset = [entry for entry in ordered if entry[0] < change]
		if match.group(3) == '*':
			held |= {name for _, name in subset}
			continue
		held |= rules_keep(parse_schedules(match.group(3)), subset)
	return held

def retention_on_host(host, obsolete):
	if list_backups(host, 'remote') is None:
		return False
	held = {row['name'] for row in S.lst.rows if row['name']}
	for key in obsolete:
		name = host_name(host, key)
		if name not in held:
			continue
		if submit(host, 'delete remote ' + name, '/backup/delete/remote/' + name, timeout=DELETE_TIMEOUT):
			log(f'{host}: retention deleted {name}')
			event('Normal', 'OrchestratorRetentionDeleted', f'{host}: retention deleted {name}')
		else:
			event('Warning', 'OrchestratorRetentionFailed', S.last_warning)
	return True

def retention():
	"""Prunes the backups of the schedule after a successful backup, on every host of RETENTION_HOSTS."""
	if not S.sched:
		return
	if is_true('RESTORE_IN_PROGRESS'):
		warn('retention skipped: a restore of the cluster is in progress')
		return
	if not env('BACKUP_NAME_PREFIX'):
		warn('retention skipped: BACKUP_NAME_PREFIX is not set')
		return
	hosts = env('RETENTION_HOSTS').split()
	if not hosts:
		warn('retention skipped: RETENTION_HOSTS is empty')
		return
	log(f"retention: keeping the newest {env('BACKUP_NAME_PREFIX')}-* backup, the newest backups of the slots "
	    "of every schedule entry it keeps, and the backups made at once no older than the oldest of those")
	S.best_effort = True
	sets, unlisted = list_all(series_key, hosts)
	if unlisted:
		warn(f"retention skipped: {' '.join(unlisted)} cannot be listed, backups are left as they are")
		event('Warning', 'OrchestratorRetentionSkipped', S.last_warning)
		return
	obsolete, held, _ = retention_plan(sorted(sets['all']), sets)
	report_held(held)
	for host in hosts:
		if not rx(HOST_RE, host):
			warn('retention skipped an invalid host name')
		elif not retention_on_host(host, obsolete):
			warn(host + ': retention failed, backups are left as they are')

def report_held(held):
	if not held:
		return
	names = [naming_render(1, key, S.naming_prefix)[0] if S.naming_parts else key for key in held]
	warn(f"retention holds {len(names)} backup(s) the schedule kept before a change of it, which it would delete "
	     f"now: {' '.join(names[:HELD_REPORT_MAX])}{(' ...' if len(names) > HELD_REPORT_MAX else '')}; keep "
	     "those to keep in spec.retention.keepBackups and release the change with spec.retention.releaseHeld")
	event('Warning', 'OrchestratorRetentionHeld', S.last_warning)
	if env('HELD_BACKUPS_MESSAGE'):
		more = f' +{len(names) - HELD_REPORT_MAX}' if len(names) > HELD_REPORT_MAX else ''
		report_message(f"{env('HELD_BACKUPS_MESSAGE')} {' '.join(names[:HELD_REPORT_MAX])}{more}")

def report_data(hosts):
	"""Tells when the backup just made holds no data, in a warning, an Event and the termination message."""
	with scoped(best_effort=True):
		sets, unlisted = list_all(lambda host, name: S.backup_key if name == S.host_backup.get(host, '') else None, hosts)
	if unlisted:
		log(f"whether {env('BACKUP_NAME')} holds data is unknown: {' '.join(unlisted)} cannot be listed")
		return
	if S.backup_key not in sets['complete'] or S.backup_key in sets['with_data']:
		return
	warn(env('BACKUP_NAME') + ' holds no data')
	event('Warning', 'OrchestratorNoData', env('BACKUP_NAME') + ' holds no data')
	if env('NO_DATA_MESSAGE'):
		report_message(env('NO_DATA_MESSAGE'))

def path_setting(index, name):
	"""The path check setting NAME of the cluster at INDEX, and whether the Job environment sets it."""
	value = S.env.get(f'PATH_{index}_{name}')
	return value or '', value is not None

def identity_default(key):
	return {'AZBLOB_ENDPOINT_SUFFIX': 'core.windows.net', 'SFTP_PORT': '22'}.get(key, '')

def trim_path(storage, value):
	both, trailing = ' \t\r\n/', ''
	if storage in ('ftp', 'sftp'):
		both, trailing = ' \t\r\n', '/'
	value = value.strip(both)
	return value.rstrip(trailing) if trailing else value

def clean_path(path):
	rooted = '/' if path.startswith('/') else ''
	elements = []
	for element in path.split('/'):
		if element in ('', '.'):
			continue
		if element == '..':
			if elements and elements[-1] != '..':
				elements.pop()
			elif not rooted:
				elements.append('..')
		else:
			elements.append(element)
	return rooted + '/'.join(elements)

def expand_macros(template, cluster, shard):
	"""TEMPLATE with the macros the operator gives every host replaced, {replica} kept; the name of an unknown macro
	is raised as KeyError."""
	def replace(match):
		name = match.group(1)
		values = {'installation': env('PATH_INSTALLATION'), 'namespace': env('PATH_NAMESPACE'), 'cluster': cluster,
				  'shard': shard, 'replica': match.group(0)}
		if name not in values:
			raise KeyError(match.group(0))
		return values[name]
	return re.sub(r'\{([^{}]*)\}', replace, template)

def default_path(index, name):
	own = S.env.get(f'PATH_{index}_{name}')
	return own if own is not None else env(name)

def path_check_inconclusive(count, name, what):
	S.self_paths_unknown = what
	if count > 1:
		die(f'the storage path check cannot tell whether cluster {name} shares a storage path with another of '
		    f'the {count} backup-enabled clusters of the installation, which would mix their backups and restore '
		    f'one with the data of the other: {what}')
	advise('the storage path check is skipped: ' + what)

def check_storage_paths():
	"""Refuses, before anything changes, a storage path the target cluster shares."""
	this, selection, operation = env('PATH_SELF'), env('PATH_REPLICA_SELECTION'), env('OPERATION')
	indexes = env('PATH_CLUSTERS').split()
	if not indexes:
		S.self_paths_unknown = 'PATH_CLUSTERS names no cluster'
		advise('the storage path check is skipped: ' + S.self_paths_unknown)
		return
	for index in indexes:
		if not rx(INDEX_RE, index):
			die('PATH_CLUSTERS must list cluster indexes')
	if this not in indexes:
		die('PATH_SELF must be one of the indexes PATH_CLUSTERS lists')
	if selection not in ('', 'FirstPerShard', 'AllReplicas'):
		die('PATH_REPLICA_SELECTION must be FirstPerShard or AllReplicas')
	owners, self_owned, self_name, shared = {}, set(), '', False
	for index in [this] + [index for index in indexes if index != this]:
		name = path_setting(index, 'CLUSTER')[0]
		if index == this:
			self_name = name
		what = CNTRL.sub(' ', path_setting(index, 'UNVERIFIABLE')[0])
		if what:
			what = f'the operator cannot tell the sidecar settings of cluster {name}: {what}'
			if index == this:
				return path_check_inconclusive(len(indexes), name, what)
			advise(f'cluster {name} is left out of the storage path check: {what}')
			continue
		storage = path_setting(index, 'REMOTE_STORAGE')[0]
		prefix = STORAGE_PREFIX.get(storage, '')
		if not prefix:
			what = 'its configuration does not set remote_storage' if not storage else \
				f'it keeps its backups in {storage} storage, where the check cannot tell their paths'
			if index == this:
				return path_check_inconclusive(len(indexes), name, what)
			advise(f'cluster {name} is left out of the storage path check: {what}')
			continue
		identity = [storage]
		for key in PATH_CHECK_KEYS:
			if key.startswith(prefix + '_') and key not in (prefix + '_PATH', prefix + '_OBJECT_DISK_PATH'):
				value, set_ = path_setting(index, key)
				identity.append(value if set_ else identity_default(key))
		value, set_ = path_setting(index, prefix + '_PATH')
		template = clean_path(trim_path(storage, value if set_ else default_path(index, 'DEFAULT_PATH')))
		value, set_ = path_setting(index, prefix + '_OBJECT_DISK_PATH')
		obj = trim_path(storage, value if set_ else default_path(index, 'DEFAULT_OBJECT_DISK_PATH'))
		if obj:
			obj = clean_path(obj) or '.'
		if index == this:
			S.self_path = template
			if operation == 'backup' and selection == 'AllReplicas' and '{replica}' not in template:
				die(f'the storage path \'{template}\' of cluster {name} lacks {LB}replica}}: a backup of every '
				    'replica would keep those of all replicas of a shard at one path')
			if operation == 'backup' and selection != 'AllReplicas' and '{replica}' in template:
				die(f'the storage path \'{template}\' of cluster {name} holds {LB}replica}}: a backup of the first '
				    'replica of each shard would not be at the path every other replica restores its schema '
				    'from')
			if operation == 'restore' and '{replica}' in template and not is_true('VALIDATE_TOPOLOGY'):
				advise(f'the storage path \'{template}\' of cluster {name} holds {LB}replica}}: every replica '
				       'restores its schema from its own path, which holds the backup only if it is one of '
				       'every replica')
		unknown = None
		for value in (template, obj):
			try:
				expand_macros(value, name, '')
			except KeyError as macro:
				unknown = (value, macro.args[0])
				break
		if unknown:
			what = (f'the path \'{unknown[0]}\' of cluster {name} holds {unknown[1]}, a macro whose value it '
			        'does not know')
			if index == this:
				return path_check_inconclusive(len(indexes), name, what)
			advise(f'cluster {name} is left out of the storage path check: its path \'{unknown[0]}\' holds {unknown[1]}, '
			       'a macro whose value it does not know')
			continue
		if index == this:
			S.self_object_disk_path, S.self_storage_prefix, S.self_cluster_name = obj, prefix, name
		for shard in path_setting(index, 'SHARDS')[0].split():
			for what, value in (('storage path', template), ('object disk path', obj)):
				if '{replica}' in value or (what != 'storage path' and not value):
					continue
				path = clean_path(expand_macros(value, name, shard))
				key = (tuple(identity), what, path)
				if key in owners:
					if index == this and S.naming_parts:
						shared = True
						continue
					if key in self_owned:
						die(f"the {what} '{path or '.'}' of shard {shard} of cluster {name} is that of {owners[key]} "
						    f"too, in the same {storage} storage: every shard of every cluster needs a path "
						    "of its own, as {cluster} and {shard} give it")
					continue
				owners[key] = f'shard {shard} of cluster {name}'
				if index == this:
					self_owned.add(key)
	if shared:
		log(f"storage path check: the shards of cluster {self_name} share a path, where the naming template "
		    f"'{env('BACKUP_NAMING')}' names the backup of each apart, and no other cluster has it")
		return
	log(f'storage path check: every shard of cluster {self_name} has a path of its own')

def days_from_civil(y, m, d):
	y -= 1 if m <= 2 else 0
	era = (y if y >= 0 else y - 399) // 400
	yoe = y - era * 400
	doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
	return era * 146097 + yoe * 365 + yoe // 4 - yoe // 100 + doy - 719468

def month_days(y, m):
	if m == 2:
		return 29 if y % 4 == 0 and (y % 100 != 0 or y % 400 == 0) else 28
	return 30 if m in (4, 6, 9, 11) else 31

def weekday(y, m, d):
	return (days_from_civil(y, m, d) % 7 + 11) % 7

def tag_valid(tag):
	if not rx(TAG_RE, tag):
		return False
	y, m, d = int(tag[0:4]), int(tag[4:6]), int(tag[6:8])
	if m < 1 or m > 12:
		return False
	return 1 <= d <= month_days(y, m) and int(tag[8:10]) < 24 and int(tag[10:12]) < 60 and int(tag[12:14]) < 60

def naming_init():
	"""Reads the naming template BACKUP_NAMING into its parts, and the shard of every host, SHARD_NUMBERS."""
	rest = env('BACKUP_NAMING')
	S.naming_parts, S.slot_tolerance, S.shard_of, S.naming_cache = [], 0, {}, None
	if not rest:
		return
	if not rx(NAMING_RE, rest) or len(rest) > NAMING_MAX_LENGTH:
		die('BACKUP_NAMING is not a valid naming template')
	seen = {}
	for literal, name in re.findall(r'([^{]*)(?:\{([A-Za-z]+)\})?', rest):
		if literal:
			S.naming_parts.append(('l', literal))
		if not name:
			continue
		if name in ('shard', 'cluster', 'chi', 'namespace', 'prefix'):
			S.naming_parts.append(('p', name))
			seen[name] = seen.get(name, 0) + 1
			continue
		tokens = name
		while tokens:
			token = next((t for t in ('YYYY', 'MM', 'DD', 'HH', 'II', 'SS') if tokens.startswith(t)), None)
			if token is None:
				die(f'BACKUP_NAMING holds {LB}{name}}}, which is no placeholder')
			S.naming_parts.append(('t', token))
			seen[token] = seen.get(token, 0) + 1
			tokens = tokens[len(token):]
	for name in ('shard', 'YYYY', 'MM', 'DD', 'HH', 'II', 'SS'):
		if seen.get(name, 0) != 1:
			die('BACKUP_NAMING must hold {shard} and every token of the time, YYYY, MM, DD, HH, II and SS, once')
	for name in ('cluster', 'chi', 'namespace', 'prefix'):
		if seen.get(name, 0) > 1:
			die(f'BACKUP_NAMING holds {LB}{name}}} twice')
	for pair in env('SHARD_NUMBERS').split():
		if not rx(r'^[^=]+=[1-9][0-9]{0,4}$', pair) or not rx(HOST_RE, pair.split('=', 1)[0]):
			die('SHARD_NUMBERS must list host=number pairs')
		host, number = pair.split('=', 1)
		S.shard_of[host] = number
	S.slot_tolerance = NAMING_SLOT_TOLERANCE

def naming_value(name):
	return {'cluster': env('CLUSTER'), 'chi': env('SOURCE_INSTALLATION', env('NAMING_INSTALLATION')),
			'namespace': env('SOURCE_NAMESPACE', env('NAMING_NAMESPACE'))}[name]

def naming_regex(prefix):
	"""The regex of the names the template makes on the cluster, PREFIX for {prefix} or any when empty, and the
	names of its groups."""
	regex, groups = '^', []
	for kind, value in S.naming_parts:
		if kind == 'l':
			regex += value.replace('.', '[.]')
		elif value == 'shard':
			regex += '([1-9][0-9]*)'
			groups.append('shard')
		elif value == 'prefix':
			if prefix:
				regex += prefix.replace('.', '[.]')
			else:
				regex += '([A-Za-z0-9._-]+)'
				groups.append('prefix')
		elif kind == 'p':
			regex += naming_value(value).replace('.', '[.]')
		else:
			if value == 'HH':
				regex += '(T?)'
				groups.append('T')
			regex += '([0-9]{4})' if value == 'YYYY' else '([0-9]{2})'
			groups.append(value)
	return regex + '$', groups

def naming_parse(name, prefix=''):
	"""The (shard, prefix, key) of a name the template makes, PREFIX for {prefix} or any when empty, or None."""
	if S.naming_cache is None or S.naming_cache[0] != prefix:
		S.naming_cache = (prefix,) + naming_regex(prefix)
	match = rx(S.naming_cache[1], name)
	if not match:
		return None
	group = dict(zip(S.naming_cache[2], match.groups()))
	key = group['YYYY'] + group['MM'] + group['DD'] + group['T'] + group['HH'] + group['II'] + group['SS']
	if not tag_valid(key.replace('T', '', 1)):
		return None
	return group['shard'], group.get('prefix', prefix), key

def naming_render(shard, key, prefix):
	"""The name the template gives the backup KEY of the shard SHARD, and whether it is a valid backup name."""
	digits, name = key.replace('T', '', 1), ''
	offsets = {'YYYY': (0, 4), 'MM': (4, 6), 'DD': (6, 8), 'HH': (8, 10), 'II': (10, 12), 'SS': (12, 14)}
	for kind, value in S.naming_parts:
		if kind == 'l':
			name += value
		elif value == 'shard':
			name += str(shard)
		elif value == 'prefix':
			name += prefix
		elif kind == 'p':
			name += naming_value(value)
		else:
			if value == 'HH' and 'T' in key:
				name += 'T'
			name += digits[offsets[value][0]:offsets[value][1]]
	return name, bool(rx(NAME_RE, name))

def naming_holds(placeholder):
	return ('p', placeholder) in S.naming_parts

def series_key(host, name):
	"""The key of a backup of the series of the schedule HOST lists, or None."""
	if not S.naming_parts:
		return name if rx(retention_pattern(), name) else None
	prefix = ''
	if naming_holds('prefix'):
		prefix = env('BACKUP_NAME_PREFIX')
		if not prefix:
			return None
	parsed = naming_parse(name, prefix)
	if not parsed or parsed[0] != S.shard_of.get(host, ''):
		return None
	return parsed[2]

def key_time(key):
	"""The time the backup of a key is named after, YYYYMMDDHHMMSS, and whether it was made at once."""
	value = key if S.naming_parts else key[len(env('BACKUP_NAME_PREFIX')) + 1:]
	return value.replace('T', '', 1), 'T' in value

def host_name(host, key):
	if not S.naming_parts:
		return key
	if not S.shard_of.get(host):
		die(f'SHARD_NUMBERS tells no shard of {host}, whose backup the naming template names by its shard')
	name, valid = naming_render(S.shard_of[host], key, S.naming_prefix)
	if not valid:
		die(f"the naming template '{env('BACKUP_NAMING')}' makes '{name}' of the backup {key} of {host}, which "
		    "is no valid backup name")
	return name

def naming_key(var, prefix=''):
	"""The key of the backup the env var VAR names, setting the prefix of its names."""
	value = env(var)
	S.naming_prefix = prefix
	if not S.naming_parts:
		return value
	if rx(NAMING_TAG_RE, value) and tag_valid(value.replace('T', '', 1)):
		if naming_holds('prefix') and not S.naming_prefix:
			die(f"{var} {value} tells the time of the backup alone, while the naming template '{env('BACKUP_NAMING')}' "
			    "holds {prefix}: name the backup in full, as the name of the backup of any shard")
		return value
	parsed = naming_parse(value, S.naming_prefix)
	if not parsed:
		die(f"{var} {value} is no name the naming template '{env('BACKUP_NAMING')}' makes on cluster {env('CLUSTER')}, "
		    "nor the time of one, YYYYMMDDHHMMSS")
	S.naming_prefix = parsed[1]
	return parsed[2]

def resolve_names(hosts):
	for host in hosts:
		S.host_backup[host] = host_name(host, S.backup_key)

def check_override(item):
	"""Why OVERRIDE is no config override a backup may be made with, '' when it is one."""
	key, value = item.split('=', 1)[0], item.split('=', 1)[1] if '=' in item else item
	if not rx(CONFIG_OVERRIDE_RE, item) or len(key) > CONFIG_OVERRIDE_KEY_MAX_LENGTH or \
			len(value) > CONFIG_OVERRIDE_VALUE_MAX_LENGTH:
		return f'the config override \'{item}\' is no KEY=VALUE of a clickhouse-backup setting'
	if f' {key} ' in CONFIG_OVERRIDE_FORBIDDEN or key.startswith('API_') or key in PATH_CHECK_KEYS:
		return (f'the config override of {key} is refused: the operator sets it, or it tells where the backups '
		        'are')
	if any(token in key for token in CONFIG_OVERRIDE_SENSITIVE):
		return (f'the config override of {key} is refused: it is a secret, which belongs in the configuration '
		        'of the sidecar')
	return ''

def parse_schedules(text):
	"""The entries of a schedule, as SCHEDULES formats them."""
	if CNTRL.search(text):
		die('SCHEDULES must be one line')
	sched, periods, index = [], set(), 0
	for entry in text.split(';'):
		fields = entry.split()
		if not fields:
			continue
		values, overrides = {'incremental': 'false'}, ''
		for field in fields:
			if '=' not in field:
				die(f'SCHEDULES entry {index}: {field} is no key=value')
			key, value = field.split('=', 1)
			if key == 'env':
				reason = check_override(value)
				if reason:
					die(f'SCHEDULES entry {index}: {reason}')
				overrides += ' --env=' + value
			elif key in ('period', 'time', 'day', 'keep', 'incremental'):
				values[key] = value
			else:
				die(f'SCHEDULES entry {index}: unknown key {key}')
		period, at, day, keep = (values.get(name, '') for name in ('period', 'time', 'day', 'keep'))
		if period not in PERIOD_WEIGHT:
			die(f'SCHEDULES entry {index}: period must be minute, hour, 6hours, daily, weekly or monthly')
		if period in periods:
			die(f'SCHEDULES entry {index}: a second {period} entry')
		periods.add(period)
		if not rx(KEEP_RE, keep):
			die(f'SCHEDULES entry {index} ({period}): keep must be a number from 1')
		if at and not rx(SCHEDULE_TIME_RE, at):
			die(f'SCHEDULES entry {index} ({period}): time must be HH:MM')
		if not at and period not in ('minute', 'hour', '6hours'):
			die(f'SCHEDULES entry {index} ({period}): time is required')
		if period == 'weekly' and not rx(r'^[0-6]$', day):
			die(f'SCHEDULES entry {index} (weekly): day must be from 0 (Sunday) to 6')
		elif period == 'monthly' and not rx(r'^([1-9]|[12][0-9]|3[01])$', day):
			die(f'SCHEDULES entry {index} (monthly): day must be from 1 to 31')
		elif period not in ('weekly', 'monthly') and day:
			die(f'SCHEDULES entry {index} ({period}): day is for weekly and monthly entries only')
		if values['incremental'] not in ('true', 'false'):
			die(f'SCHEDULES entry {index} ({period}): incremental must be true or false')
		if period == 'minute':
			at = ''
		at = at or '00:00'
		sched.append({'period': period, 'hour': int(at[:2]), 'minute': int(at[3:]), 'day': int(day) if day else None,
					  'keep': int(keep), 'incremental': values['incremental'] == 'true', 'env': overrides})
		index += 1
	return sched

def slot_matches(entry, tag):
	"""Whether the wall time TAG is a slot of the entry, to the minute, or within the slot tolerance after one."""
	if entry['period'] == 'minute':
		return True
	if S.slot_tolerance > 0:
		back = ((int(tag[10:12]) - entry['minute'] + 60) % 60) * 60 + int(tag[12:14])
		if back >= S.slot_tolerance:
			return False
		epoch = days_from_civil(int(tag[0:4]), int(tag[4:6]), int(tag[6:8])) * 86400 + int(tag[8:10]) * 3600 + \
			int(tag[10:12]) * 60 + int(tag[12:14]) - back
		tag = (datetime(1970, 1, 1) + timedelta(seconds=epoch)).strftime('%Y%m%d%H%M%S')
	y, m, d, hour, minute = int(tag[0:4]), int(tag[4:6]), int(tag[6:8]), int(tag[8:10]), int(tag[10:12])
	if minute != entry['minute']:
		return False
	if entry['period'] == 'hour':
		return True
	if entry['period'] == '6hours':
		return hour % 6 == entry['hour'] % 6
	if hour != entry['hour']:
		return False
	if entry['period'] == 'daily':
		return True
	if entry['period'] == 'weekly':
		return weekday(y, m, d) == entry['day']
	if entry['period'] == 'monthly':
		return d == min(entry['day'], month_days(y, m))
	return False

def winner_entry(tag):
	S.matched, S.winner, weight = [], None, 0
	for i, entry in enumerate(S.sched):
		if slot_matches(entry, tag):
			S.matched.append(i)
			if PERIOD_WEIGHT[entry['period']] > weight:
				weight, S.winner = PERIOD_WEIGHT[entry['period']], i

def incremental_entry():
	S.winner = next((i for i, entry in enumerate(S.sched) if entry['incremental']), None)

def zone_file(zone):
	return os.path.join(env('TZDIR', '/usr/share/zoneinfo'), zone)

def check_timezone(zone):
	if not zone or zone == 'UTC':
		return
	if not rx(ZONE_RE, zone) or '..' in zone or not os.path.isfile(zone_file(zone)):
		die('SCHEDULE_TIMEZONE is not a time zone of the tzdata of the image')

def slot_tag(epoch, zone):
	"""The wall time of EPOCH in ZONE, as YYYYMMDDHHMMSS."""
	if not zone or zone == 'UTC':
		tz = timezone.utc
	else:
		from zoneinfo import ZoneInfo
		with open(zone_file(zone), 'rb') as source:
			tz = ZoneInfo.from_file(source)
	try:
		return datetime.fromtimestamp(epoch, tz).strftime('%Y%m%d%H%M%S')
	except (OverflowError, ValueError, OSError):
		return ''

def slot_epoch():
	"""The unix seconds of the slot of the run, as the name of its Job tells it, else the current minute."""
	cron, job, slot = env('CRON_JOB_NAME'), env('JOB_NAME'), ''
	if cron and job.startswith(cron + '-'):
		slot = job[len(cron) + 1:]
	if rx(SLOT_RE, slot):
		return int(slot) * 60
	now = int(time.time())
	return now - now % 60

def schedule_slot():
	tag = slot_tag(slot_epoch(), env('SCHEDULE_TIMEZONE'))
	if not rx(TAG_RE, tag):
		now = int(time.time())
		tag = slot_tag(now - now % 60, env('SCHEDULE_TIMEZONE'))
	S.slot_tag = tag

def record_series(host):
	for row in S.lst.rows:
		if row['name'] and not row['desc'].startswith('broken'):
			key = series_key(host, row['name'])
			if key is not None and key != S.backup_key:
				S.series_required[(host, key)] = row['required']

def incremental_base(keep, hosts):
	"""The backup an incremental backup on every host is made from, '' for a full one, and the chain it ends."""
	hosts, held, newest, newest_time = set(hosts), {}, '', ''
	for host, name in S.series_required:
		if host in hosts:
			held[name] = held.get(name, 0) + 1
	for name, n in held.items():
		time_ = key_time(name)[0]
		if n == len(hosts) and (time_ > newest_time or (time_ == newest_time and name > newest)):
			newest, newest_time = name, time_
	if not newest:
		return '', 0
	chain = 0
	for host in hosts:
		names = sorted(((key_time(name)[0], name) for h, name in S.series_required if h == host), reverse=True)
		count = 0
		for time_, name in names:
			if time_ > newest_time:
				continue
			if not S.series_required[(host, name)]:
				break
			count += 1
		chain = max(chain, count)
	return (newest if chain < keep else ''), chain

def backup_pod_wait_seconds():
	value = env('BACKUP_POD_WAIT_SECONDS', str(DEFAULT_BACKUP_POD_WAIT_SECONDS))
	if not rx(NUMBER_RE, value):
		die('BACKUP_POD_WAIT_SECONDS must be a number of seconds')
	return int(value)

def check_transport():
	transport = env('BACKUP_TRANSPORT')
	if transport == 'pod':
		backup_pod_wait_seconds()
	elif transport not in ('', 'sidecar'):
		die('BACKUP_TRANSPORT must be sidecar or pod')

def wait_for_apis(hosts):
	"""Waits for the backup Pods of the pod transport to answer."""
	if env('BACKUP_TRANSPORT') != 'pod':
		return
	since, wait = seconds(), backup_pod_wait_seconds()
	for host in hosts:
		if not rx(HOST_RE, host):
			continue
		announced = False
		while True:
			request('GET', host, '/backup/version')
			if S.curl_exit == 0 and not proxy_failed():
				break
			if seconds() - since >= wait:
				die(f'the backup Pod {host} does not answer after {seconds() - since}s (curl exit {S.curl_exit}, '
				    f'HTTP {S.http_code})')
			if not announced:
				announced = True
				log(host + ': waiting for the backup Pod to answer')
			sleep()
	event('Normal', 'OrchestratorBackupPodsReady', 'the backup Pods answer')

def clickhouse_answers(host):
	request('GET', host, '/backup/tables', [('table', 'system.one')])
	return S.curl_exit == 0 and S.http_code == '200'

def fall_back_hosts(preferred):
	"""The hosts a backup runs on: a preferred replica that does not answer falls back to BACKUP_FALLBACK_HOSTS."""
	S.preferred_hosts = list(preferred)
	fallback = parse_hosts('BACKUP_FALLBACK_HOSTS', True)
	hosts = list(preferred)
	if not fallback:
		return hosts
	if len(fallback) != len(preferred):
		die('BACKUP_FALLBACK_HOSTS must name a host for every host of BACKUP_HOSTS')
	for i, host in enumerate(preferred):
		if host == fallback[i] or clickhouse_answers(host):
			continue
		advise(f'{host}: the preferred replica does not answer (curl exit {S.curl_exit}, HTTP {S.http_code}), '
		       f'backing up {fallback[i]} instead')
		event('Normal', 'OrchestratorReplicaFallback', f'{host} does not answer, the backup of its shard runs '
		                                               f'on {fallback[i]}')
		hosts[i] = fallback[i]
	return hosts

def drop_failed_backup(hosts):
	with scoped(best_effort=True):
		for host in hosts:
			name = S.host_backup[host]
			for where in ('local', 'remote'):
				if submit(host, f'delete {where} {name}', f'/backup/delete/{where}/{name}',
						  [('force', 'true')], timeout=DELETE_TIMEOUT):
					log(f'{host}: the {where} copy of the failed backup {name} deleted')
					event('Normal', 'OrchestratorFailedBackupDeleted',
						  f'{host}: the {where} copy of the failed backup {name} deleted')
				elif 'not found' in S.last_warning:
					log(f'{host}: no {where} copy of the failed backup {name} to delete')
				else:
					message = f'{host}: the {where} copy of the failed backup {name} is left: {S.last_warning}'
					advise(message)
					event('Warning', 'OrchestratorFailedBackupLeft', message)

def backup_unseen(host):
	"""Whether the storage of HOST does not hold the backup this run makes there yet; sets the step there."""
	name = S.host_backup[host]
	if list_backups(host, 'remote') is None:
		return False
	if find_backup(name):
		warn(f'backup {name} appeared in the storage of {host} during this run: hosts of one backup must not '
		     'share a storage path')
		return False
	S.step_name = name
	base = S.base_of.get(host, '')
	if S.create_kind == 'action':
		S.step_args = [(f"create_remote{S.create_flags}{(' --diff-from-remote=' + base if base else '')}{S.create_env} "
		                f"{name}")]
		return True
	S.step_args = ['/backup/create_remote', [('name', name)] + S.create_params + ([('diff-from-remote', base)] if base else [])]
	return True

def check_filter(name, pattern, maximum, what):
	value = env(name)
	if (value and not rx(pattern, value)) or len(value) > maximum:
		die(f'{name} is not a valid {what}')

def config_overrides():
	"""How the backup is made: the typed request, or with config overrides the command of the actions endpoint."""
	S.create_kind, S.create_flags = 'op', ''
	value = env('CONFIG_OVERRIDES')
	if value:
		if S.sched:
			die('CONFIG_OVERRIDES are those of a backup that runs no schedule: a scheduled backup takes those of its entries')
		if CNTRL.search(value):
			die('CONFIG_OVERRIDES must be one line')
		items = value.split()
		if len(items) > CONFIG_OVERRIDES_MAX_ITEMS:
			die(f'CONFIG_OVERRIDES holds more than {CONFIG_OVERRIDES_MAX_ITEMS} overrides')
		for item in items:
			reason = check_override(item)
			if reason:
				die('CONFIG_OVERRIDES: ' + reason)
			S.create_env += ' --env=' + item
	if not S.create_env:
		return
	S.create_kind = 'action'
	if is_true('SCHEMA_ONLY'):
		S.create_flags += ' --schema'
	tables = env('TABLES')
	if tables:
		if not rx(TABLES_RE, tables) or len(tables) > TABLES_MAX_LENGTH:
			die('TABLES is not a valid table pattern')
		S.create_flags += ' --tables=' + tables
	if env('PARTITIONS'):
		check_filter('PARTITIONS', PARTITIONS_RE, FILTER_MAX_LENGTH, 'list of partition ids')
		S.create_flags += ' --partitions=' + env('PARTITIONS')
	log(f"backup {env('BACKUP_NAME')}: made with the config overrides{S.create_env}, through the actions endpoint")
	event('Normal', 'OrchestratorConfigOverrides', f"backup {env('BACKUP_NAME')} is made with the config overrides{S.create_env}")

def do_backup():
	backup_hosts = fall_back_hosts(parse_hosts('BACKUP_HOSTS'))
	for i in range(1, len(backup_hosts)):
		if backup_hosts[i] in backup_hosts[:i]:
			die(f'BACKUP_HOSTS lists {backup_hosts[i]} twice: hosts of one backup must not share a storage path')
	prefix = env('BACKUP_NAME_PREFIX')
	if prefix:
		check_name('BACKUP_NAME_PREFIX')
	S.sched = parse_schedules(env('SCHEDULES'))
	is_true('RESTORE_IN_PROGRESS')
	check_timezone(env('SCHEDULE_TIMEZONE'))
	if S.sched or not env('BACKUP_NAME'):
		schedule_slot()
	if env('CLUSTER'):
		check_cluster()
	naming_init()
	if env('BACKUP_NAME'):
		check_name('BACKUP_NAME')
		S.backup_name_given = True
		S.backup_key = naming_key('BACKUP_NAME', prefix)
	elif S.naming_parts:
		if naming_holds('prefix') and not prefix:
			die(f"BACKUP_NAME_PREFIX must be set for the naming template '{env('BACKUP_NAMING')}', which holds "
			    "{prefix}")
		S.naming_prefix, S.backup_key = prefix, S.slot_tag
	elif prefix:
		S.env['BACKUP_NAME'] = prefix + '-' + S.slot_tag
		if not rx(NAME_RE, S.env['BACKUP_NAME']):
			die('BACKUP_NAME_PREFIX is too long for a backup name')
		S.backup_key = S.env['BACKUP_NAME']
	else:
		die('either BACKUP_NAME or BACKUP_NAME_PREFIX must be set')
	resolve_names(backup_hosts)
	if S.naming_parts:
		S.env['BACKUP_NAME'] = naming_render(1, S.backup_key, S.naming_prefix)[0]
		log("backup %s: named by the naming template '%s', %s" % (env('BACKUP_NAME'), env('BACKUP_NAMING'), ''.join(
			f'{host}={S.host_backup[host]} ' for host in backup_hosts)))
	name = env('BACKUP_NAME')
	S.create_params = []
	if is_true('SCHEMA_ONLY'):
		S.create_params.append(('schema', ''))
	if env('TABLES'):
		S.create_params.append(('table', env('TABLES')))
	if env('PARTITIONS'):
		S.create_params.append(('partitions', env('PARTITIONS')))
	S.base_of, S.create_env = {}, ''
	diff = env('DIFF_FROM_REMOTE')
	if diff:
		check_name('DIFF_FROM_REMOTE')
		key, naming_prefix = S.backup_key, S.naming_prefix
		S.backup_key = naming_key('DIFF_FROM_REMOTE', prefix)
		for host in backup_hosts:
			S.base_of[host] = host_name(host, S.backup_key)
		S.backup_key, S.naming_prefix = key, naming_prefix
	incremental, keep = False, 0
	if S.sched and S.backup_name_given:
		incremental_entry()
		if S.winner is None:
			log(f'backup {name}: runs the schedule now, which has no incremental entry: the backup is full')
		elif not diff and prefix:
			log(f"backup {name}: runs the schedule now, incremental as its {S.sched[S.winner]['period']} entry")
			incremental, keep = True, S.sched[S.winner]['keep']
	elif S.sched:
		winner_entry(S.slot_tag)
		if S.winner is None:
			log(f'backup {name}: the slot {S.slot_tag} is a slot of no schedule entry, the backup is full')
		else:
			log(f"backup {name}: the slot {S.slot_tag} is a slot of the {S.sched[S.winner]['period']} entry{(' and lighter ones' if len(S.matched) > 1 else '')}")
			if S.sched[S.winner]['incremental'] and not diff and prefix:
				incremental, keep = True, S.sched[S.winner]['keep']
			S.create_env = S.sched[S.winner]['env']
	config_overrides()
	check_storage_paths()
	wait_for_apis(backup_hosts)

	event('Normal', 'OrchestratorBackupStarted', f'backup {name} on {len(backup_hosts)} host(s)')
	log(f'backup {name}: preflight on {len(backup_hosts)} host(s)')
	S.series_required = {}
	for host in backup_hosts:
		if list_backups(host, 'remote') is None:
			fail('preflight failed on ' + host)
		if find_backup(S.host_backup[host]):
			die(f'backup {S.host_backup[host]} already exists in the storage of {host}')
		if S.naming_parts:
			for row in S.lst.rows:
				parsed = naming_parse(row['name'], S.naming_prefix)
				if parsed and parsed[2] == S.backup_key:
					die(f"backup {row['name']}, of the same time as {name}, already exists in the storage of "
					    f"{host}")
		if diff and not find_backup(S.base_of[host]):
			die(f'base backup {S.base_of[host]} is missing in the storage of {host}')
		if incremental:
			record_series(host)
	if incremental and not diff:
		base, chain = incremental_base(keep, backup_hosts)
		if base:
			for host in backup_hosts:
				S.base_of[host] = host_name(host, base)
			log(f'backup {name}: incremental from {S.base_of[backup_hosts[0]]}, which ends a chain of {chain} '
			    'incremental backups')
			event('Normal', 'OrchestratorIncrementalBase', f'{name} is incremental from {S.base_of[backup_hosts[0]]}')
		else:
			log(f'backup {name}: full, as the chain of incremental backups is {chain} long, of {keep} at most, '
			    'or there is no base every host holds')
			event('Normal', 'OrchestratorIncrementalFull', name + ' is a full backup')

	if not run_all(S.create_kind, backup_hosts, backup_unseen, 'create_remote', name):
		drop_failed_backup(S.step_failed_hosts)
		fail_all('backup ' + name)
	log(f'backup {name} completed on {len(backup_hosts)} host(s)')
	if backup_hosts != S.preferred_hosts and env('HOSTS_USED_MESSAGE'):
		report_message(f"{env('HOSTS_USED_MESSAGE')} {' '.join(backup_hosts)}")
	if not is_true('SCHEMA_ONLY'):
		report_data(backup_hosts)
	event('Normal', 'OrchestratorBackupCompleted', f'backup {name} completed on {len(backup_hosts)} host(s)')
	retention()

def do_verify():
	check_name('BACKUP_NAME')
	verify_hosts = parse_hosts('VERIFY_HOSTS')
	if env('CLUSTER'):
		check_cluster()
	naming_init()
	S.backup_key = naming_key('BACKUP_NAME', env('BACKUP_NAME_PREFIX'))
	resolve_names(verify_hosts)
	wait_for_apis(verify_hosts)
	backup = env('BACKUP_NAME')
	event('Normal', 'OrchestratorVerifyStarted', f'verify {backup} on {len(verify_hosts)} host(s)')
	for host in verify_hosts:
		name = S.host_backup[host]
		log(f'{host}: verifying {name}')
		if list_backups(host, 'local') is not None and find_backup(name):
			if not delete_local(host, name):
				fail(f'verify failed on {host}: cannot drop the local copy of {name}')
		if not run_op(host, 'download', name, '/backup/download/' + name):
			fail(f'verify failed on {host}: {name} cannot be downloaded')
		if not has_usable_backup(host, 'local', name):
			die(f'verify failed on {host}: {name} is not present locally after the download')
		if not delete_local(host, name):
			warn(f'{host}: the downloaded copy of {name} is left on local disk')
	log(f'backup {backup} verified on {len(verify_hosts)} host(s)')
	event('Normal', 'OrchestratorVerifyCompleted', f'backup {backup} verified on {len(verify_hosts)} host(s)')

def restored_db(db):
	"""The database of the backup the restore restores into the live DB, or None when it touches nothing of it."""
	if db in S.db_source:
		return S.db_source[db]
	if db in S.db_target:
		return None
	return db

def db_dropped(db):
	tables = env('TABLES')
	if not tables:
		return True
	return any(pattern == db + '.*' or glob_match(pattern, db + '.') for pattern in tables.split(','))

def glob_match(pattern, value):
	"""Whether VALUE matches the bash glob PATTERN of * and ?."""
	return re.fullmatch(''.join('.*' if c == '*' else '.' if c == '?' else re.escape(c) for c in pattern), value,
						re.DOTALL) is not None

def drops_database(db):
	return db_dropped(db) and bool(env('TABLES') or S.backup_tables_unreadable or (db + '.') in S.backup_dbs)

def list_backup_tables(host):
	"""The tables of the backup in the storage of HOST, and their databases."""
	tables, dbs, where = {}, {}, 'the storage of ' + host
	params = [('remote_backup', S.host_backup[host])]
	if S.source_env:
		source_copy(host)
		params = [('local_backup', S.host_backup[host])]
		where = f'the copy {host} downloaded from the storage path \'{S.source_path_shown}\''
	if env('TABLES'):
		params.append(('table', env('TABLES')))
	call('GET', host, '/backup/tables', params)
	if S.http_code != '200':
		die(f'preflight failed: cannot list the tables of {S.host_backup[host]} in {where} (HTTP {S.http_code}): '
		    f'{error_of()}')
	for line in S.http_body.split('\n'):
		if not line:
			continue
		match = re.search(BACKUP_TABLE_ROW_RE, line)
		if match:
			tables[match.group(1) + '.' + match.group(2)] = True
			dbs[match.group(1) + '.'] = True
		else:
			S.backup_tables_unreadable = host
	return tables, dbs

def backup_tables(hosts):
	first, S.shard_tables_differ = None, []
	for host in hosts:
		tables, dbs = list_backup_tables(host)
		S.backup_tables.update(tables)
		S.backup_dbs.update(dbs)
		if first is None:
			first = set(tables)
			continue
		S.shard_tables_differ += [f'{name} only on {host}' for name in tables if name not in first]
		S.shard_tables_differ += [f'{name} not on {host}' for name in first if name not in tables]

def live_tables(hosts):
	"""The user tables of every host - host, db, table, engine, uuid, size - and the rows that do not read as one."""
	S.live, S.live_unreadable = [], []
	for host in hosts:
		call('GET', host, '/backup/tables/all')
		if S.http_code != '200':
			die(f'preflight failed: cannot list tables on {host} (HTTP {S.http_code}): {error_of()}')
		for row in S.http_body.split('\n'):
			if not row:
				continue
			match = re.search(TABLE_ROW_RE, row)
			if not match:
				S.live_unreadable.append((host, json_get('TotalBytes', row)))
				continue
			if match.group(1) in SYSTEM_DATABASES:
				continue
			S.live.append({'host': host, 'db': match.group(1), 'table': match.group(2), 'engine': json_get('Engine', row),
						   'uuid': json_get('UUID', row), 'size': json_get('TotalBytes', row), 'dropped': False})

def dropped_tables():
	inner = set()
	for live in S.live:
		db = restored_db(live['db'])
		if db is None:
			continue
		if (db + '.' + live['table']) in S.backup_tables or drops_database(db):
			live['dropped'] = True
			inner.add(f"{live['host']} {live['db']}..inner.{live['table']}")
			if live['uuid']:
				inner.add(f"{live['host']} {live['db']}..inner_id.{live['uuid']}")
	for live in S.live:
		if f"{live['host']} {live['db']}.{live['table']}" in inner:
			live['dropped'] = True

def guard_tables(sources, hosts):
	"""Refuses a restore without overwrite over existing tables it would replace."""
	S.backup_tables, S.backup_dbs, S.backup_tables_unreadable = {}, {}, ''
	live_tables(hosts)
	found = [host + ':?' for host, _ in S.live_unreadable]
	if S.live and env('TABLES'):
		backup_tables(sources)
	for live in S.live:
		db = restored_db(live['db'])
		if db is None:
			continue
		if not env('TABLES') or S.backup_tables_unreadable or (db + '.' + live['table']) in S.backup_tables or db_dropped(db):
			found.append(f"{live['host']}:{live['db']}.{live['table']}")
	if found:
		die(f"refusing to restore over existing tables ({' '.join(found[:10])}); set overwrite to replace them")

def greater(size, limit):
	size, limit = size.lstrip('0'), limit.lstrip('0')
	if len(size) != len(limit):
		return len(size) > len(limit)
	return size > limit

def no_zookeeper():
	return env('REPLICATED') != '' and not is_true('REPLICATED')

def drop_size_limited():
	return not rx(r'^0+$', env('MAX_TABLE_SIZE_TO_DROP'))

def refuse_database_drops(sources, hosts):
	"""Refuses an overwrite whose drop of a database as a whole would stop at the second table larger than
	max_table_size_to_drop on a host of HOSTS, which drops it on its own."""
	limit, stop = env('MAX_TABLE_SIZE_TO_DROP'), 'larger than'
	unreadable, unreadable_rows, count, larger, uncertain, found = {}, {}, {}, {}, {}, []
	for host, size in S.live_unreadable:
		if not size:
			what = 'of unknown size'
		elif greater(size, limit):
			what = size + ' bytes'
		else:
			continue
		unreadable[host] = unreadable.get(host, 0) + 1
		unreadable_rows[host] = ', '.join(filter(None, [unreadable_rows.get(host, ''), (f'{host}:? (a table whose '
		                                                                                'name cannot be read, '
		                                                                                f'{what})')]))
	for live in S.live:
		host = live['host']
		if host not in hosts:
			continue
		key = (host, live['db'])
		if key not in count:
			count[key], larger[key] = unreadable.get(host, 0), unreadable_rows.get(host, '')
			uncertain[key] = bool(unreadable_rows.get(host))
		if not live['size']:
			row = f"{host}:{live['db']}.{live['table']} (of unknown size)"
			uncertain[key] = True
		elif greater(live['size'], limit):
			row = f"{host}:{live['db']}.{live['table']} ({live['size']} bytes)"
		else:
			continue
		larger[key] = ', '.join(filter(None, [larger[key], row]))
		count[key] += 1
	for key in sorted(count, key=lambda key: key[0] + ' ' + key[1]):
		if count[key] < 2:
			continue
		if not env('TABLES') and sources:
			backup_tables(sources)
			sources = []
		db = restored_db(key[1])
		if db is not None and drops_database(db):
			found.append(larger[key])
			if uncertain[key]:
				stop = 'that is, or may be, larger than'
	if found:
		die(f"refusing to overwrite: a host of cluster {env('CLUSTER')} that drops a database as a whole would "
		    f"stop at the second table of it {stop} max_table_size_to_drop ({limit} bytes) ({', '.join(found[:10])}), "
		    "as clickhouse-backup forces the drop of only one such table of each database it drops as a whole; "
		    f"{SIZE_LIMIT_ADVICE}")

def plan_overwrite(shards, hosts):
	"""Decides whether an overwrite drops the tables of the backup ON CLUSTER first, refusing one it cannot make."""
	initiator, known, replicated, name, cluster = shards[0], True, '', env('BACKUP_NAME'), env('CLUSTER')
	S.backup_tables, S.backup_dbs, S.backup_tables_unreadable = {}, {}, ''
	backup_tables(shards)
	live_tables(hosts)
	dropped_tables()
	if S.live_unreadable or S.backup_tables_unreadable:
		known = False
	for live in S.live:
		if not live['dropped']:
			continue
		if live['engine'].startswith('Replicated'):
			replicated = replicated or f"{live['host']}:{live['db']}.{live['table']}"
		elif not live['engine']:
			known = False
	if replicated:
		S.on_cluster = True
		if no_zookeeper():
			log(f'restore {name}: cluster {cluster} holds the replicated table {replicated}, which the restore '
			    'drops ON CLUSTER though the installation configures no ZooKeeper for it')
	elif not known:
		if no_zookeeper():
			die(f'refusing to overwrite: whether the restore drops a replicated table of cluster {cluster} is '
			    'unknown, as a table of a host or of the backup does not read as one; such a table needs the '
			    'drop ON CLUSTER, which needs ZooKeeper, and the installation configures none for the cluster')
		S.on_cluster = True
	else:
		log(f'restore {name}: the restore drops no replicated table, every replica drops the tables of the backup '
		    'on its own')
		if drop_size_limited():
			refuse_database_drops([], hosts)
		return
	if S.backup_tables_unreadable:
		die(f'refusing to overwrite: {S.backup_tables_unreadable} lists a table of {name} that does not read '
		    'as one, which leaves unknown whether the backup of every shard holds the same tables')
	if S.shard_tables_differ:
		die(f"refusing to overwrite: the tables of {name} differ between the shards of cluster {cluster} ({', '.join(sorted(S.shard_tables_differ)[:10])}): "
		    f"the drop ON CLUSTER drops and recreates on every shard the tables of the backup of {initiator}, "
		    "which takes the backup of every shard to hold the same; narrow the restore with tables to those "
		    "they all hold")
	if not drop_size_limited():
		return
	for host, _ in S.live_unreadable:
		if host != initiator:
			die(f'refusing to overwrite: {host} lists a table that does not read as one, whose size max_table_size_to_drop '
			    'may not allow the drop ON CLUSTER to drop')
	oversized, limit = [], env('MAX_TABLE_SIZE_TO_DROP')
	for live in S.live:
		if not live['dropped'] or live['host'] == initiator:
			continue
		if not live['size']:
			oversized.append(f"{live['host']}:{live['db']}.{live['table']} (of unknown size)")
		elif greater(live['size'], limit):
			oversized.append(f"{live['host']}:{live['db']}.{live['table']} ({live['size']} bytes)")
	if oversized:
		die(f"refusing to overwrite: the drop ON CLUSTER from {initiator} would stop at tables larger than max_table_size_to_drop "
		    f"({limit} bytes) of cluster {cluster} on other hosts ({', '.join(oversized[:10])}), as clickhouse-backup "
		    f"forces the drop of such a table on {initiator} alone; {SIZE_LIMIT_ADVICE}")
	refuse_database_drops([], [initiator])

def check_source_name(name, maximum, what):
	value = env(name)
	if value and (not rx(SOURCE_NAME_RE, value) or len(value) > maximum):
		die(f'{name} is not a valid {what}')

def source_template(template, installation, namespace):
	def replace(match):
		return {'installation': installation or match.group(0), 'namespace': namespace or match.group(0)}.get(
			match.group(1), match.group(0))
	return re.sub(r'\{([^{}]*)\}', replace, template)

def source_paths():
	"""Points a restore of the backup of another installation at its storage paths, SOURCE_ENV."""
	name = env('BACKUP_NAME')
	installation = env('SOURCE_INSTALLATION') if env('SOURCE_INSTALLATION') not in ('', env('PATH_INSTALLATION')) else ''
	namespace = env('SOURCE_NAMESPACE') if env('SOURCE_NAMESPACE') not in ('', env('PATH_NAMESPACE')) else ''
	if not installation and not namespace:
		if env('SOURCE_INSTALLATION') or env('SOURCE_NAMESPACE'):
			advise(f'restore {name} names its own installation as the one it restores the backup of, which it '
			       'reads at its own storage paths')
		return
	if not S.self_storage_prefix:
		die('a restore from another installation needs the storage paths of the sidecars, which it reads the backup at '
			'with the {installation} and {namespace} of that installation, and the storage path check cannot tell them: '
			+ S.self_paths_unknown)
	path = source_template(S.self_path, installation, namespace)
	obj = source_template(S.self_object_disk_path, installation, namespace)
	if path == S.self_path and obj == S.self_object_disk_path:
		advise("the storage paths of cluster %s hold no {installation} or {namespace} that tells the installation restore "
			   "%s names apart from this one, so it reads them as they are: '%s'%s" %
			   (S.self_cluster_name, name, S.self_path,
				f' and the object disk path \'{S.self_object_disk_path}\'' if S.self_object_disk_path else ''))
		return
	if path == S.self_path:
		S.source_own_path = True
	elif obj and obj == S.self_object_disk_path:
		die(f'the object disk path \'{S.self_object_disk_path}\' of cluster {S.self_cluster_name} holds no {LB}installation}} '
		    f'or {LB}namespace}} that tells the installation restore {name} names apart from this one, while the '
		    'storage path does: deleting the copy the restore downloads would delete the object disk data of '
		    'the backup; give the object disk path the macros of the storage path')
	for value in [path] + ([obj] if obj else []):
		if not rx(SOURCE_PATH_RE, value):
			die(f'the storage path \'{value}\' of the installation restore {name} reads the backup of cannot '
			    'be set in a command of the restore: only a path of letters, digits and _ . / { } - can')
	S.source_env = f'--env={S.self_storage_prefix}_PATH={path}'
	if obj:
		S.source_env += f' --env={S.self_storage_prefix}_OBJECT_DISK_PATH={obj}'
	if installation and '{replica}' in path:
		S.source_hint = ('; the storage path holds {replica}, the host name, which holds the name of the installation, '
						 'so the hosts of installations of different names have different paths')
	try:
		S.source_path_shown = expand_macros(path, S.self_cluster_name, '{shard}')
	except KeyError:
		S.source_path_shown = path
	log(f'restore {name}: reading the backup at the storage path \'{S.source_path_shown}\' of the installation '
	    'it names')

def refuse_local_copy(host):
	if host in S.no_local_copy:
		return
	if list_backups(host, 'local') is None:
		fail('preflight failed on ' + host)
	name = S.host_backup[host]
	if find_backup(name):
		die(f'preflight failed: {host} holds a local copy of {name}, which a restore from the storage paths '
		    'of another installation would restore in place of the backup there; delete it (clickhouse-backup '
		    f'delete local {name}) and restore again')
	S.no_local_copy.append(host)

def refuse_own_backup(host):
	if list_backups(host, 'remote') is None:
		fail('preflight failed on ' + host)
	S.own_backups[host] = {row['name'] for row in S.lst.rows}
	name = S.host_backup[host]
	if name in S.own_backups[host]:
		die(f'preflight failed: {host} holds a backup {name} at its own storage path too, which clickhouse-backup '
		    f'would take for the one at the storage path \'{S.source_path_shown}\', as it tells the backups '
		    'of every path apart by name alone: restore a backup of another name, or delete that one first')

def source_copy(host):
	"""Downloads the schema of the backup to HOST from the storage paths SOURCE_ENV sets, once."""
	if host in S.source_copies:
		return
	with scoped(read_only=True, no_step=True):
		refuse_local_copy(host)
		S.source_copies.append(host)
		name = S.host_backup[host]
		if not run_action(host, 'download', name, f'download --schema --rbac {S.source_env} {name}'):
			die(f"preflight failed: backup {name} is missing in the storage path '{S.source_path_shown}' of "
			    f"{host}, or cannot be read there{(': ' + S.last_warning if S.last_warning else '')}{S.source_hint}")

def drop_source_copies(strict=False):
	with scoped(read_only=True, best_effort=S.best_effort or not strict):
		hosts = list(S.source_copies)
		if not strict:
			S.source_copies = []
		for host in hosts:
			name = S.host_backup[host]
			if strict:
				if not delete_local(host, name):
					fail(f'{host}: cannot delete the copy of {name} the preflight downloaded, which the restore '
					     'would take for its own')
				S.source_copies = S.source_copies[1:]
			elif list_backups(host, 'local') is not None and not find_backup(name):
				continue
			elif not delete_local(host, name):
				advise(f'{host}: the copy of {name} the restore downloaded is left on local disk, which refuses '
				       f'a restore of {name} from another installation until it is deleted')

def restore_command(step, host):
	"""The arguments of run_op, or of run_action, that run the step STEP of the restore on HOST."""
	endpoint, params, flags = 'restore', [], []
	if step == 'download':
		endpoint, params, flags = 'download', [('hardlink_exists_files', '')], ['download', '--hardlink-exists-files']
	elif step == 'download_schema':
		endpoint, params, flags = 'download', [('schema', '')], ['download', '--schema']
	elif step == 'schema':
		params, flags = [('schema', ''), ('rm', '')], ['restore', '--schema', '--rm']
		if S.restore_rbac_with_schema:
			params.append(('rbac', ''))
			flags.append('--rbac')
	elif step == 'schema_on_cluster':
		flags = ['restore', '--schema', '--rm', '--env=RESTORE_SCHEMA_ON_CLUSTER=' + env('CLUSTER')]
	elif step == 'data':
		params, flags = [('data', '')], ['restore', '--data']
	if env('TABLES'):
		params.append(('table', env('TABLES')))
		flags.append('--tables=' + env('TABLES'))
	if env('PARTITIONS') and step in ('download', 'data'):
		params.append(('partitions', env('PARTITIONS')))
		flags.append('--partitions=' + env('PARTITIONS'))
	if env('DATABASE_MAPPING') and step in ('schema', 'schema_on_cluster', 'data'):
		params.append(('restore_database_mapping', env('DATABASE_MAPPING')))
		flags.append('--restore-database-mapping=' + env('DATABASE_MAPPING'))
	if S.restore_kind == 'action' or step == 'schema_on_cluster':
		return [f"{' '.join(flags)}{(' ' + S.action_env if S.action_env else '')} {S.host_backup[host]}"]
	return [f'/backup/{endpoint}/{S.host_backup[host]}', params]

def restore_step(host, what, step):
	kind = 'action' if step == 'schema_on_cluster' else S.restore_kind
	args = restore_command(step, host)
	if kind == 'action':
		return run_action(host, what, S.host_backup[host], *args)
	return run_op(host, what, S.host_backup[host], *args)

def check_restore_filters():
	check_filter('PARTITIONS', PARTITIONS_RE, FILTER_MAX_LENGTH, 'list of partition ids')
	check_filter('SKIP_TABLES', TABLES_RE, TABLES_MAX_LENGTH, 'table pattern')
	check_filter('SKIP_TABLE_ENGINES', ENGINES_RE, FILTER_MAX_LENGTH, 'list of table engines')
	check_filter('DATABASE_MAPPING', DATABASE_MAPPING_RE, FILTER_MAX_LENGTH, 'database mapping')
	S.db_source, S.db_target = {}, {}
	pairs = env('DATABASE_MAPPING').split(',') if env('DATABASE_MAPPING') else []
	for pair in pairs:
		source, target = pair.split(':', 1)
		if source in S.db_target or target in S.db_source or source == target:
			die('DATABASE_MAPPING maps a database twice, or to itself')
		S.db_target[source], S.db_source[target] = target, source
	for pair in pairs:
		if pair.split(':', 1)[0] in S.db_source:
			die('DATABASE_MAPPING maps a database another one is mapped to')

def restore_env():
	flags = [S.source_env] if S.source_env else []
	if env('SKIP_TABLES'):
		flags.append(f"--env=CLICKHOUSE_SKIP_TABLES={DEFAULT_SKIP_TABLES},{env('SKIP_TABLES')}")
	if env('SKIP_TABLE_ENGINES'):
		flags.append('--env=CLICKHOUSE_SKIP_TABLE_ENGINES=' + env('SKIP_TABLE_ENGINES'))
	if env('PARTITIONS'):
		flags.append('--env=CLICKHOUSE_CHECK_REPLICAS_BEFORE_ATTACH=0')
	S.action_env = ' '.join(flags)

def download_args(host):
	S.step_name = S.host_backup[host]
	S.step_args = restore_command('download' if host in S.data_downloads else 'download_schema', host)
	return True

def data_args(host):
	S.step_name = S.host_backup[host]
	S.step_args = restore_command('data', host)
	return True

def drop_downloads(hosts):
	with scoped(best_effort=True, read_only=True):
		for host in hosts:
			name = S.host_backup[host]
			if list_backups(host, 'local') is not None and find_backup(name) and not delete_local(host, name):
				advise(f'{host}: the copy of {name} the restore downloaded is left on local disk')

def other_installation_hint():
	macros, what, fields = [], [], []
	if '{installation}' in S.self_path:
		macros, what, fields = ['{installation}'], ['name'], ['sourceInstallation']
	if '{namespace}' in S.self_path:
		macros.append('{namespace}')
		what.append('namespace')
		fields.append('sourceNamespace')
	if not macros:
		return ''
	return ((f"; the storage path '{S.self_path}' holds {' and '.join(macros)}, which keeps apart the backups "
	         f"of installations of another {' or '.join(what)}: a backup made by one of them is found by naming "
	         f"that installation in the restore ({', '.join(fields)})"))

def unique(items):
	return list(dict.fromkeys(item for item in items if item))

def do_restore():
	check_name('BACKUP_NAME')
	schema_hosts = parse_hosts('SCHEMA_HOSTS')
	restore_data = not is_true('SCHEMA_ONLY')
	data_hosts = parse_hosts('DATA_HOSTS', not restore_data)
	restore_schema, overwrite, validate = True, is_true('OVERWRITE'), is_true('VALIDATE_TOPOLOGY')
	restore_rbac, holds_rbac, replicas = is_true('RESTORE_RBAC'), False, False
	if env('CLUSTER') or overwrite:
		check_cluster()
	check_source_name('SOURCE_INSTALLATION', SOURCE_INSTALLATION_MAX_LENGTH, 'installation name')
	check_source_name('SOURCE_NAMESPACE', SOURCE_NAMESPACE_MAX_LENGTH, 'namespace')
	naming_init()
	S.backup_key = naming_key('BACKUP_NAME')
	tables, name, cluster = env('TABLES'), env('BACKUP_NAME'), env('CLUSTER')
	if tables and (not rx(TABLES_RE, tables) or len(tables) > TABLES_MAX_LENGTH):
		die('TABLES is not a valid table pattern')
	check_restore_filters()
	if env('PARTITIONS'):
		if not restore_data:
			die('PARTITIONS restore data, which SCHEMA_ONLY leaves out')
		if restore_rbac:
			die('PARTITIONS restore no schema, which RESTORE_RBAC restores the access entities with')
		restore_schema, overwrite, schema_hosts = False, False, []
	if overwrite:
		if not rx(NUMBER_RE, env('MAX_TABLE_SIZE_TO_DROP')):
			die('MAX_TABLE_SIZE_TO_DROP must be a number of bytes')
		replicas = any(host not in data_hosts for host in schema_hosts)
	if replicas:
		no_zookeeper()
	used = unique(schema_hosts + (data_hosts if restore_data else []))
	targets = unique(schema_hosts + data_hosts)
	resolve_names(targets)
	guards = parse_hosts('GUARD_HOSTS', True)
	if not restore_schema:
		guards = []
	guarded = unique(targets + guards)
	check_storage_paths()
	source_paths()
	wait_for_apis(guarded)

	event('Normal', 'OrchestratorRestoreStarted', f'restore {name} on {len(targets)} host(s)')
	if overwrite:
		log(f'restore {name}: preflight on {len(targets)} host(s)')
	else:
		log(f'restore {name}: preflight on {len(targets)} host(s), overwrite guard on {len(guarded)}')
	for host in targets:
		call('GET', host, '/backup/version')
		if S.http_code != '200':
			die(f'preflight failed: sidecar of {host} answered HTTP {S.http_code}: {error_of()}')
	if S.source_env:
		for host in used:
			refuse_local_copy(host)
			if not S.source_own_path:
				refuse_own_backup(host)
	if validate:
		hint = other_installation_hint()
		for host in used:
			backup, where = S.host_backup[host], 'the storage of ' + host
			if S.source_env:
				where = f'the storage path \'{S.source_path_shown}\' of {host}'
				source_copy(host)
				if list_backups(host, 'local') is None:
					fail('preflight failed on ' + host)
				row = find_backup(backup)
				if not row:
					die(f'preflight failed: backup {backup} is missing in {where}: its download left no local '
					    'copy')
				base = row['required']
				if base and base in S.own_backups.get(host, set()):
					die(f'preflight failed: backup {backup} is incremental from {base}, and {host} holds a backup '
					    f'{base} at its own storage path too, which clickhouse-backup would take for the one '
					    f'at the storage path \'{S.source_path_shown}\', as it tells the backups of every path '
					    'apart by name alone: delete that one first')
			else:
				if list_backups(host, 'remote') is None:
					fail('preflight failed on ' + host)
				row = find_backup(backup)
				if not row:
					if '{replica}' in S.self_path:
						die(f'preflight failed: backup {backup} is missing in the storage of {host}: the storage '
						    f'path \'{S.self_path}\' holds {LB}replica}}, which gives every replica a path of '
						    f'its own, and the backup was made on the first replica of each shard only{hint}')
					die(f'preflight failed: backup {backup} is missing in the storage of {host}{hint}')
			if row['desc'].startswith('broken'):
				die(f"preflight failed: backup {backup} is {row['desc']} in {where}")
			if (row['rbac_size'] or '0') != '0':
				holds_rbac = True
	if not restore_schema:
		log(f"restore {name}: restores the partitions {env('PARTITIONS')} into the tables as they are, without "
		    "the schema, as the Altinity Cloud Manager does: each of them is dropped first where a table holds "
		    "it")
	elif not overwrite:
		guard_tables(used, guarded)
	elif replicas:
		plan_overwrite(unique([schema_hosts[0]] + data_hosts), guarded)
	else:
		log(f'restore {name}: every shard of cluster {cluster} has one replica, which drops the tables of the '
		    'backup on its own')
		if drop_size_limited():
			S.backup_tables, S.backup_dbs, S.backup_tables_unreadable = {}, {}, ''
			live_tables(schema_hosts)
			refuse_database_drops(schema_hosts, schema_hosts)
	had_local, resumed = [], []
	if not S.source_env:
		for host in used:
			if list_backups(host, 'local') is None:
				continue
			row = find_backup(S.host_backup[host])
			if not row:
				continue
			if row['desc'].startswith('broken'):
				log(f'{host}: holds an incomplete local copy of {S.host_backup[host]}, whose download the restore '
				    'resumes')
				resumed.append(host)
			else:
				had_local.append(host)
	else:
		drop_source_copies(True)
		S.source_copies = list(used)
	restore_env()
	S.restore_kind = 'action' if S.action_env else 'op'

	downloads = [host for host in used if host not in had_local]
	S.data_downloads = data_hosts if restore_data else []
	if downloads:
		with scoped(read_only=True):
			done = run_all(S.restore_kind, downloads, download_args, 'download', name)
		if not done:
			if not S.source_env:
				drop_downloads([host for host in downloads if host not in resumed])
			fail_all('download of ' + name)

	if S.on_cluster:
		if not restore_step(schema_hosts[0], 'restore schema on cluster ' + cluster, 'schema_on_cluster'):
			fail(f'schema restore of {name} on cluster {cluster} failed on {schema_hosts[0]}')
	S.restore_rbac_with_schema = restore_rbac
	for host in schema_hosts:
		if not restore_step(host, 'restore schema', 'schema'):
			fail(f'schema restore of {name} failed on {host}')
	if restore_data:
		if not run_all(S.restore_kind, data_hosts, data_args, 'restore data', name):
			fail_all('data restore of ' + name)
	for host in used:
		if host in had_local:
			continue
		backup = S.host_backup[host]
		if list_backups(host, 'local') is not None and find_backup(backup) and not delete_local(host, backup):
			warn(f'{host}: the downloaded copy of {backup} is left on local disk')
	log(f'restore {name} completed')
	event('Normal', 'OrchestratorRestoreCompleted', f'restore {name} completed')
	report_rbac(restore_rbac, holds_rbac, validate)

def report_rbac(restored, holds, listed):
	name = env('BACKUP_NAME')
	if restored:
		return
	if not listed:
		log(f'whether {name} holds access entities is unknown: its copies in the storage were not listed')
		return
	if not holds:
		return
	warn(name + ' holds access entities - users, roles, grants - which this restore did not restore')
	event('Warning', 'OrchestratorRBACNotRestored', S.last_warning)
	if env('RBAC_NOT_RESTORED_MESSAGE'):
		report_message(env('RBAC_NOT_RESTORED_MESSAGE'))

def main():
	if sys.version_info < (3, 10):
		die('Python 3.10 or newer is required, the image has ' + sys.version.split()[0])
	S.env['API_PORT'] = env('API_PORT')
	S.env['POLL_INTERVAL'] = env('POLL_INTERVAL', '5')
	if not rx(NUMBER_RE, S.env['API_PORT']):
		die('API_PORT must be a port number')
	if not rx(INTERVAL_RE, S.env['POLL_INTERVAL']):
		die('POLL_INTERVAL must be a number of seconds')
	if not env('API_USERNAME') or not env('API_PASSWORD'):
		die('API_USERNAME and API_PASSWORD must be set')
	S.auth = base64.b64encode((f"{env('API_USERNAME')}:{env('API_PASSWORD')}").encode('utf-8')).decode('ascii')
	check_transport()
	operation = {'backup': do_backup, 'verify': do_verify, 'restore': do_restore}.get(env('OPERATION'))
	if operation is None:
		die('OPERATION must be one of backup, verify, restore')
	operation()

if __name__ == '__main__':
	main()
