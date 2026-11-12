# backup-orchestrator.sh drives the clickhouse-backup REST API of one cluster of a ClickHouse installation: that of the
# clickhouse-backup sidecar of every ClickHouse pod (BACKUP_TRANSPORT=sidecar), or that of the backup Pod of every shard
# (BACKUP_TRANSPORT=pod), which the host lists then name by DNS. It makes a backup, verifies one or restores one, and
# prunes the backups of a schedule. It runs in the clickhouse-backup image: bash, curl, sort, awk, base64 and tzdata.
# Every input arrives through the environment (see script.go): no value is ever spliced into this text, and values
# only reach curl as URL-encoded query parameters, or as tokens validated against NAME_RE, TABLES_RE or SOURCE_PATH_RE
# in the commands the script runs through the actions endpoint.
# Every piece of logic is a function, and the script runs main only when it is run, not when it is sourced: tests source
# it and call the functions.
set -euo pipefail

NAME_RE='^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'
HOST_RE='^[A-Za-z0-9]([A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$'
NUMBER_RE='^[0-9]+$'
# A table pattern of a restore: that of a backup, without a leading dash, so it is a single token of a command too.
# Its length is checked apart: the regex library of an Alpine image refuses repetition counts above 255.
TABLES_RE='^[A-Za-z0-9_.*?,][A-Za-z0-9_.*?,-]*$'
# The longest table pattern the API accepts
TABLES_MAX_LENGTH=1024
# The partition ids of a restore, the table engines and the database mapping, "source:target" pairs, it takes: every
# value a single token of a command, as the API validates them. Their lengths are checked apart, as TABLES_MAX_LENGTH
PARTITIONS_RE='^[A-Za-z0-9_-]+(,[A-Za-z0-9_-]+)*$'
ENGINES_RE='^[A-Za-z][A-Za-z0-9_]*(,[A-Za-z][A-Za-z0-9_]*)*$'
DATABASE_MAPPING_RE='^[A-Za-z0-9_][A-Za-z0-9_-]*:[A-Za-z0-9_][A-Za-z0-9_-]*(,[A-Za-z0-9_][A-Za-z0-9_-]*:[A-Za-z0-9_][A-Za-z0-9_-]*)*$'
FILTER_MAX_LENGTH=16384
# The tables clickhouse-backup leaves out by default, which CLICKHOUSE_SKIP_TABLES replaces: a restore that leaves out
# tables of its own keeps them
DEFAULT_SKIP_TABLES='system.*,INFORMATION_SCHEMA.*,information_schema.*,_temporary_and_external_tables.*'

# The index of a cluster in the environment of the storage path check
INDEX_RE='^(0|[1-9][0-9]{0,3})$'
# The name, and the namespace, of the installation a restore reads the backup of: a Kubernetes name. Its length is
# checked apart: SOURCE_INSTALLATION_MAX_LENGTH for the name of an object, SOURCE_NAMESPACE_MAX_LENGTH for a namespace
SOURCE_NAME_RE='^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'
SOURCE_INSTALLATION_MAX_LENGTH=253
SOURCE_NAMESPACE_MAX_LENGTH=63
# A storage path of the installation a restore reads the backup of, as a value of the --env flags of its commands: the
# actions endpoint splits a command as a shell does, which none of these characters takes part in
SOURCE_PATH_RE='^[A-Za-z0-9_./{}-]+$'
# The time a scheduled backup is named after, "<prefix>-<YYYYMMDDHHMMSS>": the wall time of its slot in SCHEDULE_TIMEZONE
TAG_RE='^[0-9]{14}$'
# The number the CronJob controller ends the name of a scheduled Job with, the minutes since the epoch of its slot,
# bounded as in builder.go
SLOT_RE='^[0-9]{1,10}$'
SCHEDULE_TIME_RE='^([01][0-9]|2[0-3]):([0-5][0-9])$'
KEEP_RE='^[1-9][0-9]{0,5}$'
ZONE_RE='^[A-Za-z0-9_+-]+(/[A-Za-z0-9_+-]+)*$'
INTERVAL_RE='^[0-9]+([.][0-9]+)?$'
EXIT_CODE_RE='^[1-9][0-9]?$'
IN_PROGRESS_RE='"status":"in progress"'
TABLE_ROW_RE='^\{"Database":"([^"\\]*)","Name":"([^"\\]*)"'
# A table of a backup in the storage, as GET /backup/tables?remote_backup= lists it
BACKUP_TABLE_ROW_RE='^\{"database":"([^"\\]*)","table":"([^"\\]*)"'
SYSTEM_DATABASES=' system INFORMATION_SCHEMA information_schema _temporary_and_external_tables '
# The error of a request the sidecar refuses while it runs another operation: the actions endpoint answers it with
# HTTP 500, the others with 423
LOCKED_ERROR='another operation is currently running'
# How to get past an overwrite refused for tables larger than max_table_size_to_drop: the operator gives the smallest
# non-zero limit any level of the installation sets, so lifting it takes 0 at every level that sets one
SIZE_LIMIT_ADVICE='set max_table_size_to_drop to 0 wherever a limit is set (the smallest non-zero limit applies), or drop these tables by hand, and restore again'
# The settings the storage path check reads of every cluster, as PathCheckKeys in script.go lists them
PATH_CHECK_KEYS=(
	REMOTE_STORAGE
	S3_BUCKET S3_ENDPOINT S3_PATH S3_OBJECT_DISK_PATH
	GCS_BUCKET GCS_ENDPOINT GCS_PATH GCS_OBJECT_DISK_PATH
	AZBLOB_ACCOUNT_NAME AZBLOB_CONTAINER AZBLOB_ENDPOINT_SUFFIX AZBLOB_PATH AZBLOB_OBJECT_DISK_PATH
	COS_URL COS_PATH COS_OBJECT_DISK_PATH
	FTP_ADDRESS FTP_PATH FTP_OBJECT_DISK_PATH
	SFTP_ADDRESS SFTP_PORT SFTP_PATH SFTP_OBJECT_DISK_PATH
)
REQUEST_TIMEOUT=600
DELETE_TIMEOUT=3600
UNREACHABLE_TIMEOUT=600
MUTATED_UNREACHABLE_TIMEOUT=3600
RESUME_TIMEOUT=300
# How long the backup Pods of the pod transport are waited for to answer, unless BACKUP_POD_WAIT_SECONDS tells
DEFAULT_BACKUP_POD_WAIT_SECONDS=600
# Where the service account of the pod keeps its token and the CA of the Kubernetes API, which Events are posted to
SERVICE_ACCOUNT_DIR=/var/run/secrets/kubernetes.io/serviceaccount
EVENT_TIMEOUT=5
# Events are given up after this many failures in a row: they never fail, nor hold, the operation
EVENT_MAX_FAILURES=3
EVENT_SOURCE_COMPONENT=clickhouse-backup-orchestrator
EVENT_MESSAGE_MAX_LENGTH=1024
EVENT_FAILURES=0
EVENTS_DISABLED=
# The entries of SCHEDULES, as parse_schedules reads them, by index
SCHED_PERIOD=()
SCHED_HOUR=()
SCHED_MINUTE=()
SCHED_DAY=()
SCHED_KEEP=()
SCHED_INCREMENTAL=()
# SLOT_TAG is the wall time of the slot of the run in SCHEDULE_TIMEZONE, as YYYYMMDDHHMMSS; WINNER the index of the
# heaviest schedule entry whose slot it is, empty when none is
SLOT_TAG=
WINNER=
# BACKUP_NAME_GIVEN is set when BACKUP_NAME names the backup: a backup with schedule entries that is no scheduled run
# runs the schedule now
BACKUP_NAME_GIVEN=
# MUTATED is set once a sidecar may have accepted a request that changes something
MUTATED=
# READ_ONLY is set, as a local of a caller, while the requests it submits change nothing the Job is asked to change:
# they leave MUTATED alone
READ_ONLY=
# NO_STEP is set, as a local of a caller, while the actions it runs are no steps of the Job: a sidecar that restarts
# after them has lost nothing the steps to come build on
NO_STEP=
# LAST_WARNING is the last warning, which tells why the step that has just failed failed
LAST_WARNING=
# SELF_PATH is the storage path template of the target cluster, once the storage path check knows it
SELF_PATH=
# SELF_OBJECT_DISK_PATH, SELF_STORAGE_PREFIX and SELF_CLUSTER_NAME are the object disk path template of the target
# cluster, empty when it has none, the settings prefix of its storage and its name, once the storage path check
# knows every path of it, SELF_STORAGE_PREFIX empty until then; SELF_PATHS_UNKNOWN tells why the check does not know them
SELF_OBJECT_DISK_PATH=
SELF_STORAGE_PREFIX=
SELF_CLUSTER_NAME=
SELF_PATHS_UNKNOWN=
# SOURCE_ENV holds the --env flags that point a command at the storage paths of the installation a restore reads the
# backup of, empty for a restore from the paths of its own installation; SOURCE_PATH_SHOWN is the storage path they set,
# with the macros expanded that are the same on every host
SOURCE_ENV=
SOURCE_PATH_SHOWN=
# SOURCE_HINT tells why the storage paths a restore reads the backup of another installation at may hold none of it;
# SOURCE_OWN_PATH is set when the restore reads the backup at the own storage path, its object disk path alone
# being that of the other installation
SOURCE_HINT=
SOURCE_OWN_PATH=
# SOURCE_COPIES lists the hosts a restore from the paths of another installation may have downloaded a copy of the backup
# to, until the copy is deleted again: those its preflight downloaded the schema to, then every host the restore steps
# download the backup to; NO_LOCAL_COPY lists the hosts found without a local copy of the backup, and OWN_BACKUPS, once
# declared, the names of the backups at the own storage path of each host, between blanks
SOURCE_COPIES=()
NO_LOCAL_COPY=()
# ON_CLUSTER is set once the preflight of an overwrite found that it drops the tables of the backup ON CLUSTER
ON_CLUSTER=

log() { printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"; }
warn() {
	LAST_WARNING=$*
	log "WARNING: $*" >&2
}
# advise MESSAGE logs a warning that no step fails for, such as a check that cannot tell, leaving LAST_WARNING to them
advise() {
	log "WARNING: $*" >&2
}
# termination_message MESSAGE writes MESSAGE to the termination message of the container, which the pod keeps in its
# status after the Job and its log are gone. kubelet makes the file writable for any user
termination_message() {
	if [[ -n ${TERMINATION_MESSAGE_PATH:-} ]]; then
		{ printf '%s\n' "$*" >"${TERMINATION_MESSAGE_PATH}"; } 2>/dev/null || true
	fi
}
die() {
	log "ERROR: $*" >&2
	termination_message "$*"
	operation_title
	event Warning "Orchestrator${REPLY}Failed" "$*"
	# A copy of the backup a restore from another installation downloaded would refuse the next restore from the same paths
	if ((${#SOURCE_COPIES[@]} > 0)); then
		drop_source_copies
	fi
	# The exit code tells the operator whether anything was changed: a restore that changed nothing
	# leaves nothing behind that scheduled backups must be held for
	if [[ -z ${MUTATED} && ${UNCHANGED_EXIT_CODE:-} =~ ${EXIT_CODE_RE} ]]; then
		exit "${UNCHANGED_EXIT_CODE}"
	fi
	exit 1
}
# fail MESSAGE dies after a step that has just warned why it failed, adding that reason to MESSAGE
fail() {
	die "$*${LAST_WARNING:+: ${LAST_WARNING}}"
}

# operation_title sets REPLY to OPERATION as it names the reasons of Events: Backup, Verify or Restore, empty for another
operation_title() {
	case ${OPERATION:-} in
	backup) REPLY=Backup ;;
	verify) REPLY=Verify ;;
	restore) REPLY=Restore ;;
	*) REPLY= ;;
	esac
}

# json_escape VALUE sets REPLY to VALUE as the content of a JSON string: a backslash, a quote and every control
# character escaped. Bytes of UTF-8 beyond ASCII pass as they are
json_escape() {
	local value=$1 out= char code
	value=${value//\\/\\\\}
	value=${value//\"/\\\"}
	value=${value//$'\n'/\\n}
	value=${value//$'\r'/\\r}
	value=${value//$'\t'/\\t}
	while [[ ${value} =~ ^([^[:cntrl:]]*)([[:cntrl:]])(.*)$ ]]; do
		out+=${BASH_REMATCH[1]}
		char=${BASH_REMATCH[2]}
		value=${BASH_REMATCH[3]}
		printf -v code '\\u%04x' "'${char}"
		out+=${code}
	done
	REPLY=${out}${value}
}

# truncate_bytes VALUE MAX sets REPLY to VALUE cut to at most MAX bytes, whatever the locale, without the bytes of a
# UTF-8 character the cut splits: what is left is valid UTF-8 when VALUE is
truncate_bytes() {
	local LC_ALL=C value=$1 max=$2 i n
	if ((${#value} <= max)); then
		REPLY=${value}
		return 0
	fi
	value=${value:0:max}
	for ((i = 1; i <= 4 && i <= ${#value}; i++)); do
		printf -v n '%d' "'${value: -i:1}"
		# A byte that continues a character: the character starts before it
		if ((n >= 0x80 && n < 0xc0)); then
			continue
		fi
		# The first byte of a character of 2, 3 or 4 bytes, of which only i are left
		if ((n >= 0xf0 && i < 4 || n >= 0xe0 && i < 3 || n >= 0xc0 && i < 2)); then
			value=${value:0:${#value}-i}
		fi
		break
	done
	REPLY=${value}
}

# event_body TYPE REASON MESSAGE TIME sets REPLY to the core/v1 Event of TYPE (Normal or Warning) with REASON and MESSAGE
# on the object EVENT_OBJECT_* names, in POD_NAMESPACE, at TIME (RFC 3339). A MESSAGE longer than the Kubernetes API
# takes is cut to EVENT_MESSAGE_MAX_LENGTH bytes
event_body() {
	local type=$1 reason=$2 message at=$4 field value body= name=${EVENT_OBJECT_NAME:-}
	truncate_bytes "$3" "${EVENT_MESSAGE_MAX_LENGTH}"
	message=${REPLY}
	local -a fields=(
		generate_name "${name:0:200}." namespace "${POD_NAMESPACE:-}"
		api_version "${EVENT_OBJECT_API_VERSION:-}" kind "${EVENT_OBJECT_KIND:-}" name "${name}" uid "${EVENT_OBJECT_UID:-}"
		reason "${reason}" message "${message}" type "${type}" at "${at}"
		component "${EVENT_SOURCE_COMPONENT}" instance "${POD_NAME:-${HOSTNAME:-}}"
	)
	local -A json=()
	for ((field = 0; field < ${#fields[@]}; field += 2)); do
		json_escape "${fields[field + 1]}"
		json[${fields[field]}]="\"${REPLY}\""
	done
	body='{"apiVersion":"v1","kind":"Event"'
	body+=',"metadata":{"generateName":'${json[generate_name]}',"namespace":'${json[namespace]}'}'
	body+=',"involvedObject":{"apiVersion":'${json[api_version]}',"kind":'${json[kind]}',"name":'${json[name]}
	body+=',"namespace":'${json[namespace]}',"uid":'${json[uid]}'}'
	body+=',"reason":'${json[reason]}',"message":'${json[message]}',"type":'${json[type]}
	body+=',"source":{"component":'${json[component]}'}'
	body+=',"firstTimestamp":'${json[at]}',"lastTimestamp":'${json[at]}',"count":1'
	body+=',"reportingComponent":'${json[component]}',"reportingInstance":'${json[instance]}'}'
	REPLY=${body}
}

# event TYPE REASON MESSAGE posts a Kubernetes Event on the object EVENT_OBJECT_* names - the ClickHouseBackup,
# ClickHouseRestore or ClickHouseBackupSchedule the run works for - with the token of the service account of the pod. It
# does nothing unless all of them, POD_NAMESPACE and the token are there, and an Event that cannot be posted never fails
# the run: after EVENT_MAX_FAILURES failures in a row, none is tried any more
event() {
	local type=$1 reason=$2 message=$3 host=${KUBERNETES_SERVICE_HOST:-} at code token
	if [[ -n ${EVENTS_DISABLED} || -z ${EVENT_OBJECT_KIND:-} || -z ${EVENT_OBJECT_NAME:-} || -z ${EVENT_OBJECT_UID:-} ||
		-z ${EVENT_OBJECT_API_VERSION:-} || -z ${POD_NAMESPACE:-} || -z ${host} || ! -r ${SERVICE_ACCOUNT_DIR}/token ]]; then
		return 0
	fi
	if [[ ${host} == *:* ]]; then
		host="[${host}]"
	fi
	TZ=UTC0 printf -v at '%(%Y-%m-%dT%H:%M:%SZ)T' -1
	event_body "${type}" "${reason}" "${message}" "${at}"
	token=$(<"${SERVICE_ACCOUNT_DIR}/token") || token=
	# The token travels on stdin so it never shows up in the process list
	code=$(printf 'Authorization: Bearer %s\n' "${token}" |
		curl -sS -o /dev/null -w '%{http_code}' --noproxy '*' --cacert "${SERVICE_ACCOUNT_DIR}/ca.crt" \
			--connect-timeout "${EVENT_TIMEOUT}" --max-time "${EVENT_TIMEOUT}" -X POST -H @- \
			-H 'Content-Type: application/json' --data-binary "${REPLY}" \
			"https://${host}:${KUBERNETES_SERVICE_PORT:-443}/api/v1/namespaces/${POD_NAMESPACE}/events" 2>&1) || :
	if [[ ${code} == 2?? ]]; then
		EVENT_FAILURES=0
		return 0
	fi
	EVENT_FAILURES=$((EVENT_FAILURES + 1))
	log "WARNING: cannot post the event ${reason}: ${code}" >&2
	if ((EVENT_FAILURES >= EVENT_MAX_FAILURES)); then
		EVENTS_DISABLED=1
		log "WARNING: no more events are posted after ${EVENT_FAILURES} failures in a row" >&2
	fi
}

# json_get FIELD JSON sets REPLY to the raw (still escaped) value of the first string field FIELD of a JSON object, or
# to that of its first number field FIELD, empty when it has neither. Setting REPLY instead of printing spares the caller
# a subshell, which matters for a backup list of hundreds of rows on every host
json_get() {
	local string='"'"$1"'":"(([^"\\]|\\.)*)"' number='"'"$1"'":([0-9]+)'
	REPLY=
	if [[ $2 =~ ${string} ]] || [[ $2 =~ ${number} ]]; then
		REPLY=${BASH_REMATCH[1]}
	fi
}

# json_field FIELD JSON prints what json_get finds of FIELD in JSON
json_field() {
	json_get "$1" "$2"
	printf '%s' "${REPLY}"
}

# is_true VAR_NAME reports whether the boolean env var VAR_NAME is set to true; anything but true/false is fatal
is_true() {
	local value=${!1:-}
	case ${value,,} in
	true | 1) return 0 ;;
	false | 0 | "") return 1 ;;
	*) die "$1 must be true or false" ;;
	esac
}

check_name() {
	local value=${!1:-}
	# The sidecar silently strips ".." from names, so such a name would not round-trip
	if [[ ! ${value} =~ ${NAME_RE} || ${value} == *..* ]]; then
		die "$1 is not a valid backup name"
	fi
}

# check_cluster fails unless CLUSTER is a single token, as the installation normalizes cluster names
check_cluster() {
	if [[ ! ${CLUSTER:-} =~ ${NAME_RE} ]]; then
		die "CLUSTER is not a valid cluster name"
	fi
}

# listed VALUE ITEM... reports whether VALUE is one of ITEM
listed() {
	local value=$1 item
	shift
	for item in "$@"; do
		if [[ ${item} == "${value}" ]]; then
			return 0
		fi
	done
	return 1
}

# json_string VALUE sets REPLY to VALUE as a JSON string. Only validated tokens reach it: a quote, a backslash or a
# control character, which would take escaping, is fatal
json_string() {
	if [[ $1 == *[\"\\[:cntrl:]]* ]]; then
		die "a command value holds a character that JSON escapes"
	fi
	REPLY="\"$1\""
}

# parse_hosts VAR_NAME [optional] fills HOSTS from the space-separated host list in VAR_NAME,
# an empty list is fatal unless optional
parse_hosts() {
	local host
	HOSTS=()
	read -r -a HOSTS <<<"${!1:-}"
	if ((${#HOSTS[@]} == 0)) && [[ -z ${2:-} ]]; then
		die "$1 is empty: no target hosts"
	fi
	for host in "${HOSTS[@]}"; do
		if [[ ! ${host} =~ ${HOST_RE} ]]; then
			die "$1 contains an invalid host name"
		fi
	done
}

# request METHOD HOST PATH [CURL_ARGS...] performs one API call and sets HTTP_CODE, HTTP_BODY, CURL_EXIT,
# CURL_CONNECTED, empty when the connection was never established, and REQUEST_URL to the URL curl requested, the query
# string of -G as it encoded it, without the scheme. The credentials are in a header, never in the URL
request() {
	local method=$1 host=$2 path=$3 out last connect code url
	shift 3
	HTTP_CODE=000
	CURL_EXIT=0
	CURL_CONNECTED=1
	REQUEST_URL="${host}:${API_PORT}${path}"
	# The credentials travel on stdin so they never show up in the process list
	out=$(printf 'Authorization: Basic %s\n' "${AUTH}" |
		curl -sS -X "${method}" -H @- --connect-timeout 10 --max-time "${REQUEST_TIMEOUT}" \
			-w '\n%{time_connect} %{http_code} %{url_effective}' "$@" "http://${host}:${API_PORT}${path}" 2>&1) || CURL_EXIT=$?
	# What -w writes is always the last line, also when curl fails
	HTTP_BODY=${out%$'\n'*}
	last=${out##*$'\n'}
	read -r connect code url <<<"${last}"
	if [[ ${url} == http://* ]]; then
		REQUEST_URL=${url#http://}
	fi
	if ((CURL_EXIT == 0)); then
		HTTP_CODE=${code}
	elif [[ ${connect} =~ ^0+([.]0+)?$ ]]; then
		CURL_CONNECTED=
	fi
}

# unreachable METHOD reports whether the last request failed in a way that is safe to retry.
# A POST qualifies only when the connection was never established, so it cannot be submitted twice:
# a proxy in front of the sidecar may answer 502-504 to a request the sidecar received.
unreachable() {
	case ${CURL_EXIT} in
	0) [[ $1 == GET ]] && proxy_failed ;;
	6 | 7) return 0 ;;
	*) [[ $1 == GET || -z ${CURL_CONNECTED} ]] ;;
	esac
}

# proxy_failed reports whether the last request was answered by a proxy in front of the sidecar that failed to pass
# it on, or failed waiting for the answer
proxy_failed() {
	[[ ${HTTP_CODE} == 50[234] ]]
}

# call METHOD HOST PATH [CURL_ARGS...] performs an API call, retrying while the sidecar is unreachable
# unless BEST_EFFORT is set
call() {
	local method=$1 host=$2 last= since=${SECONDS}
	while :; do
		request "$@"
		if [[ -n ${BEST_EFFORT:-} ]] || ! unreachable "${method}"; then
			return 0
		fi
		# Giving up is clean only while nothing has changed; afterwards a restarting sidecar is waited for
		# longer rather than leaving a partial backup or restore behind, but not for a host that is gone
		# for good, scaled away or stopped, which would hold every other operation until the Job deadline
		if [[ -z ${MUTATED} ]] && ((SECONDS - since >= UNREACHABLE_TIMEOUT)); then
			die "${host}: sidecar unreachable for $((SECONDS - since))s (curl exit ${CURL_EXIT}, HTTP ${HTTP_CODE}), nothing was changed"
		fi
		if [[ -n ${MUTATED} ]] && ((SECONDS - since >= MUTATED_UNREACHABLE_TIMEOUT)); then
			die "${host}: sidecar unreachable for $((SECONDS - since))s (curl exit ${CURL_EXIT}, HTTP ${HTTP_CODE}), the ${OPERATION} is partially applied"
		fi
		if [[ ${last} != "${CURL_EXIT}/${HTTP_CODE}" ]]; then
			last="${CURL_EXIT}/${HTTP_CODE}"
			log "${host}: sidecar unreachable (curl exit ${CURL_EXIT}, HTTP ${HTTP_CODE}), retrying"
		fi
		sleep "${POLL_INTERVAL}"
	done
}

# error_of prints the sidecar error carried by the last response
error_of() {
	local err
	err=$(json_field error "${HTTP_BODY}")
	printf '%s' "${err:-${HTTP_BODY}}"
}

# wait_idle HOST waits until the sidecar runs no operation, so that a concurrent operation queues the next one
wait_idle() {
	local host=$1 announced=
	while :; do
		call GET "${host}" /backup/actions
		if [[ ${HTTP_CODE} != 200 ]]; then
			warn "${host}: cannot read sidecar actions (HTTP ${HTTP_CODE}): $(error_of)"
			return 1
		fi
		if [[ ! ${HTTP_BODY} =~ ${IN_PROGRESS_RE} ]]; then
			return 0
		fi
		if [[ -z ${announced} ]]; then
			announced=1
			log "${host}: another operation is in progress, waiting"
		fi
		sleep "${POLL_INTERVAL}"
	done
}

# changed records that a sidecar may have accepted a request that changes something, unless READ_ONLY tells that the
# requests of the caller change nothing the Job is asked to change
changed() {
	if [[ -z ${READ_ONLY:-} ]]; then
		MUTATED=1
	fi
}

# command_event HOST WHAT NOTE [CURL_ARGS...] posts the Event OrchestratorCommand of the request the last call POSTed to
# HOST for WHAT: its method, and its host, port, path and query string as curl sent them, with the body of a command of
# the actions endpoint, and NOTE when it may have been accepted without an answer telling so. The credentials travel in
# a header, which no Event shows
command_event() {
	local host=$1 what=$2 note=$3 message body=
	shift 3
	while (($# > 0)); do
		if [[ $1 == --data-binary && $# -gt 1 ]]; then
			body=" $2"
			shift
		fi
		shift
	done
	message="${host}: ${what}: POST ${REQUEST_URL}${body}${note:+ (${note})}"
	log "${message}"
	event Normal OrchestratorCommand "${message}"
}

# finished_event HOST WHAT ID STATUS posts the Event OrchestratorCommandFinished of the operation ID that has just ended
# with STATUS on HOST, with the command the sidecar tells it ran, which HTTP_BODY, its status, holds
finished_event() {
	local command
	json_get command "${HTTP_BODY}"
	command=${REPLY//\\\"/\"}
	command=${command//\\\\/\\}
	event Normal OrchestratorCommandFinished "$1: $2 (operation $3) ended with status ${4:-unknown}${command:+: ${command}}"
}

# submit HOST WHAT PATH [CURL_ARGS...] POSTs a request once the sidecar is idle, queueing while it is busy: HTTP 423, or
# the error the actions endpoint answers a locked sidecar's request with. Neither is accepted, so resubmitting is safe.
# A request the sidecar may have accepted shows in an Event, as command_event tells it
submit() {
	local host=$1 what=$2
	shift 2
	while :; do
		if ! wait_idle "${host}"; then
			return 1
		fi
		call POST "${host}" "$@"
		if ((CURL_EXIT != 0)); then
			# A request that reached the sidecar may have been accepted, only one never sent was not
			if ! unreachable POST; then
				changed
				command_event "${host}" "${what}" "sent, no answer: curl exit ${CURL_EXIT}" "$@"
			fi
			warn "${host}: ${what} request failed: ${HTTP_BODY}"
			return 1
		fi
		if proxy_failed; then
			# The proxy may have passed the request on
			changed
			command_event "${host}" "${what}" "sent, a proxy answered HTTP ${HTTP_CODE}" "$@"
			warn "${host}: ${what} failed (HTTP ${HTTP_CODE}): $(error_of)"
			return 1
		fi
		case ${HTTP_CODE} in
		2??)
			changed
			command_event "${host}" "${what}" "" "$@"
			return 0
			;;
		409 | 423) log "${host}: sidecar busy, queueing ${what}" ;;
		*)
			if [[ $(error_of) != "${LOCKED_ERROR}" ]]; then
				warn "${host}: ${what} rejected (HTTP ${HTTP_CODE}): $(error_of)"
				return 1
			fi
			log "${host}: sidecar busy, queueing ${what}"
			;;
		esac
		sleep "${POLL_INTERVAL}"
	done
}

# remember_step HOST [OPERATION_ID [START COMMAND]] records the last step that succeeded on HOST: the operation id of a
# typed request, or with OPERATION_ID empty the start time and command of the row of an action, which has no operation
# id. HOST alone records no step, as after a restart that took every step along
remember_step() {
	LAST_OP[$1]=${2:-}
	LAST_ACTION_START[$1]=${3:-}
	LAST_ACTION_COMMAND[$1]=${4:-}
}

# sidecar_kept HOST reports whether the sidecar of HOST still knows the last step this Job ran on it. One that has
# restarted since knows none, and may run with another environment - another storage path above all - which
# the steps still to come must not run with: the restore is left partially applied rather than continued
sidecar_kept() {
	local host=$1 i
	if [[ -n ${LAST_OP[${host}]:-} ]]; then
		call GET "${host}" /backup/status -G --data-urlencode "operationid=${LAST_OP[${host}]}"
		if [[ ${HTTP_CODE} != 200 ]]; then
			warn "${host}: cannot read the status of the last step (HTTP ${HTTP_CODE}): $(error_of)"
			return 1
		fi
		if [[ $(json_field status "${HTTP_BODY}") == success ]]; then
			return 0
		fi
	elif [[ -n ${LAST_ACTION_COMMAND[${host}]:-} ]]; then
		if ! action_rows "${host}" "${LAST_ACTION_COMMAND[${host}]}"; then
			return 1
		fi
		for i in "${!ACTION_STARTS[@]}"; do
			if [[ ${ACTION_STARTS[i]} == "${LAST_ACTION_START[${host}]}" && ${ACTION_STATUSES[i]} == success ]]; then
				return 0
			fi
		done
	else
		return 0
	fi
	if [[ -z ${MUTATED} ]]; then
		warn "${host}: the sidecar restarted after the last step of this ${OPERATION} on it, which it no longer knows; nothing was changed"
		return 1
	fi
	warn "${host}: the sidecar restarted after the last step of this ${OPERATION} on it, which it no longer knows: the ${OPERATION} is partially applied"
	return 1
}

# action_rows HOST COMMAND fills ACTION_STARTS, ACTION_STATUSES and ACTION_ERRORS from the rows of the actions log of
# HOST that ran COMMAND, oldest first
action_rows() {
	local host=$1 command=$2 line
	ACTION_STARTS=()
	ACTION_STATUSES=()
	ACTION_ERRORS=()
	call GET "${host}" /backup/actions
	if [[ ${HTTP_CODE} != 200 ]]; then
		warn "${host}: cannot read sidecar actions (HTTP ${HTTP_CODE}): $(error_of)"
		return 1
	fi
	while IFS= read -r line; do
		json_get command "${line}"
		if [[ ${REPLY} != "${command}" ]]; then
			continue
		fi
		json_get start "${line}"
		ACTION_STARTS+=("${REPLY}")
		json_get status "${line}"
		ACTION_STATUSES+=("${REPLY}")
		json_get error "${line}"
		ACTION_ERRORS+=("${REPLY}")
	done <<<"${HTTP_BODY}"
}

# run_action HOST WHAT NAME COMMAND runs COMMAND, a fixed template every value of which is a validated single token,
# through the actions endpoint, and polls it until it succeeds or fails. The answer names no operation: the action is
# the row of COMMAND in the actions log that runs, or that started at a time no such row had before. The sidecar keeps
# the rows of finished actions, and a restart drops them all. An action run with NO_STEP is no step of the Job
run_action() {
	if run_action_steps "$@"; then
		return 0
	fi
	if [[ -z ${NO_STEP:-} ]]; then
		event Warning OrchestratorHostFailed "$1: $2 $3 failed${LAST_WARNING:+: ${LAST_WARNING}}"
	fi
	return 1
}

# run_action_steps HOST WHAT NAME COMMAND is run_action without the Event of a failure
run_action_steps() {
	local rc
	if ! action_start "$@"; then
		return 1
	fi
	while :; do
		sleep "${POLL_INTERVAL}"
		rc=0
		action_poll "$1" "$2" "$3" || rc=$?
		if ((rc != 2)); then
			return "${rc}"
		fi
	done
}

# action_start HOST WHAT NAME COMMAND submits COMMAND to the actions endpoint of HOST, recording in ACTION_COMMAND and
# ACTION_BEFORE what action_poll tells its row by: the command, and the start times of the rows of it before
action_start() {
	local host=$1 what=$2 name=$3 command=$4 body
	log "${host}: ${what} ${name}"
	if ! sidecar_kept "${host}"; then
		return 1
	fi
	json_string "${command}"
	body="{\"command\":${REPLY}}"
	if ! action_rows "${host}" "${command}"; then
		return 1
	fi
	ACTION_COMMAND[${host}]=${command}
	ACTION_BEFORE[${host}]=$(printf '%s\n' "${ACTION_STARTS[@]}")
	if ! submit "${host}" "${what}" /backup/actions --data-binary "${body}"; then
		return 1
	fi
	if [[ $(json_field status "${HTTP_BODY}") != acknowledged ]]; then
		warn "${host}: ${what} was not acknowledged: ${HTTP_BODY}"
		return 1
	fi
}

# action_poll HOST WHAT NAME reads once the row of the action action_start submitted to HOST: it returns 0 once the
# action succeeded, 2 while it runs and 1 when it failed, with a warning that tells why
action_poll() {
	local host=$1 what=$2 name=$3 command=${ACTION_COMMAND[$1]} i start= status= error=
	if ! action_rows "${host}" "${command}"; then
		return 1
	fi
	for ((i = ${#ACTION_STARTS[@]} - 1; i >= 0; i--)); do
		if [[ ${ACTION_STATUSES[i]} == "in progress" || $'\n'"${ACTION_BEFORE[${host}]}"$'\n' != *$'\n'"${ACTION_STARTS[i]}"$'\n'* ]]; then
			start=${ACTION_STARTS[i]} status=${ACTION_STATUSES[i]} error=${ACTION_ERRORS[i]}
			break
		fi
	done
	case ${status} in
	"in progress") return 2 ;;
	success)
		log "${host}: ${what} ${name} succeeded"
		if [[ -z ${NO_STEP:-} ]]; then
			remember_step "${host}" "" "${start}" "${command}"
			event Normal OrchestratorHostStepSucceeded "${host}: ${what} ${name} succeeded"
		fi
		return 0
		;;
	"")
		if [[ -n ${NO_STEP:-} ]]; then
			warn "${host}: the sidecar restarted during ${what} ${name}"
			return 1
		fi
		warn "${host}: the sidecar restarted during ${what} ${name}, which it does not resume: the ${OPERATION} is partially applied"
		return 1
		;;
	*)
		warn "${host}: ${what} ${name} ended with status '${status}': ${error}"
		return 1
		;;
	esac
}

# run_op HOST WHAT NAME PATH [CURL_ARGS...] starts an asynchronous sidecar operation on backup NAME
# and polls it until it succeeds or fails
run_op() {
	if run_op_steps "$@"; then
		return 0
	fi
	event Warning OrchestratorHostFailed "$1: $2 $3 failed${LAST_WARNING:+: ${LAST_WARNING}}"
	return 1
}

# run_op_steps HOST WHAT NAME PATH [CURL_ARGS...] is run_op without the Event of a failure
run_op_steps() {
	local rc
	if ! op_start "$@"; then
		return 1
	fi
	while :; do
		sleep "${POLL_INTERVAL}"
		rc=0
		op_poll "$1" "$2" "$3" || rc=$?
		if ((rc != 2)); then
			return "${rc}"
		fi
	done
}

# op_start HOST WHAT NAME PATH [CURL_ARGS...] submits an asynchronous sidecar operation on backup NAME to HOST, recording
# its operation id in OP_ID
op_start() {
	local host=$1 what=$2 name=$3 id
	shift 3
	log "${host}: ${what} ${name}"
	if ! sidecar_kept "${host}"; then
		return 1
	fi
	if ! submit "${host}" "${what}" "$@"; then
		return 1
	fi
	if [[ $(json_field backup_name "${HTTP_BODY}") != "${name}" ]]; then
		warn "${host}: ${what} acknowledged a different backup: ${HTTP_BODY}"
		return 1
	fi
	id=$(json_field operation_id "${HTTP_BODY}")
	if [[ -z ${id} ]]; then
		warn "${host}: ${what} returned no operation id: ${HTTP_BODY}"
		return 1
	fi
	OP_ID[${host}]=${id}
}

# op_poll HOST WHAT NAME reads once the status of the operation op_start submitted to HOST: it returns 0 once the
# operation succeeded, 2 while it runs and 1 when it failed, with a warning that tells why
op_poll() {
	local host=$1 what=$2 name=$3 id=${OP_ID[$1]} status
	call GET "${host}" /backup/status -G --data-urlencode "operationid=${id}"
	if [[ ${HTTP_CODE} != 200 ]]; then
		warn "${host}: cannot read status of ${what} (HTTP ${HTTP_CODE}): $(error_of)"
		return 1
	fi
	status=$(json_field status "${HTTP_BODY}")
	if [[ -n ${status} && ${status} != "in progress" ]]; then
		finished_event "${host}" "${what} ${name}" "${id}" "${status}"
	fi
	case ${status} in
	"in progress") return 2 ;;
	success)
		log "${host}: ${what} ${name} succeeded"
		remember_step "${host}" "${id}"
		event Normal OrchestratorHostStepSucceeded "${host}: ${what} ${name} succeeded"
		return 0
		;;
	"")
		if ! lost_op "${host}" "${what}" "${name}"; then
			return 1
		fi
		# The restart that lost the operation took every step before it along
		remember_step "${host}"
		return 0
		;;
	*)
		warn "${host}: ${what} ${name} ended with status '${status}': $(error_of)"
		return 1
		;;
	esac
}

# run_all KIND HOSTS CHECK WHAT NAME ARGS... runs a step on every host of the space-separated HOSTS at once: an
# operation, KIND op, as run_op runs it with the arguments ARGS, or an action, KIND action, as run_action runs the
# command ARGS. It submits the step to one host after the other, each once the function CHECK, unless empty, has
# passed for it - CHECK may set STEP_ARGS, which hold ARGS until then, to the arguments of the host -, then polls every
# host it was submitted to until it runs on none. A host whose CHECK or submission fails
# gets the step submitted to no other host after it, and those it was submitted to are waited for all the same.
# FAILED_HOSTS and FAILURES list the hosts that failed and the warning of each, STEP_FAILED_HOSTS those of them the step
# itself failed on, whose CHECK passed; it returns 1 when any did
run_all() {
	local kind=$1 check=$3 what=$4 name=$5 host rc
	local -a hosts pending=() running
	read -r -a hosts <<<"$2"
	shift 5
	FAILED_HOSTS=()
	FAILURES=()
	STEP_FAILED_HOSTS=()
	for host in "${hosts[@]}"; do
		LAST_WARNING=
		STEP_ARGS=("$@")
		if [[ -n ${check} ]] && ! "${check}" "${host}"; then
			step_failed "${kind}" "${host}" "${what}" "${name}"
			break
		fi
		if ! "${kind}_start" "${host}" "${what}" "${name}" "${STEP_ARGS[@]}"; then
			step_failed "${kind}" "${host}" "${what}" "${name}"
			STEP_FAILED_HOSTS+=("${host}")
			break
		fi
		pending+=("${host}")
	done
	while ((${#pending[@]} > 0)); do
		sleep "${POLL_INTERVAL}"
		running=()
		for host in "${pending[@]}"; do
			rc=0
			LAST_WARNING=
			"${kind}_poll" "${host}" "${what}" "${name}" || rc=$?
			case ${rc} in
			0) ;;
			2) running+=("${host}") ;;
			*)
				step_failed "${kind}" "${host}" "${what}" "${name}"
				STEP_FAILED_HOSTS+=("${host}")
				;;
			esac
		done
		pending=(${running[@]+"${running[@]}"})
	done
	((${#FAILED_HOSTS[@]} == 0))
}

# step_failed KIND HOST WHAT NAME records in FAILED_HOSTS and FAILURES that the step WHAT of run_all failed on HOST, as
# LAST_WARNING tells, with the Event run_op or run_action posts
step_failed() {
	FAILED_HOSTS+=("$2")
	FAILURES+=("${LAST_WARNING:-$2: $3 $4 failed}")
	if [[ $1 == op || -z ${NO_STEP:-} ]]; then
		event Warning OrchestratorHostFailed "$2: $3 $4 failed${LAST_WARNING:+: ${LAST_WARNING}}"
	fi
}

# fail_all MESSAGE dies after run_all failed, adding to MESSAGE the hosts that failed and the warning of each
fail_all() {
	local hosts failure
	join_items "${FAILED_HOSTS[@]}"
	hosts=${REPLY}
	if ((${#FAILURES[@]} == 1)); then
		die "$1 failed on ${hosts}: ${FAILURES[0]}"
	fi
	REPLY=
	for failure in "${FAILURES[@]}"; do
		REPLY+=${REPLY:+; }${failure}
	done
	die "$1 failed on ${hosts}: ${REPLY}"
}

# resumed_action HOST COMMAND NAME sets RESUMED_STATUS and RESUMED_ERROR from the last COMMAND (upload or download)
# of backup NAME that the sidecar resumed after a restart, RESUMED_STATUS empty when there is none
resumed_action() {
	local host=$1 command=$2 name=$3 line
	RESUMED_STATUS=
	RESUMED_ERROR=
	call GET "${host}" /backup/actions
	if [[ ${HTTP_CODE} != 200 ]]; then
		warn "${host}: cannot read sidecar actions (HTTP ${HTTP_CODE}): $(error_of)"
		return 1
	fi
	while IFS= read -r line; do
		# A resumed action has no operation id and runs "<command> [options] --resumable=1 <name>"
		if [[ $(json_field command "${line}") == "${command} "*"--resumable=1 ${name}" ]]; then
			RESUMED_STATUS=$(json_field status "${line}")
			RESUMED_ERROR=$(json_field error "${line}")
		fi
	done <<<"${HTTP_BODY}"
}

# lost_op HOST WHAT NAME settles an operation that a restarted sidecar no longer knows. The sidecar resumes an
# interrupted upload or download by itself, as an action of its own, which is waited for instead; a backup that is
# complete where the operation puts it completed before the restart. Anything else leaves the outcome unknown.
# Listing backups counts as an action of the sidecar, and a sidecar that finds an action in progress resumes
# nothing, so the backups are listed only once the wait for a resumed action is over.
lost_op() {
	local host=$1 what=$2 name=$3 resumed where outcome since=${SECONDS}
	case ${what} in
	create_remote) resumed=upload where=remote outcome="the remote backup ${name} may still complete" ;;
	download) resumed=download where=local outcome="the download of ${name} may still complete" ;;
	*)
		warn "${host}: the sidecar restarted during ${what} ${name}, which it does not resume: the ${OPERATION} is partially applied"
		return 1
		;;
	esac
	log "${host}: the sidecar restarted during ${what} ${name}, waiting for it to resume the ${resumed}"
	while :; do
		if ! resumed_action "${host}" "${resumed}" "${name}"; then
			return 1
		fi
		case ${RESUMED_STATUS} in
		"in progress")
			# A sidecar that restarts again resumes again: the wait starts over
			since=${SECONDS}
			;;
		success)
			log "${host}: the resumed ${resumed} of ${name} succeeded"
			return 0
			;;
		"")
			if ((SECONDS - since >= RESUME_TIMEOUT)); then
				if has_usable_backup "${host}" "${where}" "${name}"; then
					log "${host}: ${what} ${name} completed before the sidecar restarted"
					return 0
				fi
				warn "${host}: the sidecar restarted during ${what} ${name} and resumed nothing within ${RESUME_TIMEOUT}s, the outcome is unknown: ${outcome}"
				return 1
			fi
			;;
		*)
			warn "${host}: the resumed ${resumed} of ${name} ended with status '${RESUMED_STATUS}': ${RESUMED_ERROR}"
			return 1
			;;
		esac
		sleep "${POLL_INTERVAL}"
	done
}

# list_backups HOST local|remote fills LIST_NAMES, LIST_REQUIRED, LIST_DESC and the LIST_*_SIZE arrays from the sidecar
# backup list
list_backups() {
	local host=$1 where=$2 line
	LIST_NAMES=()
	LIST_REQUIRED=()
	LIST_DESC=()
	LIST_DATA_SIZE=()
	LIST_OBJECT_DISK_SIZE=()
	LIST_METADATA_SIZE=()
	LIST_RBAC_SIZE=()
	call GET "${host}" "/backup/list/${where}"
	if [[ ${HTTP_CODE} != 200 ]]; then
		warn "${host}: cannot list ${where} backups (HTTP ${HTTP_CODE}): $(error_of)"
		return 1
	fi
	while IFS= read -r line; do
		if [[ -n ${line} ]]; then
			json_get name "${line}"
			LIST_NAMES+=("${REPLY}")
			json_get required "${line}"
			LIST_REQUIRED+=("${REPLY}")
			json_get desc "${line}"
			LIST_DESC+=("${REPLY}")
			json_get data_size "${line}"
			LIST_DATA_SIZE+=("${REPLY}")
			json_get object_disk_size "${line}"
			LIST_OBJECT_DISK_SIZE+=("${REPLY}")
			json_get metadata_size "${line}"
			LIST_METADATA_SIZE+=("${REPLY}")
			json_get rbac_size "${line}"
			LIST_RBAC_SIZE+=("${REPLY}")
		fi
	done <<<"${HTTP_BODY}"
}

# find_backup NAME sets FOUND to the index of NAME in the last list, or returns 1
find_backup() {
	local i
	for i in "${!LIST_NAMES[@]}"; do
		if [[ ${LIST_NAMES[i]} == "$1" ]]; then
			FOUND=${i}
			return 0
		fi
	done
	return 1
}

# has_usable_backup HOST local|remote NAME reports whether HOST lists NAME as a complete backup
has_usable_backup() {
	list_backups "$1" "$2" && find_backup "$3" && [[ ${LIST_DESC[FOUND]} != broken* ]]
}

# delete_local HOST NAME drops the local copy of NAME; force, because other local copies may be based on it
delete_local() {
	if ! submit "$1" "delete local $2" "/backup/delete/local/$2" -G --data-urlencode "force=true"; then
		return 1
	fi
	log "$1: local copy of $2 deleted"
}

# holds_data INDEX reports whether backup INDEX of the last list holds data, locally or on an object disk. Every
# backup of a sidecar older than 2.6.6, which lists no sizes, counts: metadata_size tells it apart, as the one size
# always listed, while the others are left out when zero
holds_data() {
	# Sizes are compared as text: a size of 2^63 or more would wrap negative in bash arithmetic
	[[ -z ${LIST_METADATA_SIZE[$1]} ]] || [[ ${LIST_DATA_SIZE[$1]:-0} != 0 ]] || [[ ${LIST_OBJECT_DISK_SIZE[$1]:-0} != 0 ]]
}

# list_all PATTERN HOST... lists the backups of every HOST and fills the sets COMPLETE, with the names matching
# PATTERN of the backups every HOST lists complete, WITH_DATA, with those of the backups that hold data on any HOST,
# EMPTY, with those of the backups a HOST lists complete with a size of 0, nothing at all, SIZED, with those of the
# backups any HOST lists, broken or not, with a size above 0 of anything, BROKEN, with those any HOST lists broken, and
# ALL_NAMES, with those of every backup any
# HOST lists, broken ones too, and UNLISTED with the hosts that cannot be listed. A run that failed on one host leaves copies on the others that cannot be restored on their own, and the copies
# of one backup on a host whose tables hold no data are as much part of it as those that do
list_all() {
	local pattern=$1 host first=1 i name
	local -A listed both
	declare -gA COMPLETE=() WITH_DATA=() EMPTY=() SIZED=() BROKEN=() ALL_NAMES=()
	UNLISTED=()
	shift
	for host in "$@"; do
		if [[ ! ${host} =~ ${HOST_RE} ]]; then
			continue
		fi
		if ! list_backups "${host}" remote; then
			UNLISTED+=("${host}")
			continue
		fi
		listed=()
		for i in "${!LIST_NAMES[@]}"; do
			if [[ ! ${LIST_NAMES[i]} =~ ${pattern} ]]; then
				continue
			fi
			ALL_NAMES[${LIST_NAMES[i]}]=1
			# Sizes are compared as text, as holds_data does; a sidecar older than 2.6.6 lists none
			if [[ -n ${LIST_METADATA_SIZE[i]} ]] && [[ ${LIST_DATA_SIZE[i]:-0} != 0 || ${LIST_OBJECT_DISK_SIZE[i]:-0} != 0 ||
				${LIST_METADATA_SIZE[i]} != 0 || ${LIST_RBAC_SIZE[i]:-0} != 0 ]]; then
				SIZED[${LIST_NAMES[i]}]=1
			fi
			if [[ ${LIST_DESC[i]} == broken* ]]; then
				BROKEN[${LIST_NAMES[i]}]=1
				continue
			fi
			listed[${LIST_NAMES[i]}]=1
			if holds_data "${i}"; then
				WITH_DATA[${LIST_NAMES[i]}]=1
			fi
			# A sidecar older than 2.6.6 lists no sizes, which leaves the size unknown rather than 0
			if [[ -n ${LIST_METADATA_SIZE[i]} && ${LIST_DATA_SIZE[i]:-0} == 0 && ${LIST_OBJECT_DISK_SIZE[i]:-0} == 0 &&
				${LIST_METADATA_SIZE[i]} == 0 && ${LIST_RBAC_SIZE[i]:-0} == 0 ]]; then
				EMPTY[${LIST_NAMES[i]}]=1
			fi
		done
		both=()
		for name in "${!listed[@]}"; do
			if [[ -n ${first} || -n ${COMPLETE[${name}]:-} ]]; then
				both[${name}]=1
			fi
		done
		COMPLETE=()
		for name in "${!both[@]}"; do
			COMPLETE[${name}]=1
		done
		first=
	done
}

# series_pattern sets REPLY to the regex of the names of the scheduled backups, "<prefix>-<YYYYMMDDHHMMSS>"
series_pattern() {
	REPLY="^${BACKUP_NAME_PREFIX//./[.]}-[0-9]{14}$"
}

# retention_pattern sets REPLY to the regex of the names retention looks at: those of the scheduled backups, and those
# of the backups made at once from the schedule, "<prefix>-<YYYYMMDD>T<HHMMSS>"
retention_pattern() {
	REPLY="^${BACKUP_NAME_PREFIX//./[.]}-[0-9]{8}T?[0-9]{6}$"
}

# retention_plan decides, as the cleanup of remote backups of the Altinity Cloud Manager does, which backups of the
# schedule retention keeps, KEEP, and which it deletes, OBSOLETE, newest first, of NAMES, the backups of the
# retention_pattern any retention host lists, as list_all classified them:
#  - a backup not every host lists complete - failed, or in progress - never counts: it is deleted, on every host that
#    holds it, once it is older than the newest backup every host lists complete, if a host lists a size above 0 of it,
#    as the Altinity Cloud Manager deletes none of size 0, or lists it broken - an upload that failed, whatever its
#    size; one newer is left, as it may be in progress;
#  - a backup a host lists complete of size 0, empty, is neither counted nor deleted, nor is one whose name tells no
#    valid time;
#  - the newest backup is always kept, as the base of the next incremental one, and so is the one just made;
#  - every entry of the schedule keeps the newest keep backups of its slots: a backup may be kept by several, the
#    weekly one counts for the daily entry too;
#  - a backup made at once from the schedule, "<prefix>-<YYYYMMDD>T<HHMMSS>", is kept while it is no older than the
#    oldest scheduled backup an entry keeps;
#  - anything else is obsolete. Incremental chains are not protected: deleted newest first, a dependent goes before its
#    base, and clickhouse-backup rebases the dependents of a base it deletes (rebase_during_delete), so the kept
#    incremental backups stay restorable
retention_plan() {
	local name tag key i oldest= first=1 newest=
	local -a ordered=() manual=() counts=() incomplete=() obsolete=()
	declare -gA KEEP=()
	OBSOLETE=()
	for name in "${NAMES[@]}"; do
		tag=${name:${#BACKUP_NAME_PREFIX}+1}
		key=${tag/T/}
		if ! tag_valid "${key}"; then
			continue
		fi
		if [[ -z ${COMPLETE[${name}]:-} ]]; then
			if [[ -n ${SIZED[${name}]:-}${BROKEN[${name}]:-} && ${name} != "${BACKUP_NAME:-}" ]]; then
				incomplete+=("${key} ${name}")
			fi
		elif [[ -z ${EMPTY[${name}]:-} ]]; then
			ordered+=("${key} ${name}")
		fi
	done
	if ((${#ordered[@]} > 0)); then
		mapfile -t ordered < <(printf '%s\n' "${ordered[@]}" | sort -r)
		newest=${ordered[0]%% *}
	fi
	for i in "${!SCHED_PERIOD[@]}"; do
		counts[i]=0
	done
	for tag in "${ordered[@]}"; do
		key=${tag%% *}
		name=${tag#* }
		if [[ -n ${first} || ${name} == "${BACKUP_NAME:-}" ]]; then
			KEEP[${name}]=1
			first=
		fi
		if [[ ${name:${#BACKUP_NAME_PREFIX}+1} == *T* ]]; then
			manual+=("${tag}")
			continue
		fi
		for i in "${!SCHED_PERIOD[@]}"; do
			if slot_matches "${i}" "${key}"; then
				counts[i]=$((counts[i] + 1))
				if ((counts[i] <= SCHED_KEEP[i])); then
					KEEP[${name}]=1
					# The backups come newest first
					oldest=${key}
				fi
			fi
		done
	done
	for tag in "${manual[@]}"; do
		key=${tag%% *}
		if [[ -z ${oldest} || ! ${key} < ${oldest} ]]; then
			KEEP[${tag#* }]=1
		fi
	done
	for tag in "${ordered[@]}"; do
		if [[ -z ${KEEP[${tag#* }]:-} ]]; then
			obsolete+=("${tag}")
		fi
	done
	for tag in "${incomplete[@]}"; do
		if [[ -n ${newest} && ${tag%% *} < ${newest} ]]; then
			obsolete+=("${tag}")
		fi
	done
	# Newest first, the incomplete backups among the others
	if ((${#obsolete[@]} > 0)); then
		mapfile -t obsolete < <(printf '%s\n' "${obsolete[@]}" | sort -r)
		for tag in "${obsolete[@]}"; do
			OBSOLETE+=("${tag#* }")
		done
	fi
}

# retention_on_host HOST deletes on HOST the backups of OBSOLETE it lists, newest first
retention_on_host() {
	local host=$1 name
	local -A held=()
	if ! list_backups "${host}" remote; then
		return 1
	fi
	for name in "${LIST_NAMES[@]}"; do
		if [[ -n ${name} ]]; then
			held[${name}]=1
		fi
	done
	for name in "${OBSOLETE[@]}"; do
		if [[ -z ${held[${name}]:-} ]]; then
			continue
		fi
		if submit "${host}" "delete remote ${name}" "/backup/delete/remote/${name}" --max-time "${DELETE_TIMEOUT}"; then
			log "${host}: retention deleted ${name}"
			event Normal OrchestratorRetentionDeleted "${host}: retention deleted ${name}"
		else
			event Warning OrchestratorRetentionFailed "${LAST_WARNING}"
		fi
	done
}

# retention prunes the backups of the schedule after a successful scheduled backup, as retention_plan decides, on every
# host of RETENTION_HOSTS: one per shard. Nothing is deleted while a restore of the cluster runs, which may read them.
# A failure only warns
retention() {
	local host
	if ((${#SCHED_PERIOD[@]} == 0)); then
		return 0
	fi
	if is_true RESTORE_IN_PROGRESS; then
		warn "retention skipped: a restore of the cluster is in progress"
		return 0
	fi
	if [[ -z ${BACKUP_NAME_PREFIX:-} ]]; then
		warn "retention skipped: BACKUP_NAME_PREFIX is not set"
		return 0
	fi
	read -r -a HOSTS <<<"${RETENTION_HOSTS:-}"
	if ((${#HOSTS[@]} == 0)); then
		warn "retention skipped: RETENTION_HOSTS is empty"
		return 0
	fi
	log "retention: keeping the newest ${BACKUP_NAME_PREFIX}-* backup, the newest backups of the slots of every schedule entry it keeps, and the backups made at once no older than the oldest of those"
	# An unreachable host skips retention until the next run instead of holding the Job until its deadline
	BEST_EFFORT=1
	retention_pattern
	list_all "${REPLY}" "${HOSTS[@]}"
	# Every host keeps and deletes the same backups, which takes knowing what each of them holds
	if ((${#UNLISTED[@]} > 0)); then
		warn "retention skipped: ${UNLISTED[*]} cannot be listed, backups are left as they are"
		event Warning OrchestratorRetentionSkipped "${LAST_WARNING}"
		return 0
	fi
	NAMES=("${!ALL_NAMES[@]}")
	retention_plan
	for host in "${HOSTS[@]}"; do
		if [[ ! ${host} =~ ${HOST_RE} ]]; then
			warn "retention skipped an invalid host name"
		elif ! retention_on_host "${host}"; then
			warn "${host}: retention failed, backups are left as they are"
		fi
	done
}

# report_data HOST... tells when the backup just made holds no data, of tables that match nothing or an installation
# recreated empty: it then writes NO_DATA_MESSAGE to the termination message, which the operator reports on the
# backup's resource after the Job and its log are gone. The backup holds data when any HOST's copy of it does; a HOST
# that cannot be listed leaves that unknown, and so does a sidecar older than 2.6.6, which lists no sizes, so nothing
# is told then
report_data() {
	# The backup is made: a host that does not answer is asked once rather than waited for
	local BEST_EFFORT=1
	list_all "^${BACKUP_NAME//./[.]}$" "$@"
	if ((${#UNLISTED[@]} > 0)); then
		log "whether ${BACKUP_NAME} holds data is unknown: ${UNLISTED[*]} cannot be listed"
		return 0
	fi
	if [[ -z ${COMPLETE[${BACKUP_NAME}]:-} || -n ${WITH_DATA[${BACKUP_NAME}]:-} ]]; then
		return 0
	fi
	warn "${BACKUP_NAME} holds no data"
	event Warning OrchestratorNoData "${BACKUP_NAME} holds no data"
	if [[ -n ${NO_DATA_MESSAGE:-} ]]; then
		termination_message "${NO_DATA_MESSAGE}"
	fi
}

# storage_prefix TYPE sets REPLY to the settings prefix of the storage type TYPE, as REMOTE_STORAGE names it, empty
# for one whose paths the storage path check does not know
storage_prefix() {
	case $1 in
	s3) REPLY=S3 ;;
	gcs) REPLY=GCS ;;
	azblob) REPLY=AZBLOB ;;
	cos) REPLY=COS ;;
	ftp) REPLY=FTP ;;
	sftp) REPLY=SFTP ;;
	*) REPLY= ;;
	esac
}

# path_setting INDEX NAME sets REPLY to the path check setting NAME of the cluster at INDEX, and SETTING_SET when the
# Job environment sets it: kubelet leaves out an optional Secret key the Secret lacks, and sets one empty that is so
path_setting() {
	local name="PATH_$1_$2"
	REPLY=${!name-}
	SETTING_SET=${!name+1}
}

# identity_default KEY sets REPLY to the value clickhouse-backup takes for the setting KEY of the storage path check when
# its environment leaves the setting unset: its default, empty for one that has none, as for a setting set empty
identity_default() {
	case $1 in
	AZBLOB_ENDPOINT_SUFFIX) REPLY=core.windows.net ;;
	SFTP_PORT) REPLY=22 ;;
	*) REPLY= ;;
	esac
}

# trim_path STORAGE PATH sets REPLY to PATH as clickhouse-backup trims it: of blanks and slashes at both ends, or for ftp
# and sftp, whose paths may be absolute, of blanks at both ends and then of slashes at the end
trim_path() {
	local value=$2 both=$' \t\r\n/' trailing=
	if [[ $1 == ftp || $1 == sftp ]]; then
		both=$' \t\r\n'
		trailing=/
	fi
	while [[ -n ${value} && ${value:0:1} == ["${both}"] ]]; do
		value=${value:1}
	done
	while [[ -n ${value} && ${value: -1} == ["${both}"] ]]; do
		value=${value::-1}
	done
	while [[ -n ${trailing} && ${value} == */ ]]; do
		value=${value::-1}
	done
	REPLY=${value}
}

# clean_path PATH sets REPLY to the location PATH names as clickhouse-backup joins it with every key it keeps there, as
# path.Join cleans it: without empty and "." elements, each ".." taking the element before it along, and empty for the
# place a relative path starts from
clean_path() {
	local rest=$1 element rooted= IFS=/
	local -a elements=()
	if [[ ${rest} == /* ]]; then
		rooted=/
	fi
	while [[ -n ${rest} ]]; do
		element=${rest%%/*}
		if [[ ${rest} == */* ]]; then
			rest=${rest#*/}
		else
			rest=
		fi
		case ${element} in
		"" | .) ;;
		..)
			if ((${#elements[@]} > 0)) && [[ ${elements[-1]} != .. ]]; then
				unset 'elements[-1]'
			elif [[ -z ${rooted} ]]; then
				elements+=(..)
			fi
			;;
		*) elements+=("${element}") ;;
		esac
	done
	REPLY=${rooted}${elements[*]}
}

# expand_macros TEMPLATE CLUSTER SHARD sets REPLY to TEMPLATE with the macros clickhouse-backup replaces from those the
# operator gives every host - {installation}, {namespace}, {cluster}, {shard} - replaced, and {replica} kept, as it
# differs on every host. Any other macro, which the check cannot know the value of, fails, leaving its name in
# UNKNOWN_MACRO
expand_macros() {
	local rest=$1 out= macro name
	while [[ ${rest} =~ \{([^{}]*)\} ]]; do
		macro=${BASH_REMATCH[0]}
		name=${BASH_REMATCH[1]}
		out+=${rest%%"${macro}"*}
		rest=${rest#*"${macro}"}
		case ${name} in
		installation) out+=${PATH_INSTALLATION:-} ;;
		namespace) out+=${PATH_NAMESPACE:-} ;;
		cluster) out+=$2 ;;
		shard) out+=$3 ;;
		replica) out+=${macro} ;;
		*)
			UNKNOWN_MACRO=${macro}
			return 1
			;;
		esac
	done
	REPLY=${out}${rest}
}

# default_path INDEX NAME sets REPLY to the default path NAME - DEFAULT_PATH or DEFAULT_OBJECT_DISK_PATH - that the
# sidecar of the cluster at INDEX sets when its settings leave the path unset: the cluster's own when the Job environment
# sets one, else that of every cluster, empty when there is none and an unset path is the root of the storage
default_path() {
	local own="PATH_$1_$2"
	if [[ -n ${!own+set} ]]; then
		REPLY=${!own}
	else
		REPLY=${!2:-}
	fi
}

# path_check_inconclusive COUNT NAME WHAT handles a target cluster whose storage paths the check cannot tell, for the
# reason WHAT, in an installation of COUNT backup-enabled clusters. Alone, it skips the check with a warning; beside
# other clusters it fails the Job before anything changes. Two clusters at one path would both make the backup of one
# run there, as the Jobs of one backup run at the same time under one name, which then lists as one backup of either;
# and a restore of one of them would restore what the other made there
path_check_inconclusive() {
	SELF_PATHS_UNKNOWN=$3
	if (($1 > 1)); then
		die "the storage path check cannot tell whether cluster $2 shares a storage path with another of the $1 backup-enabled clusters of the installation, which would mix their backups and restore one with the data of the other: $3"
	fi
	advise "the storage path check is skipped: $3"
}

# check_storage_paths refuses, before anything changes, a storage path the target cluster shares. Every shard of
# every backup-enabled cluster of the installation keeps its backups, and the object disk data of them, at a path of its
# own in its storage: two shards that share one find each other's backups as their own, restore the wrong data
# and prune it. A backup of the first replica of each shard has to be at a path without {replica}, as every replica of
# the shard restores its schema from its own path; one of every replica needs {replica}, which gives each a path of its
# own. Another cluster whose storage or path macros the check does not know - a configuration file of its
# sidecar other than the one the operator generates, say - is left out of it with a warning: only a path proven shared
# is refused.
# The target cluster's own unknown paths fail the Job, unless it is the only backup-enabled cluster.
check_storage_paths() {
	local index storage prefix identity key template object shard name what value self=${PATH_SELF:-} self_name=
	local selection=${PATH_REPLICA_SELECTION:-}
	local -a indexes others=() shards
	local -A owners=() self_owned=()
	read -r -a indexes <<<"${PATH_CLUSTERS:-}"
	if ((${#indexes[@]} == 0)); then
		SELF_PATHS_UNKNOWN="PATH_CLUSTERS names no cluster"
		advise "the storage path check is skipped: ${SELF_PATHS_UNKNOWN}"
		return 0
	fi
	for index in "${indexes[@]}"; do
		if [[ ! ${index} =~ ${INDEX_RE} ]]; then
			die "PATH_CLUSTERS must list cluster indexes"
		fi
		if [[ ${index} != "${self}" ]]; then
			others+=("${index}")
		fi
	done
	if ! listed "${self}" "${indexes[@]}"; then
		die "PATH_SELF must be one of the indexes PATH_CLUSTERS lists"
	fi
	case ${selection} in
	"" | FirstPerShard | AllReplicas) ;;
	*) die "PATH_REPLICA_SELECTION must be FirstPerShard or AllReplicas" ;;
	esac
	# The target cluster goes first, so that a path shared with it is found whichever cluster comes later
	for index in "${self}" "${others[@]}"; do
		path_setting "${index}" CLUSTER
		name=${REPLY}
		if [[ ${index} == "${self}" ]]; then
			self_name=${name}
		fi
		# The operator tells when the settings it gives do not have to be those of the sidecar
		path_setting "${index}" UNVERIFIABLE
		what=${REPLY//[[:cntrl:]]/ }
		if [[ -n ${what} ]]; then
			what="the operator cannot tell the sidecar settings of cluster ${name}: ${what}"
			if [[ ${index} == "${self}" ]]; then
				path_check_inconclusive "${#indexes[@]}" "${name}" "${what}"
				return 0
			fi
			advise "cluster ${name} is left out of the storage path check: ${what}"
			continue
		fi
		path_setting "${index}" REMOTE_STORAGE
		storage=${REPLY}
		storage_prefix "${storage}"
		prefix=${REPLY}
		if [[ -z ${prefix} ]]; then
			if [[ -z ${storage} ]]; then
				what="its configuration does not set remote_storage"
			else
				what="it keeps its backups in ${storage} storage, where the check cannot tell their paths"
			fi
			if [[ ${index} == "${self}" ]]; then
				path_check_inconclusive "${#indexes[@]}" "${name}" "${what}"
				return 0
			fi
			advise "cluster ${name} is left out of the storage path check: ${what}"
			continue
		fi
		# What tells one storage of its type from another, as clickhouse-backup takes it: an unset setting has its default
		identity=${storage}
		for key in "${PATH_CHECK_KEYS[@]}"; do
			if [[ ${key} == "${prefix}_"* && ${key} != "${prefix}_PATH" && ${key} != "${prefix}_OBJECT_DISK_PATH" ]]; then
				path_setting "${index}" "${key}"
				if [[ -z ${SETTING_SET} ]]; then
					identity_default "${key}"
				fi
				printf -v value ' %q' "${REPLY}"
				identity+=${value}
			fi
		done
		# A path, unlike them, takes the default of the sidecar only when unset: set empty, it is the root of the storage
		path_setting "${index}" "${prefix}_PATH"
		if [[ -z ${SETTING_SET} ]]; then
			default_path "${index}" DEFAULT_PATH
		fi
		trim_path "${storage}" "${REPLY}"
		clean_path "${REPLY}"
		template=${REPLY}
		path_setting "${index}" "${prefix}_OBJECT_DISK_PATH"
		if [[ -z ${SETTING_SET} ]]; then
			default_path "${index}" DEFAULT_OBJECT_DISK_PATH
		fi
		trim_path "${storage}" "${REPLY}"
		object=${REPLY}
		# An object disk path that trims to nothing is none, unlike one that names the root of the storage
		if [[ -n ${object} ]]; then
			clean_path "${object}"
			object=${REPLY:-.}
		fi
		if [[ ${index} == "${self}" ]]; then
			SELF_PATH=${template}
			if [[ ${OPERATION} == backup && ${selection} == AllReplicas && ${template} != *"{replica}"* ]]; then
				die "the storage path '${template}' of cluster ${name} lacks {replica}: a backup of every replica would keep those of all replicas of a shard at one path"
			fi
			if [[ ${OPERATION} == backup && ${selection} != AllReplicas && ${template} == *"{replica}"* ]]; then
				die "the storage path '${template}' of cluster ${name} holds {replica}: a backup of the first replica of each shard would not be at the path every other replica restores its schema from"
			fi
			if [[ ${OPERATION} == restore && ${template} == *"{replica}"* ]] && ! is_true VALIDATE_TOPOLOGY; then
				advise "the storage path '${template}' of cluster ${name} holds {replica}: every replica restores its schema from its own path, which holds the backup only if it is one of every replica"
			fi
		fi
		for value in "${template}" "${object}"; do
			if ! expand_macros "${value}" "${name}" ""; then
				if [[ ${index} == "${self}" ]]; then
					path_check_inconclusive "${#indexes[@]}" "${name}" "the path '${value}' of cluster ${name} holds ${UNKNOWN_MACRO}, a macro whose value it does not know"
					return 0
				fi
				advise "cluster ${name} is left out of the storage path check: its path '${value}' holds ${UNKNOWN_MACRO}, a macro whose value it does not know"
				continue 2
			fi
		done
		# Every path of the target cluster is known now, which a restore from another installation's paths builds on
		if [[ ${index} == "${self}" ]]; then
			SELF_OBJECT_DISK_PATH=${object}
			SELF_STORAGE_PREFIX=${prefix}
			SELF_CLUSTER_NAME=${name}
		fi
		path_setting "${index}" SHARDS
		read -r -a shards <<<"${REPLY}"
		for shard in "${shards[@]}"; do
			for what in "storage path" "object disk path"; do
				value=${template}
				if [[ ${what} != "storage path" ]]; then
					value=${object}
				fi
				# A path with {replica} is a path of its own on every host; an object disk path that is unset is none
				if [[ ${value} == *"{replica}"* ]] || [[ ${what} != "storage path" && -z ${value} ]]; then
					continue
				fi
				expand_macros "${value}" "${name}" "${shard}"
				clean_path "${REPLY}"
				printf -v key '%s %q %q' "${identity}" "${what}" "${REPLY}"
				if [[ -n ${owners[${key}]:-} ]]; then
					if [[ -n ${self_owned[${key}]:-} ]]; then
						# The root of the storage, which clean_path makes empty, reads as path.Clean writes it
						die "the ${what} '${REPLY:-.}' of shard ${shard} of cluster ${name} is that of ${owners[${key}]} too, in the same ${storage} storage: every shard of every cluster needs a path of its own, as {cluster} and {shard} give it"
					fi
					continue
				fi
				owners[${key}]="shard ${shard} of cluster ${name}"
				if [[ ${index} == "${self}" ]]; then
					self_owned[${key}]=1
				fi
			done
		done
	done
	log "storage path check: every shard of cluster ${self_name} has a path of its own"
}

# days_from_civil YEAR MONTH DAY sets REPLY to the number of days from 1970-01-01 to the date of the proleptic
# Gregorian calendar
days_from_civil() {
	local y=$1 m=$2 d=$3 era yoe doy doe
	y=$((y - (m <= 2 ? 1 : 0)))
	era=$(((y >= 0 ? y : y - 399) / 400))
	yoe=$((y - era * 400))
	doy=$(((153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1))
	doe=$((yoe * 365 + yoe / 4 - yoe / 100 + doy))
	REPLY=$((era * 146097 + doe - 719468))
}

# month_days YEAR MONTH sets REPLY to the number of days of the month
month_days() {
	case $2 in
	2) REPLY=$(($1 % 4 == 0 && ($1 % 100 != 0 || $1 % 400 == 0) ? 29 : 28)) ;;
	4 | 6 | 9 | 11) REPLY=30 ;;
	*) REPLY=31 ;;
	esac
}

# weekday YEAR MONTH DAY sets REPLY to the day of the week of the date, 0 for Sunday
weekday() {
	days_from_civil "$1" "$2" "$3"
	# 1970-01-01 was a Thursday
	REPLY=$((((REPLY % 7) + 11) % 7))
}

# tag_valid TAG reports whether TAG is YYYYMMDDHHMMSS of a valid time
tag_valid() {
	local tag=$1 y m d
	if [[ ! ${tag} =~ ${TAG_RE} ]]; then
		return 1
	fi
	y=$((10#${tag:0:4})) m=$((10#${tag:4:2})) d=$((10#${tag:6:2}))
	if ((m < 1 || m > 12)); then
		return 1
	fi
	month_days "${y}" "${m}"
	((d >= 1 && d <= REPLY && 10#${tag:8:2} < 24 && 10#${tag:10:2} < 60 && 10#${tag:12:2} < 60))
}

# period_weight PERIOD sets REPLY to the weight of a schedule period: the longer the period, the heavier
period_weight() {
	case $1 in
	minute) REPLY=1 ;;
	hour) REPLY=2 ;;
	6hours) REPLY=3 ;;
	daily) REPLY=4 ;;
	weekly) REPLY=5 ;;
	monthly) REPLY=6 ;;
	*) REPLY=0 ;;
	esac
}

# parse_schedules fills the SCHED_* arrays from SCHEDULES: entries separated by ";", each of blank-separated
# key=value fields "period=<minute|hour|6hours|daily|weekly|monthly> [time=HH:MM] [day=N] keep=N [incremental=true]".
# A minute entry is every minute, whatever its time; the time of an hour entry gives its minute, that of a 6hours entry
# its minute and its hour modulo 6, empty is 00:00; daily, weekly and monthly entries need it. A weekly entry needs a day from 0 (Sunday) to 6, a monthly one a day from 1 to 31,
# which a shorter month clamps to its last day; no other entry takes one. Anything else is fatal
parse_schedules() {
	local entry field key value index=0 period at day keep incremental
	local -a entries fields
	local -A periods=()
	SCHED_PERIOD=() SCHED_HOUR=() SCHED_MINUTE=() SCHED_DAY=() SCHED_KEEP=() SCHED_INCREMENTAL=()
	if [[ ${SCHEDULES:-} == *[[:cntrl:]]* ]]; then
		die "SCHEDULES must be one line"
	fi
	IFS=';' read -r -a entries <<<"${SCHEDULES:-}"
	for entry in "${entries[@]}"; do
		read -r -a fields <<<"${entry}"
		if ((${#fields[@]} == 0)); then
			continue
		fi
		period= at= day= keep= incremental=false
		for field in "${fields[@]}"; do
			if [[ ${field} != *=* ]]; then
				die "SCHEDULES entry ${index}: ${field} is no key=value"
			fi
			key=${field%%=*}
			value=${field#*=}
			case ${key} in
			period) period=${value} ;;
			time) at=${value} ;;
			day) day=${value} ;;
			keep) keep=${value} ;;
			incremental) incremental=${value} ;;
			*) die "SCHEDULES entry ${index}: unknown key ${key}" ;;
			esac
		done
		case ${period} in
		minute | hour | 6hours | daily | weekly | monthly) ;;
		*) die "SCHEDULES entry ${index}: period must be minute, hour, 6hours, daily, weekly or monthly" ;;
		esac
		if [[ -n ${periods[${period}]:-} ]]; then
			die "SCHEDULES entry ${index}: a second ${period} entry"
		fi
		periods[${period}]=1
		if [[ ! ${keep} =~ ${KEEP_RE} ]]; then
			die "SCHEDULES entry ${index} (${period}): keep must be a number from 1"
		fi
		if [[ -n ${at} && ! ${at} =~ ${SCHEDULE_TIME_RE} ]]; then
			die "SCHEDULES entry ${index} (${period}): time must be HH:MM"
		fi
		if [[ -z ${at} && ${period} != minute && ${period} != hour && ${period} != 6hours ]]; then
			die "SCHEDULES entry ${index} (${period}): time is required"
		fi
		case ${period} in
		weekly)
			if [[ ! ${day} =~ ^[0-6]$ ]]; then
				die "SCHEDULES entry ${index} (weekly): day must be from 0 (Sunday) to 6"
			fi
			;;
		monthly)
			if [[ ! ${day} =~ ^([1-9]|[12][0-9]|3[01])$ ]]; then
				die "SCHEDULES entry ${index} (monthly): day must be from 1 to 31"
			fi
			;;
		*)
			if [[ -n ${day} ]]; then
				die "SCHEDULES entry ${index} (${period}): day is for weekly and monthly entries only"
			fi
			;;
		esac
		case ${incremental} in
		true | false) ;;
		*) die "SCHEDULES entry ${index} (${period}): incremental must be true or false" ;;
		esac
		if [[ ${period} == minute ]]; then
			at=
		fi
		at=${at:-00:00}
		SCHED_PERIOD+=("${period}")
		SCHED_HOUR+=("$((10#${at%%:*}))")
		SCHED_MINUTE+=("$((10#${at#*:}))")
		SCHED_DAY+=("${day}")
		SCHED_KEEP+=("${keep}")
		SCHED_INCREMENTAL+=("${incremental}")
		index=$((index + 1))
	done
}

# slot_matches INDEX TAG reports whether the wall time TAG, YYYYMMDDHHMMSS, is a slot of the schedule entry INDEX, to the
# minute: a minute entry at every minute, an hour entry at the minute of its time, a 6hours one at that minute every 6 hours from its hour, a daily one
# at its time, a weekly one on its day of the week at its time, a monthly one on its day of the month, clamped to the last
# day of a shorter month, at its time
slot_matches() {
	local i=$1 tag=$2 y m d hour minute day
	y=$((10#${tag:0:4})) m=$((10#${tag:4:2})) d=$((10#${tag:6:2})) hour=$((10#${tag:8:2})) minute=$((10#${tag:10:2}))
	if [[ ${SCHED_PERIOD[i]} == minute ]]; then
		return 0
	fi
	if ((minute != SCHED_MINUTE[i])); then
		return 1
	fi
	case ${SCHED_PERIOD[i]} in
	hour) return 0 ;;
	6hours)
		((hour % 6 == SCHED_HOUR[i] % 6))
		return
		;;
	esac
	if ((hour != SCHED_HOUR[i])); then
		return 1
	fi
	case ${SCHED_PERIOD[i]} in
	daily) return 0 ;;
	weekly)
		weekday "${y}" "${m}" "${d}"
		((REPLY == SCHED_DAY[i]))
		;;
	monthly)
		month_days "${y}" "${m}"
		day=$((SCHED_DAY[i] < REPLY ? SCHED_DAY[i] : REPLY))
		((d == day))
		;;
	*) return 1 ;;
	esac
}

# winner_entry TAG sets MATCHED to the indexes of the schedule entries whose slot TAG is, and WINNER to the heaviest of
# them, empty when none matches: the run of a slot of several entries is a run of each, and of the heaviest above all,
# which decides whether it is incremental
winner_entry() {
	local i weight=0
	MATCHED=()
	WINNER=
	for i in "${!SCHED_PERIOD[@]}"; do
		if slot_matches "${i}" "$1"; then
			MATCHED+=("${i}")
			period_weight "${SCHED_PERIOD[i]}"
			if ((REPLY > weight)); then
				weight=${REPLY}
				WINNER=${i}
			fi
		fi
	done
}

# incremental_entry sets WINNER to the index of the first schedule entry whose backups are incremental, empty when
# none is
incremental_entry() {
	local i
	WINNER=
	for i in "${!SCHED_PERIOD[@]}"; do
		if [[ ${SCHED_INCREMENTAL[i]} == true ]]; then
			WINNER=${i}
			return 0
		fi
	done
}

# check_timezone fails unless SCHEDULE_TIMEZONE is empty, UTC, or a zone of the tzdata of the image
check_timezone() {
	local zone=${SCHEDULE_TIMEZONE:-}
	if [[ -z ${zone} || ${zone} == UTC ]]; then
		return 0
	fi
	if [[ ! ${zone} =~ ${ZONE_RE} || ${zone} == *..* || ! -f ${TZDIR:-/usr/share/zoneinfo}/${zone} ]]; then
		die "SCHEDULE_TIMEZONE is not a time zone of the tzdata of the image"
	fi
}

# slot_tag EPOCH sets REPLY to the wall time of EPOCH in SCHEDULE_TIMEZONE, as YYYYMMDDHHMMSS
slot_tag() {
	local zone=${SCHEDULE_TIMEZONE:-}
	if [[ -z ${zone} || ${zone} == UTC ]]; then
		zone=UTC0
	fi
	TZ=${zone} printf -v REPLY '%(%Y%m%d%H%M%S)T' "$1"
}

# slot_epoch sets REPLY to the unix seconds of the slot of the run: the CronJob controller names the Job of a scheduled
# run "<CRON_JOB_NAME>-<minutes since the epoch of its slot>", which the pod reads from the Job name label of its pod,
# JOB_NAME, so every run of one slot makes the same backup, whose name the operator derives the same way. A Job named
# otherwise, such as one created by hand from the CronJob, takes the current minute
slot_epoch() {
	local slot=
	if [[ -n ${CRON_JOB_NAME:-} && ${JOB_NAME:-} == "${CRON_JOB_NAME}-"* ]]; then
		slot=${JOB_NAME#"${CRON_JOB_NAME}-"}
	fi
	if [[ ${slot} =~ ${SLOT_RE} ]]; then
		REPLY=$((10#${slot} * 60))
		return 0
	fi
	printf -v REPLY '%(%s)T' -1
	REPLY=$((REPLY - REPLY % 60))
}

# schedule_slot sets SLOT_TAG to the wall time in SCHEDULE_TIMEZONE of the slot of the run, as YYYYMMDDHHMMSS. A slot past
# the four-digit years takes the current minute, as the operator then derives no name
schedule_slot() {
	slot_epoch
	slot_tag "${REPLY}"
	if [[ ! ${REPLY} =~ ${TAG_RE} ]]; then
		printf -v REPLY '%(%s)T' -1
		slot_tag "$((REPLY - REPLY % 60))"
	fi
	SLOT_TAG=${REPLY}
}

# record_series HOST adds to SERIES_REQUIRED, as "<host> <name>", the backup each complete backup of the series of
# the last list of HOST is incremental from, empty for a full one; the backup this run makes is none of them
record_series() {
	local host=$1 i pattern
	series_pattern
	pattern=${REPLY}
	for i in "${!LIST_NAMES[@]}"; do
		if [[ ${LIST_NAMES[i]} =~ ${pattern} && ${LIST_NAMES[i]} != "${BACKUP_NAME}" && ${LIST_DESC[i]} != broken* ]]; then
			SERIES_REQUIRED["${host} ${LIST_NAMES[i]}"]=${LIST_REQUIRED[i]}
		fi
	done
}

# incremental_base KEEP HOST... sets REPLY to the backup an incremental scheduled backup on every HOST is made from, as
# SERIES_REQUIRED holds them: the newest backup of the series every HOST holds complete, unless the chain of
# incremental backups that ends with it is KEEP long on any HOST, empty for a full backup then, as when there is none.
# CHAIN is the length of that chain
incremental_base() {
	local keep=$1 host key name newest= count n
	local -a names
	local -A hosts=() held=()
	shift
	for host in "$@"; do
		hosts[${host}]=1
	done
	for key in "${!SERIES_REQUIRED[@]}"; do
		if [[ -n ${hosts[${key%% *}]:-} ]]; then
			name=${key#* }
			held[${name}]=$((${held[${name}]:-0} + 1))
		fi
	done
	for name in "${!held[@]}"; do
		n=${held[${name}]}
		if ((n == ${#hosts[@]})) && [[ ${name} > ${newest} ]]; then
			newest=${name}
		fi
	done
	CHAIN=0
	REPLY=
	if [[ -z ${newest} ]]; then
		return 0
	fi
	for host in "${!hosts[@]}"; do
		names=()
		for key in "${!SERIES_REQUIRED[@]}"; do
			if [[ ${key%% *} == "${host}" ]]; then
				names+=("${key#* }")
			fi
		done
		mapfile -t names < <(printf '%s\n' "${names[@]}" | sort -r)
		count=0
		for name in "${names[@]}"; do
			if [[ ${name} > ${newest} ]]; then
				continue
			fi
			if [[ -z ${SERIES_REQUIRED["${host} ${name}"]} ]]; then
				break
			fi
			count=$((count + 1))
		done
		if ((count > CHAIN)); then
			CHAIN=${count}
		fi
	done
	if ((CHAIN < keep)); then
		REPLY=${newest}
	fi
}

# backup_pod_wait_seconds sets REPLY to how long the backup Pods of the pod transport are waited for, failing for a
# BACKUP_POD_WAIT_SECONDS that is no number
backup_pod_wait_seconds() {
	REPLY=${BACKUP_POD_WAIT_SECONDS:-${DEFAULT_BACKUP_POD_WAIT_SECONDS}}
	if [[ ! ${REPLY} =~ ${NUMBER_RE} ]]; then
		die "BACKUP_POD_WAIT_SECONDS must be a number of seconds"
	fi
	REPLY=$((10#${REPLY}))
}

# check_transport fails unless BACKUP_TRANSPORT is sidecar, pod or empty, which is sidecar
check_transport() {
	case ${BACKUP_TRANSPORT:-} in
	"" | sidecar) ;;
	pod) backup_pod_wait_seconds ;;
	*) die "BACKUP_TRANSPORT must be sidecar or pod" ;;
	esac
}

# wait_for_apis HOST... waits, with the pod transport, until the API of every HOST answers: the backup Pods of the shards
# start with the operation, and their DNS names resolve once they are ready. A Pod that does not answer within
# BACKUP_POD_WAIT_SECONDS fails the run, which has changed nothing yet
wait_for_apis() {
	local host since=${SECONDS} wait announced
	if [[ ${BACKUP_TRANSPORT:-} != pod ]]; then
		return 0
	fi
	backup_pod_wait_seconds
	wait=${REPLY}
	for host in "$@"; do
		if [[ ! ${host} =~ ${HOST_RE} ]]; then
			continue
		fi
		announced=
		while :; do
			request GET "${host}" /backup/version
			if ((CURL_EXIT == 0)) && ! proxy_failed; then
				break
			fi
			if ((SECONDS - since >= wait)); then
				die "the backup Pod ${host} does not answer after $((SECONDS - since))s (curl exit ${CURL_EXIT}, HTTP ${HTTP_CODE})"
			fi
			if [[ -z ${announced} ]]; then
				announced=1
				log "${host}: waiting for the backup Pod to answer"
			fi
			sleep "${POLL_INTERVAL}"
		done
	done
	event Normal OrchestratorBackupPodsReady "the backup Pods answer"
}

# clickhouse_answers HOST reports whether the ClickHouse of HOST answers through its API: a listing of a table the API
# lists none of, which takes a query of ClickHouse all the same, asked once
clickhouse_answers() {
	request GET "$1" /backup/tables -G --data-urlencode "table=system.one"
	((CURL_EXIT == 0)) && [[ ${HTTP_CODE} == 200 ]]
}

# fall_back_hosts replaces in HOSTS, the hosts of BACKUP_HOSTS, the preferred replica of every shard whose ClickHouse
# does not answer by the host of the shard BACKUP_FALLBACK_HOSTS names in its place, the first replica of the shard,
# whose storage path is the shard's as well: the Altinity Cloud Manager backs up another replica when its backup
# replica is not available. BACKUP_FALLBACK_HOSTS unset or empty leaves HOSTS as they are
fall_back_hosts() {
	local i
	local -a preferred=("${HOSTS[@]}") fallback
	parse_hosts BACKUP_FALLBACK_HOSTS optional
	fallback=("${HOSTS[@]}")
	HOSTS=("${preferred[@]}")
	if ((${#fallback[@]} == 0)); then
		return 0
	fi
	if ((${#fallback[@]} != ${#preferred[@]})); then
		die "BACKUP_FALLBACK_HOSTS must name a host for every host of BACKUP_HOSTS"
	fi
	for i in "${!preferred[@]}"; do
		if [[ ${preferred[i]} == "${fallback[i]}" ]] || clickhouse_answers "${preferred[i]}"; then
			continue
		fi
		advise "${preferred[i]}: the preferred replica does not answer (curl exit ${CURL_EXIT}, HTTP ${HTTP_CODE}), backing up ${fallback[i]} instead"
		event Normal OrchestratorReplicaFallback "${preferred[i]} does not answer, the backup of its shard runs on ${fallback[i]}"
		HOSTS[i]=${fallback[i]}
	done
}

# drop_failed_backup HOST... deletes, as well as it can, what a create_remote that failed on every HOST left there:
# clickhouse-backup deletes the local backup of a create that fails, but not that of an upload that fails, nor the part
# of the remote backup it uploaded, which lists as broken; the Job resumes no upload. A copy that is not there is no
# failure; a host that does not answer is asked once. A host whose backup only appeared in its storage, made by another
# host, is no HOST
drop_failed_backup() {
	local host where BEST_EFFORT=1
	for host in "$@"; do
		for where in local remote; do
			if submit "${host}" "delete ${where} ${BACKUP_NAME}" "/backup/delete/${where}/${BACKUP_NAME}" -G --data-urlencode "force=true" \
				--max-time "${DELETE_TIMEOUT}"; then
				log "${host}: the ${where} copy of the failed backup ${BACKUP_NAME} deleted"
				event Normal OrchestratorFailedBackupDeleted "${host}: the ${where} copy of the failed backup ${BACKUP_NAME} deleted"
			elif [[ ${LAST_WARNING} == *"not found"* ]]; then
				log "${host}: no ${where} copy of the failed backup ${BACKUP_NAME} to delete"
			else
				advise "${host}: the ${where} copy of the failed backup ${BACKUP_NAME} is left: ${LAST_WARNING}"
				event Warning OrchestratorFailedBackupLeft "${host}: the ${where} copy of the failed backup ${BACKUP_NAME} is left: ${LAST_WARNING}"
			fi
		done
	done
}

# backup_unseen HOST reports whether the storage of HOST does not hold the backup this run makes yet, as before it
# submits it there: one that does was made by another host of this run, at the same storage path
backup_unseen() {
	if ! list_backups "$1" remote; then
		return 1
	fi
	if find_backup "${BACKUP_NAME}"; then
		warn "backup ${BACKUP_NAME} appeared in the storage of $1 during this run: hosts of one backup must not share a storage path"
		return 1
	fi
}

do_backup() {
	local host incremental= keep i
	local -a backup_hosts params
	parse_hosts BACKUP_HOSTS
	fall_back_hosts
	backup_hosts=("${HOSTS[@]}")
	# The hosts make their parts of the backup at once: one host twice would make its part twice, at one storage path
	for ((i = 1; i < ${#backup_hosts[@]}; i++)); do
		if listed "${backup_hosts[i]}" "${backup_hosts[@]:0:i}"; then
			die "BACKUP_HOSTS lists ${backup_hosts[i]} twice: hosts of one backup must not share a storage path"
		fi
	done
	if [[ -n ${BACKUP_NAME_PREFIX:-} ]]; then
		check_name BACKUP_NAME_PREFIX
	fi
	parse_schedules
	is_true RESTORE_IN_PROGRESS || :
	check_timezone
	if ((${#SCHED_PERIOD[@]} > 0)) || [[ -z ${BACKUP_NAME:-} ]]; then
		schedule_slot
	fi
	if [[ -n ${BACKUP_NAME:-} ]]; then
		check_name BACKUP_NAME
		BACKUP_NAME_GIVEN=1
	elif [[ -n ${BACKUP_NAME_PREFIX:-} ]]; then
		BACKUP_NAME="${BACKUP_NAME_PREFIX}-${SLOT_TAG}"
		if [[ ! ${BACKUP_NAME} =~ ${NAME_RE} ]]; then
			die "BACKUP_NAME_PREFIX is too long for a backup name"
		fi
	else
		die "either BACKUP_NAME or BACKUP_NAME_PREFIX must be set"
	fi
	params=(-G --data-urlencode "name=${BACKUP_NAME}")
	if is_true SCHEMA_ONLY; then
		params+=(--data-urlencode "schema=")
	fi
	if [[ -n ${TABLES:-} ]]; then
		params+=(--data-urlencode "table=${TABLES}")
	fi
	if [[ -n ${PARTITIONS:-} ]]; then
		params+=(--data-urlencode "partitions=${PARTITIONS}")
	fi
	if [[ -n ${DIFF_FROM_REMOTE:-} ]]; then
		check_name DIFF_FROM_REMOTE
	fi
	if [[ -n ${CLUSTER:-} ]]; then
		check_cluster
	fi
	if ((${#SCHED_PERIOD[@]} > 0)) && [[ -n ${BACKUP_NAME_GIVEN} ]]; then
		# A backup that runs the schedule now has no slot: as the Altinity Cloud Manager makes a backup by hand, it is
		# incremental as the first incremental entry of the schedule says
		incremental_entry
		if [[ -z ${WINNER} ]]; then
			log "backup ${BACKUP_NAME}: runs the schedule now, which has no incremental entry: the backup is full"
		elif [[ -z ${DIFF_FROM_REMOTE:-} && -n ${BACKUP_NAME_PREFIX:-} ]]; then
			log "backup ${BACKUP_NAME}: runs the schedule now, incremental as its ${SCHED_PERIOD[WINNER]} entry"
			incremental=1
			keep=${SCHED_KEEP[WINNER]}
		fi
	elif ((${#SCHED_PERIOD[@]} > 0)); then
		winner_entry "${SLOT_TAG}"
		if [[ -z ${WINNER} ]]; then
			log "backup ${BACKUP_NAME}: the slot ${SLOT_TAG} is a slot of no schedule entry, the backup is full"
		else
			log "backup ${BACKUP_NAME}: the slot ${SLOT_TAG} is a slot of the ${SCHED_PERIOD[WINNER]} entry${MATCHED[1]:+ and lighter ones}"
			if [[ ${SCHED_INCREMENTAL[WINNER]} == true && -z ${DIFF_FROM_REMOTE:-} && -n ${BACKUP_NAME_PREFIX:-} ]]; then
				incremental=1
				keep=${SCHED_KEEP[WINNER]}
			fi
		fi
	fi
	check_storage_paths
	wait_for_apis "${backup_hosts[@]}"

	event Normal OrchestratorBackupStarted "backup ${BACKUP_NAME} on ${#backup_hosts[@]} host(s)"
	log "backup ${BACKUP_NAME}: preflight on ${#backup_hosts[@]} host(s)"
	declare -gA SERIES_REQUIRED=()
	for host in "${backup_hosts[@]}"; do
		if ! list_backups "${host}" remote; then
			fail "preflight failed on ${host}"
		fi
		# create_remote "resumes" into an existing backup of the same name and overwrites its manifest
		if find_backup "${BACKUP_NAME}"; then
			die "backup ${BACKUP_NAME} already exists in the storage of ${host}"
		fi
		if [[ -n ${DIFF_FROM_REMOTE:-} ]] && ! find_backup "${DIFF_FROM_REMOTE}"; then
			die "base backup ${DIFF_FROM_REMOTE} is missing in the storage of ${host}"
		fi
		if [[ -n ${incremental} ]]; then
			record_series "${host}"
		fi
	done
	if [[ -n ${DIFF_FROM_REMOTE:-} ]]; then
		params+=(--data-urlencode "diff-from-remote=${DIFF_FROM_REMOTE}")
	elif [[ -n ${incremental} ]]; then
		incremental_base "${keep}" "${backup_hosts[@]}"
		if [[ -n ${REPLY} ]]; then
			log "backup ${BACKUP_NAME}: incremental from ${REPLY}, which ends a chain of ${CHAIN} incremental backups"
			event Normal OrchestratorIncrementalBase "${BACKUP_NAME} is incremental from ${REPLY}"
			params+=(--data-urlencode "diff-from-remote=${REPLY}")
		else
			log "backup ${BACKUP_NAME}: full, as the chain of incremental backups is ${CHAIN} long, of ${keep} at most, or there is no base every host holds"
			event Normal OrchestratorIncrementalFull "${BACKUP_NAME} is a full backup"
		fi
	fi

	# Every host makes its part of the backup at the same time, as the Altinity Cloud Manager has every shard make its own;
	# one that fails leaves the others to finish, which the Job waits for before it fails
	if ! run_all op "${backup_hosts[*]}" backup_unseen create_remote "${BACKUP_NAME}" /backup/create_remote "${params[@]}"; then
		drop_failed_backup ${STEP_FAILED_HOSTS[@]+"${STEP_FAILED_HOSTS[@]}"}
		fail_all "backup ${BACKUP_NAME}"
	fi
	log "backup ${BACKUP_NAME} completed on ${#backup_hosts[@]} host(s)"
	# A schema-only backup never holds data, which needs no telling
	if ! is_true SCHEMA_ONLY; then
		report_data "${backup_hosts[@]}"
	fi
	event Normal OrchestratorBackupCompleted "backup ${BACKUP_NAME} completed on ${#backup_hosts[@]} host(s)"
	# A failed retention never fails the backup that has just been made
	if ! retention; then
		warn "retention failed"
	fi
}

do_verify() {
	local host
	local -a verify_hosts
	check_name BACKUP_NAME
	parse_hosts VERIFY_HOSTS
	verify_hosts=("${HOSTS[@]}")
	wait_for_apis "${verify_hosts[@]}"
	event Normal OrchestratorVerifyStarted "verify ${BACKUP_NAME} on ${#verify_hosts[@]} host(s)"
	for host in "${verify_hosts[@]}"; do
		log "${host}: verifying ${BACKUP_NAME}"
		# Drop any local copy so the download really pulls the backup from the storage
		if list_backups "${host}" local && find_backup "${BACKUP_NAME}"; then
			if ! delete_local "${host}" "${BACKUP_NAME}"; then
				fail "verify failed on ${host}: cannot drop the local copy of ${BACKUP_NAME}"
			fi
		fi
		if ! run_op "${host}" download "${BACKUP_NAME}" "/backup/download/${BACKUP_NAME}"; then
			fail "verify failed on ${host}: ${BACKUP_NAME} cannot be downloaded"
		fi
		if ! has_usable_backup "${host}" local "${BACKUP_NAME}"; then
			die "verify failed on ${host}: ${BACKUP_NAME} is not present locally after the download"
		fi
		if ! delete_local "${host}" "${BACKUP_NAME}"; then
			warn "${host}: the downloaded copy of ${BACKUP_NAME} is left on local disk"
		fi
	done
	log "backup ${BACKUP_NAME} verified on ${#verify_hosts[@]} host(s)"
	event Normal OrchestratorVerifyCompleted "backup ${BACKUP_NAME} verified on ${#verify_hosts[@]} host(s)"
}

# restored_db DATABASE sets REPLY to the database of the backup the restore restores into the live DATABASE: the one
# DATABASE_MAPPING maps to it, else DATABASE itself, unless the mapping maps that elsewhere, which returns 1, as the
# restore then touches nothing of DATABASE
restored_db() {
	REPLY=${DB_SOURCE["$1"]:-}
	if [[ -n ${REPLY} ]]; then
		return 0
	fi
	if [[ -n ${DB_TARGET["$1"]:-} ]]; then
		return 1
	fi
	REPLY=$1
}

# db_dropped DATABASE reports whether the schema restore drops DATABASE as a whole, with every table it holds, those
# made after the backup too: with rm it drops every database of the backup when no table pattern narrows it, and with
# one each that a pattern names as "<database>.*", or that a pattern matches with the dot that ends the name of a
# database. The patterns hold nothing but names, "*" and "?", which bash matches as clickhouse-backup does
db_dropped() {
	local db=$1 pattern
	local -a patterns
	if [[ -z ${TABLES:-} ]]; then
		return 0
	fi
	IFS=, read -r -a patterns <<<"${TABLES}"
	for pattern in "${patterns[@]}"; do
		if [[ ${pattern} == "${db}.*" || "${db}." == ${pattern} ]]; then
			return 0
		fi
	done
	return 1
}

# drops_database DATABASE reports whether the schema restore drops DATABASE as a whole, as far as the tables of the
# backup, which BACKUP_DBS holds the databases of, tell: with a pattern any database it takes as a whole, which the
# backup may hold without a table; without one any database the backup of any shard holds a table of, or any database
# at all once a table of the backup does not read as one. No listing tells a database the backup of every shard holds
# without a table, which the restore drops as a whole too
drops_database() {
	db_dropped "$1" && [[ -n ${TABLES:-} || -n ${BACKUP_TABLES_UNREADABLE:-} || -n ${BACKUP_DBS["$1."]:-} ]]
}

# list_backup_tables HOST fills HOST_TABLES with the "<database>.<table>" names of the tables of the backup that the
# table pattern matches in the storage of HOST, and HOST_DBS with their databases as "<database>.", and sets
# BACKUP_TABLES_UNREADABLE to HOST when it lists a row that does not read as one. A listing of a backup in the storage
# reads the paths of the sidecar, whatever a command sets: a restore from the paths of another installation lists the
# copy of the schema source_copy downloads from there, which holds the same tables
list_backup_tables() {
	local host=$1 line where="the storage of $1"
	local -a params
	declare -gA HOST_TABLES=() HOST_DBS=()
	params=(-G --data-urlencode "remote_backup=${BACKUP_NAME}")
	if [[ -n ${SOURCE_ENV} ]]; then
		source_copy "${host}"
		params=(-G --data-urlencode "local_backup=${BACKUP_NAME}")
		where="the copy ${host} downloaded from the storage path '${SOURCE_PATH_SHOWN}'"
	fi
	if [[ -n ${TABLES:-} ]]; then
		params+=(--data-urlencode "table=${TABLES}")
	fi
	call GET "${host}" /backup/tables "${params[@]}"
	if [[ ${HTTP_CODE} != 200 ]]; then
		die "preflight failed: cannot list the tables of ${BACKUP_NAME} in ${where} (HTTP ${HTTP_CODE}): $(error_of)"
	fi
	while IFS= read -r line; do
		if [[ -z ${line} ]]; then
			continue
		fi
		if [[ ${line} =~ ${BACKUP_TABLE_ROW_RE} ]]; then
			HOST_TABLES["${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"]=1
			HOST_DBS["${BASH_REMATCH[1]}."]=1
		else
			BACKUP_TABLES_UNREADABLE=${host}
		fi
	done <<<"${HTTP_BODY}"
}

# backup_tables HOST... adds to BACKUP_TABLES and BACKUP_DBS what list_backup_tables finds in the storage of
# every HOST, and fills SHARD_TABLES_DIFFER with each table that the backup holds there on one HOST and not on the first
backup_tables() {
	local host first= name
	local -A first_tables=()
	SHARD_TABLES_DIFFER=()
	for host in "$@"; do
		list_backup_tables "${host}"
		for name in "${!HOST_TABLES[@]}"; do
			BACKUP_TABLES[${name}]=1
		done
		for name in "${!HOST_DBS[@]}"; do
			BACKUP_DBS[${name}]=1
		done
		if [[ -z ${first} ]]; then
			first=${host}
			for name in "${!HOST_TABLES[@]}"; do
				first_tables[${name}]=1
			done
			continue
		fi
		for name in "${!HOST_TABLES[@]}"; do
			if [[ -z ${first_tables[${name}]:-} ]]; then
				SHARD_TABLES_DIFFER+=("${name} only on ${host}")
			fi
		done
		for name in "${!first_tables[@]}"; do
			if [[ -z ${HOST_TABLES[${name}]:-} ]]; then
				SHARD_TABLES_DIFFER+=("${name} not on ${host}")
			fi
		done
	done
}

# live_tables HOST... fills LIVE_HOSTS, LIVE_DBS, LIVE_TABLES, LIVE_ENGINES, LIVE_UUIDS and LIVE_SIZES with the host,
# database, name, engine, UUID and size in bytes of every user table of every HOST - a field empty when the row does not
# tell it - and LIVE_UNREADABLE and LIVE_UNREADABLE_SIZES with the host and size of every row that does not read as a
# table, whose size still reads when only its name does not
live_tables() {
	local host row db table
	LIVE_HOSTS=() LIVE_DBS=() LIVE_TABLES=() LIVE_ENGINES=() LIVE_UUIDS=() LIVE_SIZES=() LIVE_UNREADABLE=()
	LIVE_UNREADABLE_SIZES=()
	for host in "$@"; do
		call GET "${host}" /backup/tables/all
		if [[ ${HTTP_CODE} != 200 ]]; then
			die "preflight failed: cannot list tables on ${host} (HTTP ${HTTP_CODE}): $(error_of)"
		fi
		while IFS= read -r row; do
			if [[ -z ${row} ]]; then
				continue
			fi
			if [[ ! ${row} =~ ${TABLE_ROW_RE} ]]; then
				LIVE_UNREADABLE+=("${host}")
				json_get TotalBytes "${row}"
				LIVE_UNREADABLE_SIZES+=("${REPLY}")
				continue
			fi
			db=${BASH_REMATCH[1]} table=${BASH_REMATCH[2]}
			if [[ ${SYSTEM_DATABASES} == *" ${db} "* ]]; then
				continue
			fi
			LIVE_HOSTS+=("${host}")
			LIVE_DBS+=("${db}")
			LIVE_TABLES+=("${table}")
			json_get Engine "${row}"
			LIVE_ENGINES+=("${REPLY}")
			json_get UUID "${row}"
			LIVE_UUIDS+=("${REPLY}")
			# The size system.tables gives, which ClickHouse compares with max_table_size_to_drop
			json_get TotalBytes "${row}"
			LIVE_SIZES+=("${REPLY}")
		done <<<"${HTTP_BODY}"
	done
}

# dropped_tables fills LIVE_DROPPED with whether the schema restore drops each table live_tables found: one of the
# backup that the table pattern matches, BACKUP_TABLES, any of a database that it drops as a whole, and the inner table
# of a materialized view it drops, which goes with the view - each in the database the restore restores it into
dropped_tables() {
	local i
	local -A inner=()
	LIVE_DROPPED=()
	for i in "${!LIVE_TABLES[@]}"; do
		LIVE_DROPPED[i]=
		if ! restored_db "${LIVE_DBS[i]}"; then
			continue
		fi
		if [[ -n ${BACKUP_TABLES["${REPLY}.${LIVE_TABLES[i]}"]:-} ]] || drops_database "${REPLY}"; then
			LIVE_DROPPED[i]=1
			inner["${LIVE_HOSTS[i]} ${LIVE_DBS[i]}..inner.${LIVE_TABLES[i]}"]=1
			if [[ -n ${LIVE_UUIDS[i]} ]]; then
				inner["${LIVE_HOSTS[i]} ${LIVE_DBS[i]}..inner_id.${LIVE_UUIDS[i]}"]=1
			fi
		fi
	done
	for i in "${!LIVE_TABLES[@]}"; do
		if [[ -n ${inner["${LIVE_HOSTS[i]} ${LIVE_DBS[i]}.${LIVE_TABLES[i]}"]:-} ]]; then
			LIVE_DROPPED[i]=1
		fi
	done
}

# guard_tables SOURCES HOST... refuses a restore that would drop a user table of any HOST: one of the backup the table
# pattern matches in the storage of any of the space-separated hosts SOURCES, which the schema restore drops and
# recreates, or one of a database it drops as a whole - in the database DATABASE_MAPPING restores it into. Without a pattern, or when a table of the backup or of a HOST
# does not read as one, that is every user table: the guard fails closed
guard_tables() {
	local host i
	local -a sources found=()
	declare -gA BACKUP_TABLES=() BACKUP_DBS=()
	BACKUP_TABLES_UNREADABLE=
	read -r -a sources <<<"$1"
	shift
	live_tables "$@"
	for host in "${LIVE_UNREADABLE[@]}"; do
		found+=("${host}:?")
	done
	# Only a restore that some user table is in the way of needs the tables of the backup
	if ((${#LIVE_TABLES[@]} > 0)) && [[ -n ${TABLES:-} ]]; then
		backup_tables "${sources[@]}"
	fi
	for i in "${!LIVE_TABLES[@]}"; do
		# A database the mapping restores elsewhere is left alone, one it restores into holds the tables of its source
		if ! restored_db "${LIVE_DBS[i]}"; then
			continue
		fi
		if [[ -z ${TABLES:-} || -n ${BACKUP_TABLES_UNREADABLE} || -n ${BACKUP_TABLES["${REPLY}.${LIVE_TABLES[i]}"]:-} ]] ||
			db_dropped "${REPLY}"; then
			found+=("${LIVE_HOSTS[i]}:${LIVE_DBS[i]}.${LIVE_TABLES[i]}")
		fi
	done
	if ((${#found[@]} > 0)); then
		die "refusing to restore over existing tables (${found[*]:0:10}); set overwrite to replace them"
	fi
}

# join_items ITEM... sets REPLY to every ITEM, separated by commas
join_items() {
	local item
	REPLY=
	for item in "$@"; do
		REPLY+=${REPLY:+, }${item}
	done
}

# greater SIZE LIMIT reports whether the number SIZE is greater than the number LIMIT. They are compared as text: either
# may be 2^63 or more, which would wrap negative in bash arithmetic
greater() {
	local size=${1#"${1%%[!0]*}"} limit=${2#"${2%%[!0]*}"}
	if ((${#size} != ${#limit})); then
		((${#size} > ${#limit}))
		return
	fi
	[[ ${size} > ${limit} ]]
}

# no_zookeeper reports whether the operator tells that the target cluster has no ZooKeeper, which one that does not tell
# is taken to have; a value that is no boolean is fatal
no_zookeeper() {
	[[ -n ${REPLICATED:-} ]] && ! is_true REPLICATED
}

# drop_size_limited reports whether MAX_TABLE_SIZE_TO_DROP limits the size of a table ClickHouse drops: 0 lifts it
drop_size_limited() {
	[[ ! ${MAX_TABLE_SIZE_TO_DROP} =~ ^0+$ ]]
}

# refuse_database_drops SOURCES HOST... refuses, before anything changes, an overwrite that would stop half way on a HOST
# that drops a database as a whole on its own: the host that runs the drop ON CLUSTER, or any host when every replica
# drops the tables of the backup on its own. ClickHouse drops a table larger than max_table_size_to_drop only when a
# flag file on its host forces it, and takes the flag as it does: clickhouse-backup writes one for every table it drops,
# and a single one for a whole database, so two such tables of one database on one HOST stop the drop at the second,
# the databases before it already dropped. A table of unknown size counts as larger, and a row of HOST that does not
# read as a table, larger or of unknown size, counts in every database of HOST, as its name does not tell its database.
# live_tables has listed every HOST. The tables of the backup tell which databases the restore drops as a whole: they
# are listed in the storage of the space-separated hosts SOURCES only when a database could stop the drop and no
# pattern names those databases, SOURCES empty when they are listed
refuse_database_drops() {
	local host i key n what stop="larger than"
	local -a sources keys found=()
	local -A unreadable=() unreadable_rows=() count=() larger=() uncertain=()
	read -r -a sources <<<"$1"
	shift
	for i in "${!LIVE_UNREADABLE[@]}"; do
		host=${LIVE_UNREADABLE[i]}
		if [[ -z ${LIVE_UNREADABLE_SIZES[i]} ]]; then
			what="of unknown size"
		elif greater "${LIVE_UNREADABLE_SIZES[i]}" "${MAX_TABLE_SIZE_TO_DROP}"; then
			what="${LIVE_UNREADABLE_SIZES[i]} bytes"
		else
			continue
		fi
		n=${unreadable[${host}]:-0}
		unreadable[${host}]=$((n + 1))
		unreadable_rows[${host}]+="${unreadable_rows[${host}]:+, }${host}:? (a table whose name cannot be read, ${what})"
	done
	# A database name never takes part in arithmetic, where bash would expand it once more
	for i in "${!LIVE_TABLES[@]}"; do
		host=${LIVE_HOSTS[i]}
		if ! listed "${host}" "$@"; then
			continue
		fi
		key="${host} ${LIVE_DBS[i]}"
		if [[ -z ${count[${key}]:-} ]]; then
			count[${key}]=${unreadable[${host}]:-0}
			larger[${key}]=${unreadable_rows[${host}]:-}
			uncertain[${key}]=${unreadable_rows[${host}]:+1}
		fi
		if [[ -z ${LIVE_SIZES[i]} ]]; then
			larger[${key}]+="${larger[${key}]:+, }${host}:${LIVE_DBS[i]}.${LIVE_TABLES[i]} (of unknown size)"
			uncertain[${key}]=1
		elif greater "${LIVE_SIZES[i]}" "${MAX_TABLE_SIZE_TO_DROP}"; then
			larger[${key}]+="${larger[${key}]:+, }${host}:${LIVE_DBS[i]}.${LIVE_TABLES[i]} (${LIVE_SIZES[i]} bytes)"
		else
			continue
		fi
		n=${count[${key}]}
		count[${key}]=$((n + 1))
	done
	if ((${#count[@]} == 0)); then
		return 0
	fi
	mapfile -t keys < <(printf '%s\n' "${!count[@]}" | sort)
	for key in "${keys[@]}"; do
		n=${count[${key}]}
		if ((n < 2)); then
			continue
		fi
		if [[ -z ${TABLES:-} ]] && ((${#sources[@]} > 0)); then
			backup_tables "${sources[@]}"
			sources=()
		fi
		if restored_db "${key#* }" && drops_database "${REPLY}"; then
			found+=("${larger[${key}]}")
			if [[ -n ${uncertain[${key}]} ]]; then
				stop="that is, or may be, larger than"
			fi
		fi
	done
	if ((${#found[@]} > 0)); then
		join_items "${found[@]:0:10}"
		die "refusing to overwrite: a host of cluster ${CLUSTER} that drops a database as a whole would stop at the second table of it ${stop} max_table_size_to_drop (${MAX_TABLE_SIZE_TO_DROP} bytes) (${REPLY}), as clickhouse-backup forces the drop of only one such table of each database it drops as a whole; ${SIZE_LIMIT_ADVICE}"
	fi
}

# plan_overwrite SHARDS HOST... decides whether an overwrite drops the tables of the backup ON CLUSTER, setting
# ON_CLUSTER, and refuses, before anything changes, one whose drop ON CLUSTER would recreate other tables than those of
# every shard, or one that would stop half way. SHARDS lists, space-separated, the host that runs the drop - the first
# schema host - and the first replica of every shard; HOST are every replica, which the schema restore drops the tables
# of the backup on. Only a replicated table takes the drop ON CLUSTER: a replica that dropped its own would clone its
# old parts back from another replica that still holds it. clickhouse-backup lists the engine of a table and not that of
# its database, which tells nothing more: a table of a Replicated database replicates its parts only with a Replicated
# engine of its own, and clickhouse-backup drops it without ON CLUSTER, a DROP the database replicates to every replica
# of its own
plan_overwrite() {
	local initiator host i known=1 replicated=
	local -a shards oversized=()
	read -r -a shards <<<"$1"
	initiator=${shards[0]}
	shift
	declare -gA BACKUP_TABLES=() BACKUP_DBS=()
	BACKUP_TABLES_UNREADABLE=
	backup_tables "${shards[@]}"
	live_tables "$@"
	dropped_tables
	if ((${#LIVE_UNREADABLE[@]} > 0)) || [[ -n ${BACKUP_TABLES_UNREADABLE} ]]; then
		known=
	fi
	for i in "${!LIVE_TABLES[@]}"; do
		if [[ -z ${LIVE_DROPPED[i]} ]]; then
			continue
		fi
		if [[ ${LIVE_ENGINES[i]} == Replicated* ]]; then
			replicated=${replicated:-${LIVE_HOSTS[i]}:${LIVE_DBS[i]}.${LIVE_TABLES[i]}}
		elif [[ -z ${LIVE_ENGINES[i]} ]]; then
			known=
		fi
	done
	# The operator tells only what the installation configures: ZooKeeper may come from a configuration file of its own
	if [[ -n ${replicated} ]]; then
		ON_CLUSTER=1
		if no_zookeeper; then
			log "restore ${BACKUP_NAME}: cluster ${CLUSTER} holds the replicated table ${replicated}, which the restore drops ON CLUSTER though the installation configures no ZooKeeper for it"
		fi
	elif [[ -z ${known} ]]; then
		if no_zookeeper; then
			die "refusing to overwrite: whether the restore drops a replicated table of cluster ${CLUSTER} is unknown, as a table of a host or of the backup does not read as one; such a table needs the drop ON CLUSTER, which needs ZooKeeper, and the installation configures none for the cluster"
		fi
		ON_CLUSTER=1
	else
		log "restore ${BACKUP_NAME}: the restore drops no replicated table, every replica drops the tables of the backup on its own"
		if drop_size_limited; then
			refuse_database_drops "" "$@"
		fi
		return 0
	fi
	if [[ -n ${BACKUP_TABLES_UNREADABLE} ]]; then
		die "refusing to overwrite: ${BACKUP_TABLES_UNREADABLE} lists a table of ${BACKUP_NAME} that does not read as one, which leaves unknown whether the backup of every shard holds the same tables"
	fi
	# A table only another shard's backup holds would keep its replication, which then clones its old parts back, and one
	# only the backup of the shard of the host that runs the drop holds would be made on shards that never had it
	if ((${#SHARD_TABLES_DIFFER[@]} > 0)); then
		mapfile -t SHARD_TABLES_DIFFER < <(printf '%s\n' "${SHARD_TABLES_DIFFER[@]}" | sort)
		join_items "${SHARD_TABLES_DIFFER[@]:0:10}"
		die "refusing to overwrite: the tables of ${BACKUP_NAME} differ between the shards of cluster ${CLUSTER} (${REPLY}): the drop ON CLUSTER drops and recreates on every shard the tables of the backup of ${initiator}, which takes the backup of every shard to hold the same; narrow the restore with tables to those they all hold"
	fi
	# ClickHouse drops a table larger than max_table_size_to_drop only when a flag file on its own host forces it, which
	# clickhouse-backup writes on the host that runs the drop alone: on another host the drop would stop half way, with
	# the smaller tables already dropped on every host
	if ! drop_size_limited; then
		return 0
	fi
	for host in "${LIVE_UNREADABLE[@]}"; do
		if [[ ${host} != "${initiator}" ]]; then
			die "refusing to overwrite: ${host} lists a table that does not read as one, whose size max_table_size_to_drop may not allow the drop ON CLUSTER to drop"
		fi
	done
	for i in "${!LIVE_TABLES[@]}"; do
		if [[ -z ${LIVE_DROPPED[i]} || ${LIVE_HOSTS[i]} == "${initiator}" ]]; then
			continue
		fi
		if [[ -z ${LIVE_SIZES[i]} ]]; then
			oversized+=("${LIVE_HOSTS[i]}:${LIVE_DBS[i]}.${LIVE_TABLES[i]} (of unknown size)")
		elif greater "${LIVE_SIZES[i]}" "${MAX_TABLE_SIZE_TO_DROP}"; then
			oversized+=("${LIVE_HOSTS[i]}:${LIVE_DBS[i]}.${LIVE_TABLES[i]} (${LIVE_SIZES[i]} bytes)")
		fi
	done
	if ((${#oversized[@]} > 0)); then
		join_items "${oversized[@]:0:10}"
		die "refusing to overwrite: the drop ON CLUSTER from ${initiator} would stop at tables larger than max_table_size_to_drop (${MAX_TABLE_SIZE_TO_DROP} bytes) of cluster ${CLUSTER} on other hosts (${REPLY}), as clickhouse-backup forces the drop of such a table on ${initiator} alone; ${SIZE_LIMIT_ADVICE}"
	fi
	refuse_database_drops "" "${initiator}"
}

# check_source_name VAR_NAME MAX_LENGTH WHAT fails unless the env var VAR_NAME is unset, empty or a Kubernetes name of
# at most MAX_LENGTH characters, as a valid WHAT is: the value of {installation} or {namespace} in the storage paths of
# the installation a restore reads the backup of, which becomes part of the commands of the restore
check_source_name() {
	local value=${!1:-}
	if [[ -z ${value} ]]; then
		return 0
	fi
	if [[ ! ${value} =~ ${SOURCE_NAME_RE} ]] || ((${#value} > $2)); then
		die "$1 is not a valid $3"
	fi
}

# source_template TEMPLATE INSTALLATION NAMESPACE sets REPLY to the storage path TEMPLATE of the target cluster with
# {installation} replaced by INSTALLATION and {namespace} by NAMESPACE, literally, each unless empty, and every other
# macro kept: the sidecar expands the macros of the value of an --env flag as those of its own settings, on the host
# that runs the command, and a restore takes a target with the cluster names and the shard layout of the source
source_template() {
	local rest=$1 out= macro name
	while [[ ${rest} =~ \{([^{}]*)\} ]]; do
		macro=${BASH_REMATCH[0]}
		name=${BASH_REMATCH[1]}
		out+=${rest%%"${macro}"*}
		rest=${rest#*"${macro}"}
		case ${name} in
		installation) out+=${2:-${macro}} ;;
		namespace) out+=${3:-${macro}} ;;
		*) out+=${macro} ;;
		esac
	done
	REPLY=${out}${rest}
}

# source_paths sets SOURCE_ENV, before anything changes, for a restore that SOURCE_INSTALLATION or SOURCE_NAMESPACE
# points at another installation. Its backup is where the storage paths of the target cluster put it with the values of
# that installation for {installation} and {namespace}, which takes knowing those paths. The restore then sets the
# storage path in every command, and the object disk path along with it whenever the cluster has one: the data of a table
# on an object disk does not restore from a backup read at another storage path alone. A restore whose paths are the
# same either way - they hold neither macro, or the values are the target's own - runs as any other, with a warning
source_paths() {
	local installation= namespace= path object value
	if [[ -n ${SOURCE_INSTALLATION:-} && ${SOURCE_INSTALLATION} != "${PATH_INSTALLATION:-}" ]]; then
		installation=${SOURCE_INSTALLATION}
	fi
	if [[ -n ${SOURCE_NAMESPACE:-} && ${SOURCE_NAMESPACE} != "${PATH_NAMESPACE:-}" ]]; then
		namespace=${SOURCE_NAMESPACE}
	fi
	if [[ -z ${installation} && -z ${namespace} ]]; then
		if [[ -n ${SOURCE_INSTALLATION:-}${SOURCE_NAMESPACE:-} ]]; then
			advise "restore ${BACKUP_NAME} names its own installation as the one it restores the backup of, which it reads at its own storage paths"
		fi
		return 0
	fi
	if [[ -z ${SELF_STORAGE_PREFIX} ]]; then
		die "a restore from another installation needs the storage paths of the sidecars, which it reads the backup at with the {installation} and {namespace} of that installation, and the storage path check cannot tell them: ${SELF_PATHS_UNKNOWN}"
	fi
	source_template "${SELF_PATH}" "${installation}" "${namespace}"
	path=${REPLY}
	source_template "${SELF_OBJECT_DISK_PATH}" "${installation}" "${namespace}"
	object=${REPLY}
	if [[ ${path} == "${SELF_PATH}" && ${object} == "${SELF_OBJECT_DISK_PATH}" ]]; then
		advise "the storage paths of cluster ${SELF_CLUSTER_NAME} hold no {installation} or {namespace} that tells the installation restore ${BACKUP_NAME} names apart from this one, so it reads them as they are: '${SELF_PATH}'${SELF_OBJECT_DISK_PATH:+ and the object disk path '${SELF_OBJECT_DISK_PATH}'}"
		return 0
	fi
	if [[ ${path} == "${SELF_PATH}" ]]; then
		SOURCE_OWN_PATH=1
	elif [[ -n ${object} && ${object} == "${SELF_OBJECT_DISK_PATH}" ]]; then
		# clickhouse-backup deletes the object disk data of a local copy it deletes unless its own storage path holds the
		# backup, which is not the case here
		die "the object disk path '${SELF_OBJECT_DISK_PATH}' of cluster ${SELF_CLUSTER_NAME} holds no {installation} or {namespace} that tells the installation restore ${BACKUP_NAME} names apart from this one, while the storage path does: deleting the copy the restore downloads would delete the object disk data of the backup; give the object disk path the macros of the storage path"
	fi
	# An object disk path that is none stays none; a storage path is always set, at the root of the storage too
	for value in "${path}" ${object:+"${object}"}; do
		if [[ ! ${value} =~ ${SOURCE_PATH_RE} ]]; then
			die "the storage path '${value}' of the installation restore ${BACKUP_NAME} reads the backup of cannot be set in a command of the restore: only a path of letters, digits and _ . / { } - can"
		fi
	done
	SOURCE_ENV="--env=${SELF_STORAGE_PREFIX}_PATH=${path}"
	if [[ -n ${object} ]]; then
		SOURCE_ENV+=" --env=${SELF_STORAGE_PREFIX}_OBJECT_DISK_PATH=${object}"
	fi
	# {replica} is the host name, which holds the name of the installation unless its host templates name the hosts
	if [[ -n ${installation} && ${path} == *"{replica}"* ]]; then
		SOURCE_HINT="; the storage path holds {replica}, the host name, which holds the name of the installation, so the hosts of installations of different names have different paths"
	fi
	# The shard, unlike the cluster, differs between the hosts
	if ! expand_macros "${path}" "${SELF_CLUSTER_NAME}" "{shard}"; then
		REPLY=${path}
	fi
	SOURCE_PATH_SHOWN=${REPLY}
	log "restore ${BACKUP_NAME}: reading the backup at the storage path '${SOURCE_PATH_SHOWN}' of the installation it names"
}

# refuse_local_copy HOST refuses, before anything changes, a restore from the paths of another installation on a HOST
# that holds a local copy of the backup: the restore would restore what that copy holds, whichever paths it came from,
# rather than download the backup from those it reads. A restore from its own paths restores from such a copy
refuse_local_copy() {
	local host=$1
	if listed "${host}" "${NO_LOCAL_COPY[@]}"; then
		return 0
	fi
	if ! list_backups "${host}" local; then
		fail "preflight failed on ${host}"
	fi
	if find_backup "${BACKUP_NAME}"; then
		die "preflight failed: ${host} holds a local copy of ${BACKUP_NAME}, which a restore from the storage paths of another installation would restore in place of the backup there; delete it (clickhouse-backup delete local ${BACKUP_NAME}) and restore again"
	fi
	NO_LOCAL_COPY+=("${host}")
}

# refuse_own_backup HOST refuses, before anything changes, a restore from the storage path of another installation on a
# HOST whose own storage path holds a backup of the same name. clickhouse-backup keeps the metadata of the remote backups
# it lists in a cache of the storage type, whatever the path, and looks a backup up there by name before it reads the
# backup at the path a command sets: the restore would take the tables of a complete backup at the own path for those
# of the one it reads, and the next list of the own path the backup it read for a broken one there. Listing the own
# path renews the cache with the backups there, unless it finds no backup the cache lacks and as many backups as the
# cache holds, broken ones counted, which keeps a backup of another path the cache holds: backup names kept apart
# between installations avoid that. OWN_BACKUPS keeps the names for the backup the restored one is incremental from,
# looked up the same way
refuse_own_backup() {
	local host=$1
	if ! list_backups "${host}" remote; then
		fail "preflight failed on ${host}"
	fi
	OWN_BACKUPS[${host}]=" ${LIST_NAMES[*]} "
	if [[ ${OWN_BACKUPS[${host}]} == *" ${BACKUP_NAME} "* ]]; then
		die "preflight failed: ${host} holds a backup ${BACKUP_NAME} at its own storage path too, which clickhouse-backup would take for the one at the storage path '${SOURCE_PATH_SHOWN}', as it tells the backups of every path apart by name alone: restore a backup of another name, or delete that one first"
	fi
}

# source_copy HOST downloads the schema of the backup to HOST from the storage paths SOURCE_ENV sets, once: a typed
# request reads the paths of the sidecar whatever a command sets, so the preflight of a restore from another
# installation reads the backup through this copy. The copy changes nothing the Job is asked to change, and is gone
# before the restore steps, or with a Job that fails before them. It holds the access entities too, as a local copy
# lists the size of those alone that it holds. A download that fails tells the backup missing at those paths or
# unreadable there, and nothing tells which: clickhouse-backup keeps what it read of a storage in a cache of the storage
# type, whatever the path, and may find a backup the cache lists broken where it is missing
source_copy() {
	local host=$1 READ_ONLY=1 NO_STEP=1
	if listed "${host}" "${SOURCE_COPIES[@]}"; then
		return 0
	fi
	refuse_local_copy "${host}"
	# Recorded ahead of the download, which may leave a copy in part when it fails
	SOURCE_COPIES+=("${host}")
	if ! run_action "${host}" download "${BACKUP_NAME}" "download --schema --rbac ${SOURCE_ENV} ${BACKUP_NAME}"; then
		die "preflight failed: backup ${BACKUP_NAME} is missing in the storage path '${SOURCE_PATH_SHOWN}' of ${host}, or cannot be read there${LAST_WARNING:+: ${LAST_WARNING}}${SOURCE_HINT}"
	fi
}

# drop_source_copies [strict] deletes the copies of the backup SOURCE_COPIES lists, which changes nothing the Job is
# asked to change. Strict, before the restore steps, a copy of the schema source_copy downloaded that cannot be deleted
# fails the restore: the restore would take it for the backup, which holds no data. A Job that
# fails deletes what it can and leaves the rest with a warning, as such a copy refuses the next restore from the same
# paths
drop_source_copies() {
	local host strict=${1:-} READ_ONLY=1 BEST_EFFORT=${BEST_EFFORT:-}
	local -a hosts=("${SOURCE_COPIES[@]}")
	if [[ -z ${strict} ]]; then
		# The Job fails anyway: a sidecar that does not answer is asked once
		BEST_EFFORT=1
		SOURCE_COPIES=()
	fi
	for host in "${hosts[@]}"; do
		if [[ -n ${strict} ]]; then
			if ! delete_local "${host}" "${BACKUP_NAME}"; then
				fail "${host}: cannot delete the copy of ${BACKUP_NAME} the preflight downloaded, which the restore would take for its own"
			fi
			SOURCE_COPIES=("${SOURCE_COPIES[@]:1}")
		elif list_backups "${host}" local && ! find_backup "${BACKUP_NAME}"; then
			continue
		elif ! delete_local "${host}" "${BACKUP_NAME}"; then
			advise "${host}: the copy of ${BACKUP_NAME} the restore downloaded is left on local disk, which refuses a restore of ${BACKUP_NAME} from another installation until it is deleted"
		fi
	done
}

# restore_command STEP sets STEP_ARGS to the arguments of run_op, or of run_action, that run the step STEP of the
# restore of the backup: download, the backup with the data, every file of it already on the disks of the host
# hard-linked rather than downloaded again; download_schema, the backup without the data; schema, which drops the tables
# of the backup first; schema_on_cluster, which drops and creates them ON CLUSTER; data; or rbac, the access entities.
# Each restores from the copy the download made. RESTORE_KIND tells which: op, a typed request with query parameters,
# or action, the command of the actions endpoint with the flags ACTION_ENV, as a typed request takes no setting in
# place of those of the sidecar, such as the storage paths of another installation
restore_command() {
	local endpoint=restore command
	local -a params=() flags=()
	case $1 in
	download)
		endpoint=download
		params=(hardlink_exists_files=)
		flags=(download --hardlink-exists-files)
		;;
	download_schema)
		endpoint=download
		params=(schema=)
		flags=(download --schema)
		;;
	schema)
		params=(schema= rm=)
		flags=(restore --schema --rm)
		;;
	schema_on_cluster) flags=(restore --schema --rm "--env=RESTORE_SCHEMA_ON_CLUSTER=${CLUSTER}") ;;
	data)
		params=(data=)
		flags=(restore --data)
		;;
	rbac)
		params=(rbac-only=)
		flags=(restore --rbac-only)
		;;
	esac
	if [[ -n ${TABLES:-} && $1 != rbac ]]; then
		params+=("table=${TABLES}")
		flags+=("--tables=${TABLES}")
	fi
	# The partitions narrow the data, which the download of the schema alone holds none of
	if [[ -n ${PARTITIONS:-} && ($1 == download || $1 == data) ]]; then
		params+=("partitions=${PARTITIONS}")
		flags+=("--partitions=${PARTITIONS}")
	fi
	if [[ -n ${DATABASE_MAPPING:-} && ($1 == schema || $1 == schema_on_cluster || $1 == data) ]]; then
		params+=("restore_database_mapping=${DATABASE_MAPPING}")
		flags+=("--restore-database-mapping=${DATABASE_MAPPING}")
	fi
	if [[ ${RESTORE_KIND} == action || $1 == schema_on_cluster ]]; then
		command="${flags[*]}${ACTION_ENV:+ ${ACTION_ENV}} ${BACKUP_NAME}"
		STEP_ARGS=("${command}")
		return 0
	fi
	STEP_ARGS=("/backup/${endpoint}/${BACKUP_NAME}" -G)
	for command in "${params[@]}"; do
		STEP_ARGS+=(--data-urlencode "${command}")
	done
}

# restore_step HOST WHAT STEP runs the step STEP of the restore, as restore_command makes it, on HOST
restore_step() {
	local kind=${RESTORE_KIND}
	restore_command "$3"
	if [[ $3 == schema_on_cluster ]]; then
		kind=action
	fi
	"run_${kind}" "$1" "$2" "${BACKUP_NAME}" "${STEP_ARGS[@]}"
}

# check_filter VAR_NAME REGEX MAX_LENGTH WHAT fails unless the env var VAR_NAME is unset, empty, or a value of at most
# MAX_LENGTH characters that REGEX matches, as a valid WHAT is
check_filter() {
	local value=${!1:-}
	if [[ -n ${value} ]] && [[ ! ${value} =~ $2 ]] || ((${#value} > $3)); then
		die "$1 is not a valid $4"
	fi
}

# check_restore_filters fails unless the partitions, the tables and engines left out and the database mapping of a
# restore are each empty or a single token of a command, as the API validates them, and fills DB_SOURCE and DB_TARGET
# from the mapping: the source of every target database, and the target of every source database
check_restore_filters() {
	local pair
	local -a pairs
	check_filter PARTITIONS "${PARTITIONS_RE}" "${FILTER_MAX_LENGTH}" "list of partition ids"
	check_filter SKIP_TABLES "${TABLES_RE}" "${TABLES_MAX_LENGTH}" "table pattern"
	check_filter SKIP_TABLE_ENGINES "${ENGINES_RE}" "${FILTER_MAX_LENGTH}" "list of table engines"
	check_filter DATABASE_MAPPING "${DATABASE_MAPPING_RE}" "${FILTER_MAX_LENGTH}" "database mapping"
	declare -gA DB_SOURCE=() DB_TARGET=()
	IFS=, read -r -a pairs <<<"${DATABASE_MAPPING:-}"
	for pair in "${pairs[@]}"; do
		if [[ -n ${DB_TARGET["${pair%%:*}"]:-} || -n ${DB_SOURCE["${pair#*:}"]:-} || ${pair%%:*} == "${pair#*:}" ]]; then
			die "DATABASE_MAPPING maps a database twice, or to itself"
		fi
		DB_TARGET["${pair%%:*}"]=${pair#*:}
		DB_SOURCE["${pair#*:}"]=${pair%%:*}
	done
	for pair in "${pairs[@]}"; do
		if [[ -n ${DB_SOURCE["${pair%%:*}"]:-} ]]; then
			die "DATABASE_MAPPING maps a database another one is mapped to"
		fi
	done
}

# restore_env sets ACTION_ENV to the --env flags of the commands of the restore: those of SOURCE_ENV, the tables and
# table engines it leaves out, and, for a restore of partitions, no check of the replicas before the parts are
# attached, as the Altinity Cloud Manager restores partitions. A typed request takes none of them
restore_env() {
	ACTION_ENV=${SOURCE_ENV}
	if [[ -n ${SKIP_TABLES:-} ]]; then
		ACTION_ENV+="${ACTION_ENV:+ }--env=CLICKHOUSE_SKIP_TABLES=${DEFAULT_SKIP_TABLES},${SKIP_TABLES}"
	fi
	if [[ -n ${SKIP_TABLE_ENGINES:-} ]]; then
		ACTION_ENV+="${ACTION_ENV:+ }--env=CLICKHOUSE_SKIP_TABLE_ENGINES=${SKIP_TABLE_ENGINES}"
	fi
	if [[ -n ${PARTITIONS:-} ]]; then
		ACTION_ENV+="${ACTION_ENV:+ }--env=CLICKHOUSE_CHECK_REPLICAS_BEFORE_ATTACH=0"
	fi
}

# download_args HOST sets STEP_ARGS to the download of the backup to HOST: with the data on a host of DATA_DOWNLOADS,
# which restores the data, the schema alone on another
download_args() {
	if [[ " ${DATA_DOWNLOADS} " == *" $1 "* ]]; then
		restore_command download
	else
		restore_command download_schema
	fi
}

# drop_downloads HOST... deletes, as well as it can, the copies of the backup the download step of a restore made on
# every HOST, which a restore would take for its own; a host that does not answer is asked once
drop_downloads() {
	local host BEST_EFFORT=1 READ_ONLY=1
	for host in "$@"; do
		if list_backups "${host}" local && find_backup "${BACKUP_NAME}" && ! delete_local "${host}" "${BACKUP_NAME}"; then
			advise "${host}: the copy of ${BACKUP_NAME} the restore downloaded is left on local disk"
		fi
	done
}

# other_installation_hint sets REPLY to what the preflight adds to a backup it finds missing at the storage path of the
# target cluster when that path keeps apart the backups of installations of another name or namespace, empty otherwise
other_installation_hint() {
	local macros= what= fields=
	REPLY=
	if [[ ${SELF_PATH} == *"{installation}"* ]]; then
		macros="{installation}"
		what="name"
		fields="sourceInstallation"
	fi
	if [[ ${SELF_PATH} == *"{namespace}"* ]]; then
		macros+="${macros:+ and }{namespace}"
		what+="${what:+ or }namespace"
		fields+="${fields:+, }sourceNamespace"
	fi
	if [[ -n ${macros} ]]; then
		REPLY="; the storage path '${SELF_PATH}' holds ${macros}, which keeps apart the backups of installations of another ${what}: a backup made by one of them is found by naming that installation in the restore (${fields})"
	fi
}

do_restore() {
	local host restore_data=1 overwrite= validate= restore_rbac= holds_rbac= replicas= hint where base READ_ONLY=
	local -a schema_hosts data_hosts targets guarded used shards had_local=() downloads
	check_name BACKUP_NAME
	parse_hosts SCHEMA_HOSTS
	schema_hosts=("${HOSTS[@]}")
	if is_true SCHEMA_ONLY; then
		restore_data=
		parse_hosts DATA_HOSTS optional
	else
		parse_hosts DATA_HOSTS
	fi
	data_hosts=("${HOSTS[@]}")
	if is_true OVERWRITE; then
		overwrite=1
	fi
	if is_true VALIDATE_TOPOLOGY; then
		validate=1
	fi
	if is_true RESTORE_RBAC; then
		restore_rbac=1
	fi
	if [[ -n ${CLUSTER:-} || -n ${overwrite} ]]; then
		check_cluster
	fi
	if [[ -n ${TABLES:-} ]]; then
		if [[ ! ${TABLES} =~ ${TABLES_RE} ]] || ((${#TABLES} > TABLES_MAX_LENGTH)); then
			die "TABLES is not a valid table pattern"
		fi
	fi
	check_restore_filters
	check_source_name SOURCE_INSTALLATION "${SOURCE_INSTALLATION_MAX_LENGTH}" "installation name"
	check_source_name SOURCE_NAMESPACE "${SOURCE_NAMESPACE_MAX_LENGTH}" namespace
	# An overwrite may drop the tables of the backup ON CLUSTER only where a replica could clone their old parts back from
	# another replica of its shard: a shard of one replica, which every data host is, has none
	if [[ -n ${overwrite} ]]; then
		if [[ ! ${MAX_TABLE_SIZE_TO_DROP:-} =~ ${NUMBER_RE} ]]; then
			die "MAX_TABLE_SIZE_TO_DROP must be a number of bytes"
		fi
		for host in "${schema_hosts[@]}"; do
			if ! listed "${host}" "${data_hosts[@]}"; then
				replicas=1
			fi
		done
	fi
	if [[ -n ${replicas} ]]; then
		# Only read once the tables are listed, but checked before any request
		no_zookeeper || :
	fi
	used=("${schema_hosts[@]}")
	if [[ -n ${restore_data} ]]; then
		used+=("${data_hosts[@]}")
	fi
	mapfile -t used < <(printf '%s\n' "${used[@]}" | awk '!seen[$0]++')
	targets=("${schema_hosts[@]}" "${data_hosts[@]}")
	mapfile -t targets < <(printf '%s\n' "${targets[@]}" | awk 'NF && !seen[$0]++')
	# The schema restore drops the tables of the backup on every replica, so the guard checks them all
	parse_hosts GUARD_HOSTS optional
	guarded=("${targets[@]}" "${HOSTS[@]}")
	mapfile -t guarded < <(printf '%s\n' "${guarded[@]}" | awk 'NF && !seen[$0]++')
	check_storage_paths
	source_paths
	wait_for_apis "${guarded[@]}"

	event Normal OrchestratorRestoreStarted "restore ${BACKUP_NAME} on ${#targets[@]} host(s)"
	# Nothing destructive happens before every check has passed on every host
	if [[ -n ${overwrite} ]]; then
		log "restore ${BACKUP_NAME}: preflight on ${#targets[@]} host(s)"
	else
		log "restore ${BACKUP_NAME}: preflight on ${#targets[@]} host(s), overwrite guard on ${#guarded[@]}"
	fi
	for host in "${targets[@]}"; do
		call GET "${host}" /backup/version
		if [[ ${HTTP_CODE} != 200 ]]; then
			die "preflight failed: sidecar of ${host} answered HTTP ${HTTP_CODE}: $(error_of)"
		fi
	done
	# Every host that restores the backup downloads it, which a local copy of it, or a backup of its name at the own
	# storage path, would be taken for; a backup read at the own storage path is that backup
	if [[ -n ${SOURCE_ENV} ]]; then
		for host in "${used[@]}"; do
			refuse_local_copy "${host}"
			if [[ -z ${SOURCE_OWN_PATH} ]]; then
				refuse_own_backup "${host}"
			fi
		done
	fi
	if [[ -n ${validate} ]]; then
		other_installation_hint
		hint=${REPLY}
		for host in "${used[@]}"; do
			where="the storage of ${host}"
			if [[ -n ${SOURCE_ENV} ]]; then
				# A sidecar lists the backups at its own paths alone: the copy downloaded from the source tells
				where="the storage path '${SOURCE_PATH_SHOWN}' of ${host}"
				source_copy "${host}"
				if ! list_backups "${host}" local; then
					fail "preflight failed on ${host}"
				fi
				if ! find_backup "${BACKUP_NAME}"; then
					die "preflight failed: backup ${BACKUP_NAME} is missing in ${where}: its download left no local copy"
				fi
				base=${LIST_REQUIRED[FOUND]}
				if [[ -n ${base} && ${OWN_BACKUPS[${host}]:-} == *" ${base} "* ]]; then
					die "preflight failed: backup ${BACKUP_NAME} is incremental from ${base}, and ${host} holds a backup ${base} at its own storage path too, which clickhouse-backup would take for the one at the storage path '${SOURCE_PATH_SHOWN}', as it tells the backups of every path apart by name alone: delete that one first"
				fi
			else
				if ! list_backups "${host}" remote; then
					fail "preflight failed on ${host}"
				fi
				if ! find_backup "${BACKUP_NAME}"; then
					if [[ ${SELF_PATH} == *"{replica}"* ]]; then
						die "preflight failed: backup ${BACKUP_NAME} is missing in the storage of ${host}: the storage path '${SELF_PATH}' holds {replica}, which gives every replica a path of its own, and the backup was made on the first replica of each shard only${hint}"
					fi
					die "preflight failed: backup ${BACKUP_NAME} is missing in the storage of ${host}${hint}"
				fi
			fi
			if [[ ${LIST_DESC[FOUND]} == broken* ]]; then
				die "preflight failed: backup ${BACKUP_NAME} is ${LIST_DESC[FOUND]} in ${where}"
			fi
			# Sizes are compared as text, as holds_data does
			if [[ ${LIST_RBAC_SIZE[FOUND]:-0} != 0 ]]; then
				holds_rbac=1
			fi
		done
	fi
	if [[ -z ${overwrite} ]]; then
		guard_tables "${used[*]}" "${guarded[@]}"
	elif [[ -n ${replicas} ]]; then
		shards=("${schema_hosts[0]}" "${data_hosts[@]}")
		mapfile -t shards < <(printf '%s\n' "${shards[@]}" | awk '!seen[$0]++')
		plan_overwrite "${shards[*]}" "${guarded[@]}"
	else
		log "restore ${BACKUP_NAME}: every shard of cluster ${CLUSTER} has one replica, which drops the tables of the backup on its own"
		if drop_size_limited; then
			declare -gA BACKUP_TABLES=() BACKUP_DBS=()
			BACKUP_TABLES_UNREADABLE=
			live_tables "${schema_hosts[@]}"
			refuse_database_drops "${schema_hosts[*]}" "${schema_hosts[@]}"
		fi
	fi
	if [[ -z ${SOURCE_ENV} ]]; then
		for host in "${used[@]}"; do
			if list_backups "${host}" local && find_backup "${BACKUP_NAME}"; then
				had_local+=("${host}")
			fi
		done
	else
		# Only the copies of the schema the preflight downloaded are left, which a restore would take for the backup
		drop_source_copies strict
		# The restore steps download the backup to every host they restore it on, where a step that fails leaves a copy
		SOURCE_COPIES=("${used[@]}")
	fi
	restore_env
	RESTORE_KIND=op
	if [[ -n ${ACTION_ENV} ]]; then
		RESTORE_KIND=action
	fi

	# As the Altinity Cloud Manager restores, every host first downloads the backup, at once, the data to those that
	# restore it, hard-linking every file of it already on their disks: a host short of disk space fails the restore here,
	# before anything is dropped. A host that holds a copy of the backup already restores from it
	downloads=()
	for host in "${used[@]}"; do
		if ! listed "${host}" ${had_local[@]+"${had_local[@]}"}; then
			downloads+=("${host}")
		fi
	done
	DATA_DOWNLOADS=
	if [[ -n ${restore_data} ]]; then
		DATA_DOWNLOADS=${data_hosts[*]}
	fi
	if ((${#downloads[@]} > 0)); then
		# A download changes nothing the Job is asked to change: a restore that fails there leaves the cluster as it was
		READ_ONLY=1
		if ! run_all "${RESTORE_KIND}" "${downloads[*]}" download_args download "${BACKUP_NAME}"; then
			READ_ONLY=
			if [[ -z ${SOURCE_ENV} ]]; then
				drop_downloads "${downloads[@]}"
			fi
			fail_all "download of ${BACKUP_NAME}"
		fi
		READ_ONLY=
	fi

	# The drop ON CLUSTER drops the tables of the backup the pattern takes, and the databases it takes as a whole, on every
	# host from the first schema host, and recreates them as the backup of its shard holds them, which the preflight found
	# alike on every shard: no replica then keeps an old part that a replica which recreated its table would clone back.
	# clickhouse-backup rewrites on the way a Distributed table that names another cluster, which the restore of every
	# replica on its own undoes next, as it drops and creates that table again from the backup
	if [[ -n ${ON_CLUSTER} ]]; then
		if ! restore_step "${schema_hosts[0]}" "restore schema on cluster ${CLUSTER}" schema_on_cluster; then
			fail "schema restore of ${BACKUP_NAME} on cluster ${CLUSTER} failed on ${schema_hosts[0]}"
		fi
	fi
	# Every replica restores the schema on its own, dropping what it has of the backup first: a table that has come
	# through the replication of a Replicated database would fail its CREATE
	for host in "${schema_hosts[@]}"; do
		if ! restore_step "${host}" "restore schema" schema; then
			fail "schema restore of ${BACKUP_NAME} failed on ${host}"
		fi
	done
	# Every host that restores data restores it at once
	if [[ -n ${restore_data} ]]; then
		restore_command data
		if ! run_all "${RESTORE_KIND}" "${data_hosts[*]}" "" "restore data" "${BACKUP_NAME}" "${STEP_ARGS[@]}"; then
			fail_all "data restore of ${BACKUP_NAME}"
		fi
	fi
	# Access entities are stored apart on every host, and a backup holds those of the replica it was made on
	if [[ -n ${restore_rbac} ]]; then
		for host in "${schema_hosts[@]}"; do
			if ! restore_step "${host}" "restore rbac" rbac; then
				fail "access entity restore of ${BACKUP_NAME} failed on ${host}"
			fi
		done
	fi
	# Drop the copies the restore downloaded, they would otherwise double the disk usage
	for host in "${used[@]}"; do
		if [[ " ${had_local[*]} " == *" ${host} "* ]]; then
			continue
		fi
		if list_backups "${host}" local && find_backup "${BACKUP_NAME}" && ! delete_local "${host}" "${BACKUP_NAME}"; then
			warn "${host}: the downloaded copy of ${BACKUP_NAME} is left on local disk"
		fi
	done
	log "restore ${BACKUP_NAME} completed"
	event Normal OrchestratorRestoreCompleted "restore ${BACKUP_NAME} completed"
	report_rbac "${restore_rbac}" "${holds_rbac}" "${validate}"
}

# report_rbac RESTORED HOLDS LISTED tells when the restore that has just completed left out the access entities the
# backup holds: it then writes RBAC_NOT_RESTORED_MESSAGE to the termination message, which the operator reports on the
# restore's resource. The backup holds them when any host lists a size of them; one not LISTED leaves that unknown
report_rbac() {
	if [[ -n $1 ]]; then
		return 0
	fi
	if [[ -z $3 ]]; then
		log "whether ${BACKUP_NAME} holds access entities is unknown: its copies in the storage were not listed"
		return 0
	fi
	if [[ -z $2 ]]; then
		return 0
	fi
	warn "${BACKUP_NAME} holds access entities - users, roles, grants - which this restore did not restore"
	event Warning OrchestratorRBACNotRestored "${LAST_WARNING}"
	if [[ -n ${RBAC_NOT_RESTORED_MESSAGE:-} ]]; then
		termination_message "${RBAC_NOT_RESTORED_MESSAGE}"
	fi
}

# The steps of a restore on every host, and the backups at the own storage path of every host
declare -A LAST_OP=() LAST_ACTION_START=() LAST_ACTION_COMMAND=() OWN_BACKUPS=()
# The step that runs on every host: the operation id of an operation, the command of an action and the start times of
# the rows of that command before it was submitted
declare -A OP_ID=() ACTION_COMMAND=() ACTION_BEFORE=()
# The sets of backups list_all fills, which retention_plan reads
declare -A COMPLETE=() WITH_DATA=() EMPTY=() SIZED=() BROKEN=() ALL_NAMES=()
# The database mapping of a restore: the database of the backup restored into each target, and the target of each
declare -A DB_SOURCE=() DB_TARGET=()
FAILED_HOSTS=()
FAILURES=()
STEP_FAILED_HOSTS=()
# The arguments of the step of a host, as restore_command makes them; how the restore steps run, RESTORE_KIND op or
# action, and ACTION_ENV, the --env flags of their commands; the hosts DATA_DOWNLOADS lists download the data too
STEP_ARGS=()
RESTORE_KIND=op
ACTION_ENV=
DATA_DOWNLOADS=

main() {
	# Retention keeps its sets in associative arrays, which bash handles under set -u from 4.4 on
	if ((BASH_VERSINFO[0] < 4 || (BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] < 4))); then
		die "bash 4.4 or newer is required, the image has ${BASH_VERSION}"
	fi
	API_PORT=${API_PORT:-}
	POLL_INTERVAL=${POLL_INTERVAL:-5}
	if [[ ! ${API_PORT} =~ ${NUMBER_RE} ]]; then
		die "API_PORT must be a port number"
	fi
	if [[ ! ${POLL_INTERVAL} =~ ${INTERVAL_RE} ]]; then
		die "POLL_INTERVAL must be a number of seconds"
	fi
	if [[ -z ${API_USERNAME:-} || -z ${API_PASSWORD:-} ]]; then
		die "API_USERNAME and API_PASSWORD must be set"
	fi
	AUTH=$(printf '%s:%s' "${API_USERNAME}" "${API_PASSWORD}" | base64 | tr -d '\n')
	check_transport

	case ${OPERATION:-} in
	backup) do_backup ;;
	verify) do_verify ;;
	restore) do_restore ;;
	*) die "OPERATION must be one of backup, verify, restore" ;;
	esac
}

# Run as a script - bash -c, or a file - and not when sourced, which leaves the functions to the tests
if [[ ${BASH_SOURCE[0]:-$0} == "$0" ]]; then
	main "$@"
fi
