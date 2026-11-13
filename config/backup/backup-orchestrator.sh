# backup-orchestrator.sh drives the clickhouse-backup REST API of one cluster: through the sidecar of every pod
# (BACKUP_TRANSPORT=sidecar) or the backup Pod of every shard (pod). It backs up, verifies, restores and prunes.
# Every input arrives through the environment (see script.go), never spliced into this text; values reach curl as
# URL-encoded parameters, or commands of the actions endpoint as tokens validated against the *_RE regexes.
# Sourced, it runs nothing and leaves its functions to the tests. See docs/backup.md for what it does and why.
# shellcheck disable=SC2034 # arrays that merge_keys and json_row fill through namerefs
set -euo pipefail

NAME_RE='^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'
HOST_RE='^[A-Za-z0-9]([A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$'
NUMBER_RE='^[0-9]+$'
TABLES_RE='^[A-Za-z0-9_.*?,][A-Za-z0-9_.*?,-]*$'
TABLES_MAX_LENGTH=1024
PARTITIONS_RE='^[A-Za-z0-9_-]+(,[A-Za-z0-9_-]+)*$'
ENGINES_RE='^[A-Za-z][A-Za-z0-9_]*(,[A-Za-z][A-Za-z0-9_]*)*$'
DATABASE_MAPPING_RE='^[A-Za-z0-9_][A-Za-z0-9_-]*:[A-Za-z0-9_][A-Za-z0-9_-]*(,[A-Za-z0-9_][A-Za-z0-9_-]*:[A-Za-z0-9_][A-Za-z0-9_-]*)*$'
FILTER_MAX_LENGTH=16384
DEFAULT_SKIP_TABLES='system.*,INFORMATION_SCHEMA.*,information_schema.*,_temporary_and_external_tables.*'

INDEX_RE='^(0|[1-9][0-9]{0,3})$'
SOURCE_NAME_RE='^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'
SOURCE_INSTALLATION_MAX_LENGTH=253
SOURCE_NAMESPACE_MAX_LENGTH=63
SOURCE_PATH_RE='^[A-Za-z0-9_./{}-]+$'
TAG_RE='^[0-9]{14}$'
NAMING_RE='^([A-Za-z0-9._-]|\{[A-Za-z]+\})+$'
NAMING_MAX_LENGTH=128
NAMING_TAG_RE='^[0-9]{8}T?[0-9]{6}$'
NAMING_SLOT_TOLERANCE=1800
SLOT_RE='^[0-9]{1,10}$'
SCHEDULE_TIME_RE='^([01][0-9]|2[0-3]):([0-5][0-9])$'
KEEP_RE='^[1-9][0-9]{0,5}$'
ZONE_RE='^[A-Za-z0-9_+-]+(/[A-Za-z0-9_+-]+)*$'
INTERVAL_RE='^[0-9]+([.][0-9]+)?$'
EXIT_CODE_RE='^[1-9][0-9]?$'
IN_PROGRESS_RE='"status":"in progress"'
TABLE_ROW_RE='^\{"Database":"([^"\\]*)","Name":"([^"\\]*)"'
BACKUP_TABLE_ROW_RE='^\{"database":"([^"\\]*)","table":"([^"\\]*)"'
SYSTEM_DATABASES=' system INFORMATION_SCHEMA information_schema _temporary_and_external_tables '
LOCKED_ERROR='another operation is currently running'
SIZE_LIMIT_ADVICE='set max_table_size_to_drop to 0 wherever a limit is set (the smallest non-zero limit applies), or drop these tables by hand, and restore again'
PATH_CHECK_KEYS=(
	REMOTE_STORAGE
	S3_BUCKET S3_ENDPOINT S3_PATH S3_OBJECT_DISK_PATH
	GCS_BUCKET GCS_ENDPOINT GCS_PATH GCS_OBJECT_DISK_PATH
	AZBLOB_ACCOUNT_NAME AZBLOB_CONTAINER AZBLOB_ENDPOINT_SUFFIX AZBLOB_PATH AZBLOB_OBJECT_DISK_PATH
	COS_URL COS_PATH COS_OBJECT_DISK_PATH
	FTP_ADDRESS FTP_PATH FTP_OBJECT_DISK_PATH
	SFTP_ADDRESS SFTP_PORT SFTP_PATH SFTP_OBJECT_DISK_PATH
)
CONFIG_OVERRIDE_RE='^[A-Z][A-Z0-9_]*=[A-Za-z0-9_.,:/@+=-]+$'
CONFIG_OVERRIDE_KEY_MAX_LENGTH=64
CONFIG_OVERRIDE_VALUE_MAX_LENGTH=256
CONFIG_OVERRIDES_MAX_ITEMS=16
CONFIG_OVERRIDE_FORBIDDEN=' API_LISTEN API_SECURE API_ALLOW_PARALLEL RESTORE_SCHEMA_ON_CLUSTER CLICKHOUSE_RESTART_COMMAND RESTORE_DATABASE_MAPPING RESTORE_TABLE_MAPPING BACKUPS_TO_KEEP_REMOTE REBASE_DURING_DELETE REBASE_BEFORE_REMOVE_OLD_REMOTE API_USERNAME API_PASSWORD CLICKHOUSE_USERNAME CLICKHOUSE_PASSWORD CLICKHOUSE_BACKUP_CONFIG CLICKHOUSE_HOST CLICKHOUSE_PORT CLICKHOUSE_SECURE CLICKHOUSE_SKIP_VERIFY BACKUPS_TO_KEEP_LOCAL ALLOW_EMPTY_BACKUPS API_CREATE_INTEGRATION_TABLES '
CONFIG_OVERRIDE_SENSITIVE=(PASSWORD SECRET ACCESS_KEY SECRET_KEY ACCOUNT_KEY SSE_KEY SSE_CUSTOMER_KEY ENCRYPTION_KEY TOKEN CREDENTIAL PRIVATE_KEY SAS CONNECTION_STRING)
HELD_REPORT_MAX=24
REQUEST_TIMEOUT=600
DELETE_TIMEOUT=3600
UNREACHABLE_TIMEOUT=600
MUTATED_UNREACHABLE_TIMEOUT=3600
RESUME_TIMEOUT=300
DEFAULT_BACKUP_POD_WAIT_SECONDS=600
SERVICE_ACCOUNT_DIR=/var/run/secrets/kubernetes.io/serviceaccount
EVENT_TIMEOUT=5
EVENT_MAX_FAILURES=3
EVENT_SOURCE_COMPONENT=clickhouse-backup-orchestrator
EVENT_MESSAGE_MAX_LENGTH=1024
EVENT_FAILURES=0
EVENTS_DISABLED=
SCHED_PERIOD=()
SCHED_HOUR=()
SCHED_MINUTE=()
SCHED_DAY=()
SCHED_KEEP=()
SCHED_INCREMENTAL=()
SCHED_ENV=()
SLOT_TAG=
WINNER=
NAMING_PARTS=()
declare -A SHARD_OF=() HOST_BACKUP=()
SLOT_TOLERANCE=0
BACKUP_KEY=
NAMING_PREFIX=
BACKUP_NAME_GIVEN=
MUTATED=
READ_ONLY=
NO_STEP=
LAST_WARNING=
SELF_PATH=
SELF_OBJECT_DISK_PATH=
SELF_STORAGE_PREFIX=
SELF_CLUSTER_NAME=
SELF_PATHS_UNKNOWN=
SOURCE_ENV=
SOURCE_PATH_SHOWN=
SOURCE_HINT=
SOURCE_OWN_PATH=
SOURCE_COPIES=()
NO_LOCAL_COPY=()
ON_CLUSTER=

log() { TZ=UTC0 printf '%(%Y-%m-%dT%H:%M:%SZ)T %s\n' -1 "$*"; }
warn() {
	LAST_WARNING=$*
	log "WARNING: $*" >&2
}
advise() {
	log "WARNING: $*" >&2
}
termination_message() {
	if [[ -n ${TERMINATION_MESSAGE_PATH:-} ]]; then
		{ printf '%s\n' "$*" >"${TERMINATION_MESSAGE_PATH}"; } 2>/dev/null || true
	fi
}
# report LINE [DETAIL] appends LINE, and DETAIL after it, to the termination message, unless LINE is empty
report() {
	if [[ -n $1 && -n ${TERMINATION_MESSAGE_PATH:-} ]]; then
		{ printf '%s\n' "$1${2:+ $2}" >>"${TERMINATION_MESSAGE_PATH}"; } 2>/dev/null || true
	fi
}
# notice REASON MESSAGE logs MESSAGE and posts it as a Normal Event
notice() {
	log "$2"
	event Normal "$1" "$2"
}
# alert REASON MESSAGE warns of MESSAGE and posts it as a Warning Event
alert() {
	warn "$2"
	event Warning "$1" "$2"
}
die() {
	log "ERROR: $*" >&2
	termination_message "$*"
	operation_title
	event Warning "Orchestrator${REPLY}Failed" "$*"
	if ((${#SOURCE_COPIES[@]} > 0)); then
		drop_source_copies
	fi
	if [[ -z ${MUTATED} && ${UNCHANGED_EXIT_CODE:-} =~ ${EXIT_CODE_RE} ]]; then
		exit "${UNCHANGED_EXIT_CODE}"
	fi
	exit 1
}
# fail MESSAGE dies after a step that has just warned why it failed, adding that reason to MESSAGE
fail() {
	die "$*${LAST_WARNING:+: ${LAST_WARNING}}"
}

# operation_title sets REPLY to OPERATION as it names the reasons of Events
operation_title() {
	case ${OPERATION:-} in
	backup) REPLY=Backup ;;
	verify) REPLY=Verify ;;
	restore) REPLY=Restore ;;
	*) REPLY= ;;
	esac
}

# json_escape VALUE sets REPLY to VALUE as the content of a JSON string
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

truncate_bytes() {
	local LC_ALL=C value=$1 max=$2 i n
	if ((${#value} <= max)); then
		REPLY=${value}
		return 0
	fi
	value=${value:0:max}
	for ((i = 1; i <= 4 && i <= ${#value}; i++)); do
		printf -v n '%d' "'${value: -i:1}"
		if ((n >= 0x80 && n < 0xc0)); then
			continue
		fi
		if ((n >= 0xf0 && i < 4 || n >= 0xe0 && i < 3 || n >= 0xc0 && i < 2)); then
			value=${value:0:${#value}-i}
		fi
		break
	done
	REPLY=${value}
}

# event_body TYPE REASON MESSAGE TIME sets REPLY to the core/v1 Event of TYPE
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

# event TYPE REASON MESSAGE posts a Kubernetes Event on the object EVENT_OBJECT_* names
event() {
	local type=$1 reason=$2 message=$3 host=${KUBERNETES_SERVICE_HOST:-} at code token=
	if [[ -n ${EVENTS_DISABLED} || -z ${EVENT_OBJECT_KIND:-} || -z ${EVENT_OBJECT_NAME:-} || -z ${EVENT_OBJECT_UID:-} ||
		-z ${EVENT_OBJECT_API_VERSION:-} || -z ${POD_NAMESPACE:-} || -z ${host} || ! -r ${SERVICE_ACCOUNT_DIR}/token ]]; then
		return 0
	fi
	if [[ ${host} == *:* ]]; then
		host="[${host}]"
	fi
	TZ=UTC0 printf -v at '%(%Y-%m-%dT%H:%M:%SZ)T' -1
	event_body "${type}" "${reason}" "${message}" "${at}"
	IFS= read -r token <"${SERVICE_ACCOUNT_DIR}/token" || :
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
	advise "cannot post the event ${reason}: ${code}"
	if ((EVENT_FAILURES >= EVENT_MAX_FAILURES)); then
		EVENTS_DISABLED=1
		advise "no more events are posted after ${EVENT_FAILURES} failures in a row"
	fi
}

# json_get FIELD JSON sets REPLY to the raw, still escaped, value of the first string or number field FIELD
json_get() {
	local string='"'"$1"'":"(([^"\\]|\\.)*)"' number='"'"$1"'":([0-9]+)'
	REPLY=
	if [[ $2 =~ ${string} ]] || [[ $2 =~ ${number} ]]; then
		REPLY=${BASH_REMATCH[1]}
	fi
}

# json_row JSON FIELD=ARRAY... appends to every ARRAY what json_get finds of its FIELD in JSON
json_row() {
	local _json=$1 _pair
	shift
	for _pair in "$@"; do
		local -n _array=${_pair#*=}
		json_get "${_pair%%=*}" "${_json}"
		_array+=("${REPLY}")
	done
}

# is_true VAR_NAME reports whether the boolean env var VAR_NAME is set to true
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

# check_filter VAR_NAME REGEX MAX_LENGTH WHAT fails unless VAR_NAME is empty, or matches REGEX in MAX_LENGTH characters
check_filter() {
	local value=${!1:-}
	if [[ -n ${value} ]] && [[ ! ${value} =~ $2 ]] || ((${#value} > $3)); then
		die "$1 is not a valid $4"
	fi
}

# one_line VAR_NAME fails unless VAR_NAME is one line
one_line() {
	if [[ ${!1:-} == *[[:cntrl:]]* ]]; then
		die "$1 must be one line"
	fi
}

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

# unique_into ARRAY ITEM... sets ARRAY to the ITEMs but the empty ones, each once, in their order
unique_into() {
	local -n _unique=$1
	local _item
	local -A _seen=()
	shift
	_unique=()
	for _item in "$@"; do
		if [[ -n ${_item} && -z ${_seen[${_item}]:-} ]]; then
			_seen[${_item}]=1
			_unique+=("${_item}")
		fi
	done
}

# json_string VALUE sets REPLY to VALUE as a JSON string
json_string() {
	if [[ $1 == *[\"\\[:cntrl:]]* ]]; then
		die "a command value holds a character that JSON escapes"
	fi
	REPLY="\"$1\""
}

# parse_hosts VAR_NAME [optional] fills HOSTS from the space-separated host list in VAR_NAME
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

request() {
	local method=$1 host=$2 path=$3 out last connect code url
	shift 3
	HTTP_CODE=000
	CURL_EXIT=0
	CURL_CONNECTED=1
	REQUEST_URL="${host}:${API_PORT}${path}"
	out=$(printf 'Authorization: Basic %s\n' "${AUTH}" |
		curl -sS -X "${method}" -H @- --connect-timeout 10 --max-time "${REQUEST_TIMEOUT}" \
			-w '\n%{time_connect} %{http_code} %{url_effective}' "$@" "http://${host}:${API_PORT}${path}" 2>&1) || CURL_EXIT=$?
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

# unreachable METHOD reports whether the last request failed in a way that is safe to retry
unreachable() {
	case ${CURL_EXIT} in
	0) [[ $1 == GET ]] && proxy_failed ;;
	6 | 7) return 0 ;;
	*) [[ $1 == GET || -z ${CURL_CONNECTED} ]] ;;
	esac
}

proxy_failed() {
	[[ ${HTTP_CODE} == 50[234] ]]
}

# changes_so_far SEPARATOR SEPARATOR_CHANGED sets REPLY to what the run has changed so far, after the separator of its case
changes_so_far() {
	if [[ -z ${MUTATED} ]]; then
		REPLY="$1 nothing was changed"
	else
		REPLY="$2 the ${OPERATION} is partially applied"
	fi
}

# call METHOD HOST PATH [CURL_ARGS...] performs an API call, retrying while the sidecar is unreachable
call() {
	local method=$1 host=$2 last= since=${SECONDS} limit
	while :; do
		request "$@"
		if [[ -n ${BEST_EFFORT:-} ]] || ! unreachable "${method}"; then
			return 0
		fi
		limit=${UNREACHABLE_TIMEOUT}
		if [[ -n ${MUTATED} ]]; then
			limit=${MUTATED_UNREACHABLE_TIMEOUT}
		fi
		if ((SECONDS - since >= limit)); then
			changes_so_far , ,
			die "${host}: sidecar unreachable for $((SECONDS - since))s (curl exit ${CURL_EXIT}, HTTP ${HTTP_CODE})${REPLY}"
		fi
		if [[ ${last} != "${CURL_EXIT}/${HTTP_CODE}" ]]; then
			last="${CURL_EXIT}/${HTTP_CODE}"
			log "${host}: sidecar unreachable (curl exit ${CURL_EXIT}, HTTP ${HTTP_CODE}), retrying"
		fi
		sleep "${POLL_INTERVAL}"
	done
}

# error_of prints the sidecar error carried by the last response; run as $(error_of), it drops trailing newlines
error_of() {
	local err
	err=$(
		json_get error "${HTTP_BODY}"
		printf '%s' "${REPLY}"
	)
	printf '%s' "${err:-${HTTP_BODY}}"
}

# get_ok HOST WHAT PATH [CURL_ARGS...] GETs PATH of HOST, warning that it cannot WHAT unless HOST answers 200
get_ok() {
	local host=$1 what=$2
	shift 2
	call GET "${host}" "$@"
	if [[ ${HTTP_CODE} != 200 ]]; then
		warn "${host}: cannot ${what} (HTTP ${HTTP_CODE}): $(error_of)"
		return 1
	fi
}

wait_idle() {
	local host=$1 announced=
	while :; do
		if ! get_ok "${host}" "read sidecar actions" /backup/actions; then
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

changed() {
	if [[ -z ${READ_ONLY:-} ]]; then
		MUTATED=1
	fi
}

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
	notice OrchestratorCommand "${message}"
}

finished_event() {
	local command
	json_get command "${HTTP_BODY}"
	command=${REPLY//\\\"/\"}
	command=${command//\\\\/\\}
	event Normal OrchestratorCommandFinished "$1: $2 (operation $3) ended with status ${4:-unknown}${command:+: ${command}}"
}

submit() {
	local host=$1 what=$2
	shift 2
	while :; do
		if ! wait_idle "${host}"; then
			return 1
		fi
		call POST "${host}" "$@"
		if ((CURL_EXIT != 0)); then
			if ! unreachable POST; then
				changed
				command_event "${host}" "${what}" "sent, no answer: curl exit ${CURL_EXIT}" "$@"
			fi
			warn "${host}: ${what} request failed: ${HTTP_BODY}"
			return 1
		fi
		if proxy_failed; then
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

# remember_step HOST [OPERATION_ID [START COMMAND]] records the last step that succeeded on HOST
remember_step() {
	LAST_OP[$1]=${2:-}
	LAST_ACTION_START[$1]=${3:-}
	LAST_ACTION_COMMAND[$1]=${4:-}
}

# sidecar_kept HOST reports whether the sidecar of HOST still knows the last step this Job ran on it
sidecar_kept() {
	local host=$1 i
	if [[ -n ${LAST_OP[${host}]:-} ]]; then
		if ! get_ok "${host}" "read the status of the last step" /backup/status -G --data-urlencode "operationid=${LAST_OP[${host}]}"; then
			return 1
		fi
		json_get status "${HTTP_BODY}"
		if [[ ${REPLY} == success ]]; then
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
	changes_so_far ";" ":"
	warn "${host}: the sidecar restarted after the last step of this ${OPERATION} on it, which it no longer knows${REPLY}"
	return 1
}

# action_rows HOST COMMAND [NAME] sets ACTION_STARTS, ACTION_STATUSES and ACTION_ERRORS to the rows of the actions of
# HOST that ran COMMAND, or, with NAME, that resumed the COMMAND (upload or download) of the backup NAME
action_rows() {
	local host=$1 line
	ACTION_STARTS=() ACTION_STATUSES=() ACTION_ERRORS=()
	if ! get_ok "${host}" "read sidecar actions" /backup/actions; then
		return 1
	fi
	while IFS= read -r line; do
		json_get command "${line}"
		if [[ -n ${3:-} && ${REPLY} != "$2 "*"--resumable=1 $3" || -z ${3:-} && ${REPLY} != "$2" ]]; then
			continue
		fi
		json_row "${line}" start=ACTION_STARTS status=ACTION_STATUSES error=ACTION_ERRORS
	done <<<"${HTTP_BODY}"
}

# run_step KIND HOST WHAT NAME ARGS... runs a step on HOST and polls it until it ends: an op, op_start PATH
# [CURL_ARGS...], or an action, action_start COMMAND; unless NO_STEP, its failure is an Event
run_step() {
	local kind=$1 rc=2
	shift
	if "${kind}_start" "$@"; then
		while ((rc == 2)); do
			sleep "${POLL_INTERVAL}"
			rc=0
			"${kind}_poll" "$1" "$2" "$3" || rc=$?
		done
		if ((rc == 0)); then
			return 0
		fi
	fi
	if [[ -z ${NO_STEP:-} ]]; then
		event Warning OrchestratorHostFailed "$1: $2 $3 failed${LAST_WARNING:+: ${LAST_WARNING}}"
	fi
	return 1
}

# step_succeeded HOST WHAT NAME [OPERATION_ID [START COMMAND]] tells of a step that succeeded and, unless NO_STEP,
# remembers it
step_succeeded() {
	log "$1: $2 $3 succeeded"
	if [[ -z ${NO_STEP:-} ]]; then
		remember_step "$1" "${@:4}"
		event Normal OrchestratorHostStepSucceeded "$1: $2 $3 succeeded"
	fi
}

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
	json_get status "${HTTP_BODY}"
	if [[ ${REPLY} != acknowledged ]]; then
		warn "${host}: ${what} was not acknowledged: ${HTTP_BODY}"
		return 1
	fi
}

# action_poll HOST WHAT NAME reads once the row of the action action_start submitted to HOST: 2 while it runs
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
		step_succeeded "${host}" "${what}" "${name}" "" "${start}" "${command}"
		return 0
		;;
	"") ;;
	*)
		warn "${host}: ${what} ${name} ended with status '${status}': ${error}"
		return 1
		;;
	esac
	# the sidecar restarted: it lost the row, and resumes nothing but an upload
	if [[ -n ${NO_STEP:-} ]]; then
		warn "${host}: the sidecar restarted during ${what} ${name}"
		return 1
	fi
	if [[ ${what} != create_remote ]]; then
		warn "${host}: the sidecar restarted during ${what} ${name}, which it does not resume: the ${OPERATION} is partially applied"
		return 1
	fi
	if ! lost_op "${host}" "${what}" "${name}"; then
		return 1
	fi
	remember_step "${host}"
	local message="${host}: the sidecar restarted during the upload of ${name}, which it resumed without the config overrides${CREATE_ENV:-}: the parts uploaded after the restart are stored with the settings of the sidecar"
	advise "${message}"
	event Warning OrchestratorOverridesNotResumed "${message}"
}

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
	json_get backup_name "${HTTP_BODY}"
	if [[ ${REPLY} != "${name}" ]]; then
		warn "${host}: ${what} acknowledged a different backup: ${HTTP_BODY}"
		return 1
	fi
	json_get operation_id "${HTTP_BODY}"
	id=${REPLY}
	if [[ -z ${id} ]]; then
		warn "${host}: ${what} returned no operation id: ${HTTP_BODY}"
		return 1
	fi
	OP_ID[${host}]=${id}
}

# op_poll HOST WHAT NAME reads once the status of the operation op_start submitted to HOST: 2 while it runs
op_poll() {
	local host=$1 what=$2 name=$3 id=${OP_ID[$1]} status
	if ! get_ok "${host}" "read status of ${what}" /backup/status -G --data-urlencode "operationid=${id}"; then
		return 1
	fi
	json_get status "${HTTP_BODY}"
	status=${REPLY}
	if [[ -n ${status} && ${status} != "in progress" ]]; then
		finished_event "${host}" "${what} ${name}" "${id}" "${status}"
	fi
	case ${status} in
	"in progress") return 2 ;;
	success) step_succeeded "${host}" "${what}" "${name}" "${id}" ;;
	"")
		if ! lost_op "${host}" "${what}" "${name}"; then
			return 1
		fi
		remember_step "${host}"
		;;
	*)
		warn "${host}: ${what} ${name} ended with status '${status}': $(error_of)"
		return 1
		;;
	esac
}

# run_all KIND PREPARE WHAT NAME HOST... runs a step on every HOST at once, as run_step does: PREPARE HOST sets
# STEP_ARGS, and STEP_NAME, which is NAME unless it changes it, or fails, which leaves the hosts after HOST unstarted
run_all() {
	local kind=$1 prepare=$2 what=$3 name=$4 host rc
	local -a pending=() running
	local -A names=()
	shift 4
	FAILED_HOSTS=() FAILURES=() STEP_FAILED_HOSTS=()
	for host in "$@"; do
		LAST_WARNING=
		STEP_NAME=${name}
		if ! "${prepare}" "${host}"; then
			step_failed "${host}" "${what}" "${STEP_NAME}"
			break
		fi
		names[${host}]=${STEP_NAME}
		if ! "${kind}_start" "${host}" "${what}" "${STEP_NAME}" "${STEP_ARGS[@]}"; then
			step_failed "${host}" "${what}" "${STEP_NAME}"
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
			"${kind}_poll" "${host}" "${what}" "${names[${host}]}" || rc=$?
			case ${rc} in
			0) ;;
			2) running+=("${host}") ;;
			*)
				step_failed "${host}" "${what}" "${names[${host}]}"
				STEP_FAILED_HOSTS+=("${host}")
				;;
			esac
		done
		pending=(${running[@]+"${running[@]}"})
	done
	((${#FAILED_HOSTS[@]} == 0))
}

# step_failed HOST WHAT NAME records the failure of a step of run_all
step_failed() {
	FAILED_HOSTS+=("$1")
	FAILURES+=("${LAST_WARNING:-$1: $2 $3 failed}")
	event Warning OrchestratorHostFailed "$1: $2 $3 failed${LAST_WARNING:+: ${LAST_WARNING}}"
}

fail_all() {
	local hosts
	join_items ", " "${FAILED_HOSTS[@]}"
	hosts=${REPLY}
	join_items "; " "${FAILURES[@]}"
	die "$1 failed on ${hosts}: ${REPLY}"
}

# lost_op HOST WHAT NAME settles an operation that a restarted sidecar no longer knows
lost_op() {
	local host=$1 what=$2 name=$3 resumed where outcome status error since=${SECONDS}
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
		if ! action_rows "${host}" "${resumed}" "${name}"; then
			return 1
		fi
		status=${ACTION_STATUSES[*]: -1} error=${ACTION_ERRORS[*]: -1}
		case ${status} in
		"in progress")
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
			warn "${host}: the resumed ${resumed} of ${name} ended with status '${status}': ${error}"
			return 1
			;;
		esac
		sleep "${POLL_INTERVAL}"
	done
}

list_backups() {
	local host=$1 where=$2 line
	LIST_NAMES=() LIST_REQUIRED=() LIST_DESC=() LIST_DATA_SIZE=() LIST_OBJECT_DISK_SIZE=() LIST_METADATA_SIZE=() LIST_RBAC_SIZE=()
	if ! get_ok "${host}" "list ${where} backups" "/backup/list/${where}"; then
		return 1
	fi
	while IFS= read -r line; do
		if [[ -n ${line} ]]; then
			json_row "${line}" name=LIST_NAMES required=LIST_REQUIRED desc=LIST_DESC data_size=LIST_DATA_SIZE \
				object_disk_size=LIST_OBJECT_DISK_SIZE metadata_size=LIST_METADATA_SIZE rbac_size=LIST_RBAC_SIZE
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

# delete_local HOST NAME drops the local copy of NAME
delete_local() {
	if ! submit "$1" "delete local $2" "/backup/delete/local/$2" -G --data-urlencode "force=true"; then
		return 1
	fi
	log "$1: local copy of $2 deleted"
}

# drop_local HOST NAME drops the local copy of NAME that HOST lists, failing only when it cannot
drop_local() {
	if list_backups "$1" local && find_backup "$2"; then
		delete_local "$1" "$2"
	fi
}

# merge_keys INTO FROM sets to 1 every key of the associative array FROM in the associative array INTO
merge_keys() {
	local -n _into=$1 _from=$2
	local _key
	for _key in "${!_from[@]}"; do
		_into["${_key}"]=1
	done
}

# list_all FILTER HOST... lists the remote backups of every HOST, which FILTER HOST NAME keys (REPLY) or leaves out,
# into sets of keys: ALL_NAMES, COMPLETE on every host listed, SIZED, BROKEN, EMPTY and WITH_DATA; UNLISTED
list_all() {
	local filter=$1 host first=1 i name key size
	local -A listed
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
			if [[ -z ${LIST_NAMES[i]} ]] || ! "${filter}" "${host}" "${LIST_NAMES[i]}"; then
				continue
			fi
			key=${REPLY}
			ALL_NAMES[${key}]=1
			size=unknown
			if [[ -n ${LIST_METADATA_SIZE[i]} ]]; then
				size=empty
				if [[ ${LIST_DATA_SIZE[i]:-0} != 0 || ${LIST_OBJECT_DISK_SIZE[i]:-0} != 0 || ${LIST_METADATA_SIZE[i]} != 0 ||
					${LIST_RBAC_SIZE[i]:-0} != 0 ]]; then
					size=sized
					SIZED[${key}]=1
				fi
			fi
			if [[ ${LIST_DESC[i]} == broken* ]]; then
				BROKEN[${key}]=1
				continue
			fi
			listed[${key}]=1
			if [[ ${size} == unknown || ${LIST_DATA_SIZE[i]:-0} != 0 || ${LIST_OBJECT_DISK_SIZE[i]:-0} != 0 ]]; then
				WITH_DATA[${key}]=1
			fi
			if [[ ${size} == empty ]]; then
				EMPTY[${key}]=1
			fi
		done
		if [[ -n ${first} ]]; then
			merge_keys COMPLETE listed
			first=
		fi
		for name in "${!COMPLETE[@]}"; do
			if [[ -z ${listed[${name}]:-} ]]; then
				unset 'COMPLETE[${name}]'
			fi
		done
	done
}

retention_plan() {
	local name tag key newest= made=${BACKUP_KEY:-${BACKUP_NAME:-}}
	local -a ordered=() incomplete=() obsolete=() held=()
	declare -gA KEEP=()
	OBSOLETE=()
	HELD_OBSOLETE=()
	for name in ${NAMES[@]+"${NAMES[@]}"}; do
		key_time "${name}"
		key=${REPLY}
		if ! tag_valid "${key}"; then
			continue
		fi
		if [[ -z ${COMPLETE[${name}]:-} ]]; then
			if [[ -n ${SIZED[${name}]:-}${BROKEN[${name}]:-} && ${name} != "${made}" ]]; then
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
	rules_keep ${ordered[@]+"${ordered[@]}"}
	merge_keys KEEP RULES_KEEP
	for tag in ${ordered[@]+"${ordered[@]}"}; do
		if [[ ${tag#* } == "${made}" ]]; then
			KEEP[${made}]=1
		fi
	done
	pinned_keys
	merge_keys KEEP PINNED
	held_keys ${ordered[@]+"${ordered[@]}"}
	for tag in ${ordered[@]+"${ordered[@]}"}; do
		if [[ -n ${KEEP[${tag#* }]:-} ]]; then
			continue
		fi
		if [[ -n ${HELD_KEYS[${tag#* }]:-} ]]; then
			held+=("${tag#* }")
		else
			obsolete+=("${tag}")
		fi
	done
	HELD_OBSOLETE=(${held[@]+"${held[@]}"})
	for tag in ${incomplete[@]+"${incomplete[@]}"}; do
		if [[ -n ${newest} && ${tag%% *} < ${newest} ]]; then
			obsolete+=("${tag}")
		fi
	done
	if ((${#obsolete[@]} > 0)); then
		mapfile -t obsolete < <(printf '%s\n' "${obsolete[@]}" | sort -r)
		for tag in "${obsolete[@]}"; do
			OBSOLETE+=("${tag#* }")
		done
	fi
}

rules_keep() {
	local tag key name i oldest= first=1
	local -a manual=() counts=()
	declare -gA RULES_KEEP=()
	for i in "${!SCHED_PERIOD[@]}"; do
		counts[i]=0
	done
	for tag in "$@"; do
		key=${tag%% *}
		name=${tag#* }
		if [[ -n ${first} ]]; then
			RULES_KEEP[${name}]=1
			first=
		fi
		key_time "${name}"
		if [[ -n ${KEY_AT_ONCE} ]]; then
			manual+=("${tag}")
			continue
		fi
		for i in "${!SCHED_PERIOD[@]}"; do
			if slot_matches "${i}" "${key}"; then
				counts[i]=$((counts[i] + 1))
				if ((counts[i] <= SCHED_KEEP[i])); then
					RULES_KEEP[${name}]=1
					oldest=${key}
				fi
			fi
		done
	done
	for tag in ${manual[@]+"${manual[@]}"}; do
		key=${tag%% *}
		if [[ -z ${oldest} || ! ${key} < ${oldest} ]]; then
			RULES_KEEP[${tag#* }]=1
		fi
	done
}

# pinned_keys sets PINNED to the keys of the backups KEEP_BACKUPS pins, which retention never deletes
pinned_keys() {
	local value prefix
	declare -gA PINNED=()
	one_line KEEP_BACKUPS
	naming_prefix || :
	prefix=${REPLY}
	for value in ${KEEP_BACKUPS:-}; do
		if ((${#NAMING_PARTS[@]} == 0)) || [[ ${value} =~ ${NAMING_TAG_RE} ]]; then
			PINNED[${value}]=1
		elif naming_parse "${value}" "${prefix}"; then
			PINNED[${N_KEY}]=1
		fi
	done
}

held_keys() {
	local record change entries name
	# the entries of every record replace those of SCHEDULES for the call alone
	local -a records subset SCHED_PERIOD SCHED_HOUR SCHED_MINUTE SCHED_DAY SCHED_KEEP SCHED_INCREMENTAL SCHED_ENV
	declare -gA HELD_KEYS=()
	if [[ -z ${PREVIOUS_SCHEDULES:-} ]]; then
		return 0
	fi
	one_line PREVIOUS_SCHEDULES
	IFS='#' read -r -a records <<<"${PREVIOUS_SCHEDULES}"
	for record in "${records[@]}"; do
		if [[ ! ${record} =~ ^([0-9]{1,12})\|([^|]*)\|([^|]+)$ ]]; then
			die "PREVIOUS_SCHEDULES must list <epoch>|<time zone>|<entries> records separated by #"
		fi
		entries=${BASH_REMATCH[3]}
		local SCHEDULE_TIMEZONE=${BASH_REMATCH[2]} SCHEDULES=${entries}
		check_timezone
		slot_tag "${BASH_REMATCH[1]}"
		change=${REPLY}
		subset=()
		for name in "$@"; do
			if [[ ${name%% *} < ${change} ]]; then
				subset+=("${name}")
			fi
		done
		if [[ ${entries} == "*" ]]; then
			for name in ${subset[@]+"${subset[@]}"}; do
				HELD_KEYS[${name#* }]=1
			done
			continue
		fi
		parse_schedules
		rules_keep ${subset[@]+"${subset[@]}"}
		merge_keys HELD_KEYS RULES_KEEP
	done
}

# retention_on_host HOST deletes on HOST the backups of OBSOLETE it lists, newest first
retention_on_host() {
	local host=$1 name key
	local -A held=()
	if ! list_backups "${host}" remote; then
		return 1
	fi
	for name in "${LIST_NAMES[@]}"; do
		if [[ -n ${name} ]]; then
			held[${name}]=1
		fi
	done
	for key in ${OBSOLETE[@]+"${OBSOLETE[@]}"}; do
		host_name "${host}" "${key}"
		name=${REPLY}
		if [[ -z ${held[${name}]:-} ]]; then
			continue
		fi
		if submit "${host}" "delete remote ${name}" "/backup/delete/remote/${name}" --max-time "${DELETE_TIMEOUT}"; then
			notice OrchestratorRetentionDeleted "${host}: retention deleted ${name}"
		else
			event Warning OrchestratorRetentionFailed "${LAST_WARNING}"
		fi
	done
}

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
	BEST_EFFORT=1
	list_all series_key "${HOSTS[@]}"
	if ((${#UNLISTED[@]} > 0)); then
		alert OrchestratorRetentionSkipped "retention skipped: ${UNLISTED[*]} cannot be listed, backups are left as they are"
		return 0
	fi
	NAMES=("${!ALL_NAMES[@]}")
	retention_plan
	report_held
	for host in "${HOSTS[@]}"; do
		if [[ ! ${host} =~ ${HOST_RE} ]]; then
			warn "retention skipped an invalid host name"
		elif ! retention_on_host "${host}"; then
			warn "${host}: retention failed, backups are left as they are"
		fi
	done
}

this_backup() {
	REPLY=${BACKUP_KEY}
	[[ $2 == "${HOST_BACKUP[$1]:-}" ]]
}

report_held() {
	local key more=
	local -a names=()
	if ((${#HELD_OBSOLETE[@]} == 0)); then
		return 0
	fi
	for key in "${HELD_OBSOLETE[@]}"; do
		REPLY=${key}
		if ((${#NAMING_PARTS[@]} > 0)); then
			naming_render 1 "${key}" "${NAMING_PREFIX}" || :
		fi
		names+=("${REPLY}")
	done
	alert OrchestratorRetentionHeld "retention holds ${#names[@]} backup(s) the schedule kept before a change of it, which it would delete now: ${names[*]:0:HELD_REPORT_MAX}${names[HELD_REPORT_MAX]:+ ...}; keep those to keep in spec.retention.keepBackups and release the change with spec.retention.releaseHeld"
	if ((${#names[@]} > HELD_REPORT_MAX)); then
		more=" +$((${#names[@]} - HELD_REPORT_MAX))"
	fi
	report "${HELD_BACKUPS_MESSAGE:-}" "${names[*]:0:HELD_REPORT_MAX}${more}"
}

report_data() {
	local BEST_EFFORT=1
	list_all this_backup "$@"
	if ((${#UNLISTED[@]} > 0)); then
		log "whether ${BACKUP_NAME} holds data is unknown: ${UNLISTED[*]} cannot be listed"
		return 0
	fi
	if [[ -z ${COMPLETE[${BACKUP_KEY}]:-} || -n ${WITH_DATA[${BACKUP_KEY}]:-} ]]; then
		return 0
	fi
	alert OrchestratorNoData "${BACKUP_NAME} holds no data"
	report "${NO_DATA_MESSAGE:-}"
}

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

path_setting() {
	local name="PATH_$1_$2"
	REPLY=${!name-}
	SETTING_SET=${!name+1}
}

identity_default() {
	case $1 in
	AZBLOB_ENDPOINT_SUFFIX) REPLY=core.windows.net ;;
	SFTP_PORT) REPLY=22 ;;
	*) REPLY= ;;
	esac
}

# trim_path STORAGE PATH sets REPLY to PATH as clickhouse-backup trims it
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

# substitute_macros TEMPLATE STRICT INSTALLATION NAMESPACE [CLUSTER SHARD] sets REPLY to TEMPLATE with its macros
# replaced: with STRICT, {installation}, {namespace}, {cluster} and {shard}, keeping {replica} and failing at any other,
# UNKNOWN_MACRO; without, {installation} and {namespace} alone, of a value that is not empty
substitute_macros() {
	local rest=$1 out= macro value
	while [[ ${rest} =~ \{([^{}]*)\} ]]; do
		macro=${BASH_REMATCH[0]}
		case ${BASH_REMATCH[1]} in
		installation) value=$3 ;;
		namespace) value=$4 ;;
		cluster) value=${5-} ;;
		shard) value=${6-} ;;
		replica) value=${macro} ;;
		*)
			if [[ -n $2 ]]; then
				UNKNOWN_MACRO=${macro}
				return 1
			fi
			value=
			;;
		esac
		if [[ -z $2 ]]; then
			value=${value:-${macro}}
		fi
		out+=${rest%%"${macro}"*}${value}
		rest=${rest#*"${macro}"}
	done
	REPLY=${out}${rest}
}

# expand_macros PATH CLUSTER SHARD expands the macros of the storage path PATH of shard SHARD of cluster CLUSTER
expand_macros() {
	substitute_macros "$1" strict "${PATH_INSTALLATION:-}" "${PATH_NAMESPACE:-}" "$2" "$3"
}

# default_path INDEX NAME sets REPLY to the default path NAME
default_path() {
	local own="PATH_$1_$2"
	if [[ -n ${!own+set} ]]; then
		REPLY=${!own}
	else
		REPLY=${!2:-}
	fi
}

path_check_inconclusive() {
	SELF_PATHS_UNKNOWN=$3
	if (($1 > 1)); then
		die "the storage path check cannot tell whether cluster $2 shares a storage path with another of the $1 backup-enabled clusters of the installation, which would mix their backups and restore one with the data of the other: $3"
	fi
	advise "the storage path check is skipped: $3"
}

# path_unknown INDEX COUNT NAME WHY [WHY_OF_ANOTHER] stops at a cluster whose paths the check cannot tell: the target
# cluster ends it, as path_check_inconclusive does of COUNT clusters, and returns 0; another is left out of it
path_unknown() {
	if [[ $1 == "${PATH_SELF:-}" ]]; then
		path_check_inconclusive "$2" "$3" "$4"
		return 0
	fi
	advise "cluster $3 is left out of the storage path check: ${5:-$4}"
	return 1
}

# check_storage_paths refuses, before anything changes, a storage path the target cluster shares
check_storage_paths() {
	local index storage prefix identity key template object shard name what value self=${PATH_SELF:-} self_name= shared=
	local selection=${PATH_REPLICA_SELECTION:-}
	local -a indexes others=() shards
	local -A owners=() self_owned=()
	read -r -a indexes <<<"${PATH_CLUSTERS:-}"
	if ((${#indexes[@]} == 0)); then
		path_check_inconclusive 0 "" "PATH_CLUSTERS names no cluster"
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
	for index in "${self}" "${others[@]}"; do
		path_setting "${index}" CLUSTER
		name=${REPLY}
		if [[ ${index} == "${self}" ]]; then
			self_name=${name}
		fi
		path_setting "${index}" UNVERIFIABLE
		what=${REPLY//[[:cntrl:]]/ }
		if [[ -n ${what} ]]; then
			if path_unknown "${index}" "${#indexes[@]}" "${name}" "the operator cannot tell the sidecar settings of cluster ${name}: ${what}"; then
				return 0
			fi
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
			if path_unknown "${index}" "${#indexes[@]}" "${name}" "${what}"; then
				return 0
			fi
			continue
		fi
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
				if path_unknown "${index}" "${#indexes[@]}" "${name}" "the path '${value}' of cluster ${name} holds ${UNKNOWN_MACRO}, a macro whose value it does not know" \
					"its path '${value}' holds ${UNKNOWN_MACRO}, a macro whose value it does not know"; then
					return 0
				fi
				continue 2
			fi
		done
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
				if [[ ${value} == *"{replica}"* ]] || [[ ${what} != "storage path" && -z ${value} ]]; then
					continue
				fi
				expand_macros "${value}" "${name}" "${shard}"
				clean_path "${REPLY}"
				printf -v key '%s %q %q' "${identity}" "${what}" "${REPLY}"
				if [[ -n ${owners[${key}]:-} ]]; then
					if [[ ${index} == "${self}" && ${#NAMING_PARTS[@]} -gt 0 ]]; then
						shared=1
						continue
					fi
					if [[ -n ${self_owned[${key}]:-} ]]; then
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
	if [[ -n ${shared} ]]; then
		log "storage path check: the shards of cluster ${self_name} share a path, where the naming template '${BACKUP_NAMING}' names the backup of each apart, and no other cluster has it"
		return 0
	fi
	log "storage path check: every shard of cluster ${self_name} has a path of its own"
}

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

naming_init() {
	local rest=${BACKUP_NAMING:-} macro name tokens token pair
	local -A seen=()
	NAMING_PARTS=()
	SLOT_TOLERANCE=0
	declare -gA SHARD_OF=()
	if [[ -z ${rest} ]]; then
		return 0
	fi
	if [[ ! ${rest} =~ ${NAMING_RE} ]] || ((${#rest} > NAMING_MAX_LENGTH)); then
		die "BACKUP_NAMING is not a valid naming template"
	fi
	while [[ ${rest} =~ \{([A-Za-z]+)\} ]]; do
		macro=${BASH_REMATCH[0]}
		name=${BASH_REMATCH[1]}
		if [[ -n ${rest%%"${macro}"*} ]]; then
			NAMING_PARTS+=("l:${rest%%"${macro}"*}")
		fi
		rest=${rest#*"${macro}"}
		case ${name} in
		shard | cluster | chi | namespace | prefix)
			NAMING_PARTS+=("p:${name}")
			seen[${name}]=$((${seen[${name}]:-0} + 1))
			continue
			;;
		esac
		tokens=${name}
		while [[ -n ${tokens} ]]; do
			case ${tokens} in
			YYYY*) token=YYYY ;;
			MM*) token=MM ;;
			DD*) token=DD ;;
			HH*) token=HH ;;
			II*) token=II ;;
			SS*) token=SS ;;
			*) die "BACKUP_NAMING holds {${name}}, which is no placeholder" ;;
			esac
			NAMING_PARTS+=("t:${token}")
			seen[${token}]=$((${seen[${token}]:-0} + 1))
			tokens=${tokens:${#token}}
		done
	done
	if [[ -n ${rest} ]]; then
		NAMING_PARTS+=("l:${rest}")
	fi
	for name in shard YYYY MM DD HH II SS; do
		if [[ ${seen[${name}]:-0} != 1 ]]; then
			die "BACKUP_NAMING must hold {shard} and every token of the time, YYYY, MM, DD, HH, II and SS, once"
		fi
	done
	for name in cluster chi namespace prefix; do
		if ((${seen[${name}]:-0} > 1)); then
			die "BACKUP_NAMING holds {${name}} twice"
		fi
	done
	for pair in ${SHARD_NUMBERS:-}; do
		if [[ ! ${pair} =~ ^[^=]+=[1-9][0-9]{0,4}$ || ! ${pair%%=*} =~ ${HOST_RE} ]]; then
			die "SHARD_NUMBERS must list host=number pairs"
		fi
		SHARD_OF[${pair%%=*}]=${pair#*=}
	done
	SLOT_TOLERANCE=${NAMING_SLOT_TOLERANCE}
	unset NAMING_CACHED
}

naming_value() {
	case $1 in
	cluster) REPLY=${CLUSTER:-} ;;
	chi) REPLY=${SOURCE_INSTALLATION:-${NAMING_INSTALLATION:-}} ;;
	namespace) REPLY=${SOURCE_NAMESPACE:-${NAMING_NAMESPACE:-}} ;;
	esac
}

naming_regex() {
	local part group=0 regex='^'
	declare -gA NAMING_GROUP=()
	for part in "${NAMING_PARTS[@]}"; do
		case ${part} in
		l:*)
			part=${part#l:}
			regex+=${part//./[.]}
			;;
		p:shard)
			group=$((group + 1))
			NAMING_GROUP[shard]=${group}
			regex+='([1-9][0-9]*)'
			;;
		p:prefix)
			if [[ -n $1 ]]; then
				regex+=${1//./[.]}
			else
				group=$((group + 1))
				NAMING_GROUP[prefix]=${group}
				regex+='([A-Za-z0-9._-]+)'
			fi
			;;
		p:*)
			naming_value "${part#p:}"
			regex+=${REPLY//./[.]}
			;;
		t:*)
			if [[ ${part} == t:HH ]]; then
				group=$((group + 1))
				NAMING_GROUP[T]=${group}
				regex+='(T?)'
			fi
			group=$((group + 1))
			NAMING_GROUP[${part#t:}]=${group}
			if [[ ${part} == t:YYYY ]]; then
				regex+='([0-9]{4})'
			else
				regex+='([0-9]{2})'
			fi
			;;
		esac
	done
	REPLY=${regex}'$'
}

naming_parse() {
	local name=$1
	if [[ ${NAMING_CACHED-} != "p:${2:-}" ]]; then
		naming_regex "${2:-}"
		NAMING_CACHED_REGEX=${REPLY}
		NAMING_CACHED=p:${2:-}
	fi
	if [[ ! ${name} =~ ${NAMING_CACHED_REGEX} ]]; then
		return 1
	fi
	N_SHARD=${BASH_REMATCH[NAMING_GROUP[shard]]}
	N_PREFIX=${2:-}
	if [[ -n ${NAMING_GROUP[prefix]:-} ]]; then
		N_PREFIX=${BASH_REMATCH[NAMING_GROUP[prefix]]}
	fi
	N_KEY=${BASH_REMATCH[NAMING_GROUP[YYYY]]}${BASH_REMATCH[NAMING_GROUP[MM]]}${BASH_REMATCH[NAMING_GROUP[DD]]}
	N_KEY+=${BASH_REMATCH[NAMING_GROUP[T]]}${BASH_REMATCH[NAMING_GROUP[HH]]}${BASH_REMATCH[NAMING_GROUP[II]]}${BASH_REMATCH[NAMING_GROUP[SS]]}
	tag_valid "${N_KEY/T/}"
}

naming_render() {
	local part digits=${2/T/} name=
	for part in "${NAMING_PARTS[@]}"; do
		case ${part} in
		l:*) name+=${part#l:} ;;
		p:shard) name+=$1 ;;
		p:prefix) name+=$3 ;;
		p:*)
			naming_value "${part#p:}"
			name+=${REPLY}
			;;
		t:YYYY) name+=${digits:0:4} ;;
		t:MM) name+=${digits:4:2} ;;
		t:DD) name+=${digits:6:2} ;;
		t:HH)
			if [[ $2 == *T* ]]; then
				name+=T
			fi
			name+=${digits:8:2}
			;;
		t:II) name+=${digits:10:2} ;;
		t:SS) name+=${digits:12:2} ;;
		esac
	done
	REPLY=${name}
	[[ ${REPLY} =~ ${NAME_RE} ]]
}

# naming_holds PLACEHOLDER reports whether the naming template holds the placeholder
naming_holds() {
	listed "p:$1" ${NAMING_PARTS[@]+"${NAMING_PARTS[@]}"}
}

# naming_prefix sets REPLY to the value of the {prefix} of the naming template, BACKUP_NAME_PREFIX, or to nothing when it
# holds none; it fails when BACKUP_NAME_PREFIX leaves the {prefix} it holds empty
naming_prefix() {
	REPLY=
	if naming_holds prefix; then
		REPLY=${BACKUP_NAME_PREFIX:-}
		[[ -n ${REPLY} ]]
	fi
}

# series_key HOST NAME sets REPLY to the key of the backup NAME on HOST of the series retention keeps, or fails

series_key() {
	local pattern
	if ((${#NAMING_PARTS[@]} == 0)); then
		pattern="^${BACKUP_NAME_PREFIX//./[.]}-[0-9]{8}T?[0-9]{6}$"
		if [[ ! $2 =~ ${pattern} ]]; then
			return 1
		fi
		REPLY=$2
		return 0
	fi
	if ! naming_prefix || ! naming_parse "$2" "${REPLY}" || [[ ${N_SHARD} != "${SHARD_OF[$1]:-}" ]]; then
		return 1
	fi
	REPLY=${N_KEY}
}

key_time() {
	REPLY=$1
	if ((${#NAMING_PARTS[@]} == 0)); then
		REPLY=${1:${#BACKUP_NAME_PREFIX}+1}
	fi
	KEY_AT_ONCE=
	if [[ ${REPLY} == *T* ]]; then
		KEY_AT_ONCE=1
	fi
	REPLY=${REPLY/T/}
}

# host_name HOST KEY sets REPLY to the name of the backup of the key KEY on HOST
host_name() {
	if ((${#NAMING_PARTS[@]} == 0)); then
		REPLY=$2
		return 0
	fi
	if [[ -z ${SHARD_OF[$1]:-} ]]; then
		die "SHARD_NUMBERS tells no shard of $1, whose backup the naming template names by its shard"
	fi
	if ! naming_render "${SHARD_OF[$1]}" "$2" "${NAMING_PREFIX}"; then
		die "the naming template '${BACKUP_NAMING}' makes '${REPLY}' of the backup $2 of $1, which is no valid backup name"
	fi
}

naming_key() {
	local value=${!1}
	NAMING_PREFIX=${2:-}
	if ((${#NAMING_PARTS[@]} == 0)); then
		REPLY=${value}
		return 0
	fi
	if [[ ${value} =~ ${NAMING_TAG_RE} ]] && tag_valid "${value/T/}"; then
		if naming_holds prefix && [[ -z ${NAMING_PREFIX} ]]; then
			die "$1 ${value} tells the time of the backup alone, while the naming template '${BACKUP_NAMING}' holds {prefix}: name the backup in full, as the name of the backup of any shard"
		fi
		REPLY=${value}
		return 0
	fi
	if ! naming_parse "${value}" "${NAMING_PREFIX}"; then
		die "$1 ${value} is no name the naming template '${BACKUP_NAMING}' makes on cluster ${CLUSTER:-}, nor the time of one, YYYYMMDDHHMMSS"
	fi
	NAMING_PREFIX=${N_PREFIX}
	REPLY=${N_KEY}
}

# resolve_names INTO KEY HOST... sets INTO[HOST] to the name of the backup of the key KEY on every HOST
resolve_names() {
	local -n _names=$1
	local _key=$2 _host
	shift 2
	for _host in "$@"; do
		host_name "${_host}" "${_key}"
		_names["${_host}"]=${REPLY}
	done
}

check_override() {
	local key=${1%%=*} value=${1#*=} token
	REPLY=
	if [[ ! $1 =~ ${CONFIG_OVERRIDE_RE} ]] || ((${#key} > CONFIG_OVERRIDE_KEY_MAX_LENGTH || ${#value} > CONFIG_OVERRIDE_VALUE_MAX_LENGTH)); then
		REPLY="the config override '$1' is no KEY=VALUE of a clickhouse-backup setting"
		return 1
	fi
	if [[ ${CONFIG_OVERRIDE_FORBIDDEN} == *" ${key} "* || ${key} == API_* ]] || listed "${key}" "${PATH_CHECK_KEYS[@]}"; then
		REPLY="the config override of ${key} is refused: the operator sets it, or it tells where the backups are"
		return 1
	fi
	for token in "${CONFIG_OVERRIDE_SENSITIVE[@]}"; do
		if [[ ${key} == *"${token}"* ]]; then
			REPLY="the config override of ${key} is refused: it is a secret, which belongs in the configuration of the sidecar"
			return 1
		fi
	done
}

# period_weight PERIOD sets REPLY to the weight of a schedule period
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

# parse_schedules fills the SCHED_* arrays from SCHEDULES
parse_schedules() {
	local entry field key value index=0 period at day keep incremental overrides
	local -a entries fields
	local -A periods=()
	SCHED_PERIOD=() SCHED_HOUR=() SCHED_MINUTE=() SCHED_DAY=() SCHED_KEEP=() SCHED_INCREMENTAL=() SCHED_ENV=()
	one_line SCHEDULES
	IFS=';' read -r -a entries <<<"${SCHEDULES:-}"
	for entry in "${entries[@]}"; do
		read -r -a fields <<<"${entry}"
		if ((${#fields[@]} == 0)); then
			continue
		fi
		period= at= day= keep= incremental=false overrides=
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
			env)
				if ! check_override "${value}"; then
					die "SCHEDULES entry ${index}: ${REPLY}"
				fi
				overrides+=" --env=${value}"
				;;
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
		SCHED_ENV+=("${overrides}")
		index=$((index + 1))
	done
}

slot_matches() {
	local i=$1 tag=$2 y m d hour minute day back
	if [[ ${SCHED_PERIOD[i]} == minute ]]; then
		return 0
	fi
	if ((SLOT_TOLERANCE > 0)); then
		minute=$((10#${tag:10:2}))
		back=$((((minute - SCHED_MINUTE[i] + 60) % 60) * 60 + 10#${tag:12:2}))
		if ((back >= SLOT_TOLERANCE)); then
			return 1
		fi
		days_from_civil "$((10#${tag:0:4}))" "$((10#${tag:4:2}))" "$((10#${tag:6:2}))"
		TZ=UTC0 printf -v tag '%(%Y%m%d%H%M%S)T' \
			"$((REPLY * 86400 + 10#${tag:8:2} * 3600 + 10#${tag:10:2} * 60 + 10#${tag:12:2} - back))"
	fi
	y=$((10#${tag:0:4})) m=$((10#${tag:4:2})) d=$((10#${tag:6:2})) hour=$((10#${tag:8:2})) minute=$((10#${tag:10:2}))
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

# slot_epoch sets REPLY to the unix seconds of the slot of the run
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

schedule_slot() {
	slot_epoch
	slot_tag "${REPLY}"
	if [[ ! ${REPLY} =~ ${TAG_RE} ]]; then
		printf -v REPLY '%(%s)T' -1
		slot_tag "$((REPLY - REPLY % 60))"
	fi
	SLOT_TAG=${REPLY}
}

record_series() {
	local host=$1 i
	for i in "${!LIST_NAMES[@]}"; do
		if [[ -n ${LIST_NAMES[i]} && ${LIST_DESC[i]} != broken* ]] && series_key "${host}" "${LIST_NAMES[i]}" &&
			[[ ${REPLY} != "${BACKUP_KEY}" ]]; then
			SERIES_REQUIRED["${host} ${REPLY}"]=${LIST_REQUIRED[i]}
		fi
	done
}

incremental_base() {
	local keep=$1 host key name newest= newest_time= count n
	local -a names
	local -A hosts=() held=() rows=()
	shift
	for host in "$@"; do
		hosts[${host}]=1
	done
	for key in "${!SERIES_REQUIRED[@]}"; do
		host=${key%% *} name=${key#* }
		if [[ -n ${hosts[${host}]:-} ]]; then
			held[${name}]=$((${held[${name}]:-0} + 1))
			key_time "${name}"
			rows[${host}]+="${REPLY} ${name}"$'\n'
		fi
	done
	for name in "${!held[@]}"; do
		n=${held[${name}]}
		key_time "${name}"
		if ((n == ${#hosts[@]})) && [[ ${REPLY} > ${newest_time} || (${REPLY} == "${newest_time}" && ${name} > ${newest}) ]]; then
			newest=${name}
			newest_time=${REPLY}
		fi
	done
	CHAIN=0
	REPLY=
	if [[ -z ${newest} ]]; then
		return 0
	fi
	for host in "${!hosts[@]}"; do
		mapfile -t names < <(printf '%s' "${rows[${host}]:-}" | sort -r)
		count=0
		for name in ${names[@]+"${names[@]}"}; do
			if [[ ${name%% *} > ${newest_time} ]]; then
				continue
			fi
			name=${name#* }
			if [[ -z ${SERIES_REQUIRED["${host} ${name}"]} ]]; then
				break
			fi
			count=$((count + 1))
		done
		if ((count > CHAIN)); then
			CHAIN=${count}
		fi
	done
	REPLY=
	if ((CHAIN < keep)); then
		REPLY=${newest}
	fi
}

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

# clickhouse_answers HOST reports whether the ClickHouse of HOST answers through its API
clickhouse_answers() {
	request GET "$1" /backup/tables -G --data-urlencode "table=system.one"
	((CURL_EXIT == 0)) && [[ ${HTTP_CODE} == 200 ]]
}

fall_back_hosts() {
	local i
	local -a preferred=("${HOSTS[@]}") fallback
	PREFERRED_HOSTS=("${HOSTS[@]}")
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

drop_failed_backup() {
	local host where name BEST_EFFORT=1
	for host in "$@"; do
		name=${HOST_BACKUP[${host}]}
		for where in local remote; do
			if submit "${host}" "delete ${where} ${name}" "/backup/delete/${where}/${name}" -G --data-urlencode "force=true" \
				--max-time "${DELETE_TIMEOUT}"; then
				notice OrchestratorFailedBackupDeleted "${host}: the ${where} copy of the failed backup ${name} deleted"
			elif [[ ${LAST_WARNING} == *"not found"* ]]; then
				log "${host}: no ${where} copy of the failed backup ${name} to delete"
			else
				advise "${host}: the ${where} copy of the failed backup ${name} is left: ${LAST_WARNING}"
				event Warning OrchestratorFailedBackupLeft "${host}: the ${where} copy of the failed backup ${name} is left: ${LAST_WARNING}"
			fi
		done
	done
}

backup_unseen() {
	local name=${HOST_BACKUP[$1]}
	if ! list_backups "$1" remote; then
		return 1
	fi
	if find_backup "${name}"; then
		warn "backup ${name} appeared in the storage of $1 during this run: hosts of one backup must not share a storage path"
		return 1
	fi
	STEP_NAME=${name}
	if [[ ${CREATE_KIND} == action ]]; then
		STEP_ARGS=("create_remote${CREATE_FLAGS}${BASE_OF[$1]:+ --diff-from-remote=${BASE_OF[$1]}}${CREATE_ENV} ${name}")
		return 0
	fi
	STEP_ARGS=(/backup/create_remote -G --data-urlencode "name=${name}" ${CREATE_PARAMS[@]+"${CREATE_PARAMS[@]}"})
	if [[ -n ${BASE_OF[$1]:-} ]]; then
		STEP_ARGS+=(--data-urlencode "diff-from-remote=${BASE_OF[$1]}")
	fi
}

# create_args sets CREATE_PARAMS and CREATE_FLAGS to the options of the backup: parameters of the op that makes it, flags
# of the command of the actions endpoint that does
create_args() {
	CREATE_PARAMS=() CREATE_FLAGS=
	if is_true SCHEMA_ONLY; then
		CREATE_PARAMS+=(--data-urlencode "schema=")
		CREATE_FLAGS+=" --schema"
	fi
	if [[ -n ${TABLES:-} ]]; then
		CREATE_PARAMS+=(--data-urlencode "table=${TABLES}")
		CREATE_FLAGS+=" --tables=${TABLES}"
	fi
	if [[ -n ${PARTITIONS:-} ]]; then
		CREATE_PARAMS+=(--data-urlencode "partitions=${PARTITIONS}")
		CREATE_FLAGS+=" --partitions=${PARTITIONS}"
	fi
}

# config_overrides sets CREATE_KIND to how the backup is made: an op, or, with config overrides, an action
config_overrides() {
	local item
	local -a items
	CREATE_KIND=op
	if [[ -n ${CONFIG_OVERRIDES:-} ]]; then
		if ((${#SCHED_PERIOD[@]} > 0)); then
			die "CONFIG_OVERRIDES are those of a backup that runs no schedule: a scheduled backup takes those of its entries"
		fi
		one_line CONFIG_OVERRIDES
		read -r -a items <<<"${CONFIG_OVERRIDES}"
		if ((${#items[@]} > CONFIG_OVERRIDES_MAX_ITEMS)); then
			die "CONFIG_OVERRIDES holds more than ${CONFIG_OVERRIDES_MAX_ITEMS} overrides"
		fi
		for item in "${items[@]}"; do
			if ! check_override "${item}"; then
				die "CONFIG_OVERRIDES: ${REPLY}"
			fi
			CREATE_ENV+=" --env=${item}"
		done
	fi
	if [[ -z ${CREATE_ENV} ]]; then
		return 0
	fi
	CREATE_KIND=action
	check_filter TABLES "${TABLES_RE}" "${TABLES_MAX_LENGTH}" "table pattern"
	check_filter PARTITIONS "${PARTITIONS_RE}" "${FILTER_MAX_LENGTH}" "list of partition ids"
	log "backup ${BACKUP_NAME}: made with the config overrides${CREATE_ENV}, through the actions endpoint"
	event Normal OrchestratorConfigOverrides "backup ${BACKUP_NAME} is made with the config overrides${CREATE_ENV}"
}

# backup_settings HOST... validates the settings of a backup on HOST... and reads its schedule
backup_settings() {
	local i
	for ((i = 2; i <= $#; i++)); do
		if listed "${!i}" "${@:1:i-1}"; then
			die "BACKUP_HOSTS lists ${!i} twice: hosts of one backup must not share a storage path"
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
	if [[ -n ${CLUSTER:-} ]]; then
		check_cluster
	fi
}

# backup_name HOST... names the backup: BACKUP_KEY, BACKUP_NAME, and HOST_BACKUP, the name of its backup on every HOST
backup_name() {
	local host
	naming_init
	if [[ -n ${BACKUP_NAME:-} ]]; then
		check_name BACKUP_NAME
		BACKUP_NAME_GIVEN=1
		naming_key BACKUP_NAME "${BACKUP_NAME_PREFIX:-}"
		BACKUP_KEY=${REPLY}
	elif ((${#NAMING_PARTS[@]} > 0)); then
		if ! naming_prefix; then
			die "BACKUP_NAME_PREFIX must be set for the naming template '${BACKUP_NAMING}', which holds {prefix}"
		fi
		NAMING_PREFIX=${BACKUP_NAME_PREFIX:-}
		BACKUP_KEY=${SLOT_TAG}
	elif [[ -n ${BACKUP_NAME_PREFIX:-} ]]; then
		BACKUP_NAME="${BACKUP_NAME_PREFIX}-${SLOT_TAG}"
		if [[ ! ${BACKUP_NAME} =~ ${NAME_RE} ]]; then
			die "BACKUP_NAME_PREFIX is too long for a backup name"
		fi
		BACKUP_KEY=${BACKUP_NAME}
	else
		die "either BACKUP_NAME or BACKUP_NAME_PREFIX must be set"
	fi
	resolve_names HOST_BACKUP "${BACKUP_KEY}" "$@"
	if ((${#NAMING_PARTS[@]} > 0)); then
		naming_render 1 "${BACKUP_KEY}" "${NAMING_PREFIX}" || :
		BACKUP_NAME=${REPLY}
		log "backup ${BACKUP_NAME}: named by the naming template '${BACKUP_NAMING}', $(for host in "$@"; do printf '%s ' "${host}=${HOST_BACKUP[${host}]}"; done)"
	fi
}

# backup_plan HOST... sets BASE_OF, the name on every HOST of the base DIFF_FROM_REMOTE names; and, of a scheduled backup,
# CREATE_ENV to the config overrides of its entry and INCREMENTAL_KEEP to the longest chain of incremental backups of an
# entry that makes them, from which backup_preflight picks their base
backup_plan() {
	local prefix=${NAMING_PREFIX}
	declare -gA BASE_OF=()
	CREATE_ENV=
	INCREMENTAL_KEEP=
	if [[ -n ${DIFF_FROM_REMOTE:-} ]]; then
		check_name DIFF_FROM_REMOTE
		naming_key DIFF_FROM_REMOTE "${BACKUP_NAME_PREFIX:-}"
		resolve_names BASE_OF "${REPLY}" "$@"
		NAMING_PREFIX=${prefix}
	fi
	if ((${#SCHED_PERIOD[@]} == 0)); then
		return 0
	fi
	if [[ -n ${BACKUP_NAME_GIVEN} ]]; then
		incremental_entry
		if [[ -z ${WINNER} ]]; then
			log "backup ${BACKUP_NAME}: runs the schedule now, which has no incremental entry: the backup is full"
		elif [[ -z ${DIFF_FROM_REMOTE:-} && -n ${BACKUP_NAME_PREFIX:-} ]]; then
			log "backup ${BACKUP_NAME}: runs the schedule now, incremental as its ${SCHED_PERIOD[WINNER]} entry"
		fi
	else
		winner_entry "${SLOT_TAG}"
		if [[ -z ${WINNER} ]]; then
			log "backup ${BACKUP_NAME}: the slot ${SLOT_TAG} is a slot of no schedule entry, the backup is full"
		else
			log "backup ${BACKUP_NAME}: the slot ${SLOT_TAG} is a slot of the ${SCHED_PERIOD[WINNER]} entry${MATCHED[1]:+ and lighter ones}"
			CREATE_ENV=${SCHED_ENV[WINNER]}
		fi
	fi
	if [[ -n ${WINNER} && ${SCHED_INCREMENTAL[WINNER]} == true && -z ${DIFF_FROM_REMOTE:-} && -n ${BACKUP_NAME_PREFIX:-} ]]; then
		INCREMENTAL_KEEP=${SCHED_KEEP[WINNER]}
	fi
}

# backup_preflight HOST... refuses a backup that a HOST holds already, or one of its time, or whose base it lacks; it picks
# the base of an incremental backup of the schedule
backup_preflight() {
	local host i
	log "backup ${BACKUP_NAME}: preflight on $# host(s)"
	declare -gA SERIES_REQUIRED=()
	for host in "$@"; do
		if ! list_backups "${host}" remote; then
			fail "preflight failed on ${host}"
		fi
		if find_backup "${HOST_BACKUP[${host}]}"; then
			die "backup ${HOST_BACKUP[${host}]} already exists in the storage of ${host}"
		fi
		if ((${#NAMING_PARTS[@]} > 0)); then
			for i in "${!LIST_NAMES[@]}"; do
				if naming_parse "${LIST_NAMES[i]}" "${NAMING_PREFIX}" && [[ ${N_KEY} == "${BACKUP_KEY}" ]]; then
					die "backup ${LIST_NAMES[i]}, of the same time as ${BACKUP_NAME}, already exists in the storage of ${host}"
				fi
			done
		fi
		if [[ -n ${DIFF_FROM_REMOTE:-} ]] && ! find_backup "${BASE_OF[${host}]}"; then
			die "base backup ${BASE_OF[${host}]} is missing in the storage of ${host}"
		fi
		if [[ -n ${INCREMENTAL_KEEP} ]]; then
			record_series "${host}"
		fi
	done
	if [[ -z ${INCREMENTAL_KEEP} ]]; then
		return 0
	fi
	incremental_base "${INCREMENTAL_KEEP}" "$@"
	if [[ -n ${REPLY} ]]; then
		resolve_names BASE_OF "${REPLY}" "$@"
		log "backup ${BACKUP_NAME}: incremental from ${BASE_OF[$1]}, which ends a chain of ${CHAIN} incremental backups"
		event Normal OrchestratorIncrementalBase "${BACKUP_NAME} is incremental from ${BASE_OF[$1]}"
	else
		log "backup ${BACKUP_NAME}: full, as the chain of incremental backups is ${CHAIN} long, of ${INCREMENTAL_KEEP} at most, or there is no base every host holds"
		event Normal OrchestratorIncrementalFull "${BACKUP_NAME} is a full backup"
	fi
}

do_backup() {
	local -a backup_hosts
	parse_hosts BACKUP_HOSTS
	fall_back_hosts
	backup_hosts=("${HOSTS[@]}")
	backup_settings "${backup_hosts[@]}"
	backup_name "${backup_hosts[@]}"
	create_args
	backup_plan "${backup_hosts[@]}"
	config_overrides
	check_storage_paths
	wait_for_apis "${backup_hosts[@]}"

	event Normal OrchestratorBackupStarted "backup ${BACKUP_NAME} on ${#backup_hosts[@]} host(s)"
	backup_preflight "${backup_hosts[@]}"
	if ! run_all "${CREATE_KIND}" backup_unseen create_remote "${BACKUP_NAME}" "${backup_hosts[@]}"; then
		drop_failed_backup ${STEP_FAILED_HOSTS[@]+"${STEP_FAILED_HOSTS[@]}"}
		fail_all "backup ${BACKUP_NAME}"
	fi
	log "backup ${BACKUP_NAME} completed on ${#backup_hosts[@]} host(s)"
	if [[ ${backup_hosts[*]} != "${PREFERRED_HOSTS[*]}" ]]; then
		report "${HOSTS_USED_MESSAGE:-}" "${backup_hosts[*]}"
	fi
	if ! is_true SCHEMA_ONLY; then
		report_data "${backup_hosts[@]}"
	fi
	event Normal OrchestratorBackupCompleted "backup ${BACKUP_NAME} completed on ${#backup_hosts[@]} host(s)"
	if ! retention; then
		warn "retention failed"
	fi
}

do_verify() {
	local host name
	local -a verify_hosts
	check_name BACKUP_NAME
	parse_hosts VERIFY_HOSTS
	verify_hosts=("${HOSTS[@]}")
	if [[ -n ${CLUSTER:-} ]]; then
		check_cluster
	fi
	naming_init
	naming_key BACKUP_NAME "${BACKUP_NAME_PREFIX:-}"
	BACKUP_KEY=${REPLY}
	resolve_names HOST_BACKUP "${BACKUP_KEY}" "${verify_hosts[@]}"
	wait_for_apis "${verify_hosts[@]}"
	event Normal OrchestratorVerifyStarted "verify ${BACKUP_NAME} on ${#verify_hosts[@]} host(s)"
	for host in "${verify_hosts[@]}"; do
		name=${HOST_BACKUP[${host}]}
		log "${host}: verifying ${name}"
		if ! drop_local "${host}" "${name}"; then
			fail "verify failed on ${host}: cannot drop the local copy of ${name}"
		fi
		if ! run_step op "${host}" download "${name}" "/backup/download/${name}"; then
			fail "verify failed on ${host}: ${name} cannot be downloaded"
		fi
		if ! has_usable_backup "${host}" local "${name}"; then
			die "verify failed on ${host}: ${name} is not present locally after the download"
		fi
		if ! delete_local "${host}" "${name}"; then
			warn "${host}: the downloaded copy of ${name} is left on local disk"
		fi
	done
	notice OrchestratorVerifyCompleted "backup ${BACKUP_NAME} verified on ${#verify_hosts[@]} host(s)"
}

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

drops_database() {
	db_dropped "$1" && [[ -n ${TABLES:-} || -n ${BACKUP_TABLES_UNREADABLE:-} || -n ${BACKUP_DBS["$1."]:-} ]]
}

list_backup_tables() {
	local host=$1 line where="the storage of $1"
	local -a params
	declare -gA HOST_TABLES=() HOST_DBS=()
	params=(-G --data-urlencode "remote_backup=${HOST_BACKUP[${host}]}")
	if [[ -n ${SOURCE_ENV} ]]; then
		source_copy "${host}"
		params=(-G --data-urlencode "local_backup=${HOST_BACKUP[${host}]}")
		where="the copy ${host} downloaded from the storage path '${SOURCE_PATH_SHOWN}'"
	fi
	if [[ -n ${TABLES:-} ]]; then
		params+=(--data-urlencode "table=${TABLES}")
	fi
	call GET "${host}" /backup/tables "${params[@]}"
	if [[ ${HTTP_CODE} != 200 ]]; then
		die "preflight failed: cannot list the tables of ${HOST_BACKUP[${host}]} in ${where} (HTTP ${HTTP_CODE}): $(error_of)"
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

backup_tables() {
	local host first= name
	local -A first_tables=()
	SHARD_TABLES_DIFFER=()
	for host in "$@"; do
		list_backup_tables "${host}"
		merge_keys BACKUP_TABLES HOST_TABLES
		merge_keys BACKUP_DBS HOST_DBS
		if [[ -z ${first} ]]; then
			first=${host}
			merge_keys first_tables HOST_TABLES
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
				json_row "${row}" TotalBytes=LIVE_UNREADABLE_SIZES
				continue
			fi
			db=${BASH_REMATCH[1]} table=${BASH_REMATCH[2]}
			if [[ ${SYSTEM_DATABASES} == *" ${db} "* ]]; then
				continue
			fi
			LIVE_HOSTS+=("${host}")
			LIVE_DBS+=("${db}")
			LIVE_TABLES+=("${table}")
			json_row "${row}" Engine=LIVE_ENGINES UUID=LIVE_UUIDS TotalBytes=LIVE_SIZES
		done <<<"${HTTP_BODY}"
	done
}

# dropped_tables fills LIVE_DROPPED with whether the schema restore drops each table live_tables found
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

guard_tables() {
	local host i
	local -a sources found=()
	reset_backup_tables
	read -r -a sources <<<"$1"
	shift
	live_tables "$@"
	for host in "${LIVE_UNREADABLE[@]}"; do
		found+=("${host}:?")
	done
	if ((${#LIVE_TABLES[@]} > 0)) && [[ -n ${TABLES:-} ]]; then
		backup_tables "${sources[@]}"
	fi
	for i in "${!LIVE_TABLES[@]}"; do
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

# join_items SEPARATOR ITEM... sets REPLY to the items joined by SEPARATOR
join_items() {
	local separator=$1 item
	REPLY=
	shift
	for item in "$@"; do
		REPLY+=${REPLY:+${separator}}${item}
	done
}

# oversize SIZE sets REPLY to how a table of SIZE bytes, empty when unknown, is not one max_table_size_to_drop lets
# ClickHouse drop, or fails
oversize() {
	if [[ -z $1 ]]; then
		REPLY="of unknown size"
	elif greater "$1" "${MAX_TABLE_SIZE_TO_DROP}"; then
		REPLY="$1 bytes"
	else
		return 1
	fi
}

# reset_backup_tables empties what backup_tables found
reset_backup_tables() {
	declare -gA BACKUP_TABLES=() BACKUP_DBS=()
	BACKUP_TABLES_UNREADABLE=
}

# greater SIZE LIMIT reports whether the number SIZE is greater than the number LIMIT
greater() {
	local size=${1#"${1%%[!0]*}"} limit=${2#"${2%%[!0]*}"}
	if ((${#size} != ${#limit})); then
		((${#size} > ${#limit}))
		return
	fi
	[[ ${size} > ${limit} ]]
}

no_zookeeper() {
	[[ -n ${REPLICATED:-} ]] && ! is_true REPLICATED
}

# drop_size_limited reports whether MAX_TABLE_SIZE_TO_DROP limits the size of a table ClickHouse drops
drop_size_limited() {
	[[ ! ${MAX_TABLE_SIZE_TO_DROP} =~ ^0+$ ]]
}

refuse_database_drops() {
	local host i key n what stop="larger than"
	local -a sources keys found=()
	local -A unreadable=() unreadable_rows=() count=() larger=() uncertain=()
	read -r -a sources <<<"$1"
	shift
	for i in "${!LIVE_UNREADABLE[@]}"; do
		host=${LIVE_UNREADABLE[i]}
		if ! oversize "${LIVE_UNREADABLE_SIZES[i]}"; then
			continue
		fi
		what=${REPLY}
		n=${unreadable[${host}]:-0}
		unreadable[${host}]=$((n + 1))
		unreadable_rows[${host}]+="${unreadable_rows[${host}]:+, }${host}:? (a table whose name cannot be read, ${what})"
	done
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
		if ! oversize "${LIVE_SIZES[i]}"; then
			continue
		fi
		larger[${key}]+="${larger[${key}]:+, }${host}:${LIVE_DBS[i]}.${LIVE_TABLES[i]} (${REPLY})"
		if [[ -z ${LIVE_SIZES[i]} ]]; then
			uncertain[${key}]=1
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
		join_items ", " "${found[@]:0:10}"
		die "refusing to overwrite: a host of cluster ${CLUSTER} that drops a database as a whole would stop at the second table of it ${stop} max_table_size_to_drop (${MAX_TABLE_SIZE_TO_DROP} bytes) (${REPLY}), as clickhouse-backup forces the drop of only one such table of each database it drops as a whole; ${SIZE_LIMIT_ADVICE}"
	fi
}

plan_overwrite() {
	local initiator host i known=1 replicated=
	local -a shards oversized=()
	read -r -a shards <<<"$1"
	initiator=${shards[0]}
	shift
	reset_backup_tables
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
	if ((${#SHARD_TABLES_DIFFER[@]} > 0)); then
		mapfile -t SHARD_TABLES_DIFFER < <(printf '%s\n' "${SHARD_TABLES_DIFFER[@]}" | sort)
		join_items ", " "${SHARD_TABLES_DIFFER[@]:0:10}"
		die "refusing to overwrite: the tables of ${BACKUP_NAME} differ between the shards of cluster ${CLUSTER} (${REPLY}): the drop ON CLUSTER drops and recreates on every shard the tables of the backup of ${initiator}, which takes the backup of every shard to hold the same; narrow the restore with tables to those they all hold"
	fi
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
		if oversize "${LIVE_SIZES[i]}"; then
			oversized+=("${LIVE_HOSTS[i]}:${LIVE_DBS[i]}.${LIVE_TABLES[i]} (${REPLY})")
		fi
	done
	if ((${#oversized[@]} > 0)); then
		join_items ", " "${oversized[@]:0:10}"
		die "refusing to overwrite: the drop ON CLUSTER from ${initiator} would stop at tables larger than max_table_size_to_drop (${MAX_TABLE_SIZE_TO_DROP} bytes) of cluster ${CLUSTER} on other hosts (${REPLY}), as clickhouse-backup forces the drop of such a table on ${initiator} alone; ${SIZE_LIMIT_ADVICE}"
	fi
	refuse_database_drops "" "${initiator}"
}

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
	substitute_macros "${SELF_PATH}" "" "${installation}" "${namespace}"
	path=${REPLY}
	substitute_macros "${SELF_OBJECT_DISK_PATH}" "" "${installation}" "${namespace}"
	object=${REPLY}
	if [[ ${path} == "${SELF_PATH}" && ${object} == "${SELF_OBJECT_DISK_PATH}" ]]; then
		advise "the storage paths of cluster ${SELF_CLUSTER_NAME} hold no {installation} or {namespace} that tells the installation restore ${BACKUP_NAME} names apart from this one, so it reads them as they are: '${SELF_PATH}'${SELF_OBJECT_DISK_PATH:+ and the object disk path '${SELF_OBJECT_DISK_PATH}'}"
		return 0
	fi
	if [[ ${path} == "${SELF_PATH}" ]]; then
		SOURCE_OWN_PATH=1
	elif [[ -n ${object} && ${object} == "${SELF_OBJECT_DISK_PATH}" ]]; then
		die "the object disk path '${SELF_OBJECT_DISK_PATH}' of cluster ${SELF_CLUSTER_NAME} holds no {installation} or {namespace} that tells the installation restore ${BACKUP_NAME} names apart from this one, while the storage path does: deleting the copy the restore downloads would delete the object disk data of the backup; give the object disk path the macros of the storage path"
	fi
	for value in "${path}" ${object:+"${object}"}; do
		if [[ ! ${value} =~ ${SOURCE_PATH_RE} ]]; then
			die "the storage path '${value}' of the installation restore ${BACKUP_NAME} reads the backup of cannot be set in a command of the restore: only a path of letters, digits and _ . / { } - can"
		fi
	done
	SOURCE_ENV="--env=${SELF_STORAGE_PREFIX}_PATH=${path}"
	if [[ -n ${object} ]]; then
		SOURCE_ENV+=" --env=${SELF_STORAGE_PREFIX}_OBJECT_DISK_PATH=${object}"
	fi
	if [[ -n ${installation} && ${path} == *"{replica}"* ]]; then
		SOURCE_HINT="; the storage path holds {replica}, the host name, which holds the name of the installation, so the hosts of installations of different names have different paths"
	fi
	if ! expand_macros "${path}" "${SELF_CLUSTER_NAME}" "{shard}"; then
		REPLY=${path}
	fi
	SOURCE_PATH_SHOWN=${REPLY}
	log "restore ${BACKUP_NAME}: reading the backup at the storage path '${SOURCE_PATH_SHOWN}' of the installation it names"
}

refuse_local_copy() {
	local host=$1
	if listed "${host}" "${NO_LOCAL_COPY[@]}"; then
		return 0
	fi
	if ! list_backups "${host}" local; then
		fail "preflight failed on ${host}"
	fi
	if find_backup "${HOST_BACKUP[${host}]}"; then
		die "preflight failed: ${host} holds a local copy of ${HOST_BACKUP[${host}]}, which a restore from the storage paths of another installation would restore in place of the backup there; delete it (clickhouse-backup delete local ${HOST_BACKUP[${host}]}) and restore again"
	fi
	NO_LOCAL_COPY+=("${host}")
}

refuse_own_backup() {
	local host=$1
	if ! list_backups "${host}" remote; then
		fail "preflight failed on ${host}"
	fi
	OWN_BACKUPS[${host}]=" ${LIST_NAMES[*]} "
	if [[ ${OWN_BACKUPS[${host}]} == *" ${HOST_BACKUP[${host}]} "* ]]; then
		die "preflight failed: ${host} holds a backup ${HOST_BACKUP[${host}]} at its own storage path too, which clickhouse-backup would take for the one at the storage path '${SOURCE_PATH_SHOWN}', as it tells the backups of every path apart by name alone: restore a backup of another name, or delete that one first"
	fi
}

source_copy() {
	local host=$1 READ_ONLY=1 NO_STEP=1
	if listed "${host}" "${SOURCE_COPIES[@]}"; then
		return 0
	fi
	refuse_local_copy "${host}"
	SOURCE_COPIES+=("${host}")
	if ! run_step action "${host}" download "${HOST_BACKUP[${host}]}" "download --schema --rbac ${SOURCE_ENV} ${HOST_BACKUP[${host}]}"; then
		die "preflight failed: backup ${HOST_BACKUP[${host}]} is missing in the storage path '${SOURCE_PATH_SHOWN}' of ${host}, or cannot be read there${LAST_WARNING:+: ${LAST_WARNING}}${SOURCE_HINT}"
	fi
}

# drop_source_copies deletes, as it can, the copies source_copy downloaded: the restore failed
drop_source_copies() {
	local host READ_ONLY=1 BEST_EFFORT=1
	local -a copies=("${SOURCE_COPIES[@]}")
	SOURCE_COPIES=()
	for host in "${copies[@]}"; do
		if list_backups "${host}" local && ! find_backup "${HOST_BACKUP[${host}]}"; then
			continue
		fi
		if ! delete_local "${host}" "${HOST_BACKUP[${host}]}"; then
			advise "${host}: the copy of ${HOST_BACKUP[${host}]} the restore downloaded is left on local disk, which refuses a restore of ${HOST_BACKUP[${host}]} from another installation until it is deleted"
		fi
	done
}

# delete_source_copies deletes the copies source_copy downloaded before the restore downloads its own
delete_source_copies() {
	local host READ_ONLY=1
	for host in "${SOURCE_COPIES[@]}"; do
		if ! delete_local "${host}" "${HOST_BACKUP[${host}]}"; then
			fail "${host}: cannot delete the copy of ${HOST_BACKUP[${host}]} the preflight downloaded, which the restore would take for its own"
		fi
		SOURCE_COPIES=("${SOURCE_COPIES[@]:1}")
	done
}

# restore_command STEP HOST sets STEP_KIND and STEP_ARGS to the step STEP of the restore on HOST
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
		if [[ -n ${RESTORE_RBAC_WITH_SCHEMA} ]]; then
			params+=(rbac=)
			flags+=(--rbac)
		fi
		;;
	schema_on_cluster) flags=(restore --schema --rm "--env=RESTORE_SCHEMA_ON_CLUSTER=${CLUSTER}") ;;
	data)
		params=(data=)
		flags=(restore --data)
		;;
	esac
	if [[ -n ${TABLES:-} ]]; then
		params+=("table=${TABLES}")
		flags+=("--tables=${TABLES}")
	fi
	if [[ -n ${PARTITIONS:-} && ($1 == download || $1 == data) ]]; then
		params+=("partitions=${PARTITIONS}")
		flags+=("--partitions=${PARTITIONS}")
	fi
	if [[ -n ${DATABASE_MAPPING:-} && ($1 == schema || $1 == schema_on_cluster || $1 == data) ]]; then
		params+=("restore_database_mapping=${DATABASE_MAPPING}")
		flags+=("--restore-database-mapping=${DATABASE_MAPPING}")
	fi
	STEP_KIND=${RESTORE_KIND}
	if [[ $1 == schema_on_cluster ]]; then
		STEP_KIND=action
	fi
	if [[ ${STEP_KIND} == action ]]; then
		command="${flags[*]}${ACTION_ENV:+ ${ACTION_ENV}} ${HOST_BACKUP[$2]}"
		STEP_ARGS=("${command}")
		return 0
	fi
	STEP_ARGS=("/backup/${endpoint}/${HOST_BACKUP[$2]}" -G)
	for command in "${params[@]}"; do
		STEP_ARGS+=(--data-urlencode "${command}")
	done
}

# restore_step HOST WHAT STEP runs the step STEP of the restore, as restore_command makes it, on HOST
restore_step() {
	restore_command "$3" "$1"
	run_step "${STEP_KIND}" "$1" "$2" "${HOST_BACKUP[$1]}" "${STEP_ARGS[@]}"
}

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

# restore_env sets ACTION_ENV to the --env flags of the commands of the restore
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

download_args() {
	STEP_NAME=${HOST_BACKUP[$1]}
	if [[ " ${DATA_DOWNLOADS} " == *" $1 "* ]]; then
		restore_command download "$1"
	else
		restore_command download_schema "$1"
	fi
}

data_args() {
	STEP_NAME=${HOST_BACKUP[$1]}
	restore_command data "$1"
}

drop_downloads() {
	local host BEST_EFFORT=1 READ_ONLY=1
	for host in "$@"; do
		if ! drop_local "${host}" "${HOST_BACKUP[${host}]}"; then
			advise "${host}: the copy of ${HOST_BACKUP[${host}]} the restore downloaded is left on local disk"
		fi
	done
}

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

# restore_settings validates the settings of a restore and sets the hosts it runs on: SCHEMA_TARGETS restore the schema,
# DATA_TARGETS the data, USED_HOSTS download the backup, TARGETS are all of them, and GUARDED those the guard reads
restore_settings() {
	local host
	check_name BACKUP_NAME
	parse_hosts SCHEMA_HOSTS
	SCHEMA_TARGETS=("${HOSTS[@]}")
	RESTORE_DATA=1 RESTORE_SCHEMA=1 RESTORE_OVERWRITE= RESTORE_VALIDATE= RESTORE_RBAC_WITH_SCHEMA= SCHEMA_REPLICAS=
	if is_true SCHEMA_ONLY; then
		RESTORE_DATA=
		parse_hosts DATA_HOSTS optional
	else
		parse_hosts DATA_HOSTS
	fi
	DATA_TARGETS=("${HOSTS[@]}")
	if is_true OVERWRITE; then
		RESTORE_OVERWRITE=1
	fi
	if is_true VALIDATE_TOPOLOGY; then
		RESTORE_VALIDATE=1
	fi
	if is_true RESTORE_RBAC; then
		RESTORE_RBAC_WITH_SCHEMA=1
	fi
	if [[ -n ${CLUSTER:-} || -n ${RESTORE_OVERWRITE} ]]; then
		check_cluster
	fi
	check_filter SOURCE_INSTALLATION "${SOURCE_NAME_RE}" "${SOURCE_INSTALLATION_MAX_LENGTH}" "installation name"
	check_filter SOURCE_NAMESPACE "${SOURCE_NAME_RE}" "${SOURCE_NAMESPACE_MAX_LENGTH}" namespace
	naming_init
	naming_key BACKUP_NAME
	BACKUP_KEY=${REPLY}
	check_filter TABLES "${TABLES_RE}" "${TABLES_MAX_LENGTH}" "table pattern"
	check_restore_filters
	if [[ -n ${PARTITIONS:-} ]]; then
		if [[ -z ${RESTORE_DATA} ]]; then
			die "PARTITIONS restore data, which SCHEMA_ONLY leaves out"
		fi
		if [[ -n ${RESTORE_RBAC_WITH_SCHEMA} ]]; then
			die "PARTITIONS restore no schema, which RESTORE_RBAC restores the access entities with"
		fi
		RESTORE_SCHEMA= RESTORE_OVERWRITE=
		SCHEMA_TARGETS=()
	fi
	if [[ -n ${RESTORE_OVERWRITE} ]]; then
		if [[ ! ${MAX_TABLE_SIZE_TO_DROP:-} =~ ${NUMBER_RE} ]]; then
			die "MAX_TABLE_SIZE_TO_DROP must be a number of bytes"
		fi
		for host in "${SCHEMA_TARGETS[@]}"; do
			if ! listed "${host}" "${DATA_TARGETS[@]}"; then
				SCHEMA_REPLICAS=1
			fi
		done
	fi
	if [[ -n ${SCHEMA_REPLICAS} ]]; then
		no_zookeeper || :
	fi
	USED_HOSTS=(${SCHEMA_TARGETS[@]+"${SCHEMA_TARGETS[@]}"})
	if [[ -n ${RESTORE_DATA} ]]; then
		USED_HOSTS+=("${DATA_TARGETS[@]}")
	fi
	unique_into USED_HOSTS "${USED_HOSTS[@]}"
	unique_into TARGETS ${SCHEMA_TARGETS[@]+"${SCHEMA_TARGETS[@]}"} "${DATA_TARGETS[@]}"
	resolve_names HOST_BACKUP "${BACKUP_KEY}" "${TARGETS[@]}"
	parse_hosts GUARD_HOSTS optional
	if [[ -z ${RESTORE_SCHEMA} ]]; then
		HOSTS=()
	fi
	unique_into GUARDED "${TARGETS[@]}" ${HOSTS[@]+"${HOSTS[@]}"}
}

# restore_preflight checks that every target answers; it refuses a restore from another installation that a local copy,
# or a backup of the same name, would mix up; with VALIDATE_TOPOLOGY, it checks that every host holds the backup and sets
# HOLDS_RBAC when it holds access entities
restore_preflight() {
	local host where base hint
	for host in "${TARGETS[@]}"; do
		call GET "${host}" /backup/version
		if [[ ${HTTP_CODE} != 200 ]]; then
			die "preflight failed: sidecar of ${host} answered HTTP ${HTTP_CODE}: $(error_of)"
		fi
	done
	if [[ -n ${SOURCE_ENV} ]]; then
		for host in "${USED_HOSTS[@]}"; do
			refuse_local_copy "${host}"
			if [[ -z ${SOURCE_OWN_PATH} ]]; then
				refuse_own_backup "${host}"
			fi
		done
	fi
	if [[ -z ${RESTORE_VALIDATE} ]]; then
		return 0
	fi
	other_installation_hint
	hint=${REPLY}
	for host in "${USED_HOSTS[@]}"; do
		where="the storage of ${host}"
		if [[ -n ${SOURCE_ENV} ]]; then
			where="the storage path '${SOURCE_PATH_SHOWN}' of ${host}"
			source_copy "${host}"
			if ! list_backups "${host}" local; then
				fail "preflight failed on ${host}"
			fi
			if ! find_backup "${HOST_BACKUP[${host}]}"; then
				die "preflight failed: backup ${HOST_BACKUP[${host}]} is missing in ${where}: its download left no local copy"
			fi
			base=${LIST_REQUIRED[FOUND]}
			if [[ -n ${base} && ${OWN_BACKUPS[${host}]:-} == *" ${base} "* ]]; then
				die "preflight failed: backup ${HOST_BACKUP[${host}]} is incremental from ${base}, and ${host} holds a backup ${base} at its own storage path too, which clickhouse-backup would take for the one at the storage path '${SOURCE_PATH_SHOWN}', as it tells the backups of every path apart by name alone: delete that one first"
			fi
		else
			if ! list_backups "${host}" remote; then
				fail "preflight failed on ${host}"
			fi
			if ! find_backup "${HOST_BACKUP[${host}]}"; then
				if [[ ${SELF_PATH} == *"{replica}"* ]]; then
					die "preflight failed: backup ${HOST_BACKUP[${host}]} is missing in the storage of ${host}: the storage path '${SELF_PATH}' holds {replica}, which gives every replica a path of its own, and the backup was made on the first replica of each shard only${hint}"
				fi
				die "preflight failed: backup ${HOST_BACKUP[${host}]} is missing in the storage of ${host}${hint}"
			fi
		fi
		if [[ ${LIST_DESC[FOUND]} == broken* ]]; then
			die "preflight failed: backup ${HOST_BACKUP[${host}]} is ${LIST_DESC[FOUND]} in ${where}"
		fi
		if [[ ${LIST_RBAC_SIZE[FOUND]:-0} != 0 ]]; then
			HOLDS_RBAC=1
		fi
	done
}

# restore_guard refuses, without OVERWRITE, a restore over tables that exist; with it, it plans the drops: ON_CLUSTER
restore_guard() {
	local -a shards
	if [[ -z ${RESTORE_SCHEMA} ]]; then
		log "restore ${BACKUP_NAME}: restores the partitions ${PARTITIONS} into the tables as they are, without the schema, as the Altinity Cloud Manager does: each of them is dropped first where a table holds it"
	elif [[ -z ${RESTORE_OVERWRITE} ]]; then
		guard_tables "${USED_HOSTS[*]}" "${GUARDED[@]}"
	elif [[ -n ${SCHEMA_REPLICAS} ]]; then
		unique_into shards "${SCHEMA_TARGETS[0]}" "${DATA_TARGETS[@]}"
		plan_overwrite "${shards[*]}" "${GUARDED[@]}"
	else
		log "restore ${BACKUP_NAME}: every shard of cluster ${CLUSTER} has one replica, which drops the tables of the backup on its own"
		if drop_size_limited; then
			reset_backup_tables
			live_tables "${SCHEMA_TARGETS[@]}"
			refuse_database_drops "${SCHEMA_TARGETS[*]}" "${SCHEMA_TARGETS[@]}"
		fi
	fi
}

# restore_local_copies sets HAD_LOCAL to the hosts that hold a copy of the backup, which the restore keeps, and
# RESUMED_LOCAL to those whose incomplete copy it resumes; a restore from another installation drops those of its
# preflight instead, which it downloads anew
restore_local_copies() {
	local host
	HAD_LOCAL=() RESUMED_LOCAL=()
	if [[ -n ${SOURCE_ENV} ]]; then
		delete_source_copies
		SOURCE_COPIES=("${USED_HOSTS[@]}")
		return 0
	fi
	for host in "${USED_HOSTS[@]}"; do
		if ! list_backups "${host}" local || ! find_backup "${HOST_BACKUP[${host}]}"; then
			continue
		fi
		if [[ ${LIST_DESC[FOUND]} == broken* ]]; then
			log "${host}: holds an incomplete local copy of ${HOST_BACKUP[${host}]}, whose download the restore resumes"
			RESUMED_LOCAL+=("${host}")
		else
			HAD_LOCAL+=("${host}")
		fi
	done
}

# restore_downloads downloads the backup on every host of USED_HOSTS but those of HAD_LOCAL
restore_downloads() {
	local host
	local -a downloads=() drop=()
	for host in "${USED_HOSTS[@]}"; do
		if ! listed "${host}" ${HAD_LOCAL[@]+"${HAD_LOCAL[@]}"}; then
			downloads+=("${host}")
		fi
	done
	DATA_DOWNLOADS=
	if [[ -n ${RESTORE_DATA} ]]; then
		DATA_DOWNLOADS=${DATA_TARGETS[*]}
	fi
	if ((${#downloads[@]} == 0)); then
		return 0
	fi
	READ_ONLY=1
	if ! run_all "${RESTORE_KIND}" download_args download "${BACKUP_NAME}" "${downloads[@]}"; then
		READ_ONLY=
		if [[ -z ${SOURCE_ENV} ]]; then
			for host in "${downloads[@]}"; do
				if ! listed "${host}" ${RESUMED_LOCAL[@]+"${RESUMED_LOCAL[@]}"}; then
					drop+=("${host}")
				fi
			done
			drop_downloads ${drop[@]+"${drop[@]}"}
		fi
		fail_all "download of ${BACKUP_NAME}"
	fi
	READ_ONLY=
}

do_restore() {
	local host
	restore_settings
	check_storage_paths
	source_paths
	wait_for_apis "${GUARDED[@]}"

	event Normal OrchestratorRestoreStarted "restore ${BACKUP_NAME} on ${#TARGETS[@]} host(s)"
	if [[ -n ${RESTORE_OVERWRITE} ]]; then
		log "restore ${BACKUP_NAME}: preflight on ${#TARGETS[@]} host(s)"
	else
		log "restore ${BACKUP_NAME}: preflight on ${#TARGETS[@]} host(s), overwrite guard on ${#GUARDED[@]}"
	fi
	restore_preflight
	restore_guard
	restore_local_copies
	restore_env
	RESTORE_KIND=op
	if [[ -n ${ACTION_ENV} ]]; then
		RESTORE_KIND=action
	fi
	restore_downloads

	if [[ -n ${ON_CLUSTER} ]] && ! restore_step "${SCHEMA_TARGETS[0]}" "restore schema on cluster ${CLUSTER}" schema_on_cluster; then
		fail "schema restore of ${BACKUP_NAME} on cluster ${CLUSTER} failed on ${SCHEMA_TARGETS[0]}"
	fi
	for host in ${SCHEMA_TARGETS[@]+"${SCHEMA_TARGETS[@]}"}; do
		if ! restore_step "${host}" "restore schema" schema; then
			fail "schema restore of ${BACKUP_NAME} failed on ${host}"
		fi
	done
	if [[ -n ${RESTORE_DATA} ]] && ! run_all "${RESTORE_KIND}" data_args "restore data" "${BACKUP_NAME}" "${DATA_TARGETS[@]}"; then
		fail_all "data restore of ${BACKUP_NAME}"
	fi
	for host in "${USED_HOSTS[@]}"; do
		if ! listed "${host}" ${HAD_LOCAL[@]+"${HAD_LOCAL[@]}"} && ! drop_local "${host}" "${HOST_BACKUP[${host}]}"; then
			warn "${host}: the downloaded copy of ${HOST_BACKUP[${host}]} is left on local disk"
		fi
	done
	notice OrchestratorRestoreCompleted "restore ${BACKUP_NAME} completed"
	if [[ -n ${RESTORE_RBAC_WITH_SCHEMA} ]]; then
		return 0
	fi
	if [[ -z ${RESTORE_VALIDATE} ]]; then
		log "whether ${BACKUP_NAME} holds access entities is unknown: its copies in the storage were not listed"
	elif [[ -n ${HOLDS_RBAC} ]]; then
		alert OrchestratorRBACNotRestored "${BACKUP_NAME} holds access entities - users, roles, grants - which this restore did not restore"
		report "${RBAC_NOT_RESTORED_MESSAGE:-}"
	fi
}

declare -A LAST_OP=() LAST_ACTION_START=() LAST_ACTION_COMMAND=() OWN_BACKUPS=()
declare -A OP_ID=() ACTION_COMMAND=() ACTION_BEFORE=()
declare -A COMPLETE=() WITH_DATA=() EMPTY=() SIZED=() BROKEN=() ALL_NAMES=()
declare -A DB_SOURCE=() DB_TARGET=()
FAILED_HOSTS=()
PREFERRED_HOSTS=()
FAILURES=()
STEP_FAILED_HOSTS=()
STEP_ARGS=()
RESTORE_KIND=op
ACTION_ENV=
DATA_DOWNLOADS=
CREATE_KIND=op
CREATE_FLAGS=
CREATE_ENV=
INCREMENTAL_KEEP=
RESTORE_RBAC_WITH_SCHEMA=
HOLDS_RBAC=
STEP_KIND=op

main() {
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

if [[ ${BASH_SOURCE[0]:-$0} == "$0" ]]; then
	main "$@"
fi
