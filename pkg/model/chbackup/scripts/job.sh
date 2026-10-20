# Drives the clickhouse-backup sidecars of one cluster of a ClickHouse installation through their REST API.
# Every input arrives through the environment (see script.go): no value is ever spliced into this text, and values
# only reach curl as URL-encoded query parameters, or as tokens validated against NAME_RE, TABLES_RE or SOURCE_PATH_RE
# in the commands the script runs through the actions endpoint.
set -euo pipefail

NAME_RE='^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'
HOST_RE='^[A-Za-z0-9]([A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$'
NUMBER_RE='^[0-9]+$'
# A table pattern of a restore: that of a backup, without a leading dash, so it is a single token of a command too.
# Its length is checked apart: the regex library of an Alpine image refuses repetition counts above 255.
TABLES_RE='^[A-Za-z0-9_.*?,][A-Za-z0-9_.*?,-]*$'
# The longest table pattern the API accepts
TABLES_MAX_LENGTH=1024
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
# The number the CronJob controller ends the name of a scheduled Job with, bounded as in builder.go
SLOT_RE='^[0-9]{1,10}$'
TIMESTAMP_RE='^[0-9]{8}-[0-9]{6}$'
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
	CLICKHOUSE_BACKUP_CONFIG
)
REQUEST_TIMEOUT=600
DELETE_TIMEOUT=3600
UNREACHABLE_TIMEOUT=600
MUTATED_UNREACHABLE_TIMEOUT=3600
RESUME_TIMEOUT=300
# MUTATED is set once a sidecar may have accepted a request that changes something
MUTATED=
# READ_ONLY is set, as a local of a caller, while the requests it submits change nothing the Job is asked to change:
# they leave MUTATED alone
READ_ONLY=
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

# request METHOD HOST PATH [CURL_ARGS...] performs one API call and sets HTTP_CODE, HTTP_BODY and CURL_EXIT
request() {
	local method=$1 host=$2 path=$3 out
	shift 3
	HTTP_CODE=000
	CURL_EXIT=0
	# The credentials travel on stdin so they never show up in the process list
	out=$(printf 'Authorization: Basic %s\n' "${AUTH}" |
		curl -sS -X "${method}" -H @- --connect-timeout 10 --max-time "${REQUEST_TIMEOUT}" \
			-w '\n%{http_code}' "$@" "http://${host}:${API_PORT}${path}" 2>&1) || CURL_EXIT=$?
	# The status code written by -w is always the last line, also when curl fails
	HTTP_BODY=${out%$'\n'*}
	if ((CURL_EXIT == 0)); then
		HTTP_CODE=${out##*$'\n'}
	fi
}

# unreachable METHOD reports whether the last request failed in a way that is safe to retry.
# A POST qualifies only when the connection was never established, so it cannot be submitted twice:
# a proxy in front of the sidecar may answer 502-504 to a request the sidecar received.
unreachable() {
	case ${CURL_EXIT} in
	0) [[ $1 == GET ]] && proxy_failed ;;
	6 | 7) return 0 ;;
	*) [[ $1 == GET ]] ;;
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

# submit HOST WHAT PATH [CURL_ARGS...] POSTs a request once the sidecar is idle, queueing while it is busy: HTTP 423, or
# the error the actions endpoint answers a locked sidecar's request with. Neither is accepted, so resubmitting is safe
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
			fi
			warn "${host}: ${what} request failed: ${HTTP_BODY}"
			return 1
		fi
		if proxy_failed; then
			# The proxy may have passed the request on
			changed
			warn "${host}: ${what} failed (HTTP ${HTTP_CODE}): $(error_of)"
			return 1
		fi
		case ${HTTP_CODE} in
		2??)
			changed
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
# the rows of finished actions, and a restart drops them all. An action run READ_ONLY is no step of the Job: a sidecar
# that restarts after it has lost nothing the steps to come build on
run_action() {
	local host=$1 what=$2 name=$3 command=$4 body i start status error
	local -a before
	log "${host}: ${what} ${name}"
	if ! sidecar_kept "${host}"; then
		return 1
	fi
	json_string "${command}"
	body="{\"command\":${REPLY}}"
	if ! action_rows "${host}" "${command}"; then
		return 1
	fi
	before=("${ACTION_STARTS[@]}")
	if ! submit "${host}" "${what}" /backup/actions --data-binary "${body}"; then
		return 1
	fi
	if [[ $(json_field status "${HTTP_BODY}") != acknowledged ]]; then
		warn "${host}: ${what} was not acknowledged: ${HTTP_BODY}"
		return 1
	fi
	while :; do
		sleep "${POLL_INTERVAL}"
		if ! action_rows "${host}" "${command}"; then
			return 1
		fi
		start= status= error=
		for ((i = ${#ACTION_STARTS[@]} - 1; i >= 0; i--)); do
			if [[ ${ACTION_STATUSES[i]} == "in progress" ]] || ! listed "${ACTION_STARTS[i]}" "${before[@]}"; then
				start=${ACTION_STARTS[i]} status=${ACTION_STATUSES[i]} error=${ACTION_ERRORS[i]}
				break
			fi
		done
		case ${status} in
		"in progress") ;;
		success)
			log "${host}: ${what} ${name} succeeded"
			if [[ -z ${READ_ONLY:-} ]]; then
				remember_step "${host}" "" "${start}" "${command}"
			fi
			return 0
			;;
		"")
			if [[ -n ${READ_ONLY:-} ]]; then
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
	done
}

# run_op HOST WHAT NAME PATH [CURL_ARGS...] starts an asynchronous sidecar operation on backup NAME
# and polls it until it succeeds or fails
run_op() {
	local host=$1 what=$2 name=$3 id status
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
	while :; do
		sleep "${POLL_INTERVAL}"
		call GET "${host}" /backup/status -G --data-urlencode "operationid=${id}"
		if [[ ${HTTP_CODE} != 200 ]]; then
			warn "${host}: cannot read status of ${what} (HTTP ${HTTP_CODE}): $(error_of)"
			return 1
		fi
		status=$(json_field status "${HTTP_BODY}")
		case ${status} in
		"in progress") ;;
		success)
			log "${host}: ${what} ${name} succeeded"
			remember_step "${host}" "${id}"
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
	done
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
# PATTERN of the backups every HOST lists complete, and WITH_DATA, with those of the backups that hold data on any HOST,
# and UNLISTED with the hosts that cannot be listed. A run that failed on one host leaves copies on the others that
# cannot be restored on their own, and the copies of one backup on a host whose tables hold no data are as much part of
# it as those that do
list_all() {
	local pattern=$1 host first=1 i name
	local -A listed both
	declare -gA COMPLETE=() WITH_DATA=()
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
			if [[ ! ${LIST_NAMES[i]} =~ ${pattern} || ${LIST_DESC[i]} == broken* ]]; then
				continue
			fi
			listed[${LIST_NAMES[i]}]=1
			if holds_data "${i}"; then
				WITH_DATA[${LIST_NAMES[i]}]=1
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

# retention_on_host HOST KEEP PATTERN deletes the backups matching PATTERN on HOST that retention keeps no longer, as
# list_all classified them
retention_on_host() {
	local host=$1 keep=$2 pattern=$3 i name kept=0 kept_empty=0 required_by
	local -a candidates=()
	local -A deleted=()
	if ! list_backups "${host}" remote; then
		return 1
	fi
	# The timestamp suffix is fixed-width, so name order is creation order
	for i in "${!LIST_NAMES[@]}"; do
		if [[ ${LIST_NAMES[i]} =~ ${pattern} ]]; then
			candidates+=("${LIST_NAMES[i]}")
		fi
	done
	if ((${#candidates[@]} == 0)); then
		return 0
	fi
	mapfile -t candidates < <(printf '%s\n' "${candidates[@]}" | sort -r)
	# Walk newest to oldest: an incremental is always newer than its base,
	# so deleting it first can free the base within the same pass
	for name in "${candidates[@]}"; do
		# A backup that not every host holds complete - broken, a copy of a run that failed on another host, or one made
		# before a host was added - never takes the place of one they all do. One with data is kept until the backups
		# with data that every host holds fill their window, as it may still restore from the hosts that hold it; any
		# other while both windows are open: an upload in progress lists as broken until it completes, and a run in
		# progress has made its copies on some hosts only
		if [[ -z ${COMPLETE[${name}]:-} ]]; then
			if [[ -n ${WITH_DATA[${name}]:-} ]]; then
				if ((kept < keep)) || [[ ${name} == "${BACKUP_NAME}" ]]; then
					continue
				fi
			elif (((kept < keep) && (kept_empty < keep))) || [[ ${name} == "${BACKUP_NAME}" ]]; then
				continue
			fi
		# Nor does a backup without data - of tables that match nothing any more, or of an installation recreated
		# empty: the newest of those are kept apart, so that a host which never holds data still prunes
		elif [[ -z ${WITH_DATA[${name}]:-} ]]; then
			if ((kept_empty < keep)) || [[ ${name} == "${BACKUP_NAME}" ]]; then
				kept_empty=$((kept_empty + 1))
				continue
			fi
		elif ((kept < keep)) || [[ ${name} == "${BACKUP_NAME}" ]]; then
			kept=$((kept + 1))
			continue
		fi
		required_by=
		for i in "${!LIST_NAMES[@]}"; do
			# A dependent without a name, which no sidecar makes, is never one retention deleted
			if [[ ${LIST_REQUIRED[i]} == "${name}" ]] && [[ -z ${LIST_NAMES[i]} || -z ${deleted[${LIST_NAMES[i]}]:-} ]]; then
				required_by=${LIST_NAMES[i]:-a backup without a name}
				break
			fi
		done
		if [[ -n ${required_by} ]]; then
			log "${host}: retention keeps ${name}, ${required_by} is based on it"
			continue
		fi
		if submit "${host}" "delete remote ${name}" "/backup/delete/remote/${name}" --max-time "${DELETE_TIMEOUT}"; then
			deleted[${name}]=1
			log "${host}: retention deleted ${name}"
		fi
	done
}

retention() {
	local keep=$((10#${KEEP_LAST:-0})) host pattern
	if ((keep == 0)); then
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
	log "retention: keeping the ${keep} newest ${BACKUP_NAME_PREFIX}-* backups with data, and the ${keep} newest without"
	# An unreachable host skips retention until the next run instead of holding the Job until its deadline
	BEST_EFFORT=1
	pattern="^${BACKUP_NAME_PREFIX//./[.]}-[0-9]{8}-[0-9]{6}$"
	list_all "${pattern}" "${HOSTS[@]}"
	# Every host keeps and deletes the same backups, which takes knowing what each of them holds
	if ((${#UNLISTED[@]} > 0)); then
		warn "retention skipped: ${UNLISTED[*]} cannot be listed, backups are left as they are"
		return 0
	fi
	for host in "${HOSTS[@]}"; do
		if [[ ! ${host} =~ ${HOST_RE} ]]; then
			warn "retention skipped an invalid host name"
		elif ! retention_on_host "${host}" "${keep}" "${pattern}"; then
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
	local keep=$((10#${KEEP_LAST:-0}))
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
	if ((keep > 0)); then
		warn "${BACKUP_NAME} holds no data, so it does not count toward the ${keep} backups with data that retention keeps"
	else
		warn "${BACKUP_NAME} holds no data"
	fi
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
# sidecar in place of its environment, say - is left out of it with a warning: only a path proven shared is refused.
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
		# The operator tells when the settings it gives do not have to be those of the sidecar. A configuration file the
		# storage Secret names, which the operator does not read, may set any of them.
		path_setting "${index}" UNVERIFIABLE
		what=${REPLY//[[:cntrl:]]/ }
		path_setting "${index}" CLICKHOUSE_BACKUP_CONFIG
		if [[ -z ${what} && -n ${REPLY} ]]; then
			what="its storage Secret names a configuration file in CLICKHOUSE_BACKUP_CONFIG"
		fi
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
				what="its environment does not set REMOTE_STORAGE, as with a configuration file of the sidecar"
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

# schedule_timestamp prints the UTC time a scheduled run was due, as YYYYMMDD-HHMMSS. The CronJob controller names each
# Job after that time in minutes since the epoch, so every run of one slot makes the same backup, whose name the operator
# derives the same way; a Job named otherwise, such as one created by hand from the CronJob, takes the current time.
schedule_timestamp() {
	local slot= timestamp=
	if [[ -n ${CRON_JOB_NAME:-} && ${JOB_NAME:-} == "${CRON_JOB_NAME}-"* ]]; then
		slot=${JOB_NAME#"${CRON_JOB_NAME}-"}
	fi
	if [[ ${slot} =~ ${SLOT_RE} ]]; then
		timestamp=$(date -u -d "@$((10#${slot} * 60))" +%Y%m%d-%H%M%S 2>/dev/null) || timestamp=
	fi
	if [[ ! ${timestamp} =~ ${TIMESTAMP_RE} ]]; then
		timestamp=$(date -u +%Y%m%d-%H%M%S)
	fi
	printf '%s' "${timestamp}"
}

do_backup() {
	local host
	local -a backup_hosts params
	parse_hosts BACKUP_HOSTS
	backup_hosts=("${HOSTS[@]}")
	if [[ -n ${KEEP_LAST:-} && ! ${KEEP_LAST} =~ ${NUMBER_RE} ]]; then
		die "KEEP_LAST must be a non-negative integer"
	fi
	if [[ -n ${BACKUP_NAME_PREFIX:-} ]]; then
		check_name BACKUP_NAME_PREFIX
	fi
	if [[ -n ${BACKUP_NAME:-} ]]; then
		check_name BACKUP_NAME
	elif [[ -n ${BACKUP_NAME_PREFIX:-} ]]; then
		BACKUP_NAME="${BACKUP_NAME_PREFIX}-$(schedule_timestamp)"
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
		params+=(--data-urlencode "diff-from-remote=${DIFF_FROM_REMOTE}")
	fi
	if [[ -n ${CLUSTER:-} ]]; then
		check_cluster
	fi
	check_storage_paths

	log "backup ${BACKUP_NAME}: preflight on ${#backup_hosts[@]} host(s)"
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
	done

	for host in "${backup_hosts[@]}"; do
		if ! list_backups "${host}" remote; then
			fail "backup ${BACKUP_NAME} failed on ${host}"
		fi
		if find_backup "${BACKUP_NAME}"; then
			die "backup ${BACKUP_NAME} appeared in the storage of ${host} during this run: hosts of one backup must not share a storage path"
		fi
		if ! run_op "${host}" create_remote "${BACKUP_NAME}" /backup/create_remote "${params[@]}"; then
			fail "backup ${BACKUP_NAME} failed on ${host}"
		fi
	done
	log "backup ${BACKUP_NAME} completed on ${#backup_hosts[@]} host(s)"
	# A schema-only backup never holds data, which needs no telling
	if ! is_true SCHEMA_ONLY; then
		report_data "${backup_hosts[@]}"
	fi
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
# of a materialized view it drops, which goes with the view
dropped_tables() {
	local i
	local -A inner=()
	LIVE_DROPPED=()
	for i in "${!LIVE_TABLES[@]}"; do
		LIVE_DROPPED[i]=
		if [[ -n ${BACKUP_TABLES["${LIVE_DBS[i]}.${LIVE_TABLES[i]}"]:-} ]] || drops_database "${LIVE_DBS[i]}"; then
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
# recreates, or one of a database it drops as a whole. Without a pattern, or when a table of the backup or of a HOST
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
		if [[ -z ${TABLES:-} || -n ${BACKUP_TABLES_UNREADABLE} || -n ${BACKUP_TABLES["${LIVE_DBS[i]}.${LIVE_TABLES[i]}"]:-} ]] ||
			db_dropped "${LIVE_DBS[i]}"; then
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
		if drops_database "${key#* }"; then
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
# that holds a local copy of the backup: restore_remote takes a local copy of its backup for one it has downloaded in
# part, and restores what that holds, whichever paths it came from. A restore from its own paths keeps such a copy
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
	local host=$1 READ_ONLY=1
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
# fails the restore: restore_remote would take it for one it has downloaded in part and restore no data. A Job that
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

# restore_step HOST WHAT FLAGS PARAM... runs the restore step WHAT of the backup on HOST: a typed request with the query
# parameters PARAM, or for a restore from another installation the command of the actions endpoint with the flags FLAGS
# and SOURCE_ENV, as a typed request takes no setting in place of those of the sidecar
restore_step() {
	local host=$1 what=$2 flags=$3
	shift 3
	if [[ -n ${SOURCE_ENV} ]]; then
		run_action "${host}" "${what}" "${BACKUP_NAME}" "restore_remote ${flags} ${SOURCE_ENV} ${BACKUP_NAME}"
		return
	fi
	run_op "${host}" "${what}" "${BACKUP_NAME}" "/backup/restore_remote/${BACKUP_NAME}" -G "$@"
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
	local host restore_data=1 overwrite= validate= restore_rbac= holds_rbac= replicas= command hint where base
	local -a schema_hosts data_hosts targets guarded used shards table_params=() had_local=()
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
		table_params=(--data-urlencode "table=${TABLES}")
	fi
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
		# Only the copies of the schema the preflight downloaded are left, which restore_remote would restore no data from
		drop_source_copies strict
		# The restore steps download the backup to every host they restore it on, where a step that fails leaves a copy
		SOURCE_COPIES=("${used[@]}")
	fi

	# The drop ON CLUSTER drops the tables of the backup the pattern takes, and the databases it takes as a whole, on every
	# host from the first schema host, and recreates them as the backup of its shard holds them, which the preflight found
	# alike on every shard: no replica then keeps an old part that a replica which recreated its table would clone back.
	# clickhouse-backup rewrites on the way a Distributed table that names another cluster, which the restore of every
	# replica on its own undoes next, as it drops and creates that table again from the backup
	if [[ -n ${ON_CLUSTER} ]]; then
		command="restore_remote --schema --rm --env=RESTORE_SCHEMA_ON_CLUSTER=${CLUSTER}"
		if [[ -n ${TABLES:-} ]]; then
			command+=" --tables=${TABLES}"
		fi
		if [[ -n ${SOURCE_ENV} ]]; then
			command+=" ${SOURCE_ENV}"
		fi
		command+=" ${BACKUP_NAME}"
		if ! run_action "${schema_hosts[0]}" "restore_remote schema on cluster ${CLUSTER}" "${BACKUP_NAME}" "${command}"; then
			fail "schema restore of ${BACKUP_NAME} on cluster ${CLUSTER} failed on ${schema_hosts[0]}"
		fi
	fi
	# Every replica restores the schema on its own, dropping what it has of the backup first: a table that has come
	# through the replication of a Replicated database would fail its CREATE
	for host in "${schema_hosts[@]}"; do
		if ! restore_step "${host}" "restore_remote schema" "--schema --rm${TABLES:+ --tables=${TABLES}}" \
			--data-urlencode "schema=" --data-urlencode "rm=" "${table_params[@]}"; then
			fail "schema restore of ${BACKUP_NAME} failed on ${host}"
		fi
	done
	if [[ -n ${restore_data} ]]; then
		for host in "${data_hosts[@]}"; do
			if ! restore_step "${host}" "restore_remote data" "--data${TABLES:+ --tables=${TABLES}}" \
				--data-urlencode "data=" "${table_params[@]}"; then
				fail "data restore of ${BACKUP_NAME} failed on ${host}"
			fi
		done
	fi
	# Access entities are stored apart on every host, and a backup holds those of the replica it was made on
	if [[ -n ${restore_rbac} ]]; then
		for host in "${schema_hosts[@]}"; do
			if ! restore_step "${host}" "restore_remote rbac" --rbac-only --data-urlencode "rbac-only="; then
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
	if [[ -n ${RBAC_NOT_RESTORED_MESSAGE:-} ]]; then
		termination_message "${RBAC_NOT_RESTORED_MESSAGE}"
	fi
}

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
declare -A LAST_OP=() LAST_ACTION_START=() LAST_ACTION_COMMAND=() OWN_BACKUPS=()

case ${OPERATION:-} in
backup) do_backup ;;
verify) do_verify ;;
restore) do_restore ;;
*) die "OPERATION must be one of backup, verify, restore" ;;
esac
