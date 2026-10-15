# Liveness probe of the clickhouse-backup sidecar. It fails once a Secret the sidecar takes env vars from holds a
# value the sidecar did not start with, so that kubelet restarts the sidecar alone, resolving its env vars anew:
# kubelet resolves them only when it starts a container, while it keeps the files of a mounted Secret up to date.
# Arguments, never spliced into this text:
#   $1  the seconds the call of the REST API may take
#   $2  the seconds after the sidecar finished an operation during which it is not restarted
#   $3  the mount of the backup Secret, one file per env var taking its value from it, named after the env var
#   $4  the mount of the storage Secret, every key of which is an env var of its name
#   $5… the env vars the container sets itself, which take precedence over the keys of $4
# An empty directory argument stands for a Secret the sidecar does not take env vars from.
set -euo pipefail
shopt -s nullglob
# Timestamps compare as strings, byte by byte
LC_ALL=C

IDENTIFIER_RE='^[A-Za-z_][A-Za-z0-9_]*$'
IN_PROGRESS_RE='"status":"in progress"'
# clickhouse-backup stamps an action with its local time in this format
TIMESTAMP_FORMAT='%Y-%m-%d %H:%M:%S'
FINISH_RE='"finish":"([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})"'
NUMBER_RE='^[0-9]+$'

# The environment the container started with, which the probe runs with too. Shell variables would not do: the
# shell sets some of them itself, such as PWD or SHLVL. Of two equal names the first wins, as for getenv(3).
declare -A STARTED_WITH=()
while IFS= read -r -d '' entry; do
	name=${entry%%=*}
	if [[ ${name} =~ ${IDENTIFIER_RE} && -z ${STARTED_WITH[${name}]+set} ]]; then
		STARTED_WITH[${name}]=${entry#*=}
	fi
done </proc/$$/environ

# explicit NAME [EXPLICIT...] reports whether NAME is one of EXPLICIT
explicit() {
	local name=$1 other
	shift
	for other in "$@"; do
		if [[ ${other} == "${name}" ]]; then
			return 0
		fi
	done
	return 1
}

# differs DIR [EXPLICIT...] reports whether a file of DIR named after an env var holds another value than the
# container started with, leaving its path in CHANGED. Files named after an EXPLICIT env var are not compared.
# kubelet's own entries start with "..", and a key that is no shell identifier is no setting clickhouse-backup reads.
differs() {
	local dir=$1 file name content
	shift
	if [[ -z ${dir} ]]; then
		return 1
	fi
	for file in "${dir}"/*; do
		name=${file##*/}
		if [[ ! ${name} =~ ${IDENTIFIER_RE} || ! -f ${file} || ! -r ${file} ]] || explicit "${name}" "$@"; then
			continue
		fi
		# Reading up to a NUL keeps trailing newlines, which a command substitution would strip
		content=
		IFS= read -r -d '' content <"${file}" || true
		if [[ -z ${STARTED_WITH[${name}]+set} || ${STARTED_WITH[${name}]} != "${content}" ]]; then
			CHANGED=${file}
			return 0
		fi
	done
	return 1
}

# busy reports whether the sidecar runs an operation, or has finished one within QUIET_SECONDS. A restart would break
# the operation - a restore is never resumed - or a backup Job that has yet to see how its last step ended, or is
# about to start its next one. A sidecar whose REST API does not answer runs nothing a Job drives, as the Jobs reach
# it through that API alone, and neither does one that idles without its credentials: both count as idle.
busy() {
	local port auth actions epoch earliest latest row
	port=${STARTED_WITH[API_LISTEN]:-}
	port=${port##*:}
	if [[ ! ${port} =~ ${NUMBER_RE} ]]; then
		return 1
	fi
	auth=$(printf '%s:%s' "${STARTED_WITH[API_USERNAME]:-}" "${STARTED_WITH[API_PASSWORD]:-}" | base64 | tr -d '\n')
	# The credentials travel on stdin so they never show up in the process list. A proxy the environment or a
	# curlrc names would answer for the sidecar, or not at all, so curl reads neither.
	actions=$(printf 'Authorization: Basic %s\n' "${auth}" |
		curl -q -fsS --noproxy '*' -H @- --max-time "${API_TIMEOUT}" "http://localhost:${port}/backup/actions" 2>/dev/null) ||
		return 1
	if [[ ${actions} =~ ${IN_PROGRESS_RE} ]]; then
		return 0
	fi
	printf -v epoch '%(%s)T' -1
	printf -v earliest "%(${TIMESTAMP_FORMAT})T" "$((epoch - QUIET_SECONDS))"
	printf -v latest "%(${TIMESTAMP_FORMAT})T" "$((epoch + QUIET_SECONDS))"
	# One action per line. A finish time past the window is of another time zone than the probe's and tells nothing.
	while IFS= read -r row; do
		if [[ ${row} =~ ${FINISH_RE} && ! ${BASH_REMATCH[1]} < ${earliest} && ! ${BASH_REMATCH[1]} > ${latest} ]]; then
			return 0
		fi
	done <<<"${actions}"
	return 1
}

API_TIMEOUT=$1
QUIET_SECONDS=$2
if [[ ! ${API_TIMEOUT} =~ ${NUMBER_RE} || ! ${QUIET_SECONDS} =~ ${NUMBER_RE} ]]; then
	printf '%s: the REST API timeout and the quiet window must be numbers of seconds\n' "$0" >&2
	exit 1
fi
CREDENTIALS=$3
STORAGE=$4
CHANGED=
# Without the backup Secret a restarted sidecar would lack its credentials, while the running one still has them
if [[ -n ${CREDENTIALS} ]]; then
	present=("${CREDENTIALS}"/*)
	if ((${#present[@]} == 0)); then
		exit 0
	fi
fi
if ! differs "${CREDENTIALS}" && ! differs "${STORAGE}" "${@:5}"; then
	exit 0
fi
if busy; then
	printf '%s changed since the sidecar started; it restarts once it has run no operation for a while\n' "${CHANGED}"
	exit 0
fi
printf '%s changed since the sidecar started; restarting it to apply the change\n' "${CHANGED}"
exit 1
