# Starts the clickhouse-backup REST API server of the sidecar. Arguments, never spliced into this text:
#   $1  the configuration file the operator generates, which clickhouse-backup reads where it looks by default
#   $2… the command that starts the server, and its arguments
set -euo pipefail

PORT_NAME_RE='^[A-Za-z_][A-Za-z0-9_]*_PORT$'
SERVICE_LINK_RE='^(tcp|udp|sctp)://'
# How often the script looks for the configuration file while it is missing
CONFIG_POLL_SECONDS=5

# drop_service_links unsets every *_PORT variable that holds a Kubernetes service link. A Service named after a
# clickhouse-backup setting, as "sftp" is after SFTP_PORT, gives the container that setting as tcp://<ip>:<port>,
# which clickhouse-backup fails to parse as a port number - and with it its whole configuration. No port number
# looks like that, while a value the configuration file sets takes precedence over a missing variable anyway.
drop_service_links() {
	local entry name
	while IFS= read -r -d '' entry; do
		name=${entry%%=*}
		if [[ ${name} =~ ${PORT_NAME_RE} && ${entry#*=} =~ ${SERVICE_LINK_RE} ]]; then
			unset "${name}"
		fi
	done </proc/$$/environ
}

# wait_for_config waits until the configuration file $1 is there. It holds the credentials of the REST API, which
# lets in whoever presents them, so everybody while they are missing. kubelet fills the volume of the optional
# Secret of the file once the operator has created it, while the sidecar idles - its pod stays ready.
wait_for_config() {
	if [[ -s $1 ]]; then
		return
	fi
	printf '%s: %s does not exist, as the backup configuration Secret does not; the REST API is not served until it does\n' "$0" "$1" >&2
	trap 'kill "${!}" 2>/dev/null; exit 0' TERM INT
	until [[ -s $1 ]]; do
		sleep "${CONFIG_POLL_SECONDS}" &
		wait "${!}" || true
	done
	trap - TERM INT
}

if (($# < 2)) || [[ -z $1 ]]; then
	printf '%s: the configuration file and the command of the server must be given\n' "$0" >&2
	exit 1
fi
drop_service_links
wait_for_config "$1"
shift
exec "$@"
