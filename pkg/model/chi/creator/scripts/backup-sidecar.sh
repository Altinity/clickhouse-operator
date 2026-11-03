# Starts the clickhouse-backup REST API server of the sidecar. Arguments, never spliced into this text:
#   $1  the storage path of the backups, for a storage that names none
#   $2  the storage path of the object disk data of the backups, for a storage that names none
#   $3… the command that starts the server, and its arguments
set -euo pipefail

PORT_NAME_RE='^[A-Za-z_][A-Za-z0-9_]*_PORT$'
SERVICE_LINK_RE='^(tcp|udp|sctp)://'
# Where clickhouse-backup reads its configuration file from unless CLICKHOUSE_BACKUP_CONFIG names another one - or
# none, set empty. The command the operator starts names none on its command line.
DEFAULT_CONFIG_FILE=/etc/clickhouse-backup/config.yml

# drop_service_links unsets every *_PORT variable that holds a Kubernetes service link. A Service named after a
# clickhouse-backup setting, as "sftp" is after SFTP_PORT, gives the container that setting as tcp://<ip>:<port>,
# which clickhouse-backup fails to parse as a port number - and with it its whole configuration. No port number
# looks like that, while a value the Secret or the spec sets takes precedence over a link anyway.
drop_service_links() {
	local entry name
	while IFS= read -r -d '' entry; do
		name=${entry%%=*}
		if [[ ${name} =~ ${PORT_NAME_RE} && ${entry#*=} =~ ${SERVICE_LINK_RE} ]]; then
			unset "${name}"
		fi
	done </proc/$$/environ
}

# default_variable NAME VALUE exports NAME as VALUE unless it is set, even to an empty value
default_variable() {
	if [[ ! -v $1 ]]; then
		export "$1=$2"
	fi
}

# default_paths PATH OBJECT_DISK_PATH sets the paths for the storage type REMOTE_STORAGE names, where the environment
# sets none. Without a path every shard of every cluster keeps its backups at one place, where they overwrite each
# other, and a host with tables on object disks fails every backup. The paths of a configuration file are left
# alone, as an env var would take precedence over them. Each storage type's variables are named after it.
default_paths() {
	local provider
	if [[ -e ${CLICKHOUSE_BACKUP_CONFIG-${DEFAULT_CONFIG_FILE}} ]]; then
		return
	fi
	case ${REMOTE_STORAGE:-} in
	s3 | gcs | azblob | cos | ftp | sftp)
		provider=${REMOTE_STORAGE^^}
		;;
	*)
		return
		;;
	esac
	default_variable "${provider}_PATH" "$1"
	default_variable "${provider}_OBJECT_DISK_PATH" "$2"
}

if (($# < 3)) || [[ -z $1 || -z $2 ]]; then
	printf '%s: the default storage paths and the command of the server must be given\n' "$0" >&2
	exit 1
fi
drop_service_links
default_paths "$1" "$2"
shift 2
# The REST API lets in whoever presents its credentials, so everybody while they are empty. They come from the
# backup Secret: started while that Secret is missing, the sidecar idles - its pod stays ready - until its liveness
# probe restarts it once the Secret is back.
if [[ -z ${API_USERNAME:-} || -z ${API_PASSWORD:-} ]]; then
	printf '%s: API_USERNAME and API_PASSWORD are not set, as the backup Secret does not exist; the REST API is not served until it does\n' "$0" >&2
	trap 'kill "${!}" 2>/dev/null; exit 0' TERM INT
	while :; do
		sleep 3600 &
		wait "${!}" || true
	done
fi
exec "$@"
