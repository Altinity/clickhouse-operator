#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  NS=<namespace> TARGET_STS=<statefulset> NEW_SIZE=<size> ./volume_downscale.sh [switches] <phase>

Phases:
  derive       Print derived values only
  prepare      Discovery, backup, retain old PV, create new temp PVC
  warm-copy    Apply warm-copy CHIT with rsync as restartable initContainer
  swap         Patch CHI VCT, require ACM checkpoint, then swap PVCs
  final-copy   Apply final-copy CHIT, resume CHI, wait for offline sync
  bind-old-pv  Recovery helper: bind original old PV to OLD_AS_TEMP_PVC
  verify       Check PVC/PV/filesystem size
  cleanup      Delete migration CHIT after successful validation
  all          Run prepare -> warm-copy -> swap -> final-copy -> verify -> cleanup

Required inputs:
  NS           Kubernetes namespace
  TARGET_STS   Target StatefulSet name, for example chi-otel-logs-otel-logs-0-0
  NEW_SIZE     Desired smaller PVC size, for example 1500Gi

Optional:
  --auto-confirm
                         Auto-confirm single-token gates except ACM_READY.
                         Does not answer DELETE_OLD_DATA/SKIP.
  CLICKHOUSE_DATA_PATH   Default: /var/lib/clickhouse
  RSYNC_IMAGE            Default: instrumentisto/rsync-ssh:latest
  WORKDIR                Default: disk-shrink-backup/<target-pod>
  ORIGINAL_POD_TEMPLATE_NAME
                         Override if it cannot be derived safely.
  TARGET_SERVICE          Default: TARGET_STS. Service whose ready annotation should be removed before scale-down.
  OLD_AS_TEMP_PVC         Default: TARGET_POD-old-data-retained. Override when recovering from an occupied retained PVC name.
  TRAFFIC_DRAIN_SECONDS   Default: 30. Delay after removing ready markers before scale-down.
  WARM_COPY_MAX_LITERAL_BYTES
                         Default: 10737418240. Max rsync Literal data bytes for a converged warm-copy pass.
  WARM_COPY_STABLE_PASSES Default: 1. Consecutive converged warm-copy passes required.
  WARM_COPY_WAIT_TIMEOUT_SECONDS
                         Default: auto from captured data size at 500GB/hour,
                         fallback to source PVC size, minimum 7200.
                         Max time to wait for warm-copy convergence.
  WARM_COPY_TIMEOUT_MIN_SECONDS
                         Default: 7200. Minimum auto warm-copy wait timeout.
  WARM_COPY_RSYNC_BYTES_PER_HOUR
                         Default: 500000000000. Used for auto warm-copy timeout.
  WARM_COPY_POLL_SECONDS  Default: 60. Poll interval while waiting for warm-copy convergence.
  AUTO_CONFIRM           Default: no. Env equivalent of --auto-confirm.
EOF
}

require_tool() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "missing required tool: $1" >&2
        exit 2
    }
}

is_truthy() {
    case "${1:-}" in
        1|yes|true|on|YES|TRUE|ON)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

confirm() {
    local prompt="$1"
    local expected="$2"
    local got
    echo
    echo "$prompt"
    if [[ "$expected" != "ACM_READY" ]] && is_truthy "${AUTO_CONFIRM:-no}"; then
        echo "Auto-confirmed: $expected"
        return
    fi
    echo "Type exactly: $expected"
    printf "> "
    read -r got
    if [[ "$got" != "$expected" ]]; then
        echo "confirmation mismatch; expected '$expected', got '$got'. aborting." >&2
        exit 3
    fi
}

confirm_retained_old_data_cleanup() {
    local got
    echo
    echo "Retained old-data PVC $OLD_AS_TEMP_PVC is the rollback source."
    echo "Delete it only after validation and the agreed retention window are complete."
    echo "Type DELETE_OLD_DATA to delete it now, or SKIP to keep it."
    printf "> "
    read -r got
    case "$got" in
        DELETE_OLD_DATA|SKIP)
            RETAINED_OLD_DATA_CLEANUP_CHOICE="$got"
            ;;
        *)
            echo "confirmation mismatch; expected DELETE_OLD_DATA or SKIP, got '$got'. aborting." >&2
            exit 3
            ;;
    esac
}

section() {
    local message="$1"
    echo
    echo "$message"
    printf '%*s\n' "${#message}" '' | tr ' ' '='
}

phase_section() {
    section "Phase: $1"
}

k() {
    kubectl "$@"
}

delete_migration_chit_and_wait() {
    k delete chit -n "$NS" "$MIGRATION_CHIT" --ignore-not-found
    echo "Waiting for migration CHIT $MIGRATION_CHIT to be absent"
    for _ in $(seq 1 60); do
        if ! k get chit -n "$NS" "$MIGRATION_CHIT" >/dev/null 2>&1; then
            echo "Migration CHIT $MIGRATION_CHIT is absent."
            return 0
        fi
        sleep 2
    done

    echo "migration CHIT $MIGRATION_CHIT still exists after delete request" >&2
    exit 18
}

assert_clean_migration_state() {
    section "Migration state preflight"

    if k get chit -n "$NS" "$MIGRATION_CHIT" >/dev/null 2>&1; then
        echo "migration CHIT $MIGRATION_CHIT already exists; run cleanup or delete it and wait for CHI reconciliation before starting a new prepare" >&2
        exit 19
    fi

    local existing_pvcs=()
    if k get pvc -n "$NS" "$NEW_TEMP_PVC" >/dev/null 2>&1; then
        existing_pvcs+=("$NEW_TEMP_PVC")
    fi
    if k get pvc -n "$NS" "$OLD_AS_TEMP_PVC" >/dev/null 2>&1; then
        existing_pvcs+=("$OLD_AS_TEMP_PVC")
    fi
    if ((${#existing_pvcs[@]})); then
        echo "found existing migration PVC(s): ${existing_pvcs[*]}" >&2
        echo "Finish cleanup or choose a clean rollback PVC name before starting a new prepare." >&2
        exit 21
    fi

    local dirty
    dirty=$(k get sts -n "$NS" -l "clickhouse.altinity.com/chi=$CHI" -o json | jq -r '
      .items[]
      | {
          name: .metadata.name,
          labels: (.spec.template.metadata.labels // {}),
          init: [.spec.template.spec.initContainers[]?.name],
          volumes: [.spec.template.spec.volumes[]?.name]
        }
      | select(
          (.labels["maintenance.altinity.com/purpose"] // "") == "clickhouse-volume-downscale"
          or (.labels["maintenance.altinity.com/stage"] // "") != ""
          or any(.init[]; test("^clickhouse-data-(warm-copy|final-sync)$"))
          or any(.volumes[]; test("^shrink-(old|new)-data$"))
        )
      | "\(.name): labels=\(.labels) init=\(.init) volumes=\(.volumes)"
    ')

    if [[ -n "$dirty" ]]; then
        echo "found stale migration state in CHI $CHI; starting now can roll non-target replicas while the operator cleans the template:" >&2
        echo "$dirty" >&2
        echo "Finish cleanup and wait for CHI reconciliation before running prepare again." >&2
        exit 20
    fi

    echo "No active migration CHIT or stale StatefulSet migration template state found."
}

wait_for_pvc_volume() {
    local pvc="$1"
    local timeout="${2:-600}"
    local interval=5
    local waited=0
    local status_line phase volume

    while (( waited <= timeout )); do
        status_line=$(k get pvc -n "$NS" "$pvc" -o jsonpath='{.status.phase}{" "}{.spec.volumeName}' 2>/dev/null || true)
        phase="${status_line%% *}"
        if [[ "$phase" == "$status_line" ]]; then
            volume=""
        else
            volume="${status_line#* }"
        fi

        if [[ "$phase" == "Bound" && -n "$volume" ]]; then
            echo "PVC $pvc is Bound to PV $volume." >&2
            printf '%s\n' "$volume"
            return 0
        fi

        sleep "$interval"
        waited=$((waited + interval))
    done

    echo "PVC $pvc did not become Bound with a non-empty spec.volumeName within ${timeout}s." >&2
    k get pvc -n "$NS" "$pvc" -o wide >&2 || true
    exit 10
}

wait_for_final_copy_and_ready() {
    local timeout="${1:-3600}"
    local interval=10
    local waited=0
    local pod_json final_state ready

    while (( waited <= timeout )); do
        pod_json=$(k get pod -n "$NS" "$TARGET_POD" -o json 2>/dev/null || true)
        if [[ -n "$pod_json" ]]; then
            final_state=$(jq -r '
              [.status.initContainerStatuses[]? | select(.name == "clickhouse-data-final-sync")][0] as $s
              | if $s == null then
                  "missing"
                elif $s.state.terminated then
                  "terminated:" + (($s.state.terminated.exitCode // -1) | tostring) + ":" + ($s.state.terminated.reason // "")
                elif $s.state.running then
                  "running"
                elif $s.state.waiting then
                  "waiting:" + ($s.state.waiting.reason // "")
                else
                  "unknown"
                end
            ' <<<"$pod_json")
            ready=$(jq -r '[.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length' <<<"$pod_json")

            case "$final_state" in
                terminated:0:*)
                    if [[ "$ready" != "0" ]]; then
                        echo "Final copy completed and pod $TARGET_POD is Ready."
                        return 0
                    fi
                    ;;
                terminated:*)
                    echo "Final copy failed with state: $final_state" >&2
                    k logs -n "$NS" "$TARGET_POD" -c clickhouse-data-final-sync --tail=80 >&2 || true
                    exit 11
                    ;;
            esac

            echo "Waiting for final copy and Ready pod: final-copy=$final_state ready=$ready waited=${waited}s"
        else
            echo "Waiting for pod $TARGET_POD to be recreated waited=${waited}s"
        fi

        sleep "$interval"
        waited=$((waited + interval))
    done

    echo "Pod $TARGET_POD did not complete final copy and become Ready within ${timeout}s." >&2
    k get pod -n "$NS" "$TARGET_POD" -o wide >&2 || true
    k describe pod -n "$NS" "$TARGET_POD" >&2 || true
    k logs -n "$NS" "$TARGET_POD" -c clickhouse-data-final-sync --tail=80 >&2 || true
    exit 12
}

wait_for_chi_reconcile() {
    local expected_task_id="$1"
    local timeout="${2:-1800}"
    local interval=10
    local waited=0
    local status_json status task_id errors

    while (( waited <= timeout )); do
        status_json=$(k get chi -n "$NS" "$CHI" -o json 2>/dev/null || true)
        if [[ -n "$status_json" ]]; then
            status=$(jq -r '.status.status // ""' <<<"$status_json")
            task_id=$(jq -r '.status.taskID // ""' <<<"$status_json")

            if [[ "$task_id" == "$expected_task_id" ]]; then
                case "$status" in
                    Completed)
                        echo "CHI $CHI reconcile completed for taskID=$expected_task_id."
                        return 0
                        ;;
                    Aborted)
                        echo "CHI $CHI reconcile aborted for taskID=$expected_task_id." >&2
                        errors=$(jq -r '(.status.errors // [])[]' <<<"$status_json")
                        if [[ -n "$errors" ]]; then
                            echo "$errors" >&2
                        fi
                        exit 13
                        ;;
                esac
            fi

            echo "Waiting for CHI reconcile: status=${status:-unknown} statusTaskID=${task_id:-none} expectedTaskID=$expected_task_id waited=${waited}s"
        else
            echo "Waiting for CHI $CHI status waited=${waited}s"
        fi

        sleep "$interval"
        waited=$((waited + interval))
    done

    echo "CHI $CHI did not complete reconcile taskID=$expected_task_id within ${timeout}s." >&2
    k get chi -n "$NS" "$CHI" -o json | jq '{specTaskID: .spec.taskID, statusTaskID: .status.taskID, status: .status.status, errors: .status.errors}' >&2 || true
    exit 14
}

remove_ready_markers_before_scale_down() {
    section "Remove host from load balancer"

    if k get pod -n "$NS" "$TARGET_POD" >/dev/null 2>&1; then
        k label pod -n "$NS" "$TARGET_POD" clickhouse.altinity.com/ready- --overwrite
    else
        echo "Pod $TARGET_POD is already absent; ready label removal skipped."
    fi

    if ! k get svc -n "$NS" "$TARGET_SERVICE" >/dev/null 2>&1; then
        echo "corresponding service $TARGET_SERVICE not found in namespace $NS" >&2
        exit 15
    fi
    k annotate svc -n "$NS" "$TARGET_SERVICE" clickhouse.altinity.com/ready- --overwrite

    local pod_ready service_ready
    pod_ready=$(k get pod -n "$NS" "$TARGET_POD" -o jsonpath='{.metadata.labels.clickhouse\.altinity\.com/ready}' 2>/dev/null || true)
    service_ready=$(k get svc -n "$NS" "$TARGET_SERVICE" -o jsonpath='{.metadata.annotations.clickhouse\.altinity\.com/ready}' 2>/dev/null || true)
    if [[ -n "$pod_ready" || -n "$service_ready" ]]; then
        echo "ready marker removal did not stick: pod=$pod_ready service=$service_ready" >&2
        exit 16
    fi

    if [[ "$TRAFFIC_DRAIN_SECONDS" != "0" ]]; then
        echo "Waiting ${TRAFFIC_DRAIN_SECONDS}s for load balancer/controller drain."
        sleep "$TRAFFIC_DRAIN_SECONDS"
    fi
}

warm_copy_rsync_stats() {
    (k logs -n "$NS" "$TARGET_POD" -c clickhouse-data-warm-copy 2>/dev/null || true) | awk '
        /Literal data:/ {
            count++
            latest = $0
            gsub(/[^0-9]/, "", latest)
        }
        END {
            if (count > 0) {
                print count, latest
            } else {
                print 0, 0
            }
        }'
}

quantity_to_bytes() {
    local quantity="$1"
    awk -v q="$quantity" '
        BEGIN {
            if (q !~ /^[0-9]+([.][0-9]+)?([KMGTPE]i?|[numkMGTPE])?$/) {
                exit 1
            }

            number = q
            sub(/([KMGTPE]i?|[numkMGTPE])$/, "", number)
            suffix = substr(q, length(number) + 1)

            multiplier = 1
            if (suffix == "Ki") multiplier = 1024
            else if (suffix == "Mi") multiplier = 1024^2
            else if (suffix == "Gi") multiplier = 1024^3
            else if (suffix == "Ti") multiplier = 1024^4
            else if (suffix == "Pi") multiplier = 1024^5
            else if (suffix == "Ei") multiplier = 1024^6
            else if (suffix == "k" || suffix == "K") multiplier = 1000
            else if (suffix == "M") multiplier = 1000^2
            else if (suffix == "G") multiplier = 1000^3
            else if (suffix == "T") multiplier = 1000^4
            else if (suffix == "P") multiplier = 1000^5
            else if (suffix == "E") multiplier = 1000^6
            else if (suffix == "m") multiplier = 0.001
            else if (suffix == "u") multiplier = 0.000001
            else if (suffix == "n") multiplier = 0.000000001

            bytes = number * multiplier
            printf "%.0f\n", bytes
        }'
}

auto_warm_copy_wait_timeout_seconds() {
    local source_bytes="$1"
    local estimated_seconds

    estimated_seconds=$(awk \
        -v bytes="$source_bytes" \
        -v bytes_per_hour="$WARM_COPY_RSYNC_BYTES_PER_HOUR" \
        'BEGIN { printf "%d\n", int(((bytes * 3600) / bytes_per_hour) + 0.999999) }')

    if (( estimated_seconds < WARM_COPY_TIMEOUT_MIN_SECONDS )); then
        estimated_seconds="$WARM_COPY_TIMEOUT_MIN_SECONDS"
    fi

    WARM_COPY_SOURCE_BYTES="$source_bytes"
    WARM_COPY_WAIT_TIMEOUT_SECONDS="$estimated_seconds"
}

derive_warm_copy_source_bytes() {
    local source_data_bytes_file="$WORKDIR/source-data-bytes"
    local source_bytes

    if [[ -s "$source_data_bytes_file" ]]; then
        source_bytes=$(tr -d '[:space:]' < "$source_data_bytes_file")
        if [[ "$source_bytes" =~ ^[0-9]+$ ]]; then
            WARM_COPY_SOURCE_SIZE_SOURCE="data-size"
            WARM_COPY_SOURCE_BYTES="$source_bytes"
            return
        fi
        echo "Ignoring invalid warm-copy source data size in $source_data_bytes_file: $source_bytes" >&2
    fi

    source_bytes=$(quantity_to_bytes "$OLD_SIZE") || {
        echo "unable to parse source PVC size '$OLD_SIZE' for warm-copy timeout calculation" >&2
        exit 28
    }
    WARM_COPY_SOURCE_SIZE_SOURCE="pvc-request"
    WARM_COPY_SOURCE_BYTES="$source_bytes"
}

wait_for_warm_copy_convergence() {
    section "Wait for warm-copy convergence"
    echo "Threshold: latest rsync Literal data <= $WARM_COPY_MAX_LITERAL_BYTES bytes"
    echo "Required consecutive passes: $WARM_COPY_STABLE_PASSES"
    echo "Timeout: ${WARM_COPY_WAIT_TIMEOUT_SECONDS}s"

    local waited=0
    local seen_passes=0
    local stable_passes=0
    local stats pass_count literal_bytes

    while (( waited <= WARM_COPY_WAIT_TIMEOUT_SECONDS )); do
        stats=$(warm_copy_rsync_stats)
        pass_count="${stats%% *}"
        literal_bytes="${stats#* }"

        if (( pass_count > seen_passes )); then
            seen_passes="$pass_count"
            if (( literal_bytes <= WARM_COPY_MAX_LITERAL_BYTES )); then
                stable_passes=$((stable_passes + 1))
                echo "Warm-copy pass $pass_count converged: Literal data=$literal_bytes bytes stable=$stable_passes/$WARM_COPY_STABLE_PASSES"
            else
                stable_passes=0
                echo "Warm-copy pass $pass_count still transferring too much: Literal data=$literal_bytes bytes stable=0/$WARM_COPY_STABLE_PASSES"
            fi

            if (( stable_passes >= WARM_COPY_STABLE_PASSES )); then
                echo "Warm-copy convergence threshold reached."
                return 0
            fi
        else
            echo "Waiting for next completed rsync pass: seen=$seen_passes stable=$stable_passes/$WARM_COPY_STABLE_PASSES waited=${waited}s"
        fi

        sleep "$WARM_COPY_POLL_SECONDS"
        waited=$((waited + WARM_COPY_POLL_SECONDS))
    done

    echo "Warm-copy did not converge within ${WARM_COPY_WAIT_TIMEOUT_SECONDS}s." >&2
    k logs -n "$NS" "$TARGET_POD" -c clickhouse-data-warm-copy --tail=120 >&2 || true
    exit 17
}

derive() {
    : "${NS:?NS is required}"
    : "${TARGET_STS:?TARGET_STS is required}"
    : "${NEW_SIZE:?NEW_SIZE is required}"

    CLICKHOUSE_DATA_PATH="${CLICKHOUSE_DATA_PATH:-/var/lib/clickhouse}"
    RSYNC_IMAGE="${RSYNC_IMAGE:-instrumentisto/rsync-ssh:latest}"
    TARGET_POD="${TARGET_STS}-0"

    CHI=$(k get sts -n "$NS" "$TARGET_STS" -o jsonpath='{.metadata.labels.clickhouse\.altinity\.com/chi}')
    if [[ -z "$CHI" ]]; then
        echo "unable to derive CHI from StatefulSet label clickhouse.altinity.com/chi" >&2
        exit 4
    fi
    MIGRATION_CHIT="${CHI}-volume-downscale"

    CLICKHOUSE_CONTAINER=$(k get sts -n "$NS" "$TARGET_STS" -o json | jq -r \
        --arg path "$CLICKHOUSE_DATA_PATH" \
        '.spec.template.spec.containers[]
         | select(any(.volumeMounts[]?; .mountPath == $path))
         | .name' | head -1)
    if [[ -z "$CLICKHOUSE_CONTAINER" || "$CLICKHOUSE_CONTAINER" == "null" ]]; then
        echo "unable to find container mounting $CLICKHOUSE_DATA_PATH" >&2
        exit 4
    fi

    DATA_VOLUME_NAME=$(k get sts -n "$NS" "$TARGET_STS" -o json | jq -r \
        --arg container "$CLICKHOUSE_CONTAINER" \
        --arg path "$CLICKHOUSE_DATA_PATH" \
        '.spec.template.spec.containers[]
         | select(.name == $container)
         | .volumeMounts[]
         | select(.mountPath == $path)
         | .name')
    if [[ -z "$DATA_VOLUME_NAME" || "$DATA_VOLUME_NAME" == "null" ]]; then
        echo "unable to derive data volume name" >&2
        exit 4
    fi

    OLD_PVC="${DATA_VOLUME_NAME}-${TARGET_POD}"
    OLD_PV=$(k get pvc -n "$NS" "$OLD_PVC" -o jsonpath='{.spec.volumeName}')
    OLD_SIZE=$(k get pvc -n "$NS" "$OLD_PVC" -o jsonpath='{.spec.resources.requests.storage}')
    STORAGE_CLASS=$(k get pvc -n "$NS" "$OLD_PVC" -o jsonpath='{.spec.storageClassName}')

    NEW_SIZE_SUFFIX=$(echo "$NEW_SIZE" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9-]//g')
    NEW_TEMP_PVC="${OLD_PVC}-new-${NEW_SIZE_SUFFIX}"
    OLD_AS_TEMP_PVC="${OLD_AS_TEMP_PVC:-${TARGET_POD}-old-data-retained}"

    if [[ -z "${ORIGINAL_POD_TEMPLATE_NAME:-}" ]]; then
        ORIGINAL_POD_TEMPLATE_NAME=$(k get sts -n "$NS" "$TARGET_STS" -o jsonpath='{.spec.template.metadata.name}')
    fi
    if [[ -z "$ORIGINAL_POD_TEMPLATE_NAME" ]]; then
        local count
        count=$(k get chi -n "$NS" "$CHI" -o json | jq '.spec.templates.podTemplates | length')
        if [[ "$count" == "1" ]]; then
            ORIGINAL_POD_TEMPLATE_NAME=$(k get chi -n "$NS" "$CHI" -o json | jq -r '.spec.templates.podTemplates[0].name')
        fi
    fi
    if [[ -z "$ORIGINAL_POD_TEMPLATE_NAME" || "$ORIGINAL_POD_TEMPLATE_NAME" == "null" ]]; then
        echo "unable to derive ORIGINAL_POD_TEMPLATE_NAME; set it explicitly and rerun" >&2
        exit 4
    fi
    ORIGINAL_VCT_NAME="$DATA_VOLUME_NAME"

    WORKDIR="${WORKDIR:-disk-shrink-backup/$TARGET_POD}"
    TARGET_SERVICE="${TARGET_SERVICE:-$TARGET_STS}"
    TRAFFIC_DRAIN_SECONDS="${TRAFFIC_DRAIN_SECONDS:-30}"
    WARM_COPY_MAX_LITERAL_BYTES="${WARM_COPY_MAX_LITERAL_BYTES:-10737418240}"
    WARM_COPY_STABLE_PASSES="${WARM_COPY_STABLE_PASSES:-1}"
    WARM_COPY_TIMEOUT_MIN_SECONDS="${WARM_COPY_TIMEOUT_MIN_SECONDS:-7200}"
    WARM_COPY_RSYNC_BYTES_PER_HOUR="${WARM_COPY_RSYNC_BYTES_PER_HOUR:-500000000000}"
    derive_warm_copy_source_bytes
    if [[ -z "${WARM_COPY_WAIT_TIMEOUT_SECONDS:-}" ]]; then
        auto_warm_copy_wait_timeout_seconds "$WARM_COPY_SOURCE_BYTES"
    fi
    WARM_COPY_POLL_SECONDS="${WARM_COPY_POLL_SECONDS:-60}"
}

print_derived() {
    cat <<EOF
NS=$NS
CHI=$CHI
TARGET_STS=$TARGET_STS
TARGET_POD=$TARGET_POD
TARGET_SERVICE=$TARGET_SERVICE
CLICKHOUSE_CONTAINER=$CLICKHOUSE_CONTAINER
DATA_VOLUME_NAME=$DATA_VOLUME_NAME
CLICKHOUSE_DATA_PATH=$CLICKHOUSE_DATA_PATH
OLD_PVC=$OLD_PVC
OLD_PV=$OLD_PV
OLD_SIZE=$OLD_SIZE
NEW_SIZE=$NEW_SIZE
NEW_TEMP_PVC=$NEW_TEMP_PVC
OLD_AS_TEMP_PVC=$OLD_AS_TEMP_PVC
STORAGE_CLASS=$STORAGE_CLASS
MIGRATION_CHIT=$MIGRATION_CHIT
ORIGINAL_POD_TEMPLATE_NAME=$ORIGINAL_POD_TEMPLATE_NAME
ORIGINAL_VCT_NAME=$ORIGINAL_VCT_NAME
WORKDIR=$WORKDIR
TRAFFIC_DRAIN_SECONDS=$TRAFFIC_DRAIN_SECONDS
WARM_COPY_MAX_LITERAL_BYTES=$WARM_COPY_MAX_LITERAL_BYTES
WARM_COPY_STABLE_PASSES=$WARM_COPY_STABLE_PASSES
WARM_COPY_SOURCE_BYTES=$WARM_COPY_SOURCE_BYTES
WARM_COPY_SOURCE_SIZE_SOURCE=$WARM_COPY_SOURCE_SIZE_SOURCE
WARM_COPY_RSYNC_BYTES_PER_HOUR=$WARM_COPY_RSYNC_BYTES_PER_HOUR
WARM_COPY_TIMEOUT_MIN_SECONDS=$WARM_COPY_TIMEOUT_MIN_SECONDS
WARM_COPY_WAIT_TIMEOUT_SECONDS=$WARM_COPY_WAIT_TIMEOUT_SECONDS
WARM_COPY_POLL_SECONDS=$WARM_COPY_POLL_SECONDS
EOF
}

phase_prepare() {
    phase_section "prepare"
    derive
    section "Derived inputs"
    print_derived

    assert_clean_migration_state

    section "Discovery"
    k get pod -n "$NS" "$TARGET_POD" -o jsonpath='{range .spec.containers[*]}{.name}{"\n"}{end}'
    k get pod -n "$NS" "$TARGET_POD" -o jsonpath='{range .spec.containers[?(@.name=="'"$CLICKHOUSE_CONTAINER"'")].volumeMounts[*]}{.name}{" => "}{.mountPath}{"\n"}{end}'
    k exec -n "$NS" "$TARGET_POD" -c "$CLICKHOUSE_CONTAINER" -- sh -c "du -shx '$CLICKHOUSE_DATA_PATH' || true"
    k exec -n "$NS" "$TARGET_POD" -c "$CLICKHOUSE_CONTAINER" -- sh -c "df -hT '$CLICKHOUSE_DATA_PATH' || true"
    k get pvc -n "$NS" "$OLD_PVC" -o wide
    k get pv "$OLD_PV" -o custom-columns=NAME:.metadata.name,RECLAIM:.spec.persistentVolumeReclaimPolicy,STATUS:.status.phase,CAP:.spec.capacity.storage,CLAIM:.spec.claimRef.name
    k get pv "$OLD_PV" -o jsonpath='{.spec.nodeAffinity}{"\n"}'

    mkdir -p "$WORKDIR"
    section "Capture source data size"
    if k exec -n "$NS" "$TARGET_POD" -c "$CLICKHOUSE_CONTAINER" -- \
        sh -c "du -sbx '$CLICKHOUSE_DATA_PATH' 2>/dev/null | awk '{print \$1}'" > "$WORKDIR/source-data-bytes"; then
        local source_data_bytes
        source_data_bytes=$(tr -d '[:space:]' < "$WORKDIR/source-data-bytes")
        if [[ "$source_data_bytes" =~ ^[0-9]+$ ]]; then
            echo "Source data bytes: $source_data_bytes"
        else
            rm -f "$WORKDIR/source-data-bytes"
            echo "Unable to capture a numeric source data size; warm-copy timeout will fall back to PVC requested size $OLD_SIZE." >&2
        fi
    else
        rm -f "$WORKDIR/source-data-bytes"
        echo "Unable to capture source data bytes; warm-copy timeout will fall back to PVC requested size $OLD_SIZE." >&2
    fi

    section "Local backups"
    k get chi -n "$NS" "$CHI" -o yaml > "$WORKDIR/chi.yaml"
    k get sts -n "$NS" "$TARGET_STS" -o yaml > "$WORKDIR/target-sts.yaml"
    k get pod -n "$NS" "$TARGET_POD" -o yaml > "$WORKDIR/target-pod.yaml"
    k get pvc -n "$NS" "$OLD_PVC" -o yaml > "$WORKDIR/old-pvc.yaml"
    k get pv "$OLD_PV" -o yaml > "$WORKDIR/old-pv.yaml"
    k get sts -n "$NS" "$TARGET_STS" -o json | jq '.spec.template.spec.volumes' > "$WORKDIR/base-volumes.json"
    echo "Backups written under $WORKDIR"

    confirm "Prepare will patch PV $OLD_PV reclaimPolicy to Retain and create temp PVC $NEW_TEMP_PVC with size $NEW_SIZE in namespace $NS." "PREPARE"

    section "Patch old PV reclaim policy"
    k patch pv "$OLD_PV" -p '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'

    section "Create temp PVC manifest"
    k get pvc -n "$NS" "$OLD_PVC" -o json | jq \
        --arg name "$NEW_TEMP_PVC" \
        --arg size "$NEW_SIZE" \
        '{
          apiVersion: "v1",
          kind: "PersistentVolumeClaim",
          metadata: {
            name: $name,
            namespace: .metadata.namespace,
            labels: ((.metadata.labels // {}) + {"maintenance.altinity.com/purpose": "clickhouse-volume-downscale"}),
            annotations: ((.metadata.annotations // {}) | with_entries(select(.key | startswith("spec.epc.altinity.com/"))))
          },
          spec: {
            accessModes: .spec.accessModes,
            storageClassName: .spec.storageClassName,
            resources: {requests: {storage: $size}}
          }
        }' > "$WORKDIR/01-new-temp-pvc.json"

    echo "Wrote $WORKDIR/01-new-temp-pvc.json"
    k apply -f "$WORKDIR/01-new-temp-pvc.json"
    k get pvc -n "$NS" "$NEW_TEMP_PVC"
}

phase_warm_copy() {
    phase_section "warm-copy"
    derive
    [[ -f "$WORKDIR/base-volumes.json" ]] || {
        echo "missing $WORKDIR/base-volumes.json; run prepare first" >&2
        exit 5
    }

    section "Generate warm-copy CHIT"
    jq -n \
        --arg name "$MIGRATION_CHIT" \
        --arg ns "$NS" \
        --arg chi "$CHI" \
        --arg podTemplate "$ORIGINAL_POD_TEMPLATE_NAME" \
        --arg image "$RSYNC_IMAGE" \
        --arg dataVolume "$DATA_VOLUME_NAME" \
        --arg dataPath "$CLICKHOUSE_DATA_PATH" \
        --arg newPVC "$NEW_TEMP_PVC" \
        --slurpfile volumes "$WORKDIR/base-volumes.json" \
        '{
          apiVersion: "clickhouse.altinity.com/v1",
          kind: "ClickHouseInstallationTemplate",
          metadata: {
            name: $name,
            namespace: $ns
          },
          spec: {
            templating: {
              policy: "auto",
              chiSelector: {"clickhouse.altinity.com/chi": $chi}
            },
            templates: {
              podTemplates: [{
                name: $podTemplate,
                spec: {
                  volumes: (
                    ($volumes[0] | map(select(
                      ((.name | startswith("chi-" + $chi + "-")) | not)
                      and (.name != "shrink-old-data")
                      and (.name != "shrink-new-data")
                    ))) + [{
                      name: "shrink-new-data",
                      persistentVolumeClaim: {claimName: $newPVC}
                    }]
                  ),
                  initContainers: [{
                    name: "clickhouse-data-warm-copy",
                    image: $image,
                    imagePullPolicy: "IfNotPresent",
                    restartPolicy: "Always",
                    securityContext: {runAsUser: 0, runAsGroup: 0},
                    command: ["/bin/sh", "-ec"],
                    args: ["
                      set -u
                      echo \"rsync version: $(rsync --version | head -1)\"
                      while true; do
                        date
                        rsync -aHAXS --numeric-ids --delete --one-file-system --whole-file --info=stats2 \"\($dataPath)/\" /opt/clickhouse/ || echo \"rsync exited non-zero, will retry\"
                        du -shx \"\($dataPath)\" /opt/clickhouse || true
                        sleep 60
                      done
                    "],
                    resources: {
                      requests: {cpu: "200m", memory: "512Mi"},
                      limits: {cpu: "2", memory: "2Gi"}
                    },
                    volumeMounts: [
                      {name: $dataVolume, mountPath: $dataPath, readOnly: true},
                      {name: "shrink-new-data", mountPath: "/opt/clickhouse"}
                    ]
                  }]
                }
              }]
            }
          }
        }' > "$WORKDIR/02-chit-warm-copy.json"

    echo "Wrote $WORKDIR/02-chit-warm-copy.json"

    section "Apply warm-copy CHIT"
    k apply -f "$WORKDIR/02-chit-warm-copy.json"
    k patch chi -n "$NS" "$CHI" --type=merge -p '{"spec":{"taskID":"disk-shrink-warm-copy-'"$(date +%s)"'"}}'

    section "Verify warm-copy state"
    echo "Waiting for pod $TARGET_POD to exist"
    for _ in $(seq 1 120); do
        if k get pod -n "$NS" "$TARGET_POD" >/dev/null 2>&1; then
            break
        fi
        sleep 5
    done
    if ! k get pod -n "$NS" "$TARGET_POD" >/dev/null 2>&1; then
        echo "pod $TARGET_POD was not recreated; inspect StatefulSet events before continuing" >&2
        exit 9
    fi
    k get pod -n "$NS" "$TARGET_POD" -o jsonpath='containers={range .spec.containers[*]}{.name}{" "}{end}{"\n"}initContainers={range .spec.initContainers[*]}{.name}({.restartPolicy}){" "}{end}{"\n"}volumes={range .spec.volumes[*]}{.name}{" "}{end}{"\n"}'
    k get pvc -n "$NS" "$NEW_TEMP_PVC"
    echo
    echo "Warm-copy logs:"
    echo "  kubectl logs -n $NS $TARGET_POD -c clickhouse-data-warm-copy --tail=20"

    wait_for_warm_copy_convergence
    k logs -n "$NS" "$TARGET_POD" -c clickhouse-data-warm-copy --tail=80 || true
    confirm "Confirm warm-copy has converged enough to stop ClickHouse and run the final offline sync. Type WARM_COPY_READY." "WARM_COPY_READY"
}

patch_chi_requested_size_for_swap() {
    section "Patch CHI requested volume size"
    local vct_index
    vct_index=$(k get chi -n "$NS" "$CHI" -o json | jq \
        --arg name "$ORIGINAL_VCT_NAME" \
        '.spec.templates.volumeClaimTemplates | map(.name == $name) | index(true)')
    if [[ "$vct_index" == "null" ]]; then
        echo "unable to find CHI VCT $ORIGINAL_VCT_NAME" >&2
        exit 6
    fi

    echo "Target CHI: $CHI"
    echo "VolumeClaimTemplate: $ORIGINAL_VCT_NAME"
    echo "Old requested size: $OLD_SIZE"
    echo "New requested size: $NEW_SIZE"

    k patch chi -n "$NS" "$CHI" --type=json -p='[
      {"op":"replace","path":"/spec/templates/volumeClaimTemplates/'"$vct_index"'/spec/resources/requests/storage","value":"'"$NEW_SIZE"'"}
    ]'

    section "Kubernetes desired size"
    k get chi -n "$NS" "$CHI" -o json | jq \
        --arg name "$ORIGINAL_VCT_NAME" \
        '.spec.templates.volumeClaimTemplates[] | select(.name == $name) | .spec.resources.requests.storage'
    k get pvc -n "$NS" "$NEW_TEMP_PVC" -o jsonpath='{.spec.resources.requests.storage}{" "}{.status.capacity.storage}{"\n"}'

    confirm "Confirm ACM, CHI VolumeClaimTemplate, and temp PVC all show $NEW_SIZE before the PVC swap." "ACM_READY"
}

phase_swap() {
    phase_section "swap"
    derive
    patch_chi_requested_size_for_swap

    section "Pre-swap PVC binding checkpoint"
    NEW_PV=$(wait_for_pvc_volume "$NEW_TEMP_PVC")
    echo "Using new PV $NEW_PV from temp PVC $NEW_TEMP_PVC"

    section "PVC swap plan"
    echo "Namespace: $NS"
    echo "CHI: $CHI"
    echo "StatefulSet scaled to zero: $TARGET_STS"
    echo "Pod that must disappear: $TARGET_POD"
    echo "Delete PVCs: $OLD_PVC and $NEW_TEMP_PVC"
    echo "Recreate $OLD_PVC bound to new PV: $NEW_PV at $NEW_SIZE"
    echo "Recreate $OLD_AS_TEMP_PVC bound to old PV: $OLD_PV at $OLD_SIZE"
    if k get pvc -n "$NS" "$OLD_AS_TEMP_PVC" >/dev/null 2>&1; then
        echo "retained old-data PVC $OLD_AS_TEMP_PVC already exists; aborting before scale-down/PVC deletion" >&2
        k get pvc -n "$NS" "$OLD_AS_TEMP_PVC" -o wide >&2
        exit 22
    fi
    confirm "Destructive step. This will suspend the CHI, scale down the StatefulSet, delete PVC objects, remove PV claimRefs, and recreate swapped PVCs." "DELETE_PVCS"

    section "Suspend and scale down"
    k patch chi -n "$NS" "$CHI" --type=merge -p '{"spec":{"suspend":"yes","taskID":"disk-shrink-suspend-'"$(date +%s)"'"}}'
    sleep 5
    k get chi -n "$NS" "$CHI" -o jsonpath='suspend={.spec.suspend} status={.status.status}{"\n"}'

    remove_ready_markers_before_scale_down

    k scale sts -n "$NS" "$TARGET_STS" --replicas=0
    echo "Waiting for pod $TARGET_POD to disappear"
    for _ in $(seq 1 120); do
        if ! k get pod -n "$NS" "$TARGET_POD" >/dev/null 2>&1; then
            break
        fi
        sleep 5
    done
    if k get pod -n "$NS" "$TARGET_POD" >/dev/null 2>&1; then
        echo "pod still exists; aborting before PVC deletion" >&2
        exit 7
    fi

    section "Retain and backup PV/PVC objects"
    local new_pv_reclaim_policy_file
    new_pv_reclaim_policy_file="$WORKDIR/new-pv-original-reclaim-policy"
    if [[ ! -s "$new_pv_reclaim_policy_file" ]]; then
        k get pv "$NEW_PV" -o jsonpath='{.spec.persistentVolumeReclaimPolicy}' > "$new_pv_reclaim_policy_file"
        echo >> "$new_pv_reclaim_policy_file"
    fi
    echo "Original new PV reclaim policy: $(tr -d '[:space:]' < "$new_pv_reclaim_policy_file")"

    k patch pv "$OLD_PV" -p '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'
    k patch pv "$NEW_PV" -p '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'

    k get pvc -n "$NS" "$OLD_PVC" -o yaml > "$WORKDIR/old-pvc-before-swap.yaml"
    k get pvc -n "$NS" "$OLD_PVC" -o json > "$WORKDIR/old-pvc-before-swap.json"
    k get pvc -n "$NS" "$NEW_TEMP_PVC" -o yaml > "$WORKDIR/new-temp-pvc-before-swap.yaml"
    k get pv "$OLD_PV" -o yaml > "$WORKDIR/old-pv-before-swap.yaml"
    k get pv "$NEW_PV" -o yaml > "$WORKDIR/new-pv-before-swap.yaml"
    echo "Swap backups written under $WORKDIR"

    section "Delete PVCs and release PVs"
    k delete pvc -n "$NS" "$OLD_PVC" "$NEW_TEMP_PVC"
    k patch pv "$OLD_PV" --type=json -p='[{"op":"remove","path":"/spec/claimRef"}]'
    k patch pv "$NEW_PV" --type=json -p='[{"op":"remove","path":"/spec/claimRef"}]'
    k get pv "$OLD_PV" "$NEW_PV" -o custom-columns=NAME:.metadata.name,STATUS:.status.phase,CAP:.spec.capacity.storage,RECLAIM:.spec.persistentVolumeReclaimPolicy

    section "Create swapped PVC manifest"
    jq -n \
        --arg ns "$NS" \
        --arg oldPVC "$OLD_PVC" \
        --arg oldAsTempPVC "$OLD_AS_TEMP_PVC" \
        --arg storageClass "$STORAGE_CLASS" \
        --arg newPV "$NEW_PV" \
        --arg oldPV "$OLD_PV" \
        --arg newSize "$NEW_SIZE" \
        --arg oldSize "$OLD_SIZE" \
        --slurpfile oldPVCJson "$WORKDIR/old-pvc-before-swap.json" \
        '{
          apiVersion: "v1",
          kind: "List",
          items: [
            {
              apiVersion: "v1",
              kind: "PersistentVolumeClaim",
              metadata: {
                name: $oldPVC,
                namespace: $ns,
                labels: {"maintenance.altinity.com/purpose": "clickhouse-volume-downscale"},
                annotations: (($oldPVCJson[0].metadata.annotations // {}) | with_entries(select(.key | startswith("spec.epc.altinity.com/"))))
              },
              spec: {
                accessModes: $oldPVCJson[0].spec.accessModes,
                storageClassName: $storageClass,
                volumeName: $newPV,
                resources: {requests: {storage: $newSize}}
              }
            },
            {
              apiVersion: "v1",
              kind: "PersistentVolumeClaim",
              metadata: {
                name: $oldAsTempPVC,
                namespace: $ns,
                labels: {"maintenance.altinity.com/purpose": "clickhouse-volume-downscale"}
              },
              spec: {
                accessModes: $oldPVCJson[0].spec.accessModes,
                storageClassName: $storageClass,
                volumeName: $oldPV,
                resources: {requests: {storage: $oldSize}}
              }
            }
          ]
        }' > "$WORKDIR/03-pvc-swap.json"

    echo "Wrote $WORKDIR/03-pvc-swap.json"
    k apply -f "$WORKDIR/03-pvc-swap.json"
    k get pvc -n "$NS" "$OLD_PVC" "$OLD_AS_TEMP_PVC"
}

apply_final_copy_chit() {
    derive
    [[ -f "$WORKDIR/base-volumes.json" ]] || {
        echo "missing $WORKDIR/base-volumes.json; run prepare first" >&2
        exit 5
    }

    section "Generate final-copy CHIT"
    jq -n \
        --arg name "$MIGRATION_CHIT" \
        --arg ns "$NS" \
        --arg chi "$CHI" \
        --arg podTemplate "$ORIGINAL_POD_TEMPLATE_NAME" \
        --arg image "$RSYNC_IMAGE" \
        --arg dataVolume "$DATA_VOLUME_NAME" \
        --arg dataPath "$CLICKHOUSE_DATA_PATH" \
        --arg oldPVC "$OLD_AS_TEMP_PVC" \
        --slurpfile volumes "$WORKDIR/base-volumes.json" \
        '{
          apiVersion: "clickhouse.altinity.com/v1",
          kind: "ClickHouseInstallationTemplate",
          metadata: {
            name: $name,
            namespace: $ns
          },
          spec: {
            templating: {
              policy: "auto",
              chiSelector: {"clickhouse.altinity.com/chi": $chi}
            },
            templates: {
              podTemplates: [{
                name: $podTemplate,
                spec: {
                  volumes: (
                    ($volumes[0] | map(select(
                      ((.name | startswith("chi-" + $chi + "-")) | not)
                      and (.name != "shrink-old-data")
                      and (.name != "shrink-new-data")
                    ))) + [{
                      name: "shrink-old-data",
                      persistentVolumeClaim: {claimName: $oldPVC}
                    }]
                  ),
                  initContainers: [{
                    name: "clickhouse-data-final-sync",
                    image: $image,
                    imagePullPolicy: "IfNotPresent",
                    securityContext: {runAsUser: 0, runAsGroup: 0},
                    command: ["/bin/sh", "-ec"],
                    args: ["
                      set -eux
                      SRC=/mnt/old-clickhouse
                      DST=\"\($dataPath)\"
                      test -d \"$SRC\" && test -d \"$DST\"
                      test -e \"$SRC/status\" -o -d \"$SRC/store\" -o -d \"$SRC/data\"
                      rsync -aHAXS --numeric-ids --delete --one-file-system --whole-file --info=stats2 \"$SRC/\" \"$DST/\"
                      sync
                      echo \"FINAL SYNC COMPLETE\"
                    "],
                    resources: {
                      requests: {cpu: "500m", memory: "1Gi"},
                      limits: {cpu: "4", memory: "4Gi"}
                    },
                    volumeMounts: [
                      {name: "shrink-old-data", mountPath: "/mnt/old-clickhouse", readOnly: true},
                      {name: $dataVolume, mountPath: $dataPath}
                    ]
                  }]
                }
              }]
            }
          }
        }' > "$WORKDIR/04-chit-final-copy.json"

    echo "Wrote $WORKDIR/04-chit-final-copy.json"

    section "Apply final-copy CHIT"
    k apply -f "$WORKDIR/04-chit-final-copy.json"
}

phase_final_copy() {
    phase_section "final-copy"
    apply_final_copy_chit

    section "Pre-final-copy ACM checkpoint"
    k get chi -n "$NS" "$CHI" -o json | jq \
        --arg name "$ORIGINAL_VCT_NAME" \
        '.spec.templates.volumeClaimTemplates[] | select(.name == $name) | .spec.resources.requests.storage'
    k get pvc -n "$NS" "$OLD_PVC" -o jsonpath='{.spec.resources.requests.storage}{" "}{.status.capacity.storage}{"\n"}'

    confirm "Type FINAL_COPY to unsuspend CHI, run offline final copy, and wait for the pod to become Ready." "FINAL_COPY"
    section "Resume CHI and run final copy"
    k patch chi -n "$NS" "$CHI" --type=merge -p '{"spec":{"suspend":"no","taskID":"disk-shrink-final-copy-'"$(date +%s)"'"}}'
    wait_for_final_copy_and_ready
}

phase_bind_old_pv() {
    phase_section "bind-old-pv"
    derive

    local old_pvc_backup="$WORKDIR/old-pvc-before-swap.json"
    [[ -f "$old_pvc_backup" ]] || {
        echo "missing $old_pvc_backup; this recovery phase needs swap backups" >&2
        exit 23
    }

    local original_old_pv original_old_size
    original_old_pv=$(jq -r '.spec.volumeName // ""' "$old_pvc_backup")
    original_old_size=$(jq -r '.spec.resources.requests.storage // ""' "$old_pvc_backup")
    if [[ -z "$original_old_pv" || "$original_old_pv" == "null" || -z "$original_old_size" || "$original_old_size" == "null" ]]; then
        echo "unable to read original old PV/size from $old_pvc_backup" >&2
        exit 24
    fi

    section "Retained old PV binding plan"
    echo "Original old PV: $original_old_pv"
    echo "Original old size: $original_old_size"
    echo "Retained old-data PVC: $OLD_AS_TEMP_PVC"

    if k get pvc -n "$NS" "$OLD_AS_TEMP_PVC" >/dev/null 2>&1; then
        local existing_pv
        existing_pv=$(k get pvc -n "$NS" "$OLD_AS_TEMP_PVC" -o jsonpath='{.spec.volumeName}')
        if [[ "$existing_pv" == "$original_old_pv" ]]; then
            echo "Retained old-data PVC $OLD_AS_TEMP_PVC is already bound to $original_old_pv."
            return
        fi

        echo "retained old-data PVC $OLD_AS_TEMP_PVC already exists but is bound to $existing_pv, not $original_old_pv" >&2
        echo "Set OLD_AS_TEMP_PVC to a unique name and rerun bind-old-pv, then use the same OLD_AS_TEMP_PVC for final-copy/cleanup." >&2
        exit 25
    fi

    local phase reclaim
    phase=$(k get pv "$original_old_pv" -o jsonpath='{.status.phase}')
    reclaim=$(k get pv "$original_old_pv" -o jsonpath='{.spec.persistentVolumeReclaimPolicy}')
    if [[ "$phase" != "Available" ]]; then
        echo "original old PV $original_old_pv is $phase, expected Available before binding a retained PVC" >&2
        k get pv "$original_old_pv" -o wide >&2
        exit 26
    fi
    if [[ "$reclaim" != "Retain" ]]; then
        echo "original old PV $original_old_pv reclaimPolicy is $reclaim, expected Retain" >&2
        exit 27
    fi

    section "Create retained old-data PVC manifest"
    jq -n \
        --arg ns "$NS" \
        --arg oldAsTempPVC "$OLD_AS_TEMP_PVC" \
        --arg oldPV "$original_old_pv" \
        --arg oldSize "$original_old_size" \
        --slurpfile oldPVCJson "$old_pvc_backup" \
        '{
          apiVersion: "v1",
          kind: "PersistentVolumeClaim",
          metadata: {
            name: $oldAsTempPVC,
            namespace: $ns,
            labels: {"maintenance.altinity.com/purpose": "clickhouse-volume-downscale"},
            annotations: (($oldPVCJson[0].metadata.annotations // {}) | with_entries(select(.key | startswith("spec.epc.altinity.com/"))))
          },
          spec: {
            accessModes: $oldPVCJson[0].spec.accessModes,
            storageClassName: $oldPVCJson[0].spec.storageClassName,
            volumeName: $oldPV,
            resources: {requests: {storage: $oldSize}}
          }
        }' > "$WORKDIR/03b-retained-old-pvc.json"

    echo "Wrote $WORKDIR/03b-retained-old-pvc.json"
    k apply -f "$WORKDIR/03b-retained-old-pvc.json"
    k get pvc -n "$NS" "$OLD_AS_TEMP_PVC" -o wide
}

phase_verify() {
    phase_section "verify"
    derive
    NEW_PV=$(k get pvc -n "$NS" "$OLD_PVC" -o jsonpath='{.spec.volumeName}')
    section "Kubernetes verification"
    k get pvc -n "$NS" "$OLD_PVC"
    k get pv "$NEW_PV" -o jsonpath='{.spec.capacity.storage}{"\n"}'
    k exec -n "$NS" "$TARGET_POD" -c "$CLICKHOUSE_CONTAINER" -- df -hT "$CLICKHOUSE_DATA_PATH"
}

phase_cleanup() {
    phase_section "cleanup"
    derive
    confirm "Delete migration CHIT $MIGRATION_CHIT and trigger cleanup reconcile? Type CLEANUP." "CLEANUP"
    section "Cleanup migration template"
    local cleanup_task_id
    cleanup_task_id="disk-shrink-cleanup-$(date +%s)"
    delete_migration_chit_and_wait
    k patch chi -n "$NS" "$CHI" --type=merge -p '{"spec":{"taskID":"'"$cleanup_task_id"'"}}'
    wait_for_chi_reconcile "$cleanup_task_id"
    section "Post-cleanup pod shape"
    k get pod -n "$NS" "$TARGET_POD" -o jsonpath='containers={.spec.containers[*].name}{"\n"}initContainers={.spec.initContainers[*].name}{"\n"}volumes={.spec.volumes[*].name}{"\n"}'

    section "Restore current PV reclaim policy"
    local current_pv current_storage_class new_pv_reclaim_policy_file new_pv_original_reclaim_policy
    current_pv=$(k get pvc -n "$NS" "$OLD_PVC" -o jsonpath='{.spec.volumeName}')
    current_storage_class=$(k get pvc -n "$NS" "$OLD_PVC" -o jsonpath='{.spec.storageClassName}')
    new_pv_reclaim_policy_file="$WORKDIR/new-pv-original-reclaim-policy"
    if [[ -s "$new_pv_reclaim_policy_file" ]]; then
        new_pv_original_reclaim_policy=$(tr -d '[:space:]' < "$new_pv_reclaim_policy_file")
    else
        new_pv_original_reclaim_policy=$(k get storageclass "$current_storage_class" -o jsonpath='{.reclaimPolicy}' 2>/dev/null || true)
        if [[ -n "$new_pv_original_reclaim_policy" ]]; then
            echo "No saved new PV reclaim policy found at $new_pv_reclaim_policy_file; using StorageClass $current_storage_class reclaim policy: $new_pv_original_reclaim_policy"
        fi
    fi

    if [[ -n "$current_pv" ]]; then
        if [[ -n "$new_pv_original_reclaim_policy" ]]; then
            k patch pv "$current_pv" -p '{"spec":{"persistentVolumeReclaimPolicy":"'"$new_pv_original_reclaim_policy"'"}}'
            k get pv "$current_pv" -o custom-columns=NAME:.metadata.name,CLAIM:.spec.claimRef.name,RECLAIM:.spec.persistentVolumeReclaimPolicy
        else
            echo "Unable to determine original reclaim policy; leaving PV $current_pv unchanged." >&2
        fi
    else
        echo "Current PVC $OLD_PVC does not have a bound PV; reclaim policy restore skipped." >&2
    fi

    section "Retained old-data cleanup"
    if ! k get pvc -n "$NS" "$OLD_AS_TEMP_PVC" >/dev/null 2>&1; then
        echo "Retained old-data PVC $OLD_AS_TEMP_PVC is already absent."
        return
    fi

    confirm_retained_old_data_cleanup
    if [[ "$RETAINED_OLD_DATA_CLEANUP_CHOICE" == "SKIP" ]]; then
        echo "Kept retained old-data PVC $OLD_AS_TEMP_PVC."
        return
    fi

    local retained_old_pv
    retained_old_pv=$(k get pvc -n "$NS" "$OLD_AS_TEMP_PVC" -o jsonpath='{.spec.volumeName}')
    if [[ -n "$retained_old_pv" ]]; then
        k patch pv "$retained_old_pv" -p '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}'
    fi
    k delete pvc -n "$NS" "$OLD_AS_TEMP_PVC"
    if [[ -n "$retained_old_pv" ]]; then
        if k get pv "$retained_old_pv" >/dev/null 2>&1; then
            k get pv "$retained_old_pv"
        else
            echo "PV $retained_old_pv is absent after deleting PVC $OLD_AS_TEMP_PVC."
        fi
    fi
}

main() {
    require_tool kubectl
    require_tool jq
    AUTO_CONFIRM="${AUTO_CONFIRM:-no}"

    local phase=""
    while (($#)); do
        case "$1" in
            --auto-confirm)
                AUTO_CONFIRM=yes
                ;;
            -h|--help|help)
                usage
                return
                ;;
            *)
                if [[ -z "$phase" ]]; then
                    phase="$1"
                else
                    echo "unexpected argument: $1" >&2
                    usage
                    exit 1
                fi
                ;;
        esac
        shift
    done

    case "$phase" in
        derive)
            phase_section "derive"
            derive
            print_derived
            ;;
        prepare)
            phase_prepare
            ;;
        warm-copy)
            phase_warm_copy
            ;;
        pre-swap)
            echo "Phase 'pre-swap' was merged into 'swap'; run phase 'swap' to perform the CHI/ACM checkpoint and PVC swap together." >&2
            exit 1
            ;;
        swap)
            phase_swap
            ;;
        final-copy)
            phase_final_copy
            ;;
        final-sync|resume)
            echo "Phase '$phase' is deprecated; running merged phase 'final-copy'." >&2
            phase_final_copy
            ;;
        bind-old-pv)
            phase_bind_old_pv
            ;;
        verify)
            phase_verify
            ;;
        cleanup)
            phase_cleanup
            ;;
        all)
            phase_prepare
            phase_warm_copy
            phase_swap
            phase_final_copy
            phase_verify
            phase_cleanup
            ;;
        "")
            usage
            ;;
        *)
            echo "unknown phase: $phase" >&2
            usage
            exit 1
            ;;
    esac
}

main "$@"
