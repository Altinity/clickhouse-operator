# `clickhouse-operator` configuration

## Introduction

`clickhouse-operator` can be configured in a variety of ways. Configuration consists of the following main parts:
1. Operator settings -- operator settings control behaviour of operator itself.
1. ClickHouse common configuration files - ready-to-use XML files with sections of ClickHouse configuration **as-is**.
Common configuration typically contains general ClickHouse configuration sections, such as network listen endpoints, logger options, etc. Those are exposed via config maps.
1. ClickHouse user configuration files - ready-to-use XML files with sections of ClickHouse configuration **as-is**
User configuration typically contains ClickHouse configuration sections with user accounts specifications. Those are exposed via config maps as well.
1. `ClickHouseOperatorConfiguration` resource.
1. `ClickHouseInstallationTemplate`s. Operator provides functionality to specify parts of `ClickHouseInstallation` manifest as a set of templates, which would be used in all `ClickHouseInstallation`s.   

## Operator settings

Operator settings are initialized in-order from 3 sources:
* `/etc/clickhouse-operator/config.yaml`
* etc-clickhouse-operator-files configmap (also a part of default [clickhouse-operator-install-bundle.yaml][clickhouse-operator-install-bundle.yaml]
* `ClickHouseOperatorConfiguration` resource. See [example][70-chop-config.yaml] for details.

Next sources merge with the previous ones. Currently the operator does not self-reconcile its own configuration: changes to `etc-clickhouse-operator-files` or `ClickHouseOperatorConfiguration` are read only at startup and require an operator restart to apply.

`config.yaml` has following settings:

```yaml
################################################
##
## Watch Namespaces Section
##
################################################

# List of namespaces where clickhouse-operator watches for events.
# Concurrently running operators should watch on different namespaces
# watchNamespaces:
#  - dev
#  - info
#  - onemore

################################################
##
## Additional Configuration Files Section
##
################################################

# Path to folder where ClickHouse configuration files common for all instances within CHI are located.
chCommonConfigsPath: config.d

# Path to folder where ClickHouse configuration files unique for each instance (host) within CHI are located.
chHostConfigsPath: conf.d

# Path to folder where ClickHouse configuration files with users settings are located.
# Files are common for all instances within CHI
chUsersConfigsPath: users.d

# Path to folder where ClickHouseInstallation .yaml manifests are located.
# Manifests are applied in sorted alpha-numeric order
chiTemplatesPath: templates.d

################################################
##
## Cluster Create/Update/Delete Objects Section
##
################################################

# How many seconds to wait for created/updated StatefulSet to be Ready
statefulSetUpdateTimeout: 600

# How many seconds to wait between checks for created/updated StatefulSet status
statefulSetUpdatePollPeriod: 10

# What to do in case created StatefulSet is not in Ready after `statefulSetUpdateTimeout` seconds
# Possible options:
# 1. abort - do nothing, just break the process and wait for admin
# 2. delete - delete newly created problematic StatefulSet
onStatefulSetCreateFailureAction: delete

# What to do in case updated StatefulSet is not in Ready after `statefulSetUpdateTimeout` seconds
# Possible options:
# 1. abort - do nothing, just break the process and wait for admin
# 2. rollback - delete Pod and rollback StatefulSet to previous Generation.
# Pod would be recreated by StatefulSet based on rollback-ed configuration
onStatefulSetUpdateFailureAction: rollback

################################################
##
## ClickHouse Settings Section
##
################################################

# Default values for ClickHouse user configuration
# 1. user/profile - string
# 2. user/quota - string
# 3. user/networks/ip - multiple strings
# 4. user/password - string
chConfigUserDefaultProfile: default
chConfigUserDefaultQuota: default
chConfigUserDefaultNetworksIP:
  - "::/0"
chConfigUserDefaultPassword: "default"

################################################
##
## Operator's access to ClickHouse instances
##
################################################

# ClickHouse credentials (username, password and port) to be used by operator to connect to ClickHouse instances for:
# 1. Metrics requests
# 2. Schema maintenance
# 3. DROP DNS CACHE
# User with such credentials credentials can be specified in additional ClickHouse .xml config files,
# located in `chUsersConfigsPath` folder
chUsername: clickhouse_operator
chPassword: clickhouse_operator_password
chPort: 8123
```

When the operator connects over HTTPS, it verifies the ClickHouse server certificate
with the CA from `clickhouse.access.rootCA` (inline PEM) or `clickhouse.access.rootCASecretRef`
(a Secret in the operator's own namespace; key defaults to `ca.crt` then `tls.crt`, inline
`rootCA` wins). Verification is enforced when TLS hardening is opted in —
`security.clickhouse.tls.verify: Strict`, or a non-empty `minVersion`/`serverName`; otherwise
the CA is loaded but verification stays relaxed for backward compatibility.
See the [operator config example](chi-examples/70-chop-config.yaml).

## ClickHouse Installation settings

Operator deploys ClickHouse clusters with different defaults, that can be configured in a flexible way. 

### Default ClickHouse configuration files

Default ClickHouse configuration files can be found in the following config maps, that are mounted to corresponding configuration folders of ClickHouse pods:
* etc-clickhouse-operator-confd-files
* etc-clickhouse-operator-configd-files
* etc-clickhouse-operator-usersd-files

Config maps are initialized in default [clickhouse-operator-install-bundle.yaml][clickhouse-operator-install-bundle.yaml].

### Defaults for ClickHouseInstallation

Defaults for ClickHouseInstallation can be provided by `ClickHouseInstallationTemplate` it a variety of ways:
* etc-clickhouse-operator-templatesd-files configmap
* `ClickHouseInstallationTemplate` resources.

`ClickHouseInstallationTemplate` has the same structure as `ClickHouseInstallation`, but all parts and fields are optional. Templates are included into an installation with 'useTemplates' syntax. For example, one can define a template for ClickHouse pod:

```apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallationTemplate"

metadata:
  name: clickhouse-stable

spec:
  templates:
    podTemplates:
      - name: default
        spec:
          containers:
            - name: clickhouse-pod
              image: clickhouse/clickhouse-server:24.8
```

Template needs to be deployed to some namespace, and later on used in the installation:
```
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
...
spec:
  useTemplates:
    - name: clickhouse-stable
...
```

#### Applying Changes from ClickHouseInstallationTemplates

Changes applied to a ClickHouseInstallationTemaplte do not automatically trigger a reconcile of the ClickHouseInstallations using the template. This is by design and intended to preserve user control and prevent undesirable rollouts to ClickHouseInstallations. 

To apply the changes to ClickHouseInstallations, update the spec.taskID:

```
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
...
spec:
  taskID: "randomly-generated-string"
...
```

> Note, ClickHouse settings applied to the ClickHouse server through `spec.configuration.settings` in a ClickHouseInstallationTemplate will not trigger a server restart whether or not the setting requires a server restart to be applied. To apply the settings and restart the server, you should also set `spec.restart` to `'RollingUpdate'`. RollingUpdate should be used sparingly. It is typically removed after usage to prevent unecessary restarts:

```
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
...
spec:
  restart: "RollingUpdate"
...
```

### Keeper Coordination Settings

The operator can be configured to control how it interacts with referenced ClickHouseKeeper (CHK) resources during reconciliation.

```yaml
spec:
  reconcile:
    coordination:
      keeper:
        # How long the operator waits for a referenced CHK to become ready
        # before aborting CHI reconcile. In seconds. Default: 120.
        readyTimeout: 120
        # Reaction when a referenced CHK resource changes:
        #   none (default) — do nothing
        #   reconcile — trigger CHI reconcile when CHK completes
        onKeeperResourceUpdate: none
```

| Setting | Default | Description |
|---|---|---|
| `readyTimeout` | `120` | Seconds to wait for CHK pods to become Running before aborting |
| `onKeeperResourceUpdate` | `none` | `none` — ignore CHK changes; `reconcile` — auto-reconcile dependent CHIs when CHK completes |

See [Keeper Reference](keeper_reference.md) for details on how CHI references CHK resources.

### Replicated Host Catch-Up Gate

The operator can optionally block a rolling host reconcile until a recreated replicated
ClickHouse host catches up to a bounded replication baseline. This is an operator
rolling gate, not a readiness probe. It is disabled by default.

This is especially useful for local or direct-attached storage deployments, including
NVMe-backed Local PVs, where a recreated pod may start with an empty or replaced disk
and must rebuild replicated data from peer replicas before the operator rolls the next
host.

Three changes to the surrounding catch-up behaviour are **not** gated on `catchUp.enabled`, so they
apply even with this gate off:

1. A host that lost its storage volume is forced to catch up before it is returned to service:
   its `status.hostsWithReplicaCaughtUp` entry is invalidated and the wait runs. This overrides
   `reconcile.host.wait.replicas.all` and `.new` — a host whose disk is gone waits even when both
   are `no`, because a marker describing a disk that no longer exists is not evidence of anything.
   Note the wait itself is not time-capped, so a replica that cannot converge will stall that
   CHI's reconcile; it is visible as `InProgress` with periodic replication-lag log lines, and
   editing the CHI cancels the stalled pass.
2. A reconcile cancelled while a host is still catching up no longer records the marker — a
   cancelled wait is not evidence that the replica caught up.
3. The catch-up wait now runs before the host is restored to normal priority in `remote_servers`,
   rather than after, so a host that was excluded stays deprioritized for the duration of the wait
   instead of receiving distributed queries while still behind.

The marker path only polls the local host's `MAX(absolute_delay)` from `system.replicas` before
writing `status.hostsWithReplicaCaughtUp`, which is weak for recreated-host recovery
because the metric is limited to replicated objects already loaded and visible on that
local server. During recreated-host recovery, asynchronous database/table loading may
not have exposed all replicated objects on the local host yet, and a local delay metric
cannot discover replicated objects that exist on peers or issue a ClickHouse sync
barrier for their known parts. The catch-up gate adds those checks before the operator
advances to the next host.

```yaml
spec:
  reconcile:
    host:
      wait:
        replicas:
          catchUp:
            enabled: "false"
            timeout: 900
            onTimeout: "abort"
            health:
              pollInterval: 10
              successThreshold: 6
```

| Setting | Default | Description |
|---|---|---|
| `enabled` | `"false"` | Enables the replicated-host catch-up gate. Existing replica-delay behavior is unchanged when disabled. |
| `timeout` | `900` | Per-host gate budget in seconds; omit it to take the default. The CRD requires `>= 1`, and the config-file path falls back to the default for anything `<= 0`, so the gate is never unbounded - otherwise `onTimeout` could never fire. |
| `onTimeout` | `"abort"` | `abort` stops reconcile on the gate deadline. `proceed` advances without writing the caught-up marker, so a later reconcile can try again. Accepted in either case, like the other enum-valued options. |
| `health.pollInterval` | `10` | Seconds between post-sync health checks; omit it to take the default. CRD requires `>= 1`. |
| `health.successThreshold` | `6` | Consecutive healthy checks required after sync before the caught-up marker is written; omit it to take the default. CRD requires `>= 1`. |

When enabled, the gate waits for asynchronous database loading when ClickHouse exposes
`system.asynchronous_loader`, discovers replicated objects from the peer replicas of the same shard, syncs
`Replicated` databases with `SYSTEM SYNC DATABASE REPLICA`, syncs replicated tables
with `SYSTEM SYNC REPLICA ... LIGHTWEIGHT` (full `SYSTEM SYNC REPLICA` when the ClickHouse version is older than 23.4 or cannot be determined), and then requires a stable health window.
Health is based on `system.replicas`: `is_readonly = 0`, `is_session_expired = 0`, and
`absolute_delay <= reconcile.host.wait.replicas.delay`.

The `LIGHTWEIGHT` baseline is the time when the sync command runs. It waits for the
relevant part-acquisition work known at that point; it does not require
`system.replication_queue` to become empty and does not block forever on unrelated
merges, mutations, or new ingest that arrives after the sync command.

Hard failures always abort regardless of `onTimeout`: query or connection failure,
parent reconcile context cancellation, failed/canceled async load jobs, readonly
replicas, and expired Keeper sessions. The caught-up marker is written only after real
success or when peer discovery confirms that there are no replicated objects to sync.

Manual local-PV/data-loss validation:

1. Create a CHI with a replicated shard and `catchUp.enabled: "true"`.
2. Wait for the current hosts to become caught up and confirm
   `status.hostsWithReplicaCaughtUp` contains the host FQDNs.
3. Simulate storage loss for one host, for example by removing the local PV/PVC data
   in a test environment.
4. Reconcile the CHI and confirm the operator removes the stale caught-up marker for
   the recreated host.
5. Confirm the recreated host runs the catch-up gate and the next host in the shard does
   not advance while the recreated host is still behind.
6. Allow replication to catch up and confirm the recreated host receives the
   caught-up marker again, then the next host proceeds.

## Security

The `security:` block at the chopconf top level (sibling of `clickhouse:`) holds operator-wide hardening defaults across three orthogonal axes: transport hardening (`security.policy`), FIPS cryptographic-module enforcement (`security.fips.enforced`), and workload supply-chain gating (`security.images.policy`). Per-component sub-blocks under it cover ClickHouse-client TLS, ZooKeeper-client TLS, Kubernetes-client TLS, and the operator↔metrics-exporter IPC channel.

```yaml
spec:
  security:
    clickhouse:
      tls:
        verify: ""        # "Strict" | "None" | "" (inherit / legacy permissive)
        minVersion: ""    # "1.2" | "1.3" | ""
        serverName: ""
        rootCA: ""
        rootCASecretRef: { name: "", key: "" }
    zookeeper:
      tls:
        verify: ""
        minVersion: ""
    kubernetes:
      tls:
        verify: ""        # gate against kubeconfig Insecure
        minVersion: ""
    ipc:
      mode: "Plain"       # "Plain" | "Secure" (loopback + X-CHOP-Token)
      bindHost: ""
      tokenPath: ""
    policy: Permissive    # "Permissive" (default) | "Enforced" — TLS-hardening master switch
    fips:
      enforced: false     # true Fatals at startup if binary lacks GOFIPS140; also coerces TLS knobs
    images:
      policy: Permissive  # "Permissive" | "FIPSRequired" — workload image-tag gate
```

Sub-blocks at a glance:

| Block | Scope | Summary |
|---|---|---|
| `security.clickhouse.tls.{verify,minVersion,serverName,rootCA,rootCASecretRef}` | per-component, 3-level inheritance | Outbound TLS for operator→ClickHouse connections (schemer, health, metrics helpers). |
| `security.zookeeper.tls.{verify,minVersion}` | per-component, 3-level inheritance | Verification + MinVersion for the ZK/Keeper client (cert/key/CA already wired separately). |
| `security.kubernetes.tls.{verify,minVersion}` | operator-wide (chopconf only) | `verify=Strict` is a load-time gate against the kubeconfig's `Insecure` flag (rejects insecure kubeconfigs at startup). `minVersion` is declared + coerced under FIPS but not yet wired into the `rest.Config` transport — declared for shape symmetry; see `pkg/apis/clickhouse.altinity.com/v1/type_security.go` field doc. |
| `security.ipc.{mode,bindHost,tokenPath}` | operator-wide | Hardens the `/chi` REST channel between operator and metrics-exporter sidecar. |
| `security.policy` | operator-wide | TLS-hardening master switch: `Permissive` (default, preserves 0.27.0 behavior) or `Enforced` (coerce every TLS/IPC knob to Strict, reject FIPS-incompatible CRs). Transport hardening only — no longer Fatals on non-FIPS-built binaries. |
| `security.fips.enforced` | operator-wide | FIPS cryptographic-module gate: `true` Fatals at startup unless the binary was built with `GOFIPS140` and `crypto/fips140` reports Enabled. Also triggers the same TLS coercions as `policy: Enforced`. Orthogonal to `security.policy`. |
| `security.images.policy` | operator-wide | Workload supply-chain gate: `FIPSRequired` refuses CRs whose CH/Keeper images lack `fips` in their tag and aborts running CRs whose `SELECT version()` lacks `fips` (orthogonal to `security.policy` and `security.fips`). |

The per-component TLS knobs `clickhouse.tls` and `zookeeper.tls` use 3-level inheritance — chopconf → CHI `spec.configuration.clusters[].security` → cluster — with empty/absent meaning "inherit from the next level up". `kubernetes.tls`, `security.ipc`, `security.policy`, `security.fips`, and `security.images` are operator-process-scoped and chopconf-only (no CHI override).

See [security_hardening.md](security_hardening.md) for per-knob semantics, the `security.policy: Enforced` master switch, the orthogonal-axes posture table, and the externally-managed-token (Secret-backed) GitOps pattern. FIPS-specific controls (`security.fips.enforced` cryptographic-module gate, `security.images.policy: FIPSRequired` workload supply-chain gate, FIPS coercion details, ACVP responder, FIPS build and release evidence) are documented in [security_hardening_fips.md](security_hardening_fips.md).

[clickhouse-operator-install-bundle.yaml]: ../deploy/operator/clickhouse-operator-install-bundle.yaml
[70-chop-config.yaml]: ./chi-examples/70-chop-config.yaml
