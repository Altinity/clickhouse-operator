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

See [security_hardening.md](security_hardening.md) for per-knob semantics, the orthogonal-axes posture table, FIPS coercion details, image-policy details, and the externally-managed-token (Secret-backed) GitOps pattern.

[clickhouse-operator-install-bundle.yaml]: ../deploy/operator/clickhouse-operator-install-bundle.yaml
[70-chop-config.yaml]: ./chi-examples/70-chop-config.yaml
