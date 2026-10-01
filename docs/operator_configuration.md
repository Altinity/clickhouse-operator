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
* `ClickHouseOperatorConfiguration` resource. See [example][70-chop-config.yaml] for a short
  starting point, or [the full list of options][99-chopconf-max.yaml] for every setting the
  resource accepts.

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
See the [operator config example](chi-examples/70-chop-config.yaml) for a short starting point,
or [99-clickhouseoperatorconfiguration-max.yaml](chi-examples/99-clickhouseoperatorconfiguration-max.yaml)
for an annotated list of every available option.

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

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallationTemplate"

metadata:
  name: clickhouse-stable

spec:
  templates:
    podTemplates:
      - name: default
        spec:
          containers:
            - name: clickhouse
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

#### Template precedence

A `ClickHouseInstallation` is applied on top of every template it uses, so when both set the same field the installation's own value wins and template values fill what the installation leaves unset. Templates applied with `templating.policy: auto` come before the ones listed in `useTemplates`, and a later template wins over an earlier one the same way. A template's `clusters` are never merged - the installation's replace them - so cluster-, shard- and host-level settings are the installation's. The operator's own configuration rules (`clickhouse.addons.rules` in its configuration) are the bottom layer, beneath every template, and like any default lose to a template or the installation that sets the same key. The default rules supply the grants the operator's own user needs to manage the installation, and server settings such as `display_secrets_in_show_and_select`. A template or the installation that turns off `display_secrets_in_show_and_select`, or the `clickhouse_operator` profile's `format_display_secrets_in_show_and_select`, has the operator copy definitions to a new or recovered host with `'[HIDDEN]'` in place of their credentials, so the copied tables, dictionaries and databases that carry credentials do not work there. On the operator's own user, a key written with the `{clickhouseOperatorUser}` placeholder - as the rules write the grants - overrides the same key written with the user's name, whichever layer sets either, since the placeholder is expanded after the merge.

Pod, service and volume-claim templates merge with Kubernetes strategic-merge semantics: containers, env and volumes pair by name, volume mounts by mount path and ports by number and protocol, in the order the templates listed them, with the installation's own additions appended; a port that restates only some of its fields must restate a non-TCP `protocol` too, since an omitted protocol means TCP. A template's container therefore extends the installation's only under the same name - a template's `clickhouse-pod` next to the installation's `clickhouse` is two containers.

The reconcile aborts with `InvalidPodTemplate` before anything is changed when a pod template has a container without a name or an image, or two containers of one name, and when a pod template merged from several layers starts a duplicate ClickHouse server in another container: one whose image has the ClickHouse container's image name - whatever its registry, organization or tag - or is `clickhouse-server`, and whose `command` and `args` do not run something other than the server.

A template therefore sets defaults, not guardrails: a value it must enforce - an image, a `default` user's `networks`, `suspend` or `reconcile.statefulSet.recreate.onDataLoss` - holds only on installations that leave it unset. A configuration rule's value likewise holds only where neither a template nor the installation sets its key, though a grant spelled with the operator user's name, not the placeholder, still loses to the rules'.

##### Upgrading from 0.27

Before 0.28 each field followed one of two orders, and which one decides what an upgrade changes.

- **The earlier layer won** - the operator's configuration rules beat templates and the installation, a template beat the installation, and an earlier template beat a later one - for same-named pod, host and volume-claim templates; `configuration.users`, `profiles`, `quotas`, `settings` and `files`; `spec.suspend`, `restart`, `taskID` and `namespaceDomainPattern`; `reconcile.statefulSet`, `reconcile.host` (whose hooks are the union of both) and `reconcile.macros`; `defaults.replicasUseFQDN`; and the `zookeeper` `keeper` reference and `use_compression`. A later layer's lists in a same-named service template - its ports, `externalIPs` and the like - were dropped.
- **The later layer won** - the installation beat its templates - for `metadata.labels` and `annotations`; `security`; `stop`, `troubleshoot` and `templating`; a service template's `type` and its other scalars and metadata; `zookeeper` `root`, `identity` and timeouts; `reconcile.policy`, `configMapPropagationTimeout`, `cleanup` and `runtime`; and `defaults.distributedDDL`, `storageManagement` and `templates`. An env name both sides set on a container was listed twice, the installation's taking effect; on an init container the earlier layer's value won, as for the rest of the pod template.

The first group now takes the installation's value, a later template's over an earlier one's, and a template's over a configuration rule's; the second group keeps its values, though an env name both sides set on a container is now listed once, which restarts the pods once.

Two layers' containers, volumes, volume mounts and container ports - a template's and the installation's, or two templates' - were folded into one whenever they sat at the same position. They now pair by name, mounts by mount path and ports by number and protocol, so elements that differ are now two. Each such change restarts the pods once, with two exceptions. A template's container that was folded into a differently named one of the installation's - `clickhouse-pod` into `clickhouse` - is now a container of its own: without an image of its own, or starting the ClickHouse server, it aborts the reconcile with `InvalidPodTemplate` before anything is changed. The pods keep running as the previous operator deployed them, but until the installation reconciles the operator cannot query it - its user may connect only from the previous operator pod's address - so the installation's metrics stop. Naming the installation's container like the template's, the name its pods run under today, clears the abort; it also keeps that container as it is, provided the installation's container sets nothing the template's also sets - such as the image - since the installation's value now wins: drop those fields, or set them to the template's. An env name both set restarts the pods once either way, being listed once instead of twice. Renaming the template's container instead renames it in every installation using the template, and reaches each only on its next reconcile - see below - or, under `template.chi.policy: ReadOnStart`, only once the operator has restarted. And a change that newly mounts a volume-claim template changes the StatefulSet's immutable volume claims, so the StatefulSet is recreated - or, with `reconcile.statefulSet.recreate.onUpdateFailure: abort`, the reconcile aborts.

To upgrade without surprises, before upgrading fix each installation that sets a first-group key its templates or the operator's configuration rules also set - such as `display_secrets_in_show_and_select`: delete the installation's value, so the value in effect today keeps applying, or set the installation's value to it. Fix the same way each template that sets a key the rules also set. Where a template sets a key an earlier template also sets, leave the template alone - editing it changes every installation using it, including those where no earlier template sets that key - and set the value in effect today on each installation that uses both. Give a template's container and the installation's container it is meant to extend the same name. To take the upgrade one installation at a time, set `spec.suspend: "yes"` on the installations beforehand and clear it on each in turn.

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

See [security_hardening.md](security_hardening.md) for per-knob semantics, the `security.policy: Enforced` master switch, the orthogonal-axes posture table, and the externally-managed-token (Secret-backed) GitOps pattern. FIPS-specific controls (`security.fips.enforced` cryptographic-module gate, `security.images.policy: FIPSRequired` workload supply-chain gate, FIPS coercion details, ACVP responder, FIPS build and release evidence) are documented in [security_hardening_fips.md](security_hardening_fips.md).

[clickhouse-operator-install-bundle.yaml]: ../deploy/operator/clickhouse-operator-install-bundle.yaml
[70-chop-config.yaml]: ./chi-examples/70-chop-config.yaml
[99-chopconf-max.yaml]: ./chi-examples/99-clickhouseoperatorconfiguration-max.yaml
