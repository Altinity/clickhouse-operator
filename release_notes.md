## Release 0.27.4
### Security
* **Removed the `k8s_secret_` and `k8s_secret_env_` user-settings syntax.** These prefixes took a value of the form `namespace/secret/key` and were resolved with the operator's own ServiceAccount, which on a cluster-scoped install can read Secrets everywhere — so a `ClickHouseInstallation` in one namespace could read a Secret out of **any** namespace, and `k8s_secret_password*` additionally inlined the plaintext into the generated users ConfigMap. The whole syntax is gone, along with the address parser branch that accepted an explicit namespace.

  **Action required before upgrading.** A CHI that still uses any `k8s_secret_*` / `k8s_secret_env_*` field is now **rejected**: reconcile aborts with reason `RemovedSecretRefSyntax` and the CHI is not reconciled until the manifest is migrated. This is deliberate — ignoring the field instead would leave the account with no password of its own, and the normalizer would then hand it `ClickHouse.Config.User.Default.Password`, which ships as the literal string `"default"`. A Secret-protected account would silently become reachable with a documented credential.

  Migrate to `valueFrom`/`secretKeyRef`, which resolves only within the CHI's own namespace:

  Before (removed):

  ```yaml
  users:
    user1/k8s_secret_password: clickhouse-secret/pwduser1
  ```

  After:

  ```yaml
  users:
    user1/password:
      valueFrom:
        secretKeyRef:
          name: clickhouse-secret
          key: pwduser1
  ```

  Note `valueFrom`/`secretKeyRef` always passes the value via an environment variable (`from_env=...` in the generated XML) and does not hash it. Where you relied on `k8s_secret_password` being hashed into `password_sha256_hex` for you, store the already-hashed value in the Secret and reference it from `password_sha256_hex`. See [docs/security_hardening.md](docs/security_hardening.md).

* **Bumped the Go toolchain to address stdlib CVEs in the operator and metrics-exporter images.** The `go` directive in `go.mod` — the single source the Dockerfiles and CI derive `GO_VERSION` from — is bumped `1.26.5` → `1.26.6`. No API or behavior changes.

  `govulncheck` reports five standard-library vulnerabilities reachable from operator/exporter code on a 1.26.5 build, and none on 1.26.6:
  * **GO-2026-6090** — `crypto/tls`, unbounded post-handshake KeyUpdate. Reached from the ZooKeeper/Keeper TLS client and the metrics REST server.
  * **GO-2026-6089** — `net/http`, `ReadHeaderTimeout` not applied during the unencrypted HTTP/2 check.
  * **GO-2026-6218** — `net/url`, quadratic complexity in `resolvePath`.
  * **GO-2026-5972** — `encoding/asn1`, unbounded recursion depth. Reached when parsing an operator-supplied root CA.
  * **GO-2026-5026** — `golang.org/x/net/idna` as bundled into the standard library.

  The FIPS module is unaffected: `GOFIPS140=v1.0.0` selects a frozen snapshot that is byte-identical between the 1.26.5 and 1.26.6 toolchains, so FIPS evidence and ACVP results do not change.

  Not covered by this bump: **GO-2026-5158** in `go.opentelemetry.io/otel` (v1.43.0, fixed in v1.44.0). It is imported but not called — `govulncheck`'s symbol scan does not flag it — though SCA/image scanners that match on module versions will continue to report it until the dependency is bumped.


### Behavior Changes
* **Helm CRD-install hook no longer uses a Docker Hub Bitnami image** ([PR #2065](https://github.com/Altinity/clickhouse-operator/pull/2065)). `crdHook.image.repository` changes `bitnami/kubectl` → `registry.k8s.io/kubectl`, and `crdHook.image.tag` changes from a floating `latest` to a pinned `v1.36.3`. Bitnami restructured their Docker Hub catalog in September 2025: every versioned tag was removed from `docker.io/bitnami` (semver now exists only in the frozen `bitnamilegacy` namespace), leaving a single floating `latest` that cannot be pinned.

  **If you mirror images into a private registry or install air-gapped, add `registry.k8s.io/kubectl:v1.36.3` to your mirror before upgrading.** The hook runs at `pre-install`/`pre-upgrade`, so an unpullable image blocks the Helm release until `--timeout` expires and then fails it. Point `crdHook.image.repository`/`.tag` at your mirror, or set `crdHook.enabled: false` and apply the CRDs manually. Affects Helm installs only.

  Two related changes come with it: the hook now execs `kubectl` directly rather than wrapping it in `/bin/sh`, because `registry.k8s.io/kubectl` is distroless and ships no shell; and it runs as root (uid 0) where the Bitnami image ran as uid 1001 — set `crdHook.containerSecurityContext` if your admission policies require a non-root uid.

### Fixed
* **Oversized CRD ConfigMap broke GitOps installs of the Helm chart.** The CRD-install hook packed three CRDs into a single ConfigMap that had grown to 297,849 bytes, past the 262,144-byte `metadata.annotations` ceiling that client-side `kubectl apply` needs for its `last-applied-configuration`. `helm install` and `helm upgrade` were unaffected — Helm POSTs hook objects and writes no such annotation — so this surfaced only for **ArgoCD with default client-side apply and for `helm template | kubectl apply`**, which failed with `ConfigMap "…-crds-1" is invalid: metadata.annotations: Too long`. Present in 0.27.2 and 0.27.3. Each CRD now gets its own ConfigMap (largest 120,151 bytes) projected into one directory for the hook, and `dev/test_helm_chart.sh` asserts the one-CRD-per-ConfigMap pairing so a future CRD cannot silently reintroduce the grouping.

## Release 0.27.3
### Security
* **Bumped dependencies to address CVEs in the operator and metrics-exporter images.** An image scan flagged four CVEs; all are addressed by patch/minor bumps with no API or behavior changes:
  * **CVE-2026-39822** (HIGH) and **CVE-2026-42505** (MEDIUM) — Go standard library. Fixed by building on Go **1.26.5** (the `go` directive in `go.mod` — the single source the Dockerfiles and CI derive `GO_VERSION` from — is bumped `1.26.4` → `1.26.5`).
  * **CVE-2026-46600** — `golang.org/x/net` bumped `v0.55.0` → `v0.56.0`.
  * **CVE-2026-56852** — `golang.org/x/text` bumped `v0.37.0` → `v0.39.0`.

  Reachability note: `govulncheck` confirms the stdlib `crypto/tls` and `x/text` issues are reachable from operator/exporter code; the `x/net` and stdlib `os` issues are present-in-image but not reachable. All four are bumped for image hygiene regardless. The FIPS module (`GOFIPS140=v1.0.0`) is pinned independently of the Go toolchain and is unaffected.

## Release 0.27.2
### New Features
* **Keeper split client/peer Services** ([#1982](https://github.com/Altinity/clickhouse-operator/issues/1982)). Each `ClickHouseKeeper` host now exposes two headless Services instead of one:
  * a **peer** Service (unchanged name) with `publishNotReadyAddresses: true` — used for intra-Keeper Raft traffic and the StatefulSet pod DNS, so Raft peers can reach each other before pods are `Ready` and bootstrap quorum;
  * a **client** Service (`<peer>-client`) with `publishNotReadyAddresses: false` — used by ClickHouse, so clients only ever resolve `Ready` Keeper nodes and never connect to a node still starting up.

  A user-supplied `replicaServiceTemplate` still produces a single Service (the split applies only to the operator's default Services).
* **Configurable CHK reconcile concurrency** ([#2032](https://github.com/Altinity/clickhouse-operator/issues/2032)). A new `reconcile.runtime.reconcileCHKsThreadsNumber` option in `ClickHouseOperatorConfiguration` sets the `ClickHouseKeeper` controller's maximum concurrent reconciles, mirroring the existing `reconcileCHIsThreadsNumber` for `ClickHouseInstallation`. Defaults to `1` (serial reconciliation — unchanged behavior); raise it to reconcile many CHK resources in parallel when running at scale. See [PR #2033](https://github.com/Altinity/clickhouse-operator/pull/2033).
* **Helm `watchNamespaces` value** ([#1919](https://github.com/Altinity/clickhouse-operator/issues/1919)). The Helm chart gains a top-level `watchNamespaces` value that wires directly into the operator config's `watch.namespaces.include`, so the set of watched namespaces can be set at install time (`--set` / `values.yaml`) instead of hand-patching the generated ConfigMap. Empty by default (operator watches its own namespace — unchanged); use `[".*"]` to watch all namespaces. Helm installs only. See [PR #2007](https://github.com/Altinity/clickhouse-operator/pull/2007).

### Behavior Changes
* **OLM install modes now honor the OperatorGroup** ([#2008](https://github.com/Altinity/clickhouse-operator/issues/2008)). The OperatorHub bundle previously hard-wired `WATCH_NAMESPACE` to the operator's own namespace, so `SingleNamespace`, `MultiNamespace`, and `AllNamespaces` installs were all silently scoped to the operator's namespace only. The CSV now reads `metadata.annotations['olm.targetNamespaces']`, so each advertised install mode watches the namespaces the OperatorGroup actually selects. **On upgrade, an OLM install configured for Single/Multi/AllNamespaces will start watching the intended namespaces for the first time** — if you relied on the old own-namespace-only behavior, scope the OperatorGroup (or a `ClickHouseOperatorConfiguration` `watch.namespaces`) accordingly. Affects OLM/OperatorHub installs only; plain manifest and Helm installs are unchanged.
* **One-time ClickHouse rolling restart on upgrade for CHI→CHK keeper references** ([#1982](https://github.com/Altinity/clickhouse-operator/issues/1982)). A `ClickHouseInstallation` that references a `ClickHouseKeeper` via a `keeper:` ref (the default `serviceType: Replicas`) now resolves to the new ready-only Keeper **client** Service. The first reconcile after upgrade rewrites the CHI's `<zookeeper>` endpoints from `chk-…` to `chk-…-client`, which the operator treats as a configuration change requiring a restart — so **every ClickHouse pod of an affected CHI restarts once**. This is a one-time event; the resolved endpoints are stable afterwards. No action required.
* **Backward-incompatible config rename** in `ClickHouseOperatorConfiguration`: `reconcile.recovery.from.{aborted,completed}` → `reconcile.recovery.onStatus.{aborted,completed}`. The `from` grouping level is removed; the per-status scopes and their action keys (`onPodReady`/`onPodNotReady`/`onPodNotReadyThreshold`) are unchanged. The obsolete `from` key is silently ignored on load. **If you set `reconcile.recovery.from.aborted.onPodReady: none` on 0.27.0/0.27.1 to disable Aborted auto-recovery, re-apply it as `reconcile.recovery.onStatus.aborted.onPodReady: none`** — otherwise the default (`retry`) silently re-enables it. The `completed` scope (sustained-NotReady host recovery) is new in 0.27.2 and off by default. See [docs/operator_upgrade.md](docs/operator_upgrade.md).

### Fixed
* **Scaled-up replica no longer stays permanently broken after `remote_servers` is published** ([#2013](https://github.com/Altinity/clickhouse-operator/issues/2013)). When a replica was added to an existing multi-host cluster, it could start ClickHouse before the operator published the full `remote_servers`; cluster-dependent objects (`Distributed`, `DICTIONARY`, refreshable `MATERIALIZED VIEW`) then failed their async startup load with `CLUSTER_DOESNT_EXIST` and — because ClickHouse never re-runs terminal loader jobs after a `SYSTEM RELOAD CONFIG` — stayed broken until a manual pod restart. The operator now detects this on the newly-added host (it recorded a `CLUSTER_DOESNT_EXIST` error) and restarts that host once, after the complete `remote_servers` is published, so those objects re-load against the real cluster. The restart is scoped to a newly-added host that actually hit the failure — pre-existing replicas, fresh installs, single-host clusters, and replicas with no cluster-dependent objects (e.g. a replica still catching up replication) are never restarted.
* **Config values containing `&`, `<`, or `>` no longer break generated ClickHouse config** ([#1578](https://github.com/Altinity/clickhouse-operator/issues/1578)). Setting values such as a generated password that contained XML-reserved characters were injected verbatim into the rendered `users.xml`/`config.xml`, producing malformed XML that prevented ClickHouse from starting. Element text is now XML-escaped; embedded pre-rendered XML fragments (e.g. Keeper `raft_configuration`) are still emitted verbatim. See [PR #2034](https://github.com/Altinity/clickhouse-operator/pull/2034).
* **Lower metrics-exporter memory footprint for excluded metrics** ([PR #2028](https://github.com/Altinity/clickhouse-operator/pull/2028)). The metrics collector fetched every ClickHouse system metric into an in-memory buffer and applied the `excludeRegexp` filter only just before emitting to Prometheus, so excluded (e.g. high-cardinality per-CPU) metrics still inflated peak memory and could OOM the exporter sidecar. Excluded metric names are now dropped during the SQL row scan, before entering the buffer. Prometheus output is unchanged.

### Security
* **Operator metrics port no longer exposes `/debug/pprof`**. The operator's metrics HTTP listener (default `:9999`) served Go's shared `http.DefaultServeMux`, onto which `net/http/pprof` is transitively registered through a controller-runtime dependency. That unintentionally exposed the `/debug/pprof/*` endpoints — including the CPU `profile` and `trace` handlers — to anything able to reach the operator's metrics Service, enabling unauthenticated profiling/DoS and heap/goroutine dumps. The listener now uses a dedicated mux that serves only the metrics endpoint; `/metrics` is unchanged.

## Release 0.27.1
### Behavior Changes
* `StatefulSet` create returning `AlreadyExists` (e.g. due to a stale informer cache or a prior failed delete) is no longer silently treated as a successful create. The reconciler now propagates the recreate sentinel so the host correctly enters the recreate path. See https://github.com/Altinity/clickhouse-operator/pull/1993.

**Note**: Pre-existing CHIs that were sitting in the previously-swallowed state (typically observable as a host stuck at `Replicas=0` while the CHI was reported as reconciled) will now correctly transition to `Aborted` status on first reconcile after upgrade. This is the intended behavior - the prior code masked a real failure. To recover, re-apply the CHI spec to trigger an informer re-reconcile.

## Release 0.20.3
## What's Changed
* Use alpine base image instead of UBI
* Fix error handling when table already exists in ZooKeeper for new ClickHouse versions

**Full Changelog**: https://github.com/Altinity/clickhouse-operator/compare/release-0.20.2...release-0.20.3

## Release 0.20.2
### What's Changed
* Added 'hostsCompleted' to the CHI status and events
* Changed some 'default' profile settings:
    * Enabled 'do_not_merge_across_partitions_select_final'
    * Set 'load_balancing' to 'nearest_hostname'
    * Set niceness ('os_thread_priority') to 2.
* Improved stability of metric-exporter when some ClickHouse nodes are responding slowly
* Changed sequence of LB service creation to avoid a situation when service exists with no available endpoints
* Added 'secure' flag at cluster level for enabling distributed queries over TLS
* Addressed CVEs in dependent packages

**Note**: datatype for 'secure' flag has been changed from boolean to String (accepting, 'true', 'yes', '1' etc.)

## Release 0.20.1
### What's Changed
* Improvements to Grafana dashboards by @Slach
* Generate operator helm chart by @slamdev in https://github.com/Altinity/clickhouse-operator/pull/1049
* Added cluster to PodDesruptionBudget selector.  Closes #996. **Note**: PDB requires Kubernetes version 1.21 or above.
* Added status.hostsCompleted to track reconcile progress
* Do not restart CHI with restart attribute when operator is restarted
* Address https://nvd.nist.gov/vuln/detail/CVE-2022-27664. Closes #1039
* Fixed RBAC permissions for secrets. Closes #1051

### New Contributors
* @slamdev made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/1049

**Full Changelog**: https://github.com/Altinity/clickhouse-operator/compare/release-0.20.0...release-0.20.1

## Release 0.20.0
### What's Changed
* Add support for setting secure communication between clickhouse instances. (Inspried by @chancez in https://github.com/Altinity/clickhouse-operator/pull/938). See examples:
    * [auto secret](https://github.com/Altinity/clickhouse-operator/blob/0.20.0/docs/chi-examples/21-secure-cluster-secret-01-auto.yaml)
    * [plain text secret](https://github.com/Altinity/clickhouse-operator/blob/0.20.0/docs/chi-examples/21-secure-cluster-secret-02-plaintext.yaml)
    * [k8s secret reference](https://github.com/Altinity/clickhouse-operator/blob/0.20.0/docs/chi-examples/21-secure-cluster-secret-03-secret-ref.yaml)
* Operator managed PV provisioning. (Inspired by @chhan-coupang in https://github.com/Altinity/clickhouse-operator/pull/947). Allows to re-scale volumes w/o a downtime. In order to enable, use:
```
  defaults:
    storageManagement:
      provisioner: Operator
```
* Use secret for clickhouse_operator credentials as default
* Improve zookeeper scaleout by @Slach in https://github.com/Altinity/clickhouse-operator/pull/984
* add loadaverage + network + disk panel to clickhouse dashboard by @Slach in https://github.com/Altinity/clickhouse-operator/pull/1015
* comment out chopusername check by @SuzyWangIBMer in https://github.com/Altinity/clickhouse-operator/pull/1009
* Reduced a size of status field
* Improved logging

### Fixed
* Fixed a bug with cluster definitions being reset when CHI has been updated via k8s API POST method.
* Fixed a problem of metrics exporter not being able to scrape any managed cluster when one of clickhouse nodes is slow
* Fix zookeeper advanced setup with empty dir by @luluz66 in https://github.com/Altinity/clickhouse-operator/pull/993
* Delete duplicate return by @Abirdcfly in https://github.com/Altinity/clickhouse-operator/pull/987
* Fixes #991 - fix watch namespace regex by @mcgrawia in https://github.com/Altinity/clickhouse-operator/pull/992
* Address CVE-2022-32149 by @bkuschel in https://github.com/Altinity/clickhouse-operator/pull/1035

### New Contributors
* @Abirdcfly made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/987
* @mcgrawia made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/992
* @chhan-coupang made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/947
* @luluz66 made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/993
* @chancez made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/938
* @antip00 made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/1034

**Full Changelog**: https://github.com/Altinity/clickhouse-operator/compare/release-0.19.2...release-0.20.0

## Release 0.19.3
### What's Changed
* Fixed a possible loose of cluster state when operator has been restarted in the middle of reconcile.
* Backport of https://github.com/Altinity/clickhouse-operator/pull/1009
* add loadaverage + network + disk panel to clickhouse dashboard by @Slach in https://github.com/Altinity/clickhouse-operator/pull/1015

**NOTE:** There is a change in ClickHouseInstallation custom resource type, so make sure you deploy full installation manifest when upgrading.

**Full Changelog**: https://github.com/Altinity/clickhouse-operator/compare/release-0.19.2...release-0.19.3

## Release 0.19.2
## What's Changed
* Fixed a bug in 0.19.0 and 0.19.1 versions when operator could be rebuilding remote-servers.xml from scratch after restart. When it happened, distributed queries might behave unexpectedly.

**Full Changelog**: https://github.com/Altinity/clickhouse-operator/compare/release-0.19.1...release-0.19.2

## Release 0.19.1
### What's Changed
* Fixed MVs in Atomic databases support for ClickHouse versions 22.6 and above. Closes https://github.com/Altinity/clickhouse-operator/issues/989
* Fixed tables with '{uuid}' macro in Atomic databases for ClickHouse versions 22.6 and above.
* Fixed watch namespaces regexp by https://github.com/Altinity/clickhouse-operator/pull/992. Closes https://github.com/Altinity/clickhouse-operator/issues/991
* Added "clickhouse.altinity.com/ready: yes|no" annotations for replica services for the use by external load balancer.
* Updated podDisruptionBudget API version

Full changelog: https://github.com/Altinity/clickhouse-operator/compare/release-0.19.0...release-0.19.1

## Release 0.19.0
### What's Changed
* Now 'clickhouse-operator' user is restricted to an IP address of the pod where clickhouse-operator is running. It can not be used from anywhere else.
* In addition to hostRegexp, 'default' user is restricted to IP addresses of pods of the CHI. This fixes connectivity issues between shards in GKE.
* ClickHouse 'interserver_http_host' is set to the service name and matches 'remote_servers'. It can be changed to full names with 'replicasUseFQDN' setting if needed. This fixes connectivity issues between replicas in GKE.
* Changed the way schema is propagated to new shards. It is currently controlled by [two cluster level settings](https://github.com/Altinity/clickhouse-operator/blob/7f5ee13e9e2185e245c12e9821db188e542aa98e/deploy/operator/parts/crd.yaml#L668):
```
  schemaPolicy:
    replica: None | All (default)
    shard: None | All (default) | DistributedTablesOnly
```
Previously, when adding shards only distributed tables and dependent objects were created (DistributedTablesOnly).
* Added support for adding custom CA for operator connection to ClickHouse by @SuzyWangIBMer in https://github.com/Altinity/clickhouse-operator/pull/930
* Fixed support of '{uuid}' macro for Atomic databases. Closes https://github.com/Altinity/clickhouse-operator/issues/857
* Fixed a bug when replica sometimes was not removed from ZooKeeper when scaling down the cluster
* Set container-log on ubi instead of busybox  by @SuzyWangIBMer in https://github.com/Altinity/clickhouse-operator/pull/940
* upgrade vertamedia-clickhouse-grafana to 2.5.0 in dashboard and grafana 7.5.16 by @Slach in https://github.com/Altinity/clickhouse-operator/pull/951
* Improve grafana dashboards by @Slach in https://github.com/Altinity/clickhouse-operator/pull/952
* Improve grafana-operator + prometheus-operator by @Slach in https://github.com/Altinity/clickhouse-operator/pull/953

### New Contributors
* @roshanths made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/936
* @SuzyWangIBMer made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/940
* @meob made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/949

**Full Changelog**: https://github.com/Altinity/clickhouse-operator/compare/0.18.5...release-0.19.0

## Release 0.18.5
### What's Changed
* Dependencies were updated to address CVE-2022-21698 and CVE-2021-38561
* generate_chart.sh by @ganievs in https://github.com/Altinity/clickhouse-operator/pull/925

### New Contributors
* @ganievs made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/925

**Full Changelog**: https://github.com/Altinity/clickhouse-operator/compare/0.18.4...0.18.5

## Release 0.18.4
### What's Changed
* Base image has been updated to address https://access.redhat.com/errata/RHSA-2022:0896 (closes https://github.com/Altinity/clickhouse-operator/issues/913)
* Https support for health checks by @bkuschel in https://github.com/Altinity/clickhouse-operator/pull/912
* Fixed number of SERVERS from 1 to 3 for a 3 node clickhouse-keeper deployment by @a-dot in https://github.com/Altinity/clickhouse-operator/pull/902
* clickhouse-keeper and ZooKeeper manifests were updated  by @Slach in https://github.com/Altinity/clickhouse-operator/pull/911 and  https://github.com/Altinity/clickhouse-operator/pull/916

### New Contributors
* @a-dot made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/902
* @bkuschel made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/912

**Full Changelog**: https://github.com/Altinity/clickhouse-operator/compare/0.18.3...0.18.4


## Release 0.18.3
### What's Changed
* Fixed a bug that might result in broken CHI if operator has been restarted during the reconcile
* Added 'AGE' to CHI status
* Added ClickHouse Keeper examples

**Full Changelog**: https://github.com/Altinity/clickhouse-operator/compare/0.18.2...0.18.3


## Release 0.18.2
### What's Changed
* Operator now correctly respects configuration settings to exclude annotations from managed objects. Fixes [#859](https://github.com/Altinity/clickhouse-operator/issues/859)
* Make CHI status messages more human friendly

**Full Changelog**: https://github.com/Altinity/clickhouse-operator/compare/0.18.1...0.18.2

## Release 0.18.1
### What's Changed
* fix non pointer mistake in metrics-exporter by @adu-bricks in https://github.com/Altinity/clickhouse-operator/pull/870
* Helper files for operatorhub.io integration

### New Contributors
* @adu-bricks made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/870

**Full Changelog**: https://github.com/Altinity/clickhouse-operator/compare/0.18.0...0.18.1

## Release 0.18.0
### New features
* arm64 packages are published (closes https://github.com/Altinity/clickhouse-operator/issues/852)
* 'access_management' can be specified when defining users in CHI.
* k8s secrets can be referenced when defining user passwords. See [05-settings-01-overview.yaml](https://github.com/Altinity/clickhouse-operator/blob/0.18.0/docs/chi-examples/05-settings-01-overview.yaml).

### Changed
* When CRD is deleted, operator keeps all dependent objects now (statefulsets, volumes). That prevents from incidental delete of a cluster.
* When operator restarts it does not run a reconcile cycle anymore if CHI has not been changed. That prevents unneeded pod restarts. (Closes https://github.com/Altinity/clickhouse-operator/issues/855)
* Operator configuration file format has been changed. See https://github.com/Altinity/clickhouse-operator/blob/0.18.0/config/config.yaml. Configuration in old format is supported for backwards compatibility.

### Fixed
* Fixed a bug 'unable to decode watch event: no kind \"ClickHouseOperatorConfiguration\" is registered' that could appear in some k8s configurations.
* Removed INFORMATION_SCHEMA from schema propagation. (closes https://github.com/Altinity/clickhouse-operator/issues/854)
* Added a containerPort to metrics-exporter (https://github.com/Altinity/clickhouse-operator/pull/834)

### New Contributors
* @johnhummelAltinity made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/838
* @jiangxinqi1995 made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/846
* @johnny made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/834

**Full Changelog**: https://github.com/Altinity/clickhouse-operator/compare/0.17.0...0.18.0

## Release 0.17.0
### New features:
* Labels and annotations from auto templates are now supported
* Macros can be used in service annotations the same way as in generateName. Fixes https://github.com/Altinity/clickhouse-operator/issues/795

### Changed:
* Status object has been cleaned up

### Fixed:
* Database engine is now respected during schema migration
* Fixed schema migration for ClickHouse 21.11+
* Removed extra waits for single-node CHI changes
* Fixed a possible race conditions with labeling on operator startup

## Release 0.16.1
This is a bugfixing release with a number of internal changes:
* CRD definition of Status has been modified. **Note:** CRD needs to be updated with this release
* Default terminationGracePeriod for ClickHouse pod templates was moved to operator configuration. Default is 30 seconds as in 0.15.0 and before.
* Improved installation templates
* Fixed a bug with replicas not being correctly added when CHI has been modified from a Python Kubernetes client.

**Upgrade notes:**
* CRD needs to be updated with this release
* 0.16.0 had hardcoded 60 seconds for terminationGracePeriod that resulted in ClickHouse restarts when upgrading from 0.15.0 to 0.16.0. Upgrade from 0.15.0 to 0.16.1 does result in ClickHouse restarts. If you are upgrading from 0.16.0 to 0.16.1 set [terminationGracePeriod](https://github.com/Altinity/clickhouse-operator/blob/50134723c388eda208a8a02a3c333a8fca73133a/config/config.yaml#L163) to 60 in operator config file. Refer to [Operator Configuration](https://github.com/Altinity/clickhouse-operator/blob/master/docs/operator_configuration.md) for more details.



## Release 0.16.0
### New features:
* PodDisruption budget is configured automatically in order to prevent multiple pods being shut down by k8s
* topologyKey is now configurable. It addresses https://github.com/Altinity/clickhouse-operator/issues/772
* spec.restart: "RollingRestart" attribute has been added in order to initiate graceful restart of a cluster. Use it with a patch command.
* Added a support for Kubernetes secrets in settings section, e.g. for ClickHouse user/password.

### Changed:
* Pod maintenance logic during all operations that require pod restart has been improved:
    1) Wait condition is enabled for a pod to be removed from ClickHouse cluster.
    2) Wait condition is added for running queries to complete (up to 5 minutes).
    3) terminationGracePeriod has been increased to 60 seconds.
* Timeout for DDL operations has been increased to 60 seconds. That addresses issues with schema management on slow clusters.
* Base image is switched to RedHat UBI
* ZooKeeper image version has been rolled back to 3.6.1 since we have some problems with 3.6.3
* CRD apiVersion has been upgraded from apiextensions.k8s.io/v1beta1 to apiextensions.k8s.io/v1. The v1beta1 manifests are still available under descriptive names.

### Fixed:
* Fixed a bug with non-working custom ports
* Fixed a bug with replicas being incorrectly deleted from ZooKeeper for retained volumes

**Note**: Upgrade from the previous version will result in restart of ClickHouse clusters.

## Release 0.15.0
### New features:
* Added 90s delay before restarting the stateful set, when ClickHouse server settings are modified. That makes sure ClickHouse server starts with an updated configuration. Before that there was a race condition between configmap updates and statefulset restart. The default timeout can be modified by 'spec.reconciling.configMapPropagationTimeout' property. See [example](https://github.com/Altinity/clickhouse-operator/blob/0926d00a22be23499d633ce455bc5473a9620d15/docs/chi-examples/99-clickhouseinstallation-max.yaml#L28)
* Added 'troubleshooting' mode that allows pod to start even if ClickHouse server is failing. In troubleshooting mode liveness check is removed, and extra 'sleep' is added to the startup command. Controlled by 'spc.troubleshoot' property. See [example](https://github.com/Altinity/clickhouse-operator/blob/master/tests/configs/test-027-troubleshooting-2.yaml)
* Added a cleanup reconcilation logic that will remove k8s object that are labeled by a particular CHI but do not exist in the CHI manifest. The default behaviour can be altered with 'spec.reconciling.cleanup.unknownObjects' . See [example](https://github.com/Altinity/clickhouse-operator/blob/0926d00a22be23499d633ce455bc5473a9620d15/docs/chi-examples/99-clickhouseinstallation-max.yaml#L30)
* ZooKeeper manifests have been modified to use 'standaloneEnabled=false' for single node setups as recommended in [ZooKeeper documentation](https://zookeeper.apache.org/doc/r3.6.3/zookeeperReconfig.html#sc_reconfig_standaloneEnabled). ZooKeeper version has been bumped to 3.6.3

### Bug fixes:
* Fixed a bug when replica has not been deleted in ZooKeeper when scaling down (https://github.com/Altinity/clickhouse-operator/issues/735). The bug has been introduced in 0.14.0
* Fixed a bug when PVCs modified outside of operator could be re-created during the reconcile (https://github.com/Altinity/clickhouse-operator/issues/730).
* Fixed a bug when schema was not created on newly added replica sometimes

## Release 0.14.0
### New features:
* Operator reconciles CHI with the actual state of objects in k8s. In previous releases it compared new CHI and old CHI, but not with k8s state.
* The current reconcile cycle can be interrupted with a new CHI update now. In previous releases user had to wait until reconcile is complete for all nodes..
* Added volumeClaimTemplate annotations. Closes https://github.com/Altinity/clickhouse-operator/issues/578
* clickhouse_operator user/password can be stored in a secret instead of a configuration file. Closes: https://github.com/Altinity/clickhouse-operator/issues/386
* Added 'excludeFromPropagationLabels' option
* Added readiness check
* LabelScope metrics are removed by default since it causes pods to restart when changing a cluster with circular replication. Closes: https://github.com/Altinity/clickhouse-operator/issues/666. If you need those labels, they can be turned on with 'appendScopeLabels' configuration option
* Monitoring of detached parts
* Operator ClusterRole is restricted. Closes https://github.com/Altinity/clickhouse-operator/issues/646
* Logging improvements

###  Bug fixes:
* Fixed a bug when CHI pods could get stuck in ErrImagePull status when wrong image has been used
* Fixed a bug when operator tried to apply schema to non-existing pods, and entered a lengthy retry cycle

### Upgrade notes:
* Existing clusters will be restarted with the operator upgrade due to signature change
* Due to ClusterRole change the upgrade with re-applying installation manifest may fail. See https://github.com/Altinity/clickhouse-operator/issues/684

## Release 0.13.5
This is a follow-up release to 0.13.0:

### Improvements:
* Readiness check has been removed in favour of custom readiness controller, liveness check has been adjusted for slower startup times
* Operator currently uses 'ready' label for graceful pods creation and modifications. That reduces possible service downtime
* More graceful re-scale and other operations
* New StatefulSet fingerprint
* Operator log has been improved and properly annotated
* Test suite has been upgraded to the recent TestFlows version

**Note 1**: We recommend using 0.13.5 instead of 0.13.0. We have found that implementation of liveness/readiness probes in 0.13.0 release was not optimal for healthy production operation.
**Note 2**: ClickHouse clusters will be restarted after operator upgrade due to pod template and labels changes.

## Release 0.13.0
### New features:
* Added liveness (/ping) and readiness (/replicas_status) probes to ClickHouse pods. Those can be overwritten on podTemplate level
* Customizable graceful reconcile logic. Now it is possible to turn-on 'wait' behaviour that would apply configuration changes and upgrades using the following algorithm:
    * Host is (optionally) excluded from ClickHouse remote_servers
    * Operator (optionally) waits for exclusion to take an effect
    * Host is updated
    * If it is a new host, schema is created at this stage. That ensures host is not in the cluster until schema is created. Fixes #561
    * Host is added back to remote_servers
    * Operator (optionally) waits for inclusion to take an effect before moving to the next host.

'Optional' steps are turned-off by default and can be turned on in operator configuration:
```
reconcileWaitInclude: false
reconcileWaitExclude: false
```
or enabled for particular CHI update:
```
spec:
  reconciling:
    policy: "nowait"|"wait"
```

* Cluster 'stop' operation now correctly removes CHI from monitoring and deletes LoadBalancer service
* podTemplate metadata is now supported (annotations, labels). Fixes #554
* 'auto' templates are now available. If templating.policy=auto is specified for ClickHouseInstallationTemplate object, those templates are automatically applied to all ClickHouseInstallations.
* Minor changes to ClickHouse default profile settings.

### Bug fixes:
* External labels, annotations and finalizers are correctly preserved for services.
* It was possible that finalizer has been inserted multiple times to CHI

## Note:
existing ClickHouse clusters will be restarted after operator upgrade because of adding Liveness/Readiness probes 

## Release 0.12.0
This release includes a number of improvements in order to eliminate unneeded restarts and reduce service downtime:
* Pods are no longer restarted when new shards/replicas are added
* Cluster configuration (remote_servers.xml) is now updated after shards and replicas are added. That reduces a chance to get an error querying distributed tables.
* LoadBalancer node ports are no longer modified on service upgrade. That reduces possible downtime
* Service is re-created if it can not be updated for some reason (e.g. change from ClusterIP to LoadBalancer or vice versa)
* Fixed several race conditions when creating/updating a cluster


## Release 0.11.0
### New features:
* ClickHouse settings can be configured at shard/replica level
* Custom ClickHouse configuration files may be supplied both for 'users.d' and 'config.d' sections using 'users.d/my_users.xml:' syntax.
* Operator is registered as a finalizer for CHI, so CHI deletion is more reliable now.

### Bug fixes:
* Operator does not attempt to create a schema when cluster is rolled over initially anymore. Fixes #479 and may improve #478 as well
* Schema creation logic when adding new shards/replicas has been improved to address #308
* Operator restart should not cause ClickHouse pods restart anymore in most cases

**Important Note**: CHI can not be deleted without operator installed anymore. Also, if operator and CHI is installed in one namespace, and namespace is deleted, it may stuck forever in 'Terminating' state due to an arbitrary deletion sequence.

## Release 0.10.0
### General:
* Kubernetes APIs used by operator have been updated to 1.15. Operator does not use features from new APIs, so it keeps working with Kubernetes 1.12 and above. Operator is tested for compatibility with different k8s versions, and we did not face any issues.

### New features:
* Ability to re-scale storage, both by enlarging volume size for existing installations in volumeClaimTemplate, and by adding extra volumes. That requires pod restarts.
* Operator re-creates a StatefulSet if there are changes that can not be applied to an existing object
* Configurable namespaceNamePattern and statefulSetNamePattern (#360 by @qixiaogang), see example
    * https://github.com/Altinity/clickhouse-operator/blob/master/docs/chi-examples/18-namespace-domain-pattern.yaml
    * https://github.com/Altinity/clickhouse-operator/blob/master/docs/chi-examples/19-pod-generate-name.yaml
* New exported metrics 'metric.ChangedSettingsHash', 'metric.LongestRunningQuery' and 'active 'label for table sizes.
* Numerous improvements to Grafana dashboard

## Bug fixes:
* Set 'internal_replication' to false for single replica clusters (#422 by @yuzhichang)
* Fixed schema creation logic when adding new replica (#385), improved stability in general for different types of objects
* Service selectors did not include namespace, that could lead to a conflict in multi-namespace environment. It is fixed now.
* Operator generated ClickHouse configuration files using standard names, like 'settings.xml'. It might lead to a conflict with user configuration files. Now all generated config files are prefixed with 'chop-generated-'.

**Upgrade notes**:  When operator is upgraded from the previous version, it re-creates statefulsets and pods to ensure they match the installation. The storage should be correctly re-attached. As a side effect it ensures there are no backwards incompatibility issues between operator versions.

## Release 0.9.9
### Bug fixes:
* Fixed ZooKeeper installation manifests that did not mount PV properly. The bug could result in ZooKeeper data loss if ZK pods were restarted.
* Fixed metrics exporter to work across multiple ClickHouse versions -- metric descriptions are temporary removed, Prometheus does not work good if those change.

### Other changes:
* Bumped prometheus/golang_client to the latest version
* Added extra logging into chi status field.

## Release 0.9.8
### Bug fixes:
* Fixed multi-threading model for proper handling or concurrent updates
* Fixed schema propagation logic to work with database names that require quoting

Other changes:
* Disabled query_thread_log by default (only applies to new instances)

## Release 0.9.7
### Bug fixes:
* Fixed a bug with schema management queries failing at ClickHouse 20.3.+
* Fixed a bug with logging level configuration not being respected by metrics-exporter
* Fixed schema management logic to use ClickHouse pods directly bypassing LoadBalancer service
* Fixed a bug with host being deleted twice when deleting cluster or shards/replicas

Grafana dashboard has been also improved in this release.

## Release 0.9.6
### Bug fixes:
* Sensitive data is masked now in operator logs
* Fixed a bug with replicated tables being deleted even when PVC has been retained (#314)
* Fixed a bug with annotations not being propagated to the pods (#289). Example [16-custom-labels-and-annotations-01.yaml](https://github.com/Altinity/clickhouse-operator/blob/master/docs/chi-examples/16-custom-labels-and-annotations-01.yaml)

Documentation and examples has been updated to reflect recent changes.

## Release 0.9.5
### Bug fixes:
* Fixed incorrect initialization of installation template in some cases

### Improvements:
* Added podDistribution.scope in order to control scope of anti-affinity rules

## Release 0.9.4
###  Bug fixes:
* Fixed regression with Zookeeper configuration (#310)
* Fixed a bug in table_partitions metric export
* Removed excessive logging in metrics exporter

### Improvements:
* Load balancer service now uses externalTrafficPolicy: Local by default
* ZooKeeper configuration is currently supported per cluster level (#209 via #288)

## Release 0.9.3
### Bug fixes:
* Fixed a problem with upgrading operator for ClickHouseInstallations created by operator 0.8.0 and earlier
* Fixed a problem with updates to ClickHouse users configuration not always being propagated to ClickHouse servers (#282 )

### Tweak:
* Added operator log level configuration

### IMPORTANT NOTES:
1. Please upgrade operator via re-installation. There are change in resource definitions.
2. Operator versions 0.9.1 and 0.9.2 **are not** compatible with 0.9.0 and 0.9.3 in regards to upgrade. Clusters created with those versions have to be re-created.

## Release 0.9.2
### Bug fixes:
* Fixed memory leak in metrics exporter (#176)

## Release 0.9.1
### Bug fixes
* Fixed regression with 'files' section of configuration (#261)
* Fixed discovery of monitored ClickHouse clusters by metrics exporter when it is being restarted (#239)

## Release 0.9.0

### New features:
* Option to define template per ClickHouse replica (before it was only possible to have global, shard based or
* 'circular' replication shortcut (podDistribution.type: CircularReplication)
* hostTemplates in order to propagate custom ports across configuration
* hostNetwork support and portDistribution types. See examples starting from '14-' in [examples](https://github.com/Altinity/clickhouse-operator/tree/master/docs/chi-examples) folder.
  host based templates)
* New Grafana dashboard

## Release 0.8.0
### New features:
* Introduced a set of 'podDistribution' affinity/anti-affinity rules to manage pod deployment in ClickHouse terms. See https://github.com/Altinity/clickhouse-operator/blob/master/docs/chi-examples/10-zones-03-advanced-03-pod-per-host-default-storage-class.yaml
* Default security configuration is improved:
    * All networks are disabled now. Localhost connections are enabled.
    * Connections from the cluster nodes are enabled via host_regexp.
    * ClickHouse user passwords are automatically hashed sha256 and stored as password_sha256_hex in config maps and files.
* All operator and user labels are propagated from ClickHouseInstallation to dependent objects: statefulsets, pods, services and configmaps
* Added some ClickHouse defaults that are automatically applied. It can be changed in operator configuration if necessary. See:
    - https://github.com/Altinity/clickhouse-operator/tree/master/config/config.d
    - https://github.com/Altinity/clickhouse-operator/tree/master/config/users.d
    - https://github.com/Altinity/clickhouse-operator/blob/master/config/config.yaml

### Monitoring improvements:
* ClickHouse mutations are now available in monitoring
* Metrics fetch/error events were added
* Fixed a bug with monitoring events not properly labeled (also fixes #225)
* Fixed a bug when all hosts were dropped out of monitoring if one host fails

### Other bug fixes:
* Fixed a bug when schema could not be created automatically on newer versions of ClickHouse (#227).
* Fixed a bug with zookeeper root entry not propagated to ClickHouse configuration

### Upgrade notes:
* There were changes in ClickHouseInstallation CRD. It is recommended to remove and re-install the operator. Existing ClickHouse clusters will be picked up automatically.
* **IMPORTANT**: If you upgrade operator from 0.6.0 or earlier to 0.8.0 please make sure your ClickHouseInstallation name is shorter than 15 symbols. Otherwise DO NOT UPGRADE an operator.


## Release 0.7.0

### New features:
 * Added 'podVolumeClaimTemplate' and 'logVolumeClaimTemplate' in order to map data and logs separately. Old syntax is deprecated but supported.
 * Sidecar clickhouse-logs container to view logs
 * Significantly cleaned up templates model and 'useTemplates' extension
 * new system_replicas_is_session_expired monitoring metric ([#187][a187] by @teralype)

### Bug fixes:
 * Fixed bug with installation name being truncated to 15 chars. The current limit is 60 chars. Cluster name is limited to 15.
 * General stability improvements and fixes for corner cases

**Upgrade notes:**
There were changes in ClickHouseInstallation CRD, so it is recommended to remove and re-install the operator. Existing ClickHouse clusters will be picked up automatically.

## Release 0.6.0

### New features:
 * Added spec.stop property to start/stop all ClickHouse pods in installation for maintenance
 * ClickHouseInstallationTemplate custom resource definition
 * ClickHouseOperatorConfiguration custom resource definition

### Improvements:
 * Split operator into two binaries/containers - operator and monitor
 * Added 10 second timeout for queries to ClickHouse ([#159][a159] by @kcking)
 * Improved create/update logic
 * Operator now looks at its own namespace if not specified explicitly
 * Enhance multi-thread support for concurrent operations

[a187]: https://github.com/Altinity/clickhouse-operator/pull/187
[a159]: https://github.com/Altinity/clickhouse-operator/pull/159

