# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.23.4](https://github.com/Altinity/clickhouse-operator/compare/release-0.23.3...release-0.23.4)

### Changed
* Allow adding pod labels in Helm chart by @bruno-s-coelho in https://github.com/Altinity/clickhouse-operator/pull/1369. Closes https://github.com/Altinity/clickhouse-operator/issues/1356
* The default service type has been changed from LoadBalancer to ClusterIP
* Cluster restart rules were fixed for some structured settings like 'logger/*'. Now changing of those would not cause a restart of pods.
* Operator does not fail if ClickHouserKeeperInstallation resource type is missing in k8s
* Added 'clickhouse_operator_chi_reconciles_aborted' operator metric

> **NOTE:** There was a lot of internal refactoring related to Keeper code, but no functional changes yet. The Keeper configuration functionality will be improved in the next major release.

## [0.23.3](https://github.com/Altinity/clickhouse-operator/compare/release-0.23.2...release-0.23.3)

### Changed
* Enabled parameterized databases propagation to shards and replicas in ClickHouse 22.12+. That includes support for Replicated and MySQL database engines. Closes https://github.com/Altinity/clickhouse-operator/issues/1076
* Removed object storage disks from DiskTotal/Free metrics since those do not make any sense
* Added ability to import packages with operator APIs by @dmvolod in https://github.com/Altinity/clickhouse-operator/pull/1229
* Introduced number of unchanged hosts in CHI status

### Fixed
* Fixed an issue with cluster stop could be taking long time. Closes https://github.com/Altinity/clickhouse-operator/issues/1346
* Fixed a bug when inconsistent cluster definition might result in a crash. Closes https://github.com/Altinity/clickhouse-operator/issues/1319
* Fixed a bug when hosts-completed could be incorrectly reported in status when reconcile is re-started in the middle


## [0.23.2](https://github.com/Altinity/clickhouse-operator/compare/release-0.23.1...release-0.23.2)

### Changed
* Fix environment variables generation for secrets that might be off in some cases. Closes https://github.com/Altinity/clickhouse-operator/issues/1344
* Golang is upgraded to 1.20. Closes CVEs in dependent libraries.


## [0.23.1](https://github.com/Altinity/clickhouse-operator/compare/release-0.23.0...release-0.23.1)

### Changed
* Updated grants example by @lesandie in https://github.com/Altinity/clickhouse-operator/pull/1333
* Upgrade ClickHouse version to 23.8-lts by @orginux in https://github.com/Altinity/clickhouse-operator/pull/1338

### Fixed
* Fixed generation of users that could be broken in some cases. Closes https://github.com/Altinity/clickhouse-operator/issues/1324 and https://github.com/Altinity/clickhouse-operator/issues/1332
* Fixed metrics-exporter that might fail to export metrics in some cases. Closes https://github.com/Altinity/clickhouse-operator/issues/1336
* Fixed Keeper examples
* Include installation of ClickhouseKeeperInstallations CRD in Helm chart readme by @echozio in https://github.com/Altinity/clickhouse-operator/pull/1330


## [0.23.0](https://github.com/Altinity/clickhouse-operator/compare/release-0.22.2...release-0.23.0)

### Added
* Kubernetes secrets are currently supported with the standard syntax for user passwords, configuration settings, and configuration files, for example:
    ```yaml
        users:
          user1/password:
            valueFrom:
              secretKeyRef:
                name: clickhouse_secret
                key: pwduser1
        settings:
          s3/my_bucket/access_key:
            valueFrom:
              secretKeyRef:
                name: s3-credentials
                key: AWS_ACCESS_KEY_ID
        files:
          server.key:
            valueFrom:
              secretKeyRef:
                name: clickhouse-certs
                key: server.key
    ```
    > See updated [Security Hardening Guide](https://github.com/Altinity/clickhouse-operator/blob/0.23.0/docs/security_hardening.md#securing-clickhouse-server-settings) for more detail.
* Added **experimental** support for ClickHouse Keeper by @frankwg in https://github.com/Altinity/clickhouse-operator/pull/1218

    ```yaml
    kind: ClickHouseKeeperInstallation
    ```
    See examples in there: https://github.com/Altinity/clickhouse-operator/tree/0.23.0/docs/chk-examples
    The implementation is not final, following things yet needs to be done:
    1) dynamic reconfiguration, that is required in order to support adding and removing Keeper replicas
    2) integration with ClickHouseInstallation, so Keeper could be referenced by a reference, instead by a service
* CHI labels are now added to exported Prometheus metrics

### Changed

* Services are now re-created if ServiceType is changed in order to workaround [Kubernetes issue](https://github.com/kubernetes/kubernetes/issues/24040). Closes https://github.com/Altinity/clickhouse-operator/issues/1302
* Operator now waits for ClickHouse service endpoints to respond when checking node is up.
* CHI templates are now automatically reloaded by operator. Before, templates were only reloaded during startup. In order to apply changes, CHI update needs to be triggered.
* Operator will now crash if operator configuration is broken or can not be parsed. That prevents the fallback to the defaults in case of errors.
* Improve helm, update values.yaml to properly generate helm/README.md by @Slach in https://github.com/Altinity/clickhouse-operator/pull/1278
* Improve clickhouse-keeper manifests by @Slach in https://github.com/Altinity/clickhouse-operator/pull/1234
* chore: remove refs to deprecated io/ioutil by @testwill in https://github.com/Altinity/clickhouse-operator/pull/1273
* Update URL for accepted logging levels by @madrisan in https://github.com/Altinity/clickhouse-operator/pull/1270
* Add a chi example for sync users by @ccsxs in https://github.com/Altinity/clickhouse-operator/pull/1304
* Bump zookepper operator version to 0.2.15 by @GrahamCampbell in https://github.com/Altinity/clickhouse-operator/pull/1303
* Optional values.rbac to deploy rbac resources by @Salec in https://github.com/Altinity/clickhouse-operator/pull/1316
* update helm chart generator to treat config.yaml as yaml in values by @echozio in https://github.com/Altinity/clickhouse-operator/pull/1317

### Fixed

* Fixed schema propagation on new replicas for ClickHouse 23.11 and above
* Fixed data recovery when PVC is deleted by a user. Closes https://github.com/Altinity/clickhouse-operator/issues/1310

## [0.22.2](https://github.com/Altinity/clickhouse-operator/compare/release-0.22.1...release-0.22.2)

### Changed
* Fixed a bug when operator did not restart ClickHouse pods if 'files' section was changed without 'config.d' destination, e.g. ```files/settings.xml```.
* Fix ServiceMonitor endpoints [#1276](https://github.com/Altinity/clickhouse-operator/pull/1276) by @MiguelNdeCarvalho, and [#1290](https://github.com/Altinity/clickhouse-operator/pull/1290) by @muicoder. Closes [#1287](https://github.com/Altinity/clickhouse-operator/issues/1287)
* Disabled prefer_localhost_replica in default profile

## [0.22.1](https://github.com/Altinity/clickhouse-operator/compare/release-0.22.0...release-0.22.1)

### Added
* New 'Aborted' status for CHI is set when reconcile is aborted by an operator

### Changed
* Allow shard weight to be zero ([#1192](https://github.com/Altinity/clickhouse-operator/pull/1192) by [maxistua](https://github.com/maxistua))
* Removed excessive logging for pod update events
* Removed 30s delay after creating a service
* Allow empty values for CRD status and some other fields in order to facilitate migration from old operator versions that were upgraded without upgrading CRD first.  Fixes #842, #890 and similar issues.

## [0.22.0]( https://github.com/Altinity/clickhouse-operator/compare/release-0.21.3...release-0.22.0)

### Added
* Support volume re-provisioning. If volume is broken and PVC detects it as lost, operator re-provisions the volume
* When new CHI is created, all hosts are created in parallel
* Allow to turn off waiting for running queries to complete. This can be done both in operator configuration or in CHI itself:
    In operator configuration:
    ```yaml
    spec:
      reconcile:
        host:
          wait:
            queries: "false"
    ```

    In CHI:
    ```yaml
    spec:
      reconciling:
        policy: nowait
    ```
* When changes are applied to clusters with a lot of shards, the change is probed on a first node only. Is successul, it is applied on 50% of shards. This can be configured in operator configuration:
    ```yaml
    reconcile:
      # Reconcile runtime settings
      runtime:
        # Max number of concurrent CHI reconciles in progress
        reconcileCHIsThreadsNumber: 10

        # The operator reconciles shards concurrently in each CHI with the following limitations:
        #   1. Number of shards being reconciled (and thus having hosts down) in each CHI concurrently
        #      can not be greater than 'reconcileShardsThreadsNumber'.
        #   2. Percentage of shards being reconciled (and thus having hosts down) in each CHI concurrently
        #      can not be greater than 'reconcileShardsMaxConcurrencyPercent'.
        #   3. The first shard is always reconciled alone. Concurrency starts from the second shard and onward.
        # Thus limiting number of shards being reconciled (and thus having hosts down) in each CHI by both number and percentage

        # Max number of concurrent shard reconciles within one CHI in progress
        reconcileShardsThreadsNumber: 5
        # Max percentage of concurrent shard reconciles within one CHI in progress
        reconcileShardsMaxConcurrencyPercent: 50
    ```
* Operator-related metrics are exposed to Prometheus now:
    ```
    clickhouse_operator_chi_reconciles_started
    clickhouse_operator_chi_reconciles_completed
    clickhouse_operator_chi_reconciles_timings

    clickhouse_operator_host_reconciles_started
    clickhouse_operator_host_reconciles_completed
    clickhouse_operator_host_reconciles_restarts
    clickhouse_operator_host_reconciles_errors
    clickhouse_operator_host_reconciles_timings

    clickhouse_operator_pod_add_events
    clickhouse_operator_pod_update_events
    clickhouse_operator_pod_delete_events
    ```

### Changed
* fix typo in operator_installation_details.md by @seeekr in https://github.com/Altinity/clickhouse-operator/pull/1219
* Set operator release date fot createdAt CSV field by @dmvolod in https://github.com/Altinity/clickhouse-operator/pull/1223
* Fix type for exclude and include fields in 70-chop-config.yaml example by @dmvolod in https://github.com/Altinity/clickhouse-operator/pull/1222
* change dashboard refresh rate 1m and add min_duration_ms, max_duration_ms dashboard variables, rename query_type to query_kind by @Slach in https://github.com/Altinity/clickhouse-operator/pull/1235
* add securityContext to helm chart by @farodin91 in https://github.com/Altinity/clickhouse-operator/pull/1255
* metrics-exporter collects all hosts and queries in parallel

### Fixed
* Fixed a bug when operator could break multiple nodes if incorrect configuration has been deployed several times in a row
* Fixed a bug when schema could not be created on new nodes, if nodes took too long to start
* Fixed a bug when services were not reconciled in rare cases

### New Contributors
* @seeekr made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/1219
* @dmvolod made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/1223
* @farodin91 made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/1255

**Full Changelog**:

## [0.21.3](https://github.com/Altinity/clickhouse-operator/compare/release-0.21.2...release-0.21.3)

### Added
* Added selectors to CHITemplates. Now it is possible to define a template that is applied to certain CHI. Example here: https://github.com/Altinity/clickhouse-operator/blob/0.21.3/docs/chi-examples/50-CHIT-04-auto-template-volume-with-selector.yaml
* Added '.status.useTemplates' to reflect all templates used in CHI manually or auto

### Changed
* CHITemplates are now re-loaded automatically without a need to restart operator. Changes in CHITemplates are not applied automatically to affected CHI.

### Fixed
* Fix nil pointer deref in metrics exporter (#1187)  by @zcross in https://github.com/Altinity/clickhouse-operator/pull/1188
* Migrate piechart plugin on Grafana Dashboard by @MiguelNdeCarvalho in https://github.com/Altinity/clickhouse-operator/pull/1190
* Permission error when deleting Pod sometimes

### New Contributors
* @MiguelNdeCarvalho made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/1190

## [0.21.2](https://github.com/Altinity/clickhouse-operator/compare/release-0.21.1...release-0.21.2)

### Added
* Added support of clickhouse-log via podTemplates by @dmmarkov in https://github.com/Altinity/clickhouse-operator/pull/1012
* Added SQL UDFs replication when adding shards/replicas. Closes https://github.com/Altinity/clickhouse-operator/issues/1174

### Changed
* Operator and metrics-exporter now automatically select 'http' or 'https' for connecting to cluster based on cluster configuration
* Changed statefulSet update behaviour to abort the update on failure. That protects the rest of the cluster from a breaking changes
* Removed unneeded operations on persistent volumes
* Run tests in parallel by @antip00 in https://github.com/Altinity/clickhouse-operator/pull/1171
* fix build script to adapt to macos m1. by @xiedeyantu in https://github.com/Altinity/clickhouse-operator/pull/1169
* Improvements to Grafana and Prometheus manifests

### Fixed
* fix crash when reconcilePVC() failed by @jewelzqiu in https://github.com/Altinity/clickhouse-operator/pull/1168
* fix reconcile threads number by @jewelzqiu in https://github.com/Altinity/clickhouse-operator/pull/1170

### New Contributors
* @jewelzqiu made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/1168
* @xiedeyantu made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/1169
* @dmmarkov made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/1012


## [0.21.1](https://github.com/Altinity/clickhouse-operator/compare/release-0.21.0...release-0.21.1)

### Added
* Added configurable shard-level concurrent reconciliation by @zcross in https://github.com/Altinity/clickhouse-operator/pull/1124. It is controlled by the following operator configuration settings:
    ```yaml
    reconcile:
      runtime:
        # Max number of concurrent CHI reconciles in progress
        reconcileCHIsThreadsNumber: 10
        # Max number of concurrent shard reconciles in progress
        reconcileShardsThreadsNumber: 1
        # The maximum percentage of cluster shards that may be reconciled in parallel
        reconcileShardsMaxConcurrencyPercent: 50
    ```
* Added default configuration for ClickHouse system.trace_log table with 30 days TTL

### Changed
* ZooKeeper manifests were rewritten to store configuration separately

### Fixed
* Fixed a bug in metrics-exporter that might stop working on ClickHouse nodes with certain types of system.errors. Closes #1161
* Fixed a bug with ClickHouse major version detection for Altinity.Stable builds

## [0.21.0]( https://github.com/Altinity/clickhouse-operator/compare/release-0.20.3...release-0.21.0)

### Changed
* Changed the way Operator applies ClickHouse settings. In the previous version, every change in settings required a restart via re-creating a StatefulSet. In this version it does not re-create StatefulSet anymore, but maintains a logic that decide if ClickHouse needs to be restarted in order to pickup a change or not. In case of restart it is performed using scaling stateful set down and up. The restart logic is controlled by configurationRestartPolicy configuration setting. Here is the default configuration:
    ``` yaml
    configurationRestartPolicy:
      rules:
        - version: "*"
          rules:
            - settings/*: "yes"
            - settings/dictionaries_config: "no"
            - settings/logger: "no"
            - settings/macros/*: "no"
            - settings/max_server_memory_*: "no"
            - settings/max_*_to_drop: "no"
            - settings/max_concurrent_queries: "no"
            - settings/models_config: "no"
            - settings/user_defined_executable_functions_config: "no"

            - zookeeper/*: "yes"

            - files/config.d/*.xml: "yes"
            - files/config.d/*dict*.xml: "no"

            - profiles/default/background_*_pool_size: "yes"
            - profiles/default/max_*_for_server: "yes"
        - version: "21.*"
          rules:
            - settings/logger: "yes"
    ```
* Improved reliability of schema creation for new shards/replicas
* Added "secure" option for Zookeeper connections by @Tvion in https://github.com/Altinity/clickhouse-operator/pull/1114
* Added "insecure" flag that closes insecure TCP/HTTP ClickHouse ports. See [security_hardening.md](https://github.com/Altinity/clickhouse-operator/blob/0.21.0/docs/security_hardening.md#disabling-insecure-connections) for more detail
* Added an option to disable metrics exporter by @roimor in https://github.com/Altinity/clickhouse-operator/pull/1131
* Added system.errors scrapping into metrics-exporter
* Refactor Registry internals to make concurrent access safe by @zcross in https://github.com/Altinity/clickhouse-operator/pull/1115
* Changed Grafana deployment to allow persisting custom dashboards
* Changed ZooKeeper version to 3.8.1

### Fixed
* Fixed a bug that might result in PDB being deleted. Closes https://github.com/Altinity/clickhouse-operator/issues/1139
* Fixed propagation of podTemplate environment variables from ClickHouseInstallationTemplate to ClickHouseInstallation
* Fixed propagation of startup probe from ClickHouseInstallationTemplate to ClickHouseInstallation

### New Contributors
* @roimor made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/1131
* @Tvion made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/1114
* @zcross made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/1115

## [0.20.3](https://github.com/Altinity/clickhouse-operator/compare/release-0.20.2...release-0.20.3)

### Changed
* Use alpine base image instead of UBI
* Fix error handling when table already exists in ZooKeeper for new ClickHouse versions

## [0.20.2](https://github.com/Altinity/clickhouse-operator/compare/release-0.20.1...release-0.20.2)

### Changed
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

## [0.20.1](https://github.com/Altinity/clickhouse-operator/compare/release-0.20.0...release-0.20.1)

### Changed
* Improvements to Grafana dashboards by @Slach
* Generate operator helm chart by @slamdev in https://github.com/Altinity/clickhouse-operator/pull/1049
* Added cluster to PodDesruptionBudget selector.  Closes #996. **Note**: PDB requires Kubernetes version 1.21 or above.
* Added status.hostsCompleted to track reconcile progress
* Do not restart CHI with restart attribute when operator is restarted
* Address https://nvd.nist.gov/vuln/detail/CVE-2022-27664. Closes #1039
* Fixed RBAC permissions for secrets. Closes #1051

### New Contributors
* @slamdev made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/1049

## [0.20.0](https://github.com/Altinity/clickhouse-operator/compare/release-0.19.2...release-0.20.0)

### Changed
* Add support for setting secure communication between clickhouse instances. (Inspried by @chancez in https://github.com/Altinity/clickhouse-operator/pull/938). See examples:
  * [auto secret](https://github.com/Altinity/clickhouse-operator/blob/0.20.0/docs/chi-examples/21-secure-cluster-secret-01-auto.yaml)
  * [plain text secret](https://github.com/Altinity/clickhouse-operator/blob/0.20.0/docs/chi-examples/21-secure-cluster-secret-02-plaintext.yaml)
  * [k8s secret reference](https://github.com/Altinity/clickhouse-operator/blob/0.20.0/docs/chi-examples/21-secure-cluster-secret-03-secret-ref.yaml)
* Operator managed PV provisioning. (Inspired by @chhan-coupang in https://github.com/Altinity/clickhouse-operator/pull/947). Allows to re-scale volumes w/o a downtime. In order to enable, use:
    ```yaml
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

## [0.19.3]( https://github.com/Altinity/clickhouse-operator/compare/release-0.19.2...release-0.19.3)

### Changed
* Fixed a possible loose of cluster state when operator has been restarted in the middle of reconcile.
* Backport of https://github.com/Altinity/clickhouse-operator/pull/1009
* add loadaverage + network + disk panel to clickhouse dashboard by @Slach in https://github.com/Altinity/clickhouse-operator/pull/1015

> **NOTE:** There is a change in ClickHouseInstallation custom resource type, so make sure you deploy full installation manifest when upgrading.

## [0.19.2](https://github.com/Altinity/clickhouse-operator/compare/release-0.19.1...release-0.19.2)

### Changed
* Fixed a bug in 0.19.0 and 0.19.1 versions when operator could be rebuilding remote-servers.xml from scratch after restart. When it happened, distributed queries might behave unexpectedly.

**Full Changelog**:

## [0.19.1](https://github.com/Altinity/clickhouse-operator/compare/release-0.19.0...release-0.19.1)

### Changed
* Fixed MVs in Atomic databases support for ClickHouse versions 22.6 and above. Closes https://github.com/Altinity/clickhouse-operator/issues/989
* Fixed tables with '{uuid}' macro in Atomic databases for ClickHouse versions 22.6 and above.
* Fixed watch namespaces regexp by https://github.com/Altinity/clickhouse-operator/pull/992. Closes https://github.com/Altinity/clickhouse-operator/issues/991
* Added "clickhouse.altinity.com/ready: yes|no" annotations for replica services for the use by external load balancer.
* Updated podDisruptionBudget API version

## [0.19.0](https://github.com/Altinity/clickhouse-operator/compare/0.18.5...release-0.19.0)

### Changed
* Now 'clickhouse-operator' user is restricted to an IP address of the pod where clickhouse-operator is running. It can not be used from anywhere else.
* In addition to hostRegexp, 'default' user is restricted to IP addresses of pods of the CHI. This fixes connectivity issues between shards in GKE.
* ClickHouse 'interserver_http_host' is set to the service name and matches 'remote_servers'. It can be changed to full names with 'replicasUseFQDN' setting if needed. This fixes connectivity issues between replicas in GKE.
* Changed the way schema is propagated to new shards. It is currently controlled by [two cluster level settings](https://github.com/Altinity/clickhouse-operator/blob/7f5ee13e9e2185e245c12e9821db188e542aa98e/deploy/operator/parts/crd.yaml#L668):
    ```yaml
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

## [0.18.5](https://github.com/Altinity/clickhouse-operator/compare/0.18.4...0.18.5)

### Changed
* Dependencies were updated to address CVE-2022-21698 and CVE-2021-38561
* generate_chart.sh by @ganievs in https://github.com/Altinity/clickhouse-operator/pull/925

### New Contributors
* @ganievs made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/925

## [0.18.4](https://github.com/Altinity/clickhouse-operator/compare/0.18.3...0.18.4)

### Changed
* Base image has been updated to address https://access.redhat.com/errata/RHSA-2022:0896 (closes https://github.com/Altinity/clickhouse-operator/issues/913)
* Https support for health checks by @bkuschel in https://github.com/Altinity/clickhouse-operator/pull/912
* Fixed number of SERVERS from 1 to 3 for a 3 node clickhouse-keeper deployment by @a-dot in https://github.com/Altinity/clickhouse-operator/pull/902
* clickhouse-keeper and ZooKeeper manifests were updated  by @Slach in https://github.com/Altinity/clickhouse-operator/pull/911 and  https://github.com/Altinity/clickhouse-operator/pull/916

### New Contributors
* @a-dot made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/902
* @bkuschel made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/912

## [0.18.3](https://github.com/Altinity/clickhouse-operator/compare/0.18.2...0.18.3)

### Changed
* Fixed a bug that might result in broken CHI if operator has been restarted during the reconcile
* Added 'AGE' to CHI status
* Added ClickHouse Keeper examples


## [0.18.2](https://github.com/Altinity/clickhouse-operator/compare/0.18.1...0.18.2)

### Changed
* Operator now correctly respects configuration settings to exclude annotations from managed objects. Fixes [#859](https://github.com/Altinity/clickhouse-operator/issues/859)
* Make CHI status messages more human friendly
:

## [0.18.1](https://github.com/Altinity/clickhouse-operator/compare/0.18.0...0.18.1)

### Changed
* fix non pointer mistake in metrics-exporter by @adu-bricks in https://github.com/Altinity/clickhouse-operator/pull/870
* Helper files for operatorhub.io integration

### New Contributors
* @adu-bricks made their first contribution in https://github.com/Altinity/clickhouse-operator/pull/870

## [0.18.0](https://github.com/Altinity/clickhouse-operator/compare/0.17.0...0.18.0)

### Added
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

## [0.17.0](https://github.com/Altinity/clickhouse-operator/compare/0.16.1...0.17.0)

## Added
* Labels and annotations from auto templates are now supported
* Macros can be used in service annotations the same way as in generateName. Fixes https://github.com/Altinity/clickhouse-operator/issues/795

## Changed
* Status object has been cleaned up

## Fixed
* Database engine is now respected during schema migration
* Fixed schema migration for ClickHouse 21.11+
* Removed extra waits for single-node CHI changes
* Fixed a possible race conditions with labeling on operator startup

## [0.16.1](https://github.com/Altinity/clickhouse-operator/compare/0.16.0...0.16.1)

### Fixed
* CRD definition of Status has been modified. **Note:** CRD needs to be updated with this release
* Default terminationGracePeriod for ClickHouse pod templates was moved to operator configuration. Default is 30 seconds as in 0.15.0 and before.
* Improved installation templates
* Fixed a bug with replicas not being correctly added when CHI has been modified from a Python Kubernetes client.

### Upgrade notes
* CRD needs to be updated with this release
* 0.16.0 had hardcoded 60 seconds for terminationGracePeriod that resulted in ClickHouse restarts when upgrading from 0.15.0 to 0.16.0. Upgrade from 0.15.0 to 0.16.1 does result in ClickHouse restarts. If you are upgrading from 0.16.0 to 0.16.1 set [terminationGracePeriod](https://github.com/Altinity/clickhouse-operator/blob/50134723c388eda208a8a02a3c333a8fca73133a/config/config.yaml#L163) to 60 in operator config file. Refer to [Operator Configuration](https://github.com/Altinity/clickhouse-operator/blob/master/docs/operator_configuration.md) for more details.


## [0.16.0](https://github.com/Altinity/clickhouse-operator/compare/0.15.0...0.16.0)

### Added
* PodDisruption budget is configured automatically in order to prevent multiple pods being shut down by k8s
* topologyKey is now configurable. It addresses https://github.com/Altinity/clickhouse-operator/issues/772
* spec.restart: "RollingRestart" attribute has been added in order to initiate graceful restart of a cluster. Use it with a patch command.
* Added a support for Kubernetes secrets in settings section, e.g. for ClickHouse user/password.

### Changed
* Pod maintenance logic during all operations that require pod restart has been improved:
   1) Wait condition is enabled for a pod to be removed from ClickHouse cluster.
   2) Wait condition is added for running queries to complete (up to 5 minutes).
   3) terminationGracePeriod has been increased to 60 seconds.
* Timeout for DDL operations has been increased to 60 seconds. That addresses issues with schema management on slow clusters.
* Base image is switched to RedHat UBI
* ZooKeeper image version has been rolled back to 3.6.1 since we have some problems with 3.6.3
* CRD apiVersion has been upgraded from apiextensions.k8s.io/v1beta1 to apiextensions.k8s.io/v1. The v1beta1 manifests are still available under descriptive names.

### Fixed
* Fixed a bug with non-working custom ports
* Fixed a bug with replicas being incorrectly deleted from ZooKeeper for retained volumes

**Note**: Upgrade from the previous version will result in restart of ClickHouse clusters.

## [0.15.0](https://github.com/Altinity/clickhouse-operator/compare/0.16.0...0.15.0)

### Added
* Added 90s delay before restarting the stateful set, when ClickHouse server settings are modified. That makes sure ClickHouse server starts with an updated configuration. Before that there was a race condition between configmap updates and statefulset restart. The default timeout can be modified by 'spec.reconciling.configMapPropagationTimeout' property. See [example](https://github.com/Altinity/clickhouse-operator/blob/0926d00a22be23499d633ce455bc5473a9620d15/docs/chi-examples/99-clickhouseinstallation-max.yaml#L28)
* Added 'troubleshooting' mode that allows pod to start even if ClickHouse server is failing. In troubleshooting mode liveness check is removed, and extra 'sleep' is added to the startup command. Controlled by 'spc.troubleshoot' property. See [example](https://github.com/Altinity/clickhouse-operator/blob/master/tests/configs/test-027-troubleshooting-2.yaml)
* Added a cleanup reconcilation logic that will remove k8s object that are labeled by a particular CHI but do not exist in the CHI manifest. The default behaviour can be altered with 'spec.reconciling.cleanup.unknownObjects' . See [example](https://github.com/Altinity/clickhouse-operator/blob/0926d00a22be23499d633ce455bc5473a9620d15/docs/chi-examples/99-clickhouseinstallation-max.yaml#L30)
* ZooKeeper manifests have been modified to use 'standaloneEnabled=false' for single node setups as recommended in [ZooKeeper documentation](https://zookeeper.apache.org/doc/r3.6.3/zookeeperReconfig.html#sc_reconfig_standaloneEnabled). ZooKeeper version has been bumped to 3.6.3

### Fixed
* Fixed a bug when replica has not been deleted in ZooKeeper when scaling down (https://github.com/Altinity/clickhouse-operator/issues/735). The bug has been introduced in 0.14.0
* Fixed a bug when PVCs modified outside of operator could be re-created during the reconcile (https://github.com/Altinity/clickhouse-operator/issues/730).
* Fixed a bug when schema was not created on newly added replica sometimes

