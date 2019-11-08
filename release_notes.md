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
