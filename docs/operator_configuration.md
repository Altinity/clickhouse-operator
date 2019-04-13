# `clickhouse-operator` configuration

```yaml
# Namespaces where clickhouse-operator listens for events.
# Concurrently running operators should listen on different namespaces
namespaces:
  - dev
  - info
  - onemore

###########################################
##
## Additional Configuration Files Section
##
###########################################

# Path to folder where ClickHouse configuration files common for all instances within CHI are located.
chCommonConfigsPath: config.d

# Path to folder where ClickHouse configuration files unique for each instances within CHI are located.
chDeploymentConfigsPath: conf.d

# Path to folder where ClickHouse configuration files with users settings are located.
# Files are common for all instances within CHI
chUsersConfigsPath: users.d

# Path to folder where ClickHouseInstallation .yaml manifests are located.
# Manifests are applied in sorted alpha-numeric order
chiTemplatesPath: templates.d

###########################################
##
## Cluster Update Section
##
###########################################

# How many seconds to wait for created/updated StatefulSet to be Ready
statefulSetUpdateTimeout: 50

# How many seconds to wait between checks for created/updated StatefulSet status
statefulSetUpdatePollPeriod: 2

# What to do in case created/updated StatefulSet is not in Ready after `statefulSetUpdateTimeout` seconds
onStatefulSetUpdateFailureAction: abort
```
