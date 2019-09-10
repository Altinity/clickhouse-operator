# `clickhouse-operator` configuration

## Introduction

Operator provides extensive configuration options via `clickhouse-operator` configuration. 
Configuration consists of the following main parts:
1. Operator configuration options - operator's settings
1. ClickHouse common configuration files - ready-to-use XML files with sections of ClickHouse configuration **as-is**.
Common configuration typically contains general ClickHouse configuration sections, such as network listen endpoints, logger options, etc
1. ClickHouse user configuration files - ready-to-use XML files with sections of ClickHouse configuration **as-is**
User configuration typically contains ClickHouse configuration sections with user accounts specifications
1. `ClickHouseInstallationTemplate`s specification. Operator provides functionality to specify parts of `ClickHouseInstallation` manifest as a set of templates, which would be used in all `ClickHouseInstallation`s.   

## Config files layout and structure

Typically, operator configuration is located in `/etc/clickhouse-operator` and looks like the following:
```text
/etc/clickhouse-operator
  conf.d
  config.d
    ... common configuration files here ...
  templates.d
    ... templates specification files here ...
  users.d
    ... user configuration files here ...
  config.yaml
```
where [`config.yaml`](../config/config.yaml) is operator [configuration file](../config/config.yaml), of the following structure:

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

## Operator configuration CRDs

In order to provide dynamic configuration `clickhouse-operator` has the following CRDs, specified:
1. `ClickHouseInstallationTemplate`
1. `ClickHouseOperatorConfiguration`

`ClickHouseInstallationTemplate` is special flawor of `ClickHouseInstallation`, with the main difference - it can be filled partially for those sections that we'd like to be templated only. 
In all other parts - it is typical `ClickHouseInstallation`, with the same fields as in [this full example](examples/99-clickhouseinstallation-max.yaml). 
Initial `ClickHouseInstallationTemplate` is [available](examples/50-simple-template-01.yaml)

`ClickHouseOperatorConfiguration` is a special resource to provide dynamic operator configuration and it mirrors in all details specified earlier operator's configuration.
Initial `ClickHouseOperatorConfiguration` is [available](examples/70-chop-config.yaml)

### How to use operator configuration CRDs

In case we'd like to customize operator behavior with special configuration options or `ClickHouseInstallationTemplate`s we need to create appropriate manifest and apply it with `kubectl` in namespace. watched by `clickhouse-operator`. 
In most cases it is the same namespace where operator runs. `clickhouse-operator` sees new/updated object of `kind: ClickHouseInstallationTemplate` or `kind: ClickHouseOperatorConfiguration` and applies these objects over operator's configuration. 

Multiple CRDs application:
1. Multiple `ClickHouseInstallationTemplate`s observed by operator are merged altogether in alphabetical order. All changes are accumulated. 
1. Multiple `ClickHouseOperatorConfiguration`s observed by operator are stacked in alphabetical order. The latest config is applied. 
