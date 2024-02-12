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

Next sources merges with the previous one. Changes to `etc-clickhouse-operator-files` are not monitored, but picked up if operator is restarted. Changes to `ClickHouseOperatorConfiguration` are monitored by an operator and applied immediately.

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
              image: clickhouse/clickhouse-server:23.8
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

[clickhouse-operator-install-bundle.yaml]: ../deploy/operator/clickhouse-operator-install-bundle.yaml
[70-chop-config.yaml]: ./chi-examples/70-chop-config.yaml
