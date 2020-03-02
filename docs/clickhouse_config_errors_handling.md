# ClickHouse Configuration Errors Handling

When operator performs rolling update and create procedures, it monitors health of the updated/created StatefulSets.
Operator polls StatefulSet status and wait for some configurable time for StatefulSet to report **Ready** status reached. 
In case StatefulSet successfully reports **Ready** status, all is good and operator can move on to the next StatefulSet.
However, in case ClickHouse configuration is incorrect or due to any other reason ClickHouse is unable to start, StatefulSet would not reach **Ready** status.
In tis case, operator has to do something and decide what to do regarding the following questions:
1. What to do with current failed StatefulSet?
1. Should operator continue with rolling update?

Regarding failed StatefulSet, operator can either do nothing and leave the situation to admin to solve or can try to rollback StatefulSet to previous state.
In case of newly created StatefulSet, rollback means *to delete StatefulSet*
This behavior is configured with the following operator configuration options:
```yaml
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
``` 

Regarding should operator continue with rolling update/create in case of failed StatefulSet it met - current behavior is to abort rolling process and let admin to decide how to proceed with current situation.

# Misconfiguration Examples
Let's take a look on real-life examples of misconfiguration opeartor can deal with.
There are several erroneous configurations located in 
[examples which demonstrate how to withstand errors][chi-examples-withstand-errors]
Operator can withstand this misconfiguration and continue to serve ClickHouse installation.  
- Incorrect ClickHouse image specified. Create new `ClickHouseInstallation` with incorrect image. Kubernetes can't create container with incorrect image.\
[manifest][01-incorrect-image-create.yaml]
- Incorrect ClickHouse image specified. Update existing `ClickHouseInstallation` with incorrect image. Kubernetes can't create container with incorrect image.\
[initial position][02-incorrect-image-update-01-initial-position.yaml]\
[apply incorrect update][02-incorrect-image-update-02-apply-incorrect-update.yaml]\
[revert back][02-incorrect-image-update-03-revert-and-apply.yaml]
- Incorrect ClickHouse settings specified. Create new `ClickHouseInstallation` with incorrect ClickHouse settings. ClickHouse instance can't start.\
[manifest][03-incorrect-settings-create.yaml]
- Incorrect ClickHouse settings specified. Update existing `ClickHouseInstallation` with incorrect ClickHouse settings. ClickHouse instance can't start.\
[initial position][04-incorrect-settings-update-01-initial-position.yaml]\
[apply incorrect update][04-incorrect-settings-update-02-apply-incorrect-update.yaml]\
[revert back][04-incorrect-settings-update-03-revert-and-apply.yaml]
- Incorrect `PodTemplate` specified. Create new `ClickHouseInstallation` with incorrect `PodTemplate`. Kubernetes can't create Pod.\
[manifest][05-incorrect-pod-template.yaml]

`clickhouse-operator` is able to detect unsuccessful create/update operation. Exact behavior of `clickhouse-operator` deals with the situation depends on 
```yaml
onStatefulSetCreateFailureAction
onStatefulSetUpdateFailureAction
```
configuration settings. 

# Plans and discussion
Interesting question is what to do with StatefulSets that were already successfully updated on the same run, before failed StatefulSet met.
Available options are:
1. Do nothing. In this case ClickHouse cluster may be in some inconsistent state, because some replicas may be updated and some not.
1. try to rollback the whole cluster to some **previous** state. What this **previous** state be is a matter of discussion.
Currently operator goes with 'do nothing' approach.


[chi-examples-withstand-errors]: ./chi-examples-withstand-errors
[01-incorrect-image-create.yaml]: ./chi-examples-withstand-errors/01-incorrect-image-create.yaml
[02-incorrect-image-update-01-initial-position.yaml]: ./chi-examples-withstand-errors/02-incorrect-image-update-01-initial-position.yaml
[02-incorrect-image-update-02-apply-incorrect-update.yaml]: ./chi-examples-withstand-errors/02-incorrect-image-update-02-apply-incorrect-update.yaml
[02-incorrect-image-update-03-revert-and-apply.yaml]: ./chi-examples-withstand-errors/02-incorrect-image-update-03-revert-and-apply.yaml
[03-incorrect-settings-create.yaml]: ./chi-examples-withstand-errors/03-incorrect-settings-create.yaml
[04-incorrect-settings-update-01-initial-position.yaml]: ./chi-examples-withstand-errors/04-incorrect-settings-update-01-initial-position.yaml
[04-incorrect-settings-update-02-apply-incorrect-update.yaml]: ./chi-examples-withstand-errors/04-incorrect-settings-update-02-apply-incorrect-update.yaml
[04-incorrect-settings-update-03-revert-and-apply.yaml]: ./chi-examples-withstand-errors/04-incorrect-settings-update-03-revert-and-apply.yaml
[05-incorrect-pod-template.yaml]: ./chi-examples-withstand-errors/05-incorrect-pod-template.yaml
