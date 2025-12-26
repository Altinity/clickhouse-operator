# altinity-clickhouse-operator

![Version: 0.26.0](https://img.shields.io/badge/Version-0.26.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.26.0](https://img.shields.io/badge/AppVersion-0.26.0-informational?style=flat-square)

Helm chart to deploy [altinity-clickhouse-operator](https://github.com/Altinity/clickhouse-operator).

The ClickHouse Operator creates, configures and manages ClickHouse clusters running on Kubernetes.

## CRD Management

CRDs are automatically installed and updated during `helm install` and `helm upgrade` using pre-install/pre-upgrade hooks (enabled by default).

To disable automatic CRD updates, set `crdHook.enabled: false` in values.yaml. When disabled, CRDs must be installed manually:
```bash
  kubectl apply -f https://github.com/Altinity/clickhouse-operator/raw/master/deploy/helm/clickhouse-operator/crds/CustomResourceDefinition-clickhouseinstallations.clickhouse.altinity.com.yaml
  kubectl apply -f https://github.com/Altinity/clickhouse-operator/raw/master/deploy/helm/clickhouse-operator/crds/CustomResourceDefinition-clickhouseinstallationtemplates.clickhouse.altinity.com.yaml
  kubectl apply -f https://github.com/Altinity/clickhouse-operator/raw/master/deploy/helm/clickhouse-operator/crds/CustomResourceDefinition-clickhouseoperatorconfigurations.clickhouse.altinity.com.yaml
  kubectl apply -f https://github.com/Altinity/clickhouse-operator/raw/master/deploy/helm/clickhouse-operator/crds/CustomResourceDefinition-clickhousekeeperinstallations.clickhouse-keeper.altinity.com.yaml
```

**Homepage:** <https://github.com/Altinity/clickhouse-operator>

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| altinity | <support@altinity.com> |  |

## CRD Management

This chart includes automatic CRD installation and update functionality using Helm hooks. CRDs are automatically applied during `helm install` and `helm upgrade` operations.

### How It Works

- **Automatic Updates**: CRDs are installed/updated via pre-install and pre-upgrade hooks (enabled by default)
- **Backward Compatible**: CRDs remain in the `crds/` directory for standard Helm 3 behavior
- **Server-Side Apply**: Uses kubectl with `--server-side` flag for better conflict resolution
- **Configurable**: Can be disabled via `crdHook.enabled: false` in values.yaml

### Manual CRD Management

If you prefer to manage CRDs manually or have disabled the automatic hooks, you can apply CRDs using kubectl:

```bash
kubectl apply -f https://github.com/Altinity/clickhouse-operator/raw/master/deploy/helm/clickhouse-operator/crds/CustomResourceDefinition-clickhouseinstallations.clickhouse.altinity.com.yaml
kubectl apply -f https://github.com/Altinity/clickhouse-operator/raw/master/deploy/helm/clickhouse-operator/crds/CustomResourceDefinition-clickhouseinstallationtemplates.clickhouse.altinity.com.yaml
kubectl apply -f https://github.com/Altinity/clickhouse-operator/raw/master/deploy/helm/clickhouse-operator/crds/CustomResourceDefinition-clickhouseoperatorconfigurations.clickhouse.altinity.com.yaml
kubectl apply -f https://github.com/Altinity/clickhouse-operator/raw/master/deploy/helm/clickhouse-operator/crds/CustomResourceDefinition-clickhousekeeperinstallations.clickhouse-keeper.altinity.com.yaml
```

### Troubleshooting

**Hook Job Fails with Permission Denied**
- Ensure the cluster has RBAC enabled and the ServiceAccount has proper permissions to manage CRDs
- The hook requires cluster-level permissions for `apiextensions.k8s.io/customresourcedefinitions`

**CRDs Not Updating**
- Check if `crdHook.enabled` is set to `true` in your values
- Verify the hook Job ran successfully: `kubectl get jobs -n <namespace> | grep crd-install`
- Check Job logs: `kubectl logs -n <namespace> job/<release-name>-crd-install`

**Disabling Automatic CRD Updates**
```yaml
crdHook:
  enabled: false
```

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| additionalResources | list | `[]` | list of additional resources to create (processed via `tpl` function), useful for create ClickHouse clusters together with clickhouse-operator. check `kubectl explain chi` for details |
| affinity | object | `{}` | affinity for scheduler pod assignment, check `kubectl explain pod.spec.affinity` for details |
| commonAnnotations | object | `{}` | set of annotations that will be applied to all the resources for the operator |
| commonLabels | object | `{}` | set of labels that will be applied to all the resources for the operator |
| configs | object | check the `values.yaml` file for the config content (auto-generated from latest operator release) | clickhouse operator configs |
| crdHook.affinity | object | `{}` | affinity for CRD installation job |
| crdHook.enabled | bool | `true` | enable automatic CRD installation/update via pre-install/pre-upgrade hooks when disabled, CRDs must be installed manually using kubectl apply |
| crdHook.image.pullPolicy | string | `"IfNotPresent"` | image pull policy for CRD installation job |
| crdHook.image.repository | string | `"bitnami/kubectl"` | image repository for CRD installation job |
| crdHook.image.tag | string | `"latest"` | image tag for CRD installation job |
| crdHook.nodeSelector | object | `{}` | node selector for CRD installation job |
| crdHook.resources | object | `{}` | resource limits and requests for CRD installation job |
| crdHook.tolerations | list | `[]` | tolerations for CRD installation job |
| dashboards.additionalLabels | object | `{"grafana_dashboard":""}` | labels to add to a secret with dashboards |
| dashboards.annotations | object | `{"grafana_folder":"clickhouse-operator"}` | annotations to add to a secret with dashboards |
| dashboards.annotations.grafana_folder | string | `"clickhouse-operator"` | folder where will place dashboards, requires define values in official grafana helm chart sidecar.dashboards.folderAnnotation: grafana_folder |
| dashboards.enabled | bool | `false` | provision grafana dashboards as configMaps (can be synced by grafana dashboards sidecar https://github.com/grafana/helm-charts/blob/grafana-8.3.4/charts/grafana/values.yaml#L778 ) |
| deployment.strategy.type | string | `"Recreate"` |  |
| fullnameOverride | string | `""` | full name of the chart. |
| imagePullSecrets | list | `[]` | image pull secret for private images in clickhouse-operator pod possible value format `[{"name":"your-secret-name"}]`, check `kubectl explain pod.spec.imagePullSecrets` for details |
| metrics.containerSecurityContext | object | `{}` |  |
| metrics.enabled | bool | `true` |  |
| metrics.env | list | `[]` | additional environment variables for the deployment of metrics-exporter containers possible format value `[{"name": "SAMPLE", "value": "text"}]` |
| metrics.image.pullPolicy | string | `"IfNotPresent"` | image pull policy |
| metrics.image.repository | string | `"altinity/metrics-exporter"` | image repository |
| metrics.image.tag | string | `""` | image tag (chart's appVersion value will be used if not set) |
| metrics.resources | object | `{}` | custom resource configuration |
| nameOverride | string | `""` | override name of the chart |
| namespaceOverride | string | `""` |  |
| nodeSelector | object | `{}` | node for scheduler pod assignment, check `kubectl explain pod.spec.nodeSelector` for details |
| operator.containerSecurityContext | object | `{}` |  |
| operator.env | list | `[]` | additional environment variables for the clickhouse-operator container in deployment possible format value `[{"name": "SAMPLE", "value": "text"}]` |
| operator.image.pullPolicy | string | `"IfNotPresent"` | image pull policy |
| operator.image.repository | string | `"altinity/clickhouse-operator"` | image repository |
| operator.image.tag | string | `""` | image tag (chart's appVersion value will be used if not set) |
| operator.priorityClassName | string | "" | priority class name for the clickhouse-operator deployment, check `kubectl explain pod.spec.priorityClassName` for details |
| operator.resources | object | `{}` | custom resource configuration, check `kubectl explain pod.spec.containers.resources` for details |
| podAnnotations | object | check the `values.yaml` file | annotations to add to the clickhouse-operator pod, check `kubectl explain pod.spec.annotations` for details |
| podLabels | object | `{}` | labels to add to the clickhouse-operator pod |
| podSecurityContext | object | `{}` |  |
| rbac.create | bool | `true` | specifies whether rbac resources should be created |
| rbac.namespaceScoped | bool | `false` | specifies whether to create roles and rolebindings at the cluster level or namespace level |
| secret.create | bool | `true` | create a secret with operator credentials |
| secret.password | string | `"clickhouse_operator_password"` | operator credentials password |
| secret.username | string | `"clickhouse_operator"` | operator credentials username |
| serviceAccount.annotations | object | `{}` | annotations to add to the service account |
| serviceAccount.create | bool | `true` | specifies whether a service account should be created |
| serviceAccount.name | string | `nil` | the name of the service account to use; if not set and create is true, a name is generated using the fullname template |
| serviceMonitor.additionalLabels | object | `{}` | additional labels for service monitor |
| serviceMonitor.clickhouseMetrics.interval | string | `"30s"` |  |
| serviceMonitor.clickhouseMetrics.metricRelabelings | list | `[]` |  |
| serviceMonitor.clickhouseMetrics.relabelings | list | `[]` |  |
| serviceMonitor.clickhouseMetrics.scrapeTimeout | string | `""` |  |
| serviceMonitor.enabled | bool | `false` | ServiceMonitor Custom resource is created for a [prometheus-operator](https://github.com/prometheus-operator/prometheus-operator) In serviceMonitor will be created two endpoints ch-metrics on port 8888 and op-metrics # 9999. Ypu can specify interval, scrapeTimeout, relabelings, metricRelabelings for each endpoint below |
| serviceMonitor.operatorMetrics.interval | string | `"30s"` |  |
| serviceMonitor.operatorMetrics.metricRelabelings | list | `[]` |  |
| serviceMonitor.operatorMetrics.relabelings | list | `[]` |  |
| serviceMonitor.operatorMetrics.scrapeTimeout | string | `""` |  |
| tolerations | list | `[]` | tolerations for scheduler pod assignment, check `kubectl explain pod.spec.tolerations` for details |
| topologySpreadConstraints | list | `[]` |  |

