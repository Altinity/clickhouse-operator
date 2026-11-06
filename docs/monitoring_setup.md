
In order to setup monitoring, we need to:

1. [Setup Prometheus][prometheus_setup] in order to pool data from ClickHouse into Prometheus
1. and after that - [setup grafana][grafana_setup] in order to display data accumulated in Prometheus

[prometheus_setup]: ./prometheus_setup.md
[grafana_setup]: ./grafana_setup.md

## Profiling the operator and metrics-exporter (pprof)

Both `clickhouse-operator` and `metrics-exporter` can serve Go [pprof](https://pkg.go.dev/net/http/pprof) profiles,
for example to find out why a container is OOM-killed. pprof is **disabled by default**. When enabled it is served
on its own listener only. It never appears on the metrics ports (`9999` and `8888`) or behind the `clickhouse-operator-metrics` Service.

| Container           | Flag             | Environment variable             | Helm values                                  |
|---------------------|------------------|----------------------------------|----------------------------------------------|
| `clickhouse-operator` | `-pprof-address` | `OPERATOR_PPROF_ADDRESS`         | `operator.pprof.enabled`, `operator.pprof.address` (default `127.0.0.1:6060`) |
| `metrics-exporter`  | `-pprof-address` | `METRICS_EXPORTER_PPROF_ADDRESS` | `metrics.pprof.enabled`, `metrics.pprof.address` (default `127.0.0.1:6061`) |

The value is a `host:port` listen address. An empty value disables pprof, and the flag takes precedence over the environment variable.
Both containers share the pod network, so give them different ports.
Profiles expose process memory, which includes credentials, so bind to loopback (`127.0.0.1`). The binaries log a warning
whenever pprof is enabled, plus another one if the address is not loopback.

Enable with Helm:

```bash
helm upgrade clickhouse-operator altinity/altinity-clickhouse-operator --reuse-values \
  --set operator.pprof.enabled=true \
  --set metrics.pprof.enabled=true
```

Without Helm, add `OPERATOR_PPROF_ADDRESS=127.0.0.1:6060` / `METRICS_EXPORTER_PPROF_ADDRESS=127.0.0.1:6061` to the `env` of the respective container.

Reach the loopback listener with `kubectl port-forward` and collect profiles:

```bash
kubectl -n <namespace> port-forward deploy/clickhouse-operator 6060:6060 6061:6061

# operator
go tool pprof http://localhost:6060/debug/pprof/heap
# metrics-exporter
go tool pprof http://localhost:6061/debug/pprof/heap
go tool pprof "http://localhost:6061/debug/pprof/profile?seconds=30"
curl -s "http://localhost:6061/debug/pprof/goroutine?debug=2"
```

Disable pprof again when you are done.
