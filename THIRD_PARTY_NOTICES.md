# Third-Party License Attribution

This file lists third-party Go dependencies used by the
`clickhouse-operator` and `metrics-exporter` binaries, with their
SPDX license identifiers.

Generated with [`go-licenses`](https://github.com/google/go-licenses):

```bash
go-licenses report ./cmd/operator ./cmd/metrics_exporter \
  --ignore github.com/altinity/clickhouse-operator
```

License texts are available under the paths in the **License file** column
(from the `vendor/` tree), or via URL when the license is not vendored.
The main project license is Apache-2.0; see [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).

## Summary

| License | Packages |
|---------|----------|
| Apache-2.0 | 41 |
| BSD-3-Clause | 24 |
| MIT | 16 |
| BSD-2-Clause | 1 |
| ISC | 1 |

**Total packages:** 83

## Packages

| Package | License | License file |
|---------|---------|--------------|
| `github.com/altinity/queue` | Apache-2.0 | `vendor/github.com/altinity/queue/LICENSE` |
| `github.com/beorn7/perks/quantile` | MIT | `vendor/github.com/beorn7/perks/LICENSE` |
| `github.com/cespare/xxhash/v2` | MIT | `vendor/github.com/cespare/xxhash/v2/LICENSE.txt` |
| `github.com/davecgh/go-spew/spew` | ISC | `vendor/github.com/davecgh/go-spew/LICENSE` |
| `github.com/emicklei/go-restful/v3` | MIT | `vendor/github.com/emicklei/go-restful/v3/LICENSE` |
| `github.com/evanphx/json-patch/v5` | BSD-3-Clause | `vendor/github.com/evanphx/json-patch/v5/LICENSE` |
| `github.com/fsnotify/fsnotify` | BSD-3-Clause | `vendor/github.com/fsnotify/fsnotify/LICENSE` |
| `github.com/go-logr/logr` | Apache-2.0 | `vendor/github.com/go-logr/logr/LICENSE` |
| `github.com/go-logr/stdr` | Apache-2.0 | `vendor/github.com/go-logr/stdr/LICENSE` |
| `github.com/go-logr/zapr` | Apache-2.0 | `vendor/github.com/go-logr/zapr/LICENSE` |
| `github.com/go-openapi/jsonpointer` | Apache-2.0 | `vendor/github.com/go-openapi/jsonpointer/LICENSE` |
| `github.com/go-openapi/jsonreference` | Apache-2.0 | `vendor/github.com/go-openapi/jsonreference/LICENSE` |
| `github.com/go-openapi/swag` | Apache-2.0 | `vendor/github.com/go-openapi/swag/LICENSE` |
| `github.com/go-zookeeper/zk` | BSD-3-Clause | `vendor/github.com/go-zookeeper/zk/LICENSE` |
| `github.com/gogo/protobuf` | BSD-3-Clause | `vendor/github.com/gogo/protobuf/LICENSE` |
| `github.com/golang/glog` | Apache-2.0 | `vendor/github.com/golang/glog/LICENSE` |
| `github.com/golang/groupcache/lru` | Apache-2.0 | `vendor/github.com/golang/groupcache/LICENSE` |
| `github.com/golang/protobuf` | BSD-3-Clause | `vendor/github.com/golang/protobuf/LICENSE` |
| `github.com/google/gnostic-models` | Apache-2.0 | `vendor/github.com/google/gnostic-models/LICENSE` |
| `github.com/google/go-cmp/cmp` | BSD-3-Clause | `vendor/github.com/google/go-cmp/LICENSE` |
| `github.com/google/gofuzz` | Apache-2.0 | `vendor/github.com/google/gofuzz/LICENSE` |
| `github.com/google/uuid` | BSD-3-Clause | `vendor/github.com/google/uuid/LICENSE` |
| `github.com/imdario/mergo` | BSD-3-Clause | `vendor/github.com/imdario/mergo/LICENSE` |
| `github.com/josharian/intern` | MIT | `vendor/github.com/josharian/intern/license.md` |
| `github.com/json-iterator/go` | MIT | `vendor/github.com/json-iterator/go/LICENSE` |
| `github.com/juliangruber/go-intersect` | MIT | [`LICENSE.md`](https://github.com/juliangruber/go-intersect/blob/v1.0.0/LICENSE.md) |
| `github.com/kubernetes-sigs/yaml` | MIT | `vendor/github.com/kubernetes-sigs/yaml/LICENSE` |
| `github.com/mailru/easyjson` | MIT | `vendor/github.com/mailru/easyjson/LICENSE` |
| `github.com/mailru/go-clickhouse/v2` | MIT | `vendor/github.com/mailru/go-clickhouse/v2/LICENSE` |
| `github.com/MakeNowJust/heredoc` | MIT | `vendor/github.com/MakeNowJust/heredoc/LICENSE` |
| `github.com/Masterminds/semver/v3` | MIT | `vendor/github.com/Masterminds/semver/v3/LICENSE.txt` |
| `github.com/modern-go/concurrent` | Apache-2.0 | `vendor/github.com/modern-go/concurrent/LICENSE` |
| `github.com/modern-go/reflect2` | Apache-2.0 | `vendor/github.com/modern-go/reflect2/LICENSE` |
| `github.com/munnerz/goautoneg` | BSD-3-Clause | `vendor/github.com/munnerz/goautoneg/LICENSE` |
| `github.com/novln/docker-parser` | Apache-2.0 | `vendor/github.com/novln/docker-parser/LICENSE` |
| `github.com/pkg/errors` | BSD-2-Clause | `vendor/github.com/pkg/errors/LICENSE` |
| `github.com/prometheus/client_golang/internal/github.com/golang/gddo/httputil` | BSD-3-Clause | `vendor/github.com/prometheus/client_golang/internal/github.com/golang/gddo/LICENSE` |
| `github.com/prometheus/client_golang/prometheus` | Apache-2.0 | `vendor/github.com/prometheus/client_golang/LICENSE` |
| `github.com/prometheus/client_model/go` | Apache-2.0 | `vendor/github.com/prometheus/client_model/LICENSE` |
| `github.com/prometheus/common` | Apache-2.0 | `vendor/github.com/prometheus/common/LICENSE` |
| `github.com/prometheus/otlptranslator` | Apache-2.0 | `vendor/github.com/prometheus/otlptranslator/LICENSE` |
| `github.com/sanity-io/litter` | MIT | `vendor/github.com/sanity-io/litter/LICENSE` |
| `github.com/spf13/pflag` | BSD-3-Clause | `vendor/github.com/spf13/pflag/LICENSE` |
| `go.opentelemetry.io/auto/sdk` | Apache-2.0 | `vendor/go.opentelemetry.io/auto/sdk/LICENSE` |
| `go.opentelemetry.io/otel` | Apache-2.0 | `vendor/go.opentelemetry.io/otel/LICENSE` |
| `go.opentelemetry.io/otel/exporters/prometheus` | Apache-2.0 | `vendor/go.opentelemetry.io/otel/exporters/prometheus/LICENSE` |
| `go.opentelemetry.io/otel/metric` | Apache-2.0 | `vendor/go.opentelemetry.io/otel/metric/LICENSE` |
| `go.opentelemetry.io/otel/sdk` | Apache-2.0 | `vendor/go.opentelemetry.io/otel/sdk/LICENSE` |
| `go.opentelemetry.io/otel/sdk/metric` | Apache-2.0 | `vendor/go.opentelemetry.io/otel/sdk/metric/LICENSE` |
| `go.opentelemetry.io/otel/trace` | Apache-2.0 | `vendor/go.opentelemetry.io/otel/trace/LICENSE` |
| `go.uber.org/multierr` | MIT | `vendor/go.uber.org/multierr/LICENSE.txt` |
| `go.uber.org/zap` | MIT | `vendor/go.uber.org/zap/LICENSE.txt` |
| `go.yaml.in/yaml/v2` | Apache-2.0 | `vendor/go.yaml.in/yaml/v2/LICENSE` |
| `golang.org/x/exp` | BSD-3-Clause | `vendor/golang.org/x/exp/LICENSE` |
| `golang.org/x/net` | BSD-3-Clause | `vendor/golang.org/x/net/LICENSE` |
| `golang.org/x/oauth2` | BSD-3-Clause | `vendor/golang.org/x/oauth2/LICENSE` |
| `golang.org/x/sync/semaphore` | BSD-3-Clause | `vendor/golang.org/x/sync/LICENSE` |
| `golang.org/x/sys/unix` | BSD-3-Clause | `vendor/golang.org/x/sys/LICENSE` |
| `golang.org/x/term` | BSD-3-Clause | `vendor/golang.org/x/term/LICENSE` |
| `golang.org/x/text` | BSD-3-Clause | `vendor/golang.org/x/text/LICENSE` |
| `golang.org/x/time/rate` | BSD-3-Clause | `vendor/golang.org/x/time/LICENSE` |
| `gomodules.xyz/jsonpatch/v2` | Apache-2.0 | `vendor/gomodules.xyz/jsonpatch/v2/LICENSE` |
| `google.golang.org/protobuf` | BSD-3-Clause | `vendor/google.golang.org/protobuf/LICENSE` |
| `gopkg.in/d4l3k/messagediff.v1` | MIT | `vendor/gopkg.in/d4l3k/messagediff.v1/LICENSE` |
| `gopkg.in/inf.v0` | BSD-3-Clause | `vendor/gopkg.in/inf.v0/LICENSE` |
| `gopkg.in/yaml.v2` | Apache-2.0 | `vendor/gopkg.in/yaml.v2/LICENSE` |
| `gopkg.in/yaml.v3` | MIT | `vendor/gopkg.in/yaml.v3/LICENSE` |
| `k8s.io/api` | Apache-2.0 | `vendor/k8s.io/api/LICENSE` |
| `k8s.io/apiextensions-apiserver/pkg` | Apache-2.0 | `vendor/k8s.io/apiextensions-apiserver/LICENSE` |
| `k8s.io/apimachinery/pkg` | Apache-2.0 | `vendor/k8s.io/apimachinery/LICENSE` |
| `k8s.io/apimachinery/third_party/forked/golang` | BSD-3-Clause | `vendor/k8s.io/apimachinery/third_party/forked/golang/LICENSE` |
| `k8s.io/client-go` | Apache-2.0 | `vendor/k8s.io/client-go/LICENSE` |
| `k8s.io/klog/v2` | Apache-2.0 | `vendor/k8s.io/klog/v2/LICENSE` |
| `k8s.io/kube-openapi/pkg` | Apache-2.0 | `vendor/k8s.io/kube-openapi/LICENSE` |
| `k8s.io/kube-openapi/pkg/internal/third_party/go-json-experiment/json` | BSD-3-Clause | `vendor/k8s.io/kube-openapi/pkg/internal/third_party/go-json-experiment/json/LICENSE` |
| `k8s.io/kube-openapi/pkg/validation/spec` | Apache-2.0 | `vendor/k8s.io/kube-openapi/pkg/validation/spec/LICENSE` |
| `k8s.io/utils` | Apache-2.0 | `vendor/k8s.io/utils/LICENSE` |
| `k8s.io/utils/internal/third_party/forked/golang/net` | BSD-3-Clause | `vendor/k8s.io/utils/internal/third_party/forked/golang/LICENSE` |
| `sigs.k8s.io/controller-runtime` | Apache-2.0 | `vendor/sigs.k8s.io/controller-runtime/LICENSE` |
| `sigs.k8s.io/json` | Apache-2.0 | `vendor/sigs.k8s.io/json/LICENSE` |
| `sigs.k8s.io/structured-merge-diff/v4` | Apache-2.0 | `vendor/sigs.k8s.io/structured-merge-diff/v4/LICENSE` |
| `sigs.k8s.io/yaml` | Apache-2.0 | `vendor/sigs.k8s.io/yaml/LICENSE` |
| `sigs.k8s.io/yaml/goyaml.v2` | Apache-2.0 | `vendor/sigs.k8s.io/yaml/goyaml.v2/LICENSE` |

