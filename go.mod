module github.com/altinity/clickhouse-operator

go 1.23
replace (
	github.com/emicklei/go-restful/v3 => github.com/emicklei/go-restful/v3 v3.10.0
	github.com/golang/glog => github.com/sunsingerus/glog v1.0.1-0.20220103184348-48242e35873d
	github.com/imdario/mergo => github.com/sunsingerus/mergo v0.0.0-20230507185449-fc6fffa94450
)

require github.com/altinity/queue v0.0.0-20210114142043-ddb7da66064f

require (
	k8s.io/api v0.29.14
	k8s.io/apiextensions-apiserver v0.29.14
	k8s.io/apimachinery v0.29.14
	k8s.io/client-go v0.29.14
	k8s.io/code-generator v0.29.14
)

require (
	github.com/MakeNowJust/heredoc v1.0.0
	github.com/Masterminds/semver/v3 v3.2.0
	github.com/go-logr/logr v1.4.1
	github.com/go-zookeeper/zk v1.0.3
	github.com/golang/glog v1.0.0
	github.com/google/uuid v1.4.0
	github.com/imdario/mergo v0.3.15
	github.com/juliangruber/go-intersect v1.0.0
	github.com/kubernetes-sigs/yaml v1.1.0
	github.com/mailru/go-clickhouse/v2 v2.1.0
	github.com/prometheus/client_golang v1.18.0
	github.com/sanity-io/litter v1.3.0
	github.com/securego/gosec/v2 v2.8.1
	github.com/stretchr/testify v1.8.4
	go.opentelemetry.io/otel v1.24.0
	go.opentelemetry.io/otel/exporters/prometheus v0.46.0
	go.opentelemetry.io/otel/metric v1.24.0
	go.opentelemetry.io/otel/sdk v1.24.0
	go.opentelemetry.io/otel/sdk/metric v1.24.0
	golang.org/x/sync v0.11.0
	golang.org/x/exp v0.0.0-20220722155223-a9213eeb770e
	gopkg.in/d4l3k/messagediff.v1 v1.2.1
	gopkg.in/yaml.v3 v3.0.1
	sigs.k8s.io/controller-runtime v0.15.1
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/d4l3k/messagediff v1.2.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/emicklei/go-restful/v3 v3.11.0 // indirect
	github.com/evanphx/json-patch v4.12.0+incompatible // indirect
	github.com/evanphx/json-patch/v5 v5.6.0 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-logr/zapr v1.2.4 // indirect
	github.com/go-openapi/jsonpointer v0.19.6 // indirect
	github.com/go-openapi/jsonreference v0.20.2 // indirect
	github.com/go-openapi/swag v0.22.3 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/gnostic-models v0.6.8 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/gookit/color v1.4.2 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/matttproud/golang_protobuf_extensions/v2 v2.0.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/nbutton23/zxcvbn-go v0.0.0-20210217022336-fa2cb2858354 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.6.0 // indirect
	github.com/prometheus/common v0.45.0 // indirect
	github.com/prometheus/procfs v0.12.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/xo/terminfo v0.0.0-20210125001918-ca9a967f8778 // indirect
	go.opentelemetry.io/otel/trace v1.24.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.24.0 // indirect
	golang.org/x/mod v0.17.0 // indirect
	golang.org/x/net v0.36.0 // indirect
	golang.org/x/oauth2 v0.12.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/term v0.29.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	golang.org/x/time v0.3.0 // indirect
	golang.org/x/tools v0.21.1-0.20240508182429-e35e4ccd0d2d // indirect
	gomodules.xyz/jsonpatch/v2 v2.3.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	k8s.io/component-base v0.29.14 // indirect
	k8s.io/gengo v0.0.0-20230829151522-9cce18d56c01 // indirect
	k8s.io/klog/v2 v2.110.1 // indirect
	k8s.io/kube-openapi v0.0.0-20231010175941-2dd684a91f00 // indirect
	k8s.io/utils v0.0.0-20230726121419-3b25d923346b // indirect
	sigs.k8s.io/json v0.0.0-20221116044647-bc3834ca7abd // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.4.1 // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
)
