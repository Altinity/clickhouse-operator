module github.com/altinity/clickhouse-operator

go 1.16

require (
	github.com/MakeNowJust/heredoc v1.0.0
	github.com/altinity/queue v0.0.0-20210114142043-ddb7da66064f
	github.com/golang/glog v1.0.0
	github.com/google/uuid v1.3.0
	github.com/imdario/mergo v0.3.11
	github.com/juliangruber/go-intersect v1.0.0
	github.com/kubernetes-sigs/yaml v1.1.0
	github.com/mailru/go-clickhouse v1.6.0
	github.com/prometheus/client_golang v1.11.0
	github.com/r3labs/diff v0.0.0-20191120142937-b4ed99a31f5a
	github.com/sanity-io/litter v1.3.0
	github.com/securego/gosec/v2 v2.8.1
	gopkg.in/d4l3k/messagediff.v1 v1.2.1
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/api v0.23.1
	k8s.io/apiextensions-apiserver v0.23.1
	k8s.io/apimachinery v0.23.1
	k8s.io/client-go v0.23.1
	k8s.io/code-generator v0.23.1
)

require (
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/d4l3k/messagediff v1.2.1 // indirect
	github.com/gosimple/slug v1.12.0
)

replace (
	github.com/golang/glog => github.com/sunsingerus/glog v1.0.1-0.20220103184348-48242e35873d
	k8s.io/api => k8s.io/api v0.21.7
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.21.7
	k8s.io/apimachinery => k8s.io/apimachinery v0.21.7
	k8s.io/client-go => k8s.io/client-go v0.21.7
	k8s.io/code-generator => k8s.io/code-generator v0.21.7
)
