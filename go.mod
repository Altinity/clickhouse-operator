module github.com/altinity/clickhouse-operator

go 1.13

require (
	github.com/MakeNowJust/heredoc v1.0.0
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/d4l3k/messagediff v1.2.1 // indirect
	github.com/evanphx/json-patch v4.5.0+incompatible // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/hashicorp/golang-lru v0.5.3 // indirect
	github.com/imdario/mergo v0.3.8
	github.com/juliangruber/go-intersect v1.0.0
	github.com/kubernetes-sigs/yaml v1.1.0
	github.com/mailru/go-clickhouse v1.3.0
	github.com/prometheus/client_golang v1.6.0
	github.com/r3labs/diff v0.0.0-20191120142937-b4ed99a31f5a
	github.com/sanity-io/litter v1.3.0
	github.com/spf13/pflag v1.0.5 // indirect
	gopkg.in/d4l3k/messagediff.v1 v1.2.1
	gopkg.in/yaml.v2 v2.2.8
	k8s.io/api v0.15.11
	k8s.io/apimachinery v0.15.11
	k8s.io/client-go v0.0.0-00010101000000-000000000000
	k8s.io/code-generator v0.0.0-00010101000000-000000000000
)

replace (
	k8s.io/api => k8s.io/api v0.15.11
	k8s.io/apimachinery => k8s.io/apimachinery v0.15.11
	k8s.io/client-go => k8s.io/client-go v0.15.11
	k8s.io/code-generator => k8s.io/code-generator v0.15.11
)
