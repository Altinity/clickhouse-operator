module github.com/altinity/clickhouse-operator

go 1.13

require (
	github.com/MakeNowJust/heredoc v1.0.0
	github.com/d4l3k/messagediff v1.2.1 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/golang/groupcache v0.0.0-20191027212112-611e8accdfc9 // indirect
	github.com/googleapis/gnostic v0.3.1 // indirect
	github.com/hashicorp/golang-lru v0.5.3 // indirect
	github.com/imdario/mergo v0.3.8
	github.com/kubernetes-sigs/yaml v1.1.0
	github.com/mailru/go-clickhouse v1.3.0
	github.com/prometheus/client_golang v1.6.0
	github.com/r3labs/diff v0.0.0-20191120142937-b4ed99a31f5a
	github.com/stretchr/testify v1.4.0
	golang.org/x/oauth2 v0.0.0-20191202225959-858c2ad4c8b6 // indirect
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0 // indirect
	gopkg.in/d4l3k/messagediff.v1 v1.2.1
	gopkg.in/yaml.v2 v2.2.8
	k8s.io/api v0.17.0
	k8s.io/apimachinery v0.17.0
	k8s.io/client-go v11.0.0+incompatible
	k8s.io/code-generator v0.0.0-00010101000000-000000000000
	k8s.io/utils v0.0.0-20200414100711-2df71ebbae66 // indirect
)

replace (
	k8s.io/api => k8s.io/api v0.17.0
	k8s.io/apimachinery => k8s.io/apimachinery v0.17.0
	k8s.io/client-go => k8s.io/client-go v0.17.0
	k8s.io/code-generator => k8s.io/code-generator v0.17.0
)
