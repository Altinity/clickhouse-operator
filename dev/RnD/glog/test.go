package main

import (
	"flag"

	"github.com/golang/glog"
)

func i() {
	glog.Info("----")
	glog.Info("v: ", flag.Lookup("v").Value)
	glog.Info("logtostderr: ", flag.Lookup("logtostderr").Value)
	glog.Info("alsologtostderr: ", flag.Lookup("alsologtostderr").Value)
	glog.Info("----")
}

func main() {
	flag.Parse()
	glog.Info("hi_a")
	i()
	// flag.Lookup("logtostderr").Value.Set("true")
	flag.Set("logtostderr", "true")
	glog.Info("hi_b")
	i()

	//flag.Lookup("log_dir").Value.Set("/path/to/log/dir")

	glog.Info("Try V() with default v")
	glog.V(0).Info("v0a")
	glog.V(1).Info("v1a")
	glog.V(4).Info("v4a")
	glog.V(5).Info("v5a")
	glog.V(6).Info("v6a")
	v := "5"
	glog.Info("Try V() = ", v)
	//flag.Lookup("v").Value.Set(v)
	flag.Set("v", v)
	glog.V(0).Info("v0b")
	glog.V(1).Info("v1b")
	glog.V(4).Info("v4b")
	glog.V(5).Info("v5b")
	glog.V(6).Info("v6b")
	i()
	//etc.
}

// >>> hi_b
// >>> v4b
