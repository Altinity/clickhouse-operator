package app

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	clientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	informers "github.com/altinity/clickhouse-operator/pkg/client/informers/externalversions"

	"github.com/altinity/clickhouse-operator/pkg/controllers/chi"
	"github.com/golang/glog"

	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Version defines current build version
const Version = "0.1.0beta"

var (
	kubeconfig string
	masterURL  string
)

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "",
		"Paths to kubeconfig. Only required if called outside of the cluster.")
	flag.StringVar(&masterURL, "master", "",
		"The address of the Kubernetes API server. Only required if called outside of the cluster.")
	flag.Parse()
}

// getConfig builds rest.Config out of either:
// 1. CLI-provided config file path
// 2. env var
// 3. in-cluster
// 4. /home/user/.kube/config
func getConfig() (*rest.Config, error) {
	// 1. --kubeconfig=/path/to/config option has top-priority
	// 2. KUBECONFIG=/path/to/config env var has second priority
	// 3. Try to fetch in-cluster config
	// 4. If still no config - try to find /home/user/.kube/config file
	// still no success - return error

	if len(kubeconfig) > 0 {
		return clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	}
	if len(os.Getenv("KUBECONFIG")) > 0 {
		return clientcmd.BuildConfigFromFlags(masterURL, os.Getenv("KUBECONFIG"))
	}
	if conf, err := rest.InClusterConfig(); err == nil {
		return conf, nil
	}
	if usr, err := user.Current(); err == nil {
		// Try to build config from /home/user/.kube/config
		conf, err := clientcmd.BuildConfigFromFlags(
			"", filepath.Join(usr.HomeDir, ".kube", "config"))
		if err == nil {
			// .kube/config found, config produced
			return conf, nil
		}
	}
	return nil, fmt.Errorf("kubeconfig not found")
}

// createClientsets creates two clientsets:
// 1. kubernetes clientset
// 2. clickhouse clientset
func createClientsets() (*kubernetes.Clientset, *clientset.Clientset) {
	config, err := getConfig()
	if err != nil {
		glog.Fatalf("Unable to initialize cluster configuration: %s", err.Error())
	}
	kubeClientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		glog.Fatalf("Unable to initialize kubernetes API clientset: %s", err.Error())
	}
	customClientset, err := clientset.NewForConfig(config)
	if err != nil {
		glog.Fatalf("Unable to initialize Custom Resource API clientset: %s", err.Error())
	}
	return kubeClientset, customClientset
}

// Run is an entry point of the application
func Run() {
	glog.V(1).Infof("Starting clickhouse-operator version '%s'\n", Version)

	// Prepare context with cancel function
	ctx, cancelFunc := context.WithCancel(context.Background())
	stop := make(chan os.Signal, 2)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		cancelFunc()
		<-stop
		os.Exit(1)
	}()

	// Create kube and chi clientsets out of proviede config
	kubeClient, chiClient := createClientsets()
	// Create kube and chi informer factories out of clientsets
	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeClient, time.Second*30)
	chiInformerFactory := informers.NewSharedInformerFactory(chiClient, time.Second*30)

	chiController := chi.CreateController(
		chiClient,
		kubeClient,
		chiInformerFactory.Clickhouse().V1().ClickHouseInstallations(),
		kubeInformerFactory.Apps().V1().StatefulSets(),
		kubeInformerFactory.Core().V1().ConfigMaps(),
		kubeInformerFactory.Core().V1().Services())

	// Start informers
	kubeInformerFactory.Start(ctx.Done())
	chiInformerFactory.Start(ctx.Done())

	// Main function
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		chiController.Run(ctx, 10)
	}()
	<-ctx.Done()
	wg.Wait()
}
