package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-logr/logr"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	k8sLabels "k8s.io/apimachinery/pkg/labels"
	apiMachineryRuntime "k8s.io/apimachinery/pkg/runtime"
	clientGoScheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlRuntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	ctrlController "sigs.k8s.io/controller-runtime/pkg/controller"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	controller "github.com/altinity/clickhouse-operator/pkg/controller/chk"
	chkLabeler "github.com/altinity/clickhouse-operator/pkg/model/chk/tags/labeler"
)

var (
	scheme  *apiMachineryRuntime.Scheme
	manager ctrlRuntime.Manager
	logger  logr.Logger
)

// newKeeperCacheOptions builds the manager cache config, narrowed to operator-generated objects.
//
// GetCacheNamespaces falls back to NamespaceAll unless every configured watch namespace is a
// literal DNS label - so an unset include list, or a single regexp among them, leaves this cache
// holding every object of every watched type in the cluster.
//
// Narrowed per type, NOT through DefaultLabelSelector, and the difference matters: this cache also
// backs manager.GetClient(), which the reconciler uses to read the ClickHouseKeeperInstallation
// itself - a user-authored object that carries no operator label. A cache-wide selector would make
// the controller unable to read the very CR it reconciles, and it would fail as a silent
// IsNotFound early-return rather than as an error.
//
// A filter is safe here only because of the rule the kube adapter enforces - list through the
// cache, get live - which NewAdapter in pkg/controller/chk/kube states in full. In short: the
// informers end up holding only operator-generated objects, which is where the memory goes, while
// no by-name read can mistake the filter for an absence.
//
// Narrowed is every type a Keeper reconcile actually reads through this cache, plus a margin:
// Pods no longer have a cached read at all now that by-name Gets are live, but an informer starts
// only on first use, so an entry for a type nobody reads costs nothing and means the next cached
// read added here is filtered by default rather than by remembering to.
//
// Three types are deliberately left out. The CR must stay readable, as above. Deployments and
// ReplicaSets have Get methods on the same adapter but no caller anywhere on the Keeper path, so
// their informers never start - narrowing them would be config for a read that does not exist.
//
// A CR that sets the operator's own app label in its metadata overrides the generated value, so
// its objects fall out of this selector - and out of SelectorCRScope discovery. That installation
// cannot run regardless: the StatefulSet's spec.selector is built from getSelectorHostScope, which
// ignores CR-provided labels, so the overridden pod template no longer matches its own selector
// and the API server refuses the StatefulSet.
//
// log is a parameter rather than the package-level logger because a logr zero value discards Info
// silently, so reading that var would let this line vanish if the sole call site ever moved.
func newKeeperCacheOptions(log logr.Logger, cacheNamespaces []string) cache.Options {
	defaultNamespaces := make(map[string]cache.Config)
	for _, ns := range cacheNamespaces {
		defaultNamespaces[ns] = cache.Config{}
	}

	selector := k8sLabels.SelectorFromSet(chkLabeler.New(nil).CHOPGeneratedSelector())
	byObject := make(map[client.Object]cache.ByObject)
	for _, object := range []client.Object{
		&apps.StatefulSet{},
		&core.Pod{},
		&core.ConfigMap{},
		&core.Secret{},
		&core.Service{},
		&core.PersistentVolumeClaim{},
		&policy.PodDisruptionBudget{},
	} {
		byObject[object] = cache.ByObject{Label: selector}
	}

	// As in NewInformerFactoryForCHOPGeneratedObjects, a wrong selector here fails silently -
	// the cache just stays empty - so the effective one goes in the boot log.
	log.Info("init keeper - cache label selector",
		"selector", selector.String(), "types", len(byObject))

	return cache.Options{
		// GetCacheNamespaces returns exact namespace names when all configured watch namespaces are
		// valid DNS labels, enabling per-namespace cache scoping. Falls back to NamespaceAll when
		// the include list is unset or any watch namespace is a regexp pattern (the controller's
		// Reconcile guard handles filtering in that case). ByObject entries leave Namespaces nil,
		// so they inherit this scoping.
		DefaultNamespaces: defaultNamespaces,
		ByObject:          byObject,
	}
}

// errKeeperInitTerminal marks an initKeeper failure that no retry can clear, as opposed to one
// that merely reflects the state of the API server at this instant. initKeeper attaches it to its
// own deterministic failures so the retry never has to infer intent from an error's shape.
var errKeeperInitTerminal = errors.New("keeper init cannot succeed on retry")

// newKeeperInitializer resolves the clients that must be built exactly once, then returns the step
// that is safe to retry.
//
// chop.GetClientset terminates the process when it cannot build a client, so running it on every
// attempt would put an os.Exit inside a loop whose whole purpose is not to exit - a transient read
// of the ServiceAccount token hours into a healthy run would take ClickHouse down with it. Run
// here, it is start-up behaviour, and initClickHouse has already made the identical call by this
// point, so a failure would have terminated the process long before.
func newKeeperInitializer() func(context.Context) error {
	// Build the apiextensions client for CRD deletion checks during CHK cleanup.
	// Uses the same kubeConfigFile/masterURL package vars as the CHI thread.
	_, extClient, _, _ := chop.GetClientset(kubeConfigFile, masterURL, chopConfigFile)

	return func(ctx context.Context) error {
		return initKeeper(ctx, extClient)
	}
}

func initKeeper(ctx context.Context, extClient *apiextensions.Clientset) error {
	var err error

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	logger = ctrl.Log.WithName("keeper-runner")

	// Scheme registration is compiled-in and touches no API server, so a failure here is a
	// programming fault: it will fail identically on every retry. NewManager below is the one step
	// whose outcome depends on the API server's current state - it resolves the StatefulSet REST
	// mapping eagerly - and is therefore the one worth retrying.
	scheme = apiMachineryRuntime.NewScheme()
	if err = clientGoScheme.AddToScheme(scheme); err != nil {
		logger.Error(err, "init keeper - unable to clientGoScheme.AddToScheme")
		return fmt.Errorf("%w: %w", errKeeperInitTerminal, err)
	}
	if err = api.AddToScheme(scheme); err != nil {
		logger.Error(err, "init keeper - unable to api.AddToScheme")
		return fmt.Errorf("%w: %w", errKeeperInitTerminal, err)
	}

	// GetConfig, not GetConfigOrDie: the latter exits the process, which is exactly what this
	// retry exists to avoid. A config that cannot be built right now may build on the next attempt.
	restConfig, err := ctrlRuntime.GetConfig()
	if err != nil {
		logger.Error(err, "init keeper - unable to ctrlRuntime.GetConfig")
		return err
	}

	manager, err = ctrlRuntime.NewManager(restConfig, ctrlRuntime.Options{
		Scheme: scheme,
		Cache:  newKeeperCacheOptions(logger, chop.Config().GetCacheNamespaces()),
		// Disable controller-runtime's built-in metrics listener. Default is ":8080" which
		// would bind a third HTTP endpoint on the operator pod (not in any Service, not in
		// any pod annotation, not in any containerPort) — an orphan reachable only by direct
		// PodIP routing. The operator's own /metrics endpoint already lives at :9999 via
		// pkg/metrics/operator, and the CHK reconcile counters worth exposing are surfaced
		// through that path, not through controller-runtime's manager-default exposition.
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		// Deliberately NOT wrapped as terminal: this is the one step whose outcome depends on the
		// API server, and so the one the retry exists for.
		logger.Error(err, "init keeper - unable to ctrlRuntime.NewManager")
		return err
	}

	maxConcurrentReconciles := chop.Config().Reconcile.Runtime.ReconcileCHKsThreadsNumber
	logger.Info("init keeper - CHK controller concurrency", "maxConcurrentReconciles", maxConcurrentReconciles)

	err = ctrlRuntime.
		NewControllerManagedBy(manager).
		For(
			&api.ClickHouseKeeperInstallation{},
			builder.WithPredicates(keeperPredicate()),
		).
		Owns(&apps.StatefulSet{}).
		WithOptions(ctrlController.Options{
			MaxConcurrentReconciles: maxConcurrentReconciles,
		}).
		Complete(
			controller.NewController(
				manager.GetClient(),
				manager.GetAPIReader(),
				manager.GetScheme(),
				extClient,
			),
		)
	if err != nil {
		// Building the controller resolves the scheme and records watches; it issues no API calls,
		// so like the scheme registration above this can only be a programming fault.
		logger.Error(err, "init keeper - unable to ctrlRuntime.NewControllerManagedBy")
		return fmt.Errorf("%w: %w", errKeeperInitTerminal, err)
	}

	// Initialization successful
	return nil
}

func runKeeper(ctx context.Context) error {
	if err := manager.Start(ctx); err != nil {
		logger.Error(err, "run keeper - unable to manager.Start")
		return err
	}
	// Run successful
	return nil
}

func keeperPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			new, ok := e.Object.(*api.ClickHouseKeeperInstallation)
			if !ok {
				return false
			}

			if !controller.ShouldEnqueue(new) {
				return false
			}

			return true
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			new, ok := e.ObjectNew.(*api.ClickHouseKeeperInstallation)
			if !ok {
				return false
			}

			if !controller.ShouldEnqueue(new) {
				return false
			}

			return true
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return true
		},
	}
}
