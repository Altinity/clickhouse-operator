package model

import (
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1"
)

const testNamespace = "mynamespace"
const otherNamespace = "othernamespace"

var testCmA = v1.ObjectMeta{
	Name:      "configmap-A",
	Namespace: testNamespace,
}

var testCmB = v1.ObjectMeta{
	Name:      "configmap-B",
	Namespace: testNamespace,
}

// This fixture provides some coverage for the usage of namespace *and* name in identifying ObjectMeta entries.
// Note that the name *intentionally* collides with testCmA, but the namespace is different.
var testCmAOtherNamespace = v1.ObjectMeta{
	Name:      "configmap-A",
	Namespace: otherNamespace,
}

var testPvcA = v1.ObjectMeta{
	Name:      "persistentvolumeclaim-A",
	Namespace: testNamespace,
}

var testPvcB = v1.ObjectMeta{
	Name:      "persistentvolumeclaim-B",
	Namespace: testNamespace,
}

var testPvcC = v1.ObjectMeta{
	Name:      "persistentvolumeclaim-C",
	Namespace: testNamespace,
}

var testPvcD = v1.ObjectMeta{
	Name:      "persistentvolumeclaim-D",
	Namespace: testNamespace,
}

// NB: These tests mostly exist to exercise synchronization and detect regressions related to them via the
// Golang race detector. See: https://go.dev/blog/race-detector
// In short, add -race to the go test flags when running this.
func Test_Registry_BasicOperations_ConcurrencyTest(t *testing.T) {
	reg := NewRegistry()
	if got := reg.Len(); got != 0 {
		t.Errorf("New registry should have 0 length, got %d", got)
	}

	// We'll execute two goroutines:
	// Goroutine 1 will add testCmA and testPvcA.
	// Goroutine 2 will add testCmA, testCmB, and testPvcB (some overlap to cover set properties of registry).
	// After this, we'll verify that the post conditions match the union of those operations (easy with our set property).
	startWg := sync.WaitGroup{}
	doneWg := sync.WaitGroup{}
	startWg.Add(2) // We will make sure both goroutines begin execution, i.e., that they don't execute sequentially.
	doneWg.Add(2)  // We also need to synchronize the test over the completion of both.

	go func() {
		startWg.Done()
		startWg.Wait() // Block until the other goroutine has begun execution
		reg.RegisterConfigMap(&testCmA)
		reg.RegisterPVC(&testPvcA)
		doneWg.Done()
	}()

	go func() {
		startWg.Done()
		startWg.Wait() // Block until the other goroutine has begun execution
		reg.RegisterConfigMap(&testCmA)
		reg.RegisterConfigMap(&testCmAOtherNamespace)
		reg.RegisterConfigMap(&testCmB)
		reg.RegisterPVC(&testPvcB)
		doneWg.Done()
	}()

	doneWg.Wait() // Block until both goroutines have completed execution

	// Verify the state of the Registry
	if got := reg.Len(ConfigMap); got != 3 {
		t.Errorf("Expected registry to have 3 config maps, got %d", got)
	}
	if got := reg.Len(PVC); got != 2 {
		t.Errorf("Expected registry to have 2 PVCs, got %d", got)
	}
	for expectedMetaObj, expectedEntityType := range map[*v1.ObjectMeta]EntityType{
		&testCmA:               ConfigMap,
		&testCmAOtherNamespace: ConfigMap,
		&testCmB:               ConfigMap,
		&testPvcA:              PVC,
		&testPvcB:              PVC,
	} {
		if got := reg.hasEntity(expectedEntityType, expectedMetaObj); !got {
			t.Errorf(
				"Expected registry to contain entity type %s:{Namespace = %s, Name = %s}",
				expectedEntityType,
				expectedMetaObj.Namespace,
				expectedMetaObj.Name,
			)
		}
	}

	// We'll reset both wait groups and perform some additional operations, including deletions.
	startWg.Add(2)
	doneWg.Add(2)

	go func() {
		startWg.Done()
		startWg.Wait()                                      // Block until the other goroutine has begun execution
		reg.RegisterPVC(&testPvcD)                          // Add a net-new PVC (both goroutines)
		reg.deleteEntity(ConfigMap, &testCmAOtherNamespace) // Delete testCmAOtherNamespace (only this goroutine)
		reg.deleteEntity(ConfigMap, &testCmB)               // Delete testCmB (both goroutines)
		doneWg.Done()
	}()

	go func() {
		startWg.Done()
		startWg.Wait()                        // Block until the other goroutine has begun execution
		reg.RegisterPVC(&testPvcC)            // Add a net-new PVC (only this goroutine)
		reg.RegisterPVC(&testPvcD)            // Add a net-new PVC (both goroutines)
		reg.deleteEntity(ConfigMap, &testCmB) // Delete testCmB (both goroutines)
		reg.deleteEntity(PVC, &testPvcB)      // Delete testPvcB (only this goroutine)
		doneWg.Done()
	}()

	doneWg.Wait() // Block until both goroutines have completed execution

	// Verify the state of the Registry
	if got := reg.Len(ConfigMap); got != 1 {
		t.Errorf("Expected registry to have 1 config maps, got %d", got)
	}
	if got := reg.Len(PVC); got != 3 {
		t.Errorf("Expected registry to have 3 PVCs, got %d", got)
	}
	for expectedMetaObj, expectedEntityType := range map[*v1.ObjectMeta]EntityType{
		// We deleted testCmAOtherNamespace (one of the goroutines)
		// We deleted testCmB (from both goroutines)
		// We deleted testPvcB
		&testCmA:  ConfigMap, // We didn't touch testCmA
		&testPvcA: PVC,       // We didn't touch testPvcA
		&testPvcC: PVC,       // We added testPvcC (one of the goroutines)
		&testPvcD: PVC,       // We added testPvcD (both goroutines tried)
	} {
		if got := reg.hasEntity(expectedEntityType, expectedMetaObj); !got {
			t.Errorf(
				"Expected registry to contain entity type %s:{Namespace = %s, Name = %s}",
				expectedEntityType,
				expectedMetaObj.Namespace,
				expectedMetaObj.Name,
			)
		}
	}

	// Finally, let's walk through the structure in two goroutines
	// We'll reset both wait groups and perform some additional operations, including deletions.
	startWg.Add(2)
	doneWg.Add(2)

	threadAObjsSeen := 0
	go func() {
		startWg.Done()
		startWg.Wait() // Block until the other goroutine has begun execution
		reg.Walk(func(entityType EntityType, meta v1.Object) {
			threadAObjsSeen++
		})
		doneWg.Done()
	}()

	threadBObjsSeen := 0
	go func() {
		startWg.Done()
		startWg.Wait() // Block until the other goroutine has begun execution
		reg.Walk(func(entityType EntityType, meta v1.Object) {
			threadBObjsSeen++
		})
		doneWg.Done()
	}()
	doneWg.Wait() // Block until both goroutines have completed execution

	if threadAObjsSeen == 0 || threadBObjsSeen == 0 {
		t.Errorf("Expected both goroutines to visit a nonzero number of objects via Walk")
	}
	if threadAObjsSeen != threadBObjsSeen {
		t.Errorf(
			"Expected both goroutines to see the same number of objects via Walk, got a = %d, b = %d",
			threadAObjsSeen,
			threadBObjsSeen,
		)
	}
}

// Test_Registry_ConcurrentReadersAndWriters runs readers concurrently with writers.
// The test above only ever pairs writers with writers, and its Walk phase runs after all
// mutation has finished, so a missing *read* lock is invisible to it: stripping the
// RLock/RUnlock pairs out of objectMetaSet leaves it green. This one reports a data race
// for that same edit, which is the more likely regression of the two.
func Test_Registry_ConcurrentReadersAndWriters(t *testing.T) {
	const iterations = 200

	reg := NewRegistry()
	// Pre-register so the readers exercise populated entity types, not only the
	// create-on-miss path, and so there is a stable entry to assert on at the end.
	reg.RegisterConfigMap(&testCmA)
	reg.RegisterPVC(&testPvcA)

	churn := func() {
		for i := 0; i < iterations; i++ {
			reg.RegisterConfigMap(&testCmB)
			reg.RegisterPVC(&testPvcB)
			reg.deleteEntity(ConfigMap, &testCmB)
			reg.deleteEntity(PVC, &testPvcB)
		}
	}

	// Point lookups on exactly the keys the writers add and remove.
	pointRead := func() {
		for i := 0; i < iterations; i++ {
			reg.hasEntity(ConfigMap, &testCmB)
			reg.hasEntity(PVC, &testPvcB)
		}
	}

	// Whole-map iteration during mutation, dereferencing what it yields - a bare count
	// would never touch the shared objects the registry hands out.
	iterate := func() {
		for i := 0; i < iterations; i++ {
			reg.Walk(func(entityType EntityType, meta v1.Object) {
				_ = meta.GetName()
				_ = meta.GetNamespace()
				_ = len(meta.GetLabels())
			})
			_ = reg.Len(ConfigMap)
		}
	}

	workers := []func(){churn, churn, pointRead, pointRead, iterate, iterate}

	startWg := sync.WaitGroup{}
	doneWg := sync.WaitGroup{}
	startWg.Add(len(workers))
	doneWg.Add(len(workers))
	for _, worker := range workers {
		go func(run func()) {
			startWg.Done()
			startWg.Wait() // Block until every goroutine has begun execution
			run()
			doneWg.Done()
		}(worker)
	}
	doneWg.Wait()

	// The pre-registered entries are never deleted, so they must survive the churn.
	if !reg.hasEntity(ConfigMap, &testCmA) {
		t.Errorf("expected %s to survive concurrent churn", testCmA.Name)
	}
	if !reg.hasEntity(PVC, &testPvcA) {
		t.Errorf("expected %s to survive concurrent churn", testPvcA.Name)
	}
}
