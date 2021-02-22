package litter

import (
	"reflect"
	"sort"
)

// mapReusedPointers takes a structure, and recursively maps all pointers mentioned in the tree,
// detecting circular references, and providing a list of all pointers that was referenced at
// least twice by the provided structure.
func mapReusedPointers(v reflect.Value) ptrmap {
	pm := &pointerVisitor{}
	pm.consider(v)
	return pm.reused
}

// A map of pointers.
type (
	ptrinfo struct {
		order int
	}
	ptrmap map[uintptr]ptrinfo
)

// Returns true if contains a pointer.
func (pm *ptrmap) contains(p uintptr) bool {
	if *pm != nil {
		_, ok := (*pm)[p]
		return ok
	}
	return false
}

// Removes a pointer.
func (pm *ptrmap) remove(p uintptr) {
	if *pm != nil {
		delete(*pm, p)
	}
}

// Adds a pointer.
func (pm *ptrmap) add(p uintptr) bool {
	if pm.contains(p) {
		return false
	}
	pm.put(p)
	return true
}

// Adds a pointer (slow path).
func (pm *ptrmap) put(p uintptr) {
	if *pm == nil {
		*pm = make(map[uintptr]ptrinfo, 31)
	}
	(*pm)[p] = ptrinfo{order: len(*pm)}
}

type pointerVisitor struct {
	pointers ptrmap
	reused   ptrmap
}

// Recursively consider v and each of its children, updating the map according to the
// semantics of MapReusedPointers
func (pv *pointerVisitor) consider(v reflect.Value) {
	if v.Kind() == reflect.Invalid {
		return
	}
	if isPointerValue(v) && v.Pointer() != 0 { // pointer is 0 for unexported fields
		if pv.tryAddPointer(v.Pointer()) {
			// No use descending inside this value, since it have been seen before and all its descendants
			// have been considered
			return
		}
	}

	// Now descend into any children of this value
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		numEntries := v.Len()
		for i := 0; i < numEntries; i++ {
			pv.consider(v.Index(i))
		}

	case reflect.Interface:
		pv.consider(v.Elem())

	case reflect.Ptr:
		pv.consider(v.Elem())

	case reflect.Map:
		keys := v.MapKeys()
		sort.Sort(mapKeySorter{
			keys:    keys,
			options: &Config,
		})
		for _, key := range keys {
			pv.consider(v.MapIndex(key))
		}

	case reflect.Struct:
		numFields := v.NumField()
		for i := 0; i < numFields; i++ {
			pv.consider(v.Field(i))
		}
	}
}

// addPointer to the pointerMap, update reusedPointers. Returns true if pointer was reused
func (pv *pointerVisitor) tryAddPointer(p uintptr) bool {
	// Is this allready known to be reused?
	if pv.reused.contains(p) {
		return true
	}

	// Have we seen it once before?
	if pv.pointers.contains(p) {
		// Add it to the register of pointers we have seen more than once
		pv.reused.add(p)
		return true
	}

	// This pointer was new to us
	pv.pointers.add(p)
	return false
}
