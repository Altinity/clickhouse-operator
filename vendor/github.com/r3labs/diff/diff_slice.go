/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package diff

import (
	"reflect"
)

func (cl *Changelog) diffSlice(path []string, a, b reflect.Value) error {
	if a.Kind() != b.Kind() {
		return ErrTypeMismatch
	}

	if comparative(a, b) {
		return cl.diffSliceComparative(path, a, b)
	}

	return cl.diffSliceGeneric(path, a, b)
}

func (cl *Changelog) diffSliceGeneric(path []string, a, b reflect.Value) error {
	missing := NewComparativeList()

	for i := 0; i < a.Len(); i++ {
		ae := a.Index(i)

		if !sliceHas(b, ae) {
			missing.addA(i, &ae)
		}
	}

	for i := 0; i < b.Len(); i++ {
		be := b.Index(i)

		if !sliceHas(a, be) {
			missing.addB(i, &be)
		}
	}

	// fallback to comparing based on order in slice if item is missing
	if len(missing.keys) == 0 {
		return nil
	}

	return cl.diffComparative(path, missing)
}

func (cl *Changelog) diffSliceComparative(path []string, a, b reflect.Value) error {
	c := NewComparativeList()

	for i := 0; i < a.Len(); i++ {
		ae := a.Index(i)
		ak := getFinalValue(ae)

		id := identifier(ak)
		if id != nil {
			c.addA(id, &ae)
		}
	}

	for i := 0; i < b.Len(); i++ {
		be := b.Index(i)
		bk := getFinalValue(be)

		id := identifier(bk)
		if id != nil {
			c.addB(id, &be)
		}
	}

	return cl.diffComparative(path, c)
}

func sliceHas(s, v reflect.Value) bool {
	for i := 0; i < s.Len(); i++ {
		x := s.Index(i)
		if reflect.DeepEqual(x.Interface(), v.Interface()) {
			return true
		}
	}

	return false
}

func getFinalValue(t reflect.Value) reflect.Value {
	switch t.Kind() {
	case reflect.Interface:
		return getFinalValue(t.Elem())
	case reflect.Ptr:
		return getFinalValue(reflect.Indirect(t))
	default:
		return t
	}
}
