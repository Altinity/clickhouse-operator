/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package diff

import (
	"fmt"
	"reflect"
)

func (cl *Changelog) diffMap(path []string, a, b reflect.Value) error {
	if a.Kind() == reflect.Invalid {
		return cl.mapValues(CREATE, path, b)
	}

	if b.Kind() == reflect.Invalid {
		return cl.mapValues(DELETE, path, a)
	}

	c := NewComparativeList()

	for _, k := range a.MapKeys() {
		ae := a.MapIndex(k)
		c.addA(k.Interface(), &ae)
	}

	for _, k := range b.MapKeys() {
		be := b.MapIndex(k)
		c.addB(k.Interface(), &be)
	}

	return cl.diffComparative(path, c)
}

func (cl *Changelog) mapValues(t string, path []string, a reflect.Value) error {
	if t != CREATE && t != DELETE {
		return ErrInvalidChangeType
	}

	if a.Kind() == reflect.Ptr {
		a = reflect.Indirect(a)
	}

	if a.Kind() != reflect.Map {
		return ErrTypeMismatch
	}

	x := reflect.New(a.Type()).Elem()

	for _, k := range a.MapKeys() {
		ae := a.MapIndex(k)
		xe := x.MapIndex(k)

		err := cl.diff(append(path, fmt.Sprint(k.Interface())), xe, ae)
		if err != nil {
			return err
		}
	}

	for i := 0; i < len(*cl); i++ {
		(*cl)[i] = swapChange(t, (*cl)[i])
	}

	return nil
}
