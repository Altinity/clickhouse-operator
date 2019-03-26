/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package diff

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
)

var (
	// ErrTypeMismatch : Compared types do not match
	ErrTypeMismatch = errors.New("types do not match")
	// ErrInvalidChangeType : The specified change values are not unsupported
	ErrInvalidChangeType = errors.New("change type must be one of 'create' or 'delete'")
)

const (
	// CREATE : represents when an element has been added
	CREATE = "create"
	// UPDATE : represents when an element has been updated
	UPDATE = "update"
	// DELETE : represents when an element has been removed
	DELETE = "delete"
)

// Changelog : stores a list of changed items
type Changelog []Change

// Change : stores information about a changed item
type Change struct {
	Type string      `json:"type"`
	Path []string    `json:"path"`
	From interface{} `json:"from"`
	To   interface{} `json:"to"`
}

// Changed : returns true if both values differ
func Changed(a, b interface{}) bool {
	cl, _ := Diff(a, b)
	return len(cl) > 0
}

// Diff : returns a changelog of all mutated values from both
func Diff(a, b interface{}) (Changelog, error) {
	var cl Changelog

	return cl, cl.diff([]string{}, reflect.ValueOf(a), reflect.ValueOf(b))
}

// StructValues : gets all values from a struct
// values are stored as "created" or "deleted" entries in the changelog,
// depending on the change type specified
func StructValues(t string, path []string, s interface{}) (Changelog, error) {
	var cl Changelog

	v := reflect.ValueOf(s)

	return cl, cl.structValues(t, path, v)
}

// Filter : filter changes based on path. Paths may contain valid regexp to match items
func (cl *Changelog) Filter(path []string) Changelog {
	var ncl Changelog

	for _, c := range *cl {
		if pathmatch(path, c.Path) {
			ncl = append(ncl, c)
		}
	}

	return ncl
}

func (cl *Changelog) diff(path []string, a, b reflect.Value) error {
	// check if types match or are
	if invalid(a, b) {
		return ErrTypeMismatch
	}

	switch {
	case are(a, b, reflect.Struct, reflect.Invalid):
		return cl.diffStruct(path, a, b)
	case are(a, b, reflect.Slice, reflect.Invalid):
		return cl.diffSlice(path, a, b)
	case are(a, b, reflect.String, reflect.Invalid):
		return cl.diffString(path, a, b)
	case are(a, b, reflect.Bool, reflect.Invalid):
		return cl.diffBool(path, a, b)
	case are(a, b, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Invalid):
		return cl.diffInt(path, a, b)
	case are(a, b, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Invalid):
		return cl.diffUint(path, a, b)
	case are(a, b, reflect.Float32, reflect.Float64, reflect.Invalid):
		return cl.diffFloat(path, a, b)
	case are(a, b, reflect.Map, reflect.Invalid):
		return cl.diffMap(path, a, b)
	case are(a, b, reflect.Ptr, reflect.Invalid):
		return cl.diffPtr(path, a, b)
	case are(a, b, reflect.Interface, reflect.Invalid):
		return cl.diffInterface(path, a, b)
	default:
		return errors.New("unsupported type: " + a.Kind().String())
	}
}

func (cl *Changelog) add(t string, path []string, from, to interface{}) {
	(*cl) = append((*cl), Change{
		Type: t,
		Path: path,
		From: from,
		To:   to,
	})
}

func tagName(f reflect.StructField) string {
	t := f.Tag.Get("diff")

	parts := strings.Split(t, ",")
	if len(parts) < 1 {
		return "-"
	}

	return parts[0]
}

func identifier(v reflect.Value) interface{} {
	for i := 0; i < v.NumField(); i++ {
		if hasTagOption(v.Type().Field(i), "identifier") {
			return v.Field(i).Interface()
		}
	}

	return nil
}

func hasTagOption(f reflect.StructField, opt string) bool {
	parts := strings.Split(f.Tag.Get("diff"), ",")
	if len(parts) < 2 {
		return false
	}

	for _, option := range parts[1:] {
		if option == opt {
			return true
		}
	}

	return false
}

func swapChange(t string, c Change) Change {
	nc := Change{
		Type: t,
		Path: c.Path,
	}

	switch t {
	case CREATE:
		nc.To = c.To
	case DELETE:
		nc.From = c.To
	}

	return nc
}

func idstring(v interface{}) string {
	switch v.(type) {
	case string:
		return v.(string)
	case int:
		return strconv.Itoa(v.(int))
	default:
		return ""
	}
}

func invalid(a, b reflect.Value) bool {
	if a.Kind() == b.Kind() {
		return false
	}

	if a.Kind() == reflect.Invalid {
		return false
	}
	if b.Kind() == reflect.Invalid {
		return false
	}

	return true
}

func are(a, b reflect.Value, kinds ...reflect.Kind) bool {
	var amatch, bmatch bool

	for _, k := range kinds {
		if a.Kind() == k {
			amatch = true
		}
		if b.Kind() == k {
			bmatch = true
		}
	}

	return amatch && bmatch
}

func areType(a, b reflect.Value, types ...reflect.Type) bool {
	var amatch, bmatch bool

	for _, t := range types {
		if a.Kind() != reflect.Invalid {
			if a.Type() == t {
				amatch = true
			}
		}
		if b.Kind() != reflect.Invalid {
			if b.Type() == t {
				bmatch = true
			}
		}
	}

	return amatch && bmatch
}

func copyAppend(src []string, elems ...string) []string {
	dst := make([]string, len(src)+len(elems))
	copy(dst, src)
	for i := len(src); i < len(src)+len(elems); i++ {
		dst[i] = elems[i-len(src)]
	}
	return dst
}
