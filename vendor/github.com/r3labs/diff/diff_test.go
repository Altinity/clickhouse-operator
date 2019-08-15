/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package diff

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var currentTime = time.Now()

var struct1 = tmstruct{Bar: 1, Foo: "one"}
var struct2 = tmstruct{Bar: 2, Foo: "two"}

type tistruct struct {
	Name  string `diff:"name,identifier"`
	Value int    `diff:"value"`
}

type tuistruct struct {
	Value int `diff:"value"`
}

type tnstruct struct {
	Slice []tmstruct `diff:"slice"`
}

type tmstruct struct {
	Foo string `diff:"foo"`
	Bar int    `diff:"bar"`
}

type tstruct struct {
	ID              string            `diff:"id,immutable"`
	Name            string            `diff:"name"`
	Value           int               `diff:"value"`
	Bool            bool              `diff:"bool"`
	Values          []string          `diff:"values"`
	Map             map[string]string `diff:"map"`
	Time            time.Time         `diff:"time"`
	Pointer         *string           `diff:"pointer"`
	Ignored         bool              `diff:"-"`
	Identifiables   []tistruct        `diff:"identifiables"`
	Unidentifiables []tuistruct       `diff:"unidentifiables"`
	Nested          tnstruct          `diff:"nested"`
}

func sptr(s string) *string {
	return &s
}

func TestDiff(t *testing.T) {
	cases := []struct {
		Name      string
		A, B      interface{}
		Changelog Changelog
		Error     error
	}{
		{
			"int-slice-insert", []int{1, 2, 3}, []int{1, 2, 3, 4},
			Changelog{
				Change{Type: CREATE, Path: []string{"3"}, To: 4},
			},
			nil,
		},
		{
			"int-slice-delete", []int{1, 2, 3}, []int{1, 3},
			Changelog{
				Change{Type: DELETE, Path: []string{"1"}, From: 2},
			},
			nil,
		},
		{
			"int-slice-insert-delete", []int{1, 2, 3}, []int{1, 3, 4},
			Changelog{
				Change{Type: DELETE, Path: []string{"1"}, From: 2},
				Change{Type: CREATE, Path: []string{"2"}, To: 4},
			},
			nil,
		},
		{
			"string-slice-insert", []string{"1", "2", "3"}, []string{"1", "2", "3", "4"},
			Changelog{
				Change{Type: CREATE, Path: []string{"3"}, To: "4"},
			},
			nil,
		},
		{
			"string-slice-delete", []string{"1", "2", "3"}, []string{"1", "3"},
			Changelog{
				Change{Type: DELETE, Path: []string{"1"}, From: "2"},
			},
			nil,
		},
		{
			"string-slice-insert-delete", []string{"1", "2", "3"}, []string{"1", "3", "4"},
			Changelog{
				Change{Type: DELETE, Path: []string{"1"}, From: "2"},
				Change{Type: CREATE, Path: []string{"2"}, To: "4"},
			},
			nil,
		},
		{
			"comparable-slice-insert", []tistruct{{"one", 1}}, []tistruct{{"one", 1}, {"two", 2}},
			Changelog{
				Change{Type: CREATE, Path: []string{"two", "name"}, To: "two"},
				Change{Type: CREATE, Path: []string{"two", "value"}, To: 2},
			},
			nil,
		},
		{
			"comparable-slice-delete", []tistruct{{"one", 1}, {"two", 2}}, []tistruct{{"one", 1}},
			Changelog{
				Change{Type: DELETE, Path: []string{"two", "name"}, From: "two"},
				Change{Type: DELETE, Path: []string{"two", "value"}, From: 2},
			},
			nil,
		},
		{
			"comparable-slice-update", []tistruct{{"one", 1}}, []tistruct{{"one", 50}},
			Changelog{
				Change{Type: UPDATE, Path: []string{"one", "value"}, From: 1, To: 50},
			},
			nil,
		},
		{
			"map-slice-insert", []map[string]string{{"test": "123"}}, []map[string]string{{"test": "123", "tset": "456"}},
			Changelog{
				Change{Type: CREATE, Path: []string{"0", "tset"}, To: "456"},
			},
			nil,
		},
		{
			"map-slice-update", []map[string]string{{"test": "123"}}, []map[string]string{{"test": "456"}},
			Changelog{
				Change{Type: UPDATE, Path: []string{"0", "test"}, From: "123", To: "456"},
			},
			nil,
		},
		{
			"map-slice-delete", []map[string]string{{"test": "123", "tset": "456"}}, []map[string]string{{"test": "123"}},
			Changelog{
				Change{Type: DELETE, Path: []string{"0", "tset"}, From: "456"},
			},
			nil,
		},
		{
			"map-interface-slice-update", []map[string]interface{}{{"test": nil}}, []map[string]interface{}{{"test": "456"}},
			Changelog{
				Change{Type: UPDATE, Path: []string{"0", "test"}, From: nil, To: "456"},
			},
			nil,
		},
		{
			"map-nil", map[string]string{"one": "test"}, nil,
			Changelog{
				Change{Type: DELETE, Path: []string{"one"}, From: "test", To: nil},
			},
			nil,
		},
		{
			"nil-map", nil, map[string]string{"one": "test"},
			Changelog{
				Change{Type: CREATE, Path: []string{"one"}, From: nil, To: "test"},
			},
			nil,
		},
		{
			"nested-map-insert", map[string]map[string]string{"a": {"test": "123"}}, map[string]map[string]string{"a": {"test": "123", "tset": "456"}},
			Changelog{
				Change{Type: CREATE, Path: []string{"a", "tset"}, To: "456"},
			},
			nil,
		},
		{
			"nested-map-interface-insert", map[string]map[string]interface{}{"a": {"test": "123"}}, map[string]map[string]interface{}{"a": {"test": "123", "tset": "456"}},
			Changelog{
				Change{Type: CREATE, Path: []string{"a", "tset"}, To: "456"},
			},
			nil,
		},
		{
			"nested-map-update", map[string]map[string]string{"a": {"test": "123"}}, map[string]map[string]string{"a": {"test": "456"}},
			Changelog{
				Change{Type: UPDATE, Path: []string{"a", "test"}, From: "123", To: "456"},
			},
			nil,
		},
		{
			"nested-map-delete", map[string]map[string]string{"a": {"test": "123"}}, map[string]map[string]string{"a": {}},
			Changelog{
				Change{Type: DELETE, Path: []string{"a", "test"}, From: "123", To: nil},
			},
			nil,
		},
		{
			"nested-slice-insert", map[string][]int{"a": []int{1, 2, 3}}, map[string][]int{"a": []int{1, 2, 3, 4}},
			Changelog{
				Change{Type: CREATE, Path: []string{"a", "3"}, To: 4},
			},
			nil,
		},
		{
			"nested-slice-update", map[string][]int{"a": []int{1, 2, 3}}, map[string][]int{"a": []int{1, 4, 3}},
			Changelog{
				Change{Type: UPDATE, Path: []string{"a", "1"}, From: 2, To: 4},
			},
			nil,
		},
		{
			"nested-slice-delete", map[string][]int{"a": []int{1, 2, 3}}, map[string][]int{"a": []int{1, 3}},
			Changelog{
				Change{Type: DELETE, Path: []string{"a", "1"}, From: 2, To: nil},
			},
			nil,
		},
		{
			"struct-string-update", tstruct{Name: "one"}, tstruct{Name: "two"},
			Changelog{
				Change{Type: UPDATE, Path: []string{"name"}, From: "one", To: "two"},
			},
			nil,
		},
		{
			"struct-int-update", tstruct{Value: 1}, tstruct{Value: 50},
			Changelog{
				Change{Type: UPDATE, Path: []string{"value"}, From: 1, To: 50},
			},
			nil,
		},
		{
			"struct-bool-update", tstruct{Bool: true}, tstruct{Bool: false},
			Changelog{
				Change{Type: UPDATE, Path: []string{"bool"}, From: true, To: false},
			},
			nil,
		},
		{
			"struct-time-update", tstruct{}, tstruct{Time: currentTime},
			Changelog{
				Change{Type: UPDATE, Path: []string{"time"}, From: time.Time{}, To: currentTime},
			},
			nil,
		},
		{
			"struct-map-update", tstruct{Map: map[string]string{"test": "123"}}, tstruct{Map: map[string]string{"test": "456"}},
			Changelog{
				Change{Type: UPDATE, Path: []string{"map", "test"}, From: "123", To: "456"},
			},
			nil,
		},
		{
			"struct-string-pointer-update", tstruct{Pointer: sptr("test")}, tstruct{Pointer: sptr("test2")},
			Changelog{
				Change{Type: UPDATE, Path: []string{"pointer"}, From: "test", To: "test2"},
			},
			nil,
		},
		{
			"struct-nil-string-pointer-update", tstruct{Pointer: nil}, tstruct{Pointer: sptr("test")},
			Changelog{
				Change{Type: UPDATE, Path: []string{"pointer"}, From: nil, To: sptr("test")},
			},
			nil,
		},
		{
			"struct-generic-slice-insert", tstruct{Values: []string{"one"}}, tstruct{Values: []string{"one", "two"}},
			Changelog{
				Change{Type: CREATE, Path: []string{"values", "1"}, From: nil, To: "two"},
			},
			nil,
		},
		{
			"struct-identifiable-slice-insert", tstruct{Identifiables: []tistruct{{"one", 1}}}, tstruct{Identifiables: []tistruct{{"one", 1}, {"two", 2}}},
			Changelog{
				Change{Type: CREATE, Path: []string{"identifiables", "two", "name"}, From: nil, To: "two"},
				Change{Type: CREATE, Path: []string{"identifiables", "two", "value"}, From: nil, To: 2},
			},
			nil,
		},
		{
			"struct-generic-slice-delete", tstruct{Values: []string{"one", "two"}}, tstruct{Values: []string{"one"}},
			Changelog{
				Change{Type: DELETE, Path: []string{"values", "1"}, From: "two", To: nil},
			},
			nil,
		},
		{
			"struct-identifiable-slice-delete", tstruct{Identifiables: []tistruct{{"one", 1}, {"two", 2}}}, tstruct{Identifiables: []tistruct{{"one", 1}}},
			Changelog{
				Change{Type: DELETE, Path: []string{"identifiables", "two", "name"}, From: "two", To: nil},
				Change{Type: DELETE, Path: []string{"identifiables", "two", "value"}, From: 2, To: nil},
			},
			nil,
		},
		{
			"struct-unidentifiable-slice-insert-delete", tstruct{Unidentifiables: []tuistruct{{1}, {2}, {3}}}, tstruct{Unidentifiables: []tuistruct{{5}, {2}, {3}, {4}}},
			Changelog{
				Change{Type: UPDATE, Path: []string{"unidentifiables", "0", "value"}, From: 1, To: 5},
				Change{Type: CREATE, Path: []string{"unidentifiables", "3", "value"}, From: nil, To: 4},
			},
			nil,
		},
		{
			"mismatched-values-struct-map", map[string]string{"test": "one"}, &tstruct{Identifiables: []tistruct{{"one", 1}}},
			Changelog{},
			ErrTypeMismatch,
		},
		{
			"omittable", tstruct{Ignored: false}, tstruct{Ignored: true},
			Changelog{},
			nil,
		},
		{
			"slice", &tstruct{}, &tstruct{Nested: tnstruct{Slice: []tmstruct{{"one", 1}, {"two", 2}}}},
			Changelog{
				Change{Type: CREATE, Path: []string{"nested", "slice", "0", "foo"}, From: nil, To: "one"},
				Change{Type: CREATE, Path: []string{"nested", "slice", "0", "bar"}, From: nil, To: 1},
				Change{Type: CREATE, Path: []string{"nested", "slice", "1", "foo"}, From: nil, To: "two"},
				Change{Type: CREATE, Path: []string{"nested", "slice", "1", "bar"}, From: nil, To: 2},
			},
			nil,
		},
		{
			"slice-duplicate-items", []int{1}, []int{1, 1},
			Changelog{
				Change{Type: CREATE, Path: []string{"1"}, From: nil, To: 1},
			},
			nil,
		},
		{
			"mixed-slice-map", []map[string]interface{}{{"name": "name1", "type": []string{"null", "string"}}}, []map[string]interface{}{{"name": "name1", "type": []string{"null", "int"}}, {"name": "name2", "type": []string{"null", "string"}}},
			Changelog{
				Change{Type: UPDATE, Path: []string{"0", "type", "1"}, From: "string", To: "int"},
				Change{Type: CREATE, Path: []string{"1", "name"}, From: nil, To: "name2"},
				Change{Type: CREATE, Path: []string{"1", "type"}, From: nil, To: []string{"null", "string"}},
			},
			nil,
		},
		{
			"map-string-pointer-create",
			map[string]*tmstruct{"one": &struct1},
			map[string]*tmstruct{"one": &struct1, "two": &struct2},
			Changelog{
				Change{Type: CREATE, Path: []string{"two", "foo"}, From: nil, To: "two"},
				Change{Type: CREATE, Path: []string{"two", "bar"}, From: nil, To: 2},
			},
			nil,
		},
		{
			"map-string-pointer-delete",
			map[string]*tmstruct{"one": &struct1, "two": &struct2},
			map[string]*tmstruct{"one": &struct1},
			Changelog{
				Change{Type: DELETE, Path: []string{"two", "foo"}, From: "two", To: nil},
				Change{Type: DELETE, Path: []string{"two", "bar"}, From: 2, To: nil},
			},
			nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			cl, err := Diff(tc.A, tc.B)

			assert.Equal(t, tc.Error, err)
			require.Equal(t, len(tc.Changelog), len(cl))

			for i, c := range cl {
				assert.Equal(t, tc.Changelog[i].Type, c.Type)
				assert.Equal(t, tc.Changelog[i].Path, c.Path)
				assert.Equal(t, tc.Changelog[i].From, c.From)
				assert.Equal(t, tc.Changelog[i].To, c.To)
			}
		})
	}
}

func TestDiffSliceOrdering(t *testing.T) {
	cases := []struct {
		Name      string
		A, B      interface{}
		Changelog Changelog
		Error     error
	}{
		{
			"int-slice-insert-in-middle", []int{1, 2, 4}, []int{1, 2, 3, 4},
			Changelog{
				Change{Type: UPDATE, Path: []string{"2"}, From: 4, To: 3},
				Change{Type: CREATE, Path: []string{"3"}, To: 4},
			},
			nil,
		},
		{
			"int-slice-delete", []int{1, 2, 3}, []int{1, 3},
			Changelog{
				Change{Type: UPDATE, Path: []string{"1"}, From: 2, To: 3},
				Change{Type: DELETE, Path: []string{"2"}, From: 3},
			},
			nil,
		},
		{
			"int-slice-insert-delete", []int{1, 2, 3}, []int{1, 3, 4},
			Changelog{
				Change{Type: UPDATE, Path: []string{"1"}, From: 2, To: 3},
				Change{Type: UPDATE, Path: []string{"2"}, From: 3, To: 4},
			},
			nil,
		},
		{
			"int-slice-reorder", []int{1, 2, 3}, []int{1, 3, 2},
			Changelog{
				Change{Type: UPDATE, Path: []string{"1"}, From: 2, To: 3},
				Change{Type: UPDATE, Path: []string{"2"}, From: 3, To: 2},
			},
			nil,
		},
		{
			"string-slice-delete", []string{"1", "2", "3"}, []string{"1", "3"},
			Changelog{
				Change{Type: UPDATE, Path: []string{"1"}, From: "2", To: "3"},
				Change{Type: DELETE, Path: []string{"2"}, From: "3"},
			},
			nil,
		},
		{
			"string-slice-insert-delete", []string{"1", "2", "3"}, []string{"1", "3", "4"},
			Changelog{
				Change{Type: UPDATE, Path: []string{"1"}, From: "2", To: "3"},
				Change{Type: UPDATE, Path: []string{"2"}, From: "3", To: "4"},
			},
			nil,
		},
		{
			"string-slice-reorder", []string{"1", "2", "3"}, []string{"1", "3", "2"},
			Changelog{
				Change{Type: UPDATE, Path: []string{"1"}, From: "2", To: "3"},
				Change{Type: UPDATE, Path: []string{"2"}, From: "3", To: "2"},
			},
			nil,
		},
		{
			"nested-slice-delete", map[string][]int{"a": []int{1, 2, 3}}, map[string][]int{"a": []int{1, 3}},
			Changelog{
				Change{Type: UPDATE, Path: []string{"a", "1"}, From: 2, To: 3},
				Change{Type: DELETE, Path: []string{"a", "2"}, From: 3},
			},
			nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			d, err := NewDiffer(SliceOrdering(true))
			require.Nil(t, err)
			cl, err := d.Diff(tc.A, tc.B)

			assert.Equal(t, tc.Error, err)
			require.Equal(t, len(tc.Changelog), len(cl))

			for i, c := range cl {
				assert.Equal(t, tc.Changelog[i].Type, c.Type)
				assert.Equal(t, tc.Changelog[i].Path, c.Path)
				assert.Equal(t, tc.Changelog[i].From, c.From)
				assert.Equal(t, tc.Changelog[i].To, c.To)
			}
		})
	}

}

func TestFilter(t *testing.T) {
	cases := []struct {
		Name     string
		Filter   []string
		Expected [][]string
	}{
		{"simple", []string{"item-1", "subitem"}, [][]string{{"item-1", "subitem"}}},
		{"regex", []string{"item-*"}, [][]string{{"item-1", "subitem"}, {"item-2", "subitem"}}},
	}

	cl := Changelog{
		{Path: []string{"item-1", "subitem"}},
		{Path: []string{"item-2", "subitem"}},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			ncl := cl.Filter(tc.Filter)
			assert.Len(t, ncl, len(tc.Expected))
			for i, e := range tc.Expected {
				assert.Equal(t, e, ncl[i].Path)
			}
		})
	}
}

func TestStructValues(t *testing.T) {
	cases := []struct {
		Name       string
		ChangeType string
		X          interface{}
		Changelog  Changelog
		Error      error
	}{
		{
			"struct-create", CREATE, tstruct{ID: "xxxxx", Name: "something", Value: 1, Values: []string{"one", "two", "three"}},
			Changelog{
				Change{Type: CREATE, Path: []string{"id"}, From: nil, To: "xxxxx"},
				Change{Type: CREATE, Path: []string{"name"}, From: nil, To: "something"},
				Change{Type: CREATE, Path: []string{"value"}, From: nil, To: 1},
				Change{Type: CREATE, Path: []string{"values", "0"}, From: nil, To: "one"},
				Change{Type: CREATE, Path: []string{"values", "1"}, From: nil, To: "two"},
				Change{Type: CREATE, Path: []string{"values", "2"}, From: nil, To: "three"},
			},
			nil,
		},
		{
			"struct-delete", DELETE, tstruct{ID: "xxxxx", Name: "something", Value: 1, Values: []string{"one", "two", "three"}},
			Changelog{
				Change{Type: DELETE, Path: []string{"id"}, From: "xxxxx", To: nil},
				Change{Type: DELETE, Path: []string{"name"}, From: "something", To: nil},
				Change{Type: DELETE, Path: []string{"value"}, From: 1, To: nil},
				Change{Type: DELETE, Path: []string{"values", "0"}, From: "one", To: nil},
				Change{Type: DELETE, Path: []string{"values", "1"}, From: "two", To: nil},
				Change{Type: DELETE, Path: []string{"values", "2"}, From: "three", To: nil},
			},
			nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			cl, err := StructValues(tc.ChangeType, []string{}, tc.X)

			assert.Equal(t, tc.Error, err)
			assert.Equal(t, len(tc.Changelog), len(cl))

			for i, c := range cl {
				assert.Equal(t, tc.Changelog[i].Type, c.Type)
				assert.Equal(t, tc.Changelog[i].Path, c.Path)
				assert.Equal(t, tc.Changelog[i].From, c.From)
				assert.Equal(t, tc.Changelog[i].To, c.To)
			}
		})
	}
}

func TestDiffingOptions(t *testing.T) {
	d, err := NewDiffer(SliceOrdering(false))
	require.Nil(t, err)

	assert.False(t, d.SliceOrdering)

	cl, err := d.Diff([]int{1, 2, 3}, []int{1, 3, 2})
	require.Nil(t, err)

	assert.Len(t, cl, 0)

	d, err = NewDiffer(SliceOrdering(true))
	require.Nil(t, err)

	assert.True(t, d.SliceOrdering)

	cl, err = d.Diff([]int{1, 2, 3}, []int{1, 3, 2})
	require.Nil(t, err)

	assert.Len(t, cl, 2)

	// some other options..
}
