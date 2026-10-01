// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/imdario/mergo"
	core "k8s.io/api/core/v1"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
)

// ErrStrategicMerge is returned when two k8s objects can not be merged by strategic-merge semantics
var ErrStrategicMerge = errors.New("strategic merge failed")

// retainKeysStrategy mirrors the unexported k8s patch-strategy name under which a keyed list element carries only
// the keys the winner sets
const retainKeysStrategy = "retainKeys"

// HasValuer is a nil-able scalar which knows whether it carries a value
type HasValuer interface {
	HasValue() bool
}

// MergeScalar resolves one nil-able scalar (String, StringBool, Int32, ...) according to merge direction.
// Every leaf that takes a MergeType resolves its scalars here, so that no such leaf can pick its own direction.
func MergeScalar[T HasValuer](to, from T, _type MergeType) T {
	switch _type {
	case MergeTypeOverrideByNonEmptyValues:
		if from.HasValue() {
			return from
		}
		return to
	default:
		if to.HasValue() {
			return to
		}
		return from
	}
}

// MergeValue resolves one plain value (string, int, ...) according to merge direction, zero value meaning "not set"
func MergeValue[T comparable](to, from T, _type MergeType) T {
	var empty T
	switch _type {
	case MergeTypeOverrideByNonEmptyValues:
		if from != empty {
			return from
		}
		return to
	default:
		if to != empty {
			return to
		}
		return from
	}
}

// MergoOptions is the only mapping from merge direction onto mergo behaviour
func MergoOptions(_type MergeType) []func(*mergo.Config) {
	switch _type {
	case MergeTypeOverrideByNonEmptyValues:
		return []func(*mergo.Config){mergo.WithOverride}
	default:
		return nil
	}
}

// mergeStrategic merges two json-tagged objects the way kubernetes merges its own API objects:
// keyed lists (containers, env, volumes, ports, ...) are paired by their merge key, maps are merged per key,
// and everything else present on the winning side replaces the losing side.
// Direction decides which side wins: `from` for override, `to` for fill-empty.
// The receiver keeps its original value when the merge fails.
func mergeStrategic[T any](to, from *T, _type MergeType) error {
	winner, loser := from, to
	if _type != MergeTypeOverrideByNonEmptyValues {
		winner, loser = to, from
	}

	loserMap, err := toJSONMap(loser)
	if err != nil {
		return err
	}
	winnerMap, err := toJSONMap(winner)
	if err != nil {
		return err
	}
	schema, err := strategicpatch.NewPatchMetaFromStruct(to)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrStrategicMerge, err)
	}
	keyPortsByProtocol(loserMap, schema)
	keyPortsByProtocol(winnerMap, schema)
	pruneRetainKeys(loserMap, winnerMap, schema)

	// The patch is built out of the winner alone: a two-way diff against the loser would also
	// emit deletions for every key the winner leaves unset, and unset means "keep the loser's".
	patch, err := strategicpatch.CreateTwoWayMergeMapPatchUsingLookupPatchMeta(strategicpatch.JSONMap{}, winnerMap, schema)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrStrategicMerge, err)
	}
	mergedMap, err := strategicpatch.StrategicMergeMapPatchUsingLookupPatchMeta(loserMap, patch, schema)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrStrategicMerge, err)
	}
	// k8s returns each keyed list in the winner's order, with what only the loser had slotted after
	// it unless a shared element anchors it earlier - which would move an installation's containers
	// ahead of the sidecars its template listed and its env ahead of the template's; the stack
	// order is restored instead. Both sides are re-read from the structs because the merge rewrites
	// the loser's map in place - the receiver's, under override - so it no longer records the order
	// the receiver listed its elements in.
	toMap, err := toJSONMap(to)
	if err != nil {
		return err
	}
	fromMap, err := toJSONMap(from)
	if err != nil {
		return err
	}
	keyPortsByProtocol(toMap, schema)
	keyPortsByProtocol(fromMap, schema)
	restoreStackOrder(mergedMap, toMap, fromMap, schema)
	if err := unkeyPortsByProtocol(mergedMap, schema); err != nil {
		return err
	}

	mergedJSON, err := json.Marshal(mergedMap)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrStrategicMerge, err)
	}
	var merged T
	if err := json.Unmarshal(mergedJSON, &merged); err != nil {
		return fmt.Errorf("%w: %w", ErrStrategicMerge, err)
	}
	*to = merged
	return nil
}

// toJSONMap renders an object as a generic json map with null-valued keys removed,
// because a null in a strategic-merge patch is a delete directive, not an absent value.
func toJSONMap(obj any) (strategicpatch.JSONMap, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrStrategicMerge, err)
	}
	m := strategicpatch.JSONMap{}
	// apimachinery's decoder keeps integers as int64 where encoding/json would widen them to float64
	if err := utiljson.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrStrategicMerge, err)
	}
	pruneNulls(m)
	pruneZeroStructs(reflect.ValueOf(obj), m)
	return m, nil
}

// pruneZeroStructs drops from m the field JSON wrote for a zero-valued struct tagged omitempty, when
// that struct marshalled to anything but an object.
// Go's omitempty never omits a struct, so an unset intstr.IntOrString or resource.Quantity would
// travel as a zero value and override the other side's, where every other unset field is omitted
// and loses to it - the whole meaning of override-by-non-empty. Scalars are left alone: omitempty
// already drops their zero values.
func pruneZeroStructs(v reflect.Value, m map[string]any) {
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}
	if (v.Kind() != reflect.Struct) || (m == nil) {
		return
	}
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		name, options, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "-" {
			continue
		}
		value := v.Field(i)
		if field.Anonymous && (name == "") {
			// an embedded struct with no name of its own writes its fields into m itself
			pruneZeroStructs(value, m)
			continue
		}
		if name == "" {
			name = field.Name
		}
		if (value.Kind() == reflect.Struct) && hasJSONOption(options, jsonOptionOmitEmpty) && value.IsZero() {
			// Only a zero struct that marshalled to anything but an object is dropped: that value is what would
			// override. An empty object merges as a no-op - and inside a retain-keys list it is what
			// keeps the loser's same-named field from being stripped.
			if _, isObject := m[name].(map[string]any); !isObject {
				delete(m, name)
				continue
			}
		}
		pruneZeroStructsIn(value, m[name])
	}
}

// pruneZeroStructsIn descends into a struct or a list of structs; maps carry their values as
// entries, where a zero value is an entry someone wrote, so they are not descended into
func pruneZeroStructsIn(v reflect.Value, raw any) {
	switch typed := raw.(type) {
	case map[string]any:
		pruneZeroStructs(v, typed)
	case []any:
		for v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return
			}
			v = v.Elem()
		}
		if (v.Kind() != reflect.Slice) && (v.Kind() != reflect.Array) {
			return
		}
		for j := 0; (j < v.Len()) && (j < len(typed)); j++ {
			pruneZeroStructsIn(v.Index(j), typed[j])
		}
	}
}

const jsonOptionOmitEmpty = "omitempty"

// hasJSONOption tells whether a json tag's option list carries the option
func hasJSONOption(options, option string) bool {
	for _, o := range strings.Split(options, ",") {
		if o == option {
			return true
		}
	}
	return false
}

// pruneNulls removes null-valued keys recursively
func pruneNulls(m map[string]any) {
	for key, value := range m {
		switch typed := value.(type) {
		case nil:
			delete(m, key)
		case map[string]any:
			pruneNulls(typed)
		case []any:
			for _, item := range typed {
				if nested, ok := item.(map[string]any); ok {
					pruneNulls(nested)
				}
			}
		}
	}
}

// Strategic merge keys port lists on the number alone, while k8s declares a port's identity as
// number and protocol - the list-map keys server-side apply pairs on - so the same port on TCP
// and on UDP is two ports. Pairing on the number folds one protocol into the other and leaves two
// ports under one name, which the API server rejects. While merging, the merge-key value of every
// port is therefore stood in for by "<number>/<protocol>".
const (
	containerPortMergeKey = "containerPort"
	servicePortMergeKey   = "port"
	portProtocolField     = "protocol"
	portIdentitySeparator = "/"
)

// isPortMergeKey tells whether a keyed list is a port list, whose identity includes the protocol
func isPortMergeKey(mergeKey string) bool {
	return (mergeKey == containerPortMergeKey) || (mergeKey == servicePortMergeKey)
}

// keyPortsByProtocol rewrites the merge-key value of every port in m to its full identity. An
// omitted protocol is k8s' default, TCP, so a port written without one pairs with its TCP self.
func keyPortsByProtocol(m map[string]any, schema strategicpatch.LookupPatchMeta) {
	walkKeyedLists(m, schema, func(mergeKey string, elem map[string]any) {
		if !isPortMergeKey(mergeKey) {
			return
		}
		protocol, _ := elem[portProtocolField].(string)
		if protocol == "" {
			protocol = string(core.ProtocolTCP)
		}
		elem[mergeKey] = fmt.Sprintf("%v%s%s", elem[mergeKey], portIdentitySeparator, protocol)
	})
}

// unkeyPortsByProtocol puts the port numbers keyPortsByProtocol stood in for back
func unkeyPortsByProtocol(m map[string]any, schema strategicpatch.LookupPatchMeta) error {
	var err error
	walkKeyedLists(m, schema, func(mergeKey string, elem map[string]any) {
		if !isPortMergeKey(mergeKey) {
			return
		}
		identity, ok := elem[mergeKey].(string)
		if !ok {
			return
		}
		number, _, _ := strings.Cut(identity, portIdentitySeparator)
		value, parseErr := strconv.ParseInt(number, 10, 64)
		if parseErr != nil {
			err = fmt.Errorf("%w: port identity %q: %w", ErrStrategicMerge, identity, parseErr)
			return
		}
		elem[mergeKey] = value
	})
	return err
}

// walkKeyedLists calls visit for every element of every keyed list in m, at any depth
func walkKeyedLists(m map[string]any, schema strategicpatch.LookupPatchMeta, visit func(mergeKey string, elem map[string]any)) {
	for key, value := range m {
		switch typed := value.(type) {
		case map[string]any:
			if sub, _, err := schema.LookupPatchMetadataForStruct(key); err == nil {
				walkKeyedLists(typed, sub, visit)
			}
		case []any:
			sub, meta, err := schema.LookupPatchMetadataForSlice(key)
			if err != nil {
				continue
			}
			mergeKey := meta.GetPatchMergeKey()
			for _, item := range typed {
				elem, ok := item.(map[string]any)
				if !ok {
					continue
				}
				if mergeKey != "" {
					visit(mergeKey, elem)
				}
				walkKeyedLists(elem, sub, visit)
			}
		}
	}
}

// restoreStackOrder puts every keyed list of a merged map back into the order the layers were
// stacked in: the receiver's elements first, as it listed them, then what only the incoming layer
// carries, as it listed them. Position is never a merge input, so this changes no values; it keeps
// a template's sidecar ahead of the installation's container and a template's env ahead of the
// installation's, which is the order the by-index merge produced.
//
// Elements are ranked and stable-sorted in place rather than rebuilt by lookup, so nothing the
// merge produced is dropped or duplicated here: a list may repeat a merge key - an env name
// declared twice - and a lookup would resolve every repeat to the first. Which repeats reach this
// point is up to k8s: the losing side's all do, while the winning side's may be merged into the
// last of them - for env, the value kubelet would use anyway. Ports are keyed on number and
// protocol by now, so a port served on TCP and on UDP is two elements rather than a repeat.
func restoreStackOrder(merged, toMap, fromMap map[string]any, schema strategicpatch.LookupPatchMeta) {
	for key, mergedValue := range merged {
		switch mergedTyped := mergedValue.(type) {
		case map[string]any:
			sub, _, err := schema.LookupPatchMetadataForStruct(key)
			if err != nil {
				continue
			}
			toSub, _ := toMap[key].(map[string]any)
			fromSub, _ := fromMap[key].(map[string]any)
			restoreStackOrder(mergedTyped, toSub, fromSub, sub)
		case []any:
			sub, meta, err := schema.LookupPatchMetadataForSlice(key)
			if err != nil {
				continue
			}
			mergeKey := meta.GetPatchMergeKey()
			if mergeKey == "" {
				continue
			}
			toList, _ := toMap[key].([]any)
			fromList, _ := fromMap[key].([]any)

			rank := func(item any) int {
				elem, ok := item.(map[string]any)
				if !ok {
					return len(toList) + len(fromList)
				}
				if i := indexByMergeKey(toList, mergeKey, elem[mergeKey]); i >= 0 {
					return i
				}
				if i := indexByMergeKey(fromList, mergeKey, elem[mergeKey]); i >= 0 {
					return len(toList) + i
				}
				return len(toList) + len(fromList)
			}
			ranked := make([]struct {
				rank int
				item any
			}, len(mergedTyped))
			for i, item := range mergedTyped {
				ranked[i].rank, ranked[i].item = rank(item), item
			}
			sort.SliceStable(ranked, func(i, j int) bool { return ranked[i].rank < ranked[j].rank })
			for i := range ranked {
				mergedTyped[i] = ranked[i].item
			}

			for _, item := range mergedTyped {
				elem, ok := item.(map[string]any)
				if !ok {
					continue
				}
				restoreStackOrder(elem,
					findByMergeKey(toList, mergeKey, elem[mergeKey]),
					findByMergeKey(fromList, mergeKey, elem[mergeKey]),
					sub)
			}
		}
	}
}

// indexByMergeKey returns the position of the first element carrying value under mergeKey, or -1.
func indexByMergeKey(list []any, mergeKey string, value any) int {
	for i, item := range list {
		if elem, ok := item.(map[string]any); ok && (elem[mergeKey] == value) {
			return i
		}
	}
	return -1
}

// pruneRetainKeys applies the k8s `retainKeys` list strategy ahead of the merge: a loser element paired with
// a winner element keeps only the keys the winner sets, so e.g. two volume sources never end up side by side.
// A patch built from the winner alone can not carry this directive, hence the loser is trimmed instead.
func pruneRetainKeys(loser, winner map[string]any, schema strategicpatch.LookupPatchMeta) {
	for key, winnerValue := range winner {
		loserValue, found := loser[key]
		if !found {
			continue
		}
		switch winnerTyped := winnerValue.(type) {
		case map[string]any:
			loserTyped, ok := loserValue.(map[string]any)
			if !ok {
				continue
			}
			sub, _, err := schema.LookupPatchMetadataForStruct(key)
			if err != nil {
				continue
			}
			pruneRetainKeys(loserTyped, winnerTyped, sub)
		case []any:
			loserTyped, ok := loserValue.([]any)
			if !ok {
				continue
			}
			sub, meta, err := schema.LookupPatchMetadataForSlice(key)
			if err != nil {
				continue
			}
			mergeKey := meta.GetPatchMergeKey()
			if mergeKey == "" {
				continue
			}
			retain := slices.Contains(meta.GetPatchStrategies(), retainKeysStrategy)
			for _, item := range winnerTyped {
				winnerElem, ok := item.(map[string]any)
				if !ok {
					continue
				}
				loserElem := findByMergeKey(loserTyped, mergeKey, winnerElem[mergeKey])
				if loserElem == nil {
					continue
				}
				if retain {
					for loserKey := range loserElem {
						if _, ok := winnerElem[loserKey]; !ok {
							delete(loserElem, loserKey)
						}
					}
				}
				pruneRetainKeys(loserElem, winnerElem, sub)
			}
		}
	}
}

// findByMergeKey finds a keyed list element by the value of its merge key
func findByMergeKey(list []any, mergeKey string, value any) map[string]any {
	if i := indexByMergeKey(list, mergeKey, value); i >= 0 {
		return list[i].(map[string]any)
	}
	return nil
}
