/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package extension

import (
	"fmt"
	"reflect"
)

type rawNode struct {
	value    any
	children map[string]*rawNode
}

// NewRawNode creates an immutable RawNode from parser-neutral Go values. Map
// keys must be strings; nested maps and slices are copied recursively.
func NewRawNode(value any) (RawNode, error) {
	normalized, err := normalizeRawValue(value)
	if err != nil {
		return nil, err
	}
	return buildRawNode(normalized), nil
}

func buildRawNode(value any) *rawNode {
	node := &rawNode{value: value}
	if values, ok := value.(map[string]any); ok {
		node.children = make(map[string]*rawNode, len(values))
		for key, child := range values {
			node.children[key] = buildRawNode(child)
		}
	}
	return node
}

func (n *rawNode) Child(key string) (RawNode, bool) {
	if n == nil {
		return nil, false
	}
	child, ok := n.children[key]
	return child, ok
}

func (n *rawNode) Value() any {
	if n == nil {
		return nil
	}
	return cloneRawValue(n.value)
}

func (n *rawNode) Present() bool {
	return n != nil
}

// SelectRawNode performs exact child-key traversal without interpreting any
// delimiter in a key. It returns false when the path does not exist.
func SelectRawNode(root RawNode, keys ...string) (RawNode, bool) {
	if root == nil || !root.Present() {
		return nil, false
	}
	current := root
	for _, key := range keys {
		var ok bool
		current, ok = current.Child(key)
		if !ok {
			return nil, false
		}
	}
	return current, true
}

// BuildRawConfig creates a RawConfig whose Selected node is selected from Full
// by exact child keys. A missing selection is represented by a nil Selected
// node so callers can distinguish an absent scope branch from an empty map.
func BuildRawConfig(full RawNode, selectedKeys ...string) RawConfig {
	selected, _ := SelectRawNode(full, selectedKeys...)
	return RawConfig{Full: full, Selected: selected}
}

func normalizeRawValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("extension: raw map key type %s is not string", rv.Type().Key())
		}
		values := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			key := iter.Key().String()
			child, err := normalizeRawValue(iter.Value().Interface())
			if err != nil {
				return nil, err
			}
			values[key] = child
		}
		return values, nil
	case reflect.Slice, reflect.Array:
		values := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			child, err := normalizeRawValue(rv.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			values[i] = child
		}
		return values, nil
	case reflect.Pointer, reflect.Func, reflect.Chan, reflect.UnsafePointer, reflect.Struct:
		return nil, fmt.Errorf("extension: unsupported raw value type %s", rv.Type())
	default:
		return value, nil
	}
}

func cloneRawValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		clone := make(map[string]any, len(typed))
		for key, child := range typed {
			clone[key] = cloneRawValue(child)
		}
		return clone
	case []any:
		clone := make([]any, len(typed))
		for i, child := range typed {
			clone[i] = cloneRawValue(child)
		}
		return clone
	default:
		return value
	}
}
