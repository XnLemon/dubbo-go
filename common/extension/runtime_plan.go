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
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common"
)

// RawConfigResolver returns the immutable configuration view for one
// extension. selectedKeys are exact child keys chosen by the core for the
// current scope and role.
type RawConfigResolver func(prefix string, selectedKeys ...string) (RawConfig, bool, error)

// Plan is an immutable set of ordered extension option declarations and an
// optional core-side raw configuration resolver. Each call to Build creates a
// new Runtime with independent extension configurations.
type Plan struct {
	definitions map[string]Definition
	options     []Option
	resolver    RawConfigResolver
	rawPrefixes []string
}

// WithRawConfigPrefixes snapshots the extension prefixes found by the core
// loader. The list allows Build to reject configured but unregistered
// extensions without exposing a concrete configuration parser.
func (p Plan) WithRawConfigPrefixes(prefixes ...string) Plan {
	p.rawPrefixes = append([]string(nil), prefixes...)
	sort.Strings(p.rawPrefixes)
	return p
}

// NewPlan snapshots extension options for later Runtime construction.
func NewPlan(resolver RawConfigResolver, options ...Option) Plan {
	return Plan{
		definitions: definitions.Snapshot(),
		options:     append([]Option(nil), options...),
		resolver:    resolver,
	}
}

// Derive creates a child plan. Inherited options retain their declaration
// order and local options are applied afterwards.
func (p Plan) Derive(local ...Option) Plan {
	options := make([]Option, 0, len(p.options)+len(local))
	options = append(options, p.options...)
	options = append(options, local...)
	return Plan{
		definitions: cloneDefinitions(p.definitions),
		options:     options,
		resolver:    p.resolver,
		rawPrefixes: append([]string(nil), p.rawPrefixes...),
	}
}

// Options returns a detached copy of the ordered option declarations.
func (p Plan) Options() []Option {
	return append([]Option(nil), p.options...)
}

type runtimeEntry struct {
	definition Definition
	context    *Context
}

// Runtime owns the initialized extension state for exactly one lifecycle
// context. It must not be shared between Instance, Client, or Server objects.
type Runtime struct {
	mu      sync.RWMutex
	entries []runtimeEntry
	bound   map[string][]FilterSpec
	closed  bool
}

// Build creates an independent Runtime for one legal Scope/Role combination.
// Active extensions are initialized in prefix order. Within one extension the
// configuration precedence is defaults, raw configuration, then typed options.
func (p Plan) Build(scope Scope, role common.RoleType) (*Runtime, error) {
	baseContext := Context{Scope: scope, Role: role}
	if err := baseContext.Validate(); err != nil {
		return nil, err
	}

	definitionsSnapshot := cloneDefinitions(p.definitions)
	optionsByPrefix := make(map[string][]Option)
	active := make(map[string]struct{})
	for index, option := range p.options {
		if option == nil {
			return nil, fmt.Errorf("extension: option %d is nil", index)
		}
		prefix := strings.TrimSpace(option.Prefix())
		if prefix == "" {
			return nil, fmt.Errorf("extension: option %d has an empty prefix", index)
		}
		if _, ok := definitionsSnapshot[prefix]; !ok {
			return nil, fmt.Errorf("extension %q: no definition registered for scope %d role %d", prefix, scope, role)
		}
		optionsByPrefix[prefix] = append(optionsByPrefix[prefix], option)
		active[prefix] = struct{}{}
	}

	rawByPrefix := make(map[string]RawConfig)
	if p.resolver != nil {
		selectedKeys := rawSelection(scope)
		for _, prefix := range p.rawPrefixes {
			if _, ok := definitionsSnapshot[prefix]; ok {
				continue
			}
			raw, found, err := p.resolver(prefix, selectedKeys...)
			if err != nil {
				return nil, fmt.Errorf("extension %q: resolve raw config for scope %d role %d: %w", prefix, scope, role, err)
			}
			if found && (scope == InstanceScope || raw.Selected != nil) {
				return nil, fmt.Errorf("extension %q: configured for scope %d role %d but no definition is registered", prefix, scope, role)
			}
		}
		for prefix, definition := range definitionsSnapshot {
			if !definition.Supports(scope) {
				continue
			}
			raw, found, err := p.resolver(prefix, selectedKeys...)
			if err != nil {
				return nil, fmt.Errorf("extension %q: resolve raw config for scope %d role %d: %w", prefix, scope, role, err)
			}
			if !found || (scope != InstanceScope && raw.Selected == nil) {
				continue
			}
			rawByPrefix[prefix] = raw
			active[prefix] = struct{}{}
		}
	}

	prefixes := make([]string, 0, len(active))
	for prefix := range active {
		prefixes = append(prefixes, prefix)
	}
	sort.Strings(prefixes)

	runtime := &Runtime{bound: make(map[string][]FilterSpec)}
	for _, prefix := range prefixes {
		definition := definitionsSnapshot[prefix]
		if !definition.Supports(scope) {
			err := fmt.Errorf("extension %q: scope %d role %d is not supported", prefix, scope, role)
			return nil, errors.Join(err, runtime.closeInitialized())
		}

		config := definition.NewConfig()
		if config == nil {
			err := fmt.Errorf("extension %q: NewConfig returned nil for scope %d role %d", prefix, scope, role)
			return nil, errors.Join(err, runtime.closeInitialized())
		}
		if raw, ok := rawByPrefix[prefix]; ok && definition.Decode != nil {
			if err := definition.Decode(raw, config); err != nil {
				err = fmt.Errorf("extension %q: decode config for scope %d role %d: %w", prefix, scope, role, err)
				return nil, errors.Join(err, runtime.closeInitialized())
			}
		}
		for _, option := range optionsByPrefix[prefix] {
			if err := option.Apply(config); err != nil {
				err = fmt.Errorf("extension %q: apply option for scope %d role %d: %w", prefix, scope, role, err)
				return nil, errors.Join(err, runtime.closeInitialized())
			}
		}

		context := &Context{Scope: scope, Role: role, Config: config}
		if definition.Init != nil {
			if err := definition.Init(context); err != nil {
				err = fmt.Errorf("extension %q: initialize scope %d role %d: %w", prefix, scope, role, err)
				return nil, errors.Join(err, runtime.closeInitialized())
			}
		}
		runtime.entries = append(runtime.entries, runtimeEntry{definition: definition, context: context})
	}

	return runtime, nil
}

// BindResource resolves and validates extension filter contributions for one
// canonical RPC resource. Results are cached by service key and method. The
// base lifecycle Context remains unchanged; each callback receives a copy.
func (r *Runtime) BindResource(resource Resource) ([]FilterSpec, error) {
	if r == nil {
		return nil, nil
	}
	if err := resource.Validate(); err != nil {
		return nil, err
	}
	identity := resource.ServiceKey + "\x00" + resource.Method

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, fmt.Errorf("extension: runtime is closed for resource %q", resource.ServiceKey)
	}
	if specs, ok := r.bound[identity]; ok {
		return append([]FilterSpec(nil), specs...), nil
	}

	type ownedSpec struct {
		prefix string
		spec   FilterSpec
	}
	owned := make([]ownedSpec, 0)
	ownerByID := make(map[string]string)
	for _, entry := range r.entries {
		if entry.definition.Filters == nil {
			continue
		}
		context := *entry.context
		resourceCopy := resource
		context.Resource = &resourceCopy
		specs, err := entry.definition.Filters(&context)
		if err != nil {
			return nil, fmt.Errorf("extension %q: bind filters for resource %q: %w", entry.definition.Prefix, resource.ServiceKey, err)
		}
		seen := make(map[string]struct{})
		for index, spec := range specs {
			spec.ID = strings.TrimSpace(spec.ID)
			if spec.ID == "" {
				return nil, fmt.Errorf("extension %q: filter spec %d has an empty ID for resource %q", entry.definition.Prefix, index, resource.ServiceKey)
			}
			if spec.Factory == nil {
				return nil, fmt.Errorf("extension %q: filter spec %q has a nil Factory for resource %q", entry.definition.Prefix, spec.ID, resource.ServiceKey)
			}
			if _, duplicate := seen[spec.ID]; duplicate {
				continue
			}
			seen[spec.ID] = struct{}{}
			if owner, conflict := ownerByID[spec.ID]; conflict {
				return nil, fmt.Errorf("extension filters %q and %q contribute duplicate ID %q for resource %q", owner, entry.definition.Prefix, spec.ID, resource.ServiceKey)
			}
			ownerByID[spec.ID] = entry.definition.Prefix
			owned = append(owned, ownedSpec{prefix: entry.definition.Prefix, spec: spec})
		}
	}
	// Prefix order and callback order are already deterministic; stable sorting
	// preserves both for equal Order values.
	sort.SliceStable(owned, func(i, j int) bool {
		return owned[i].spec.Order < owned[j].spec.Order
	})
	specs := make([]FilterSpec, 0, len(owned))
	for _, item := range owned {
		specs = append(specs, item.spec)
	}
	r.bound[identity] = append([]FilterSpec(nil), specs...)
	return specs, nil
}

// MergeFilterSpecs combines framework and external filter contributions,
// validates IDs and factories, and performs a stable Order sort.
func MergeFilterSpecs(groups ...[]FilterSpec) ([]FilterSpec, error) {
	merged := make([]FilterSpec, 0)
	seen := make(map[string]struct{})
	for _, group := range groups {
		for index, spec := range group {
			spec.ID = strings.TrimSpace(spec.ID)
			if spec.ID == "" {
				return nil, fmt.Errorf("extension: filter spec %d has an empty ID", index)
			}
			if spec.Factory == nil {
				return nil, fmt.Errorf("extension: filter spec %q has a nil Factory", spec.ID)
			}
			if _, duplicate := seen[spec.ID]; duplicate {
				return nil, fmt.Errorf("extension: duplicate filter spec ID %q", spec.ID)
			}
			seen[spec.ID] = struct{}{}
			merged = append(merged, spec)
		}
	}
	sort.SliceStable(merged, func(i, j int) bool {
		return merged[i].Order < merged[j].Order
	})
	return merged, nil
}

func cloneDefinitions(source map[string]Definition) map[string]Definition {
	cloned := make(map[string]Definition, len(source))
	maps.Copy(cloned, source)
	return cloned
}

func rawSelection(scope Scope) []string {
	switch scope {
	case ClientScope:
		return []string{"consumer"}
	case ServerScope:
		return []string{"provider"}
	default:
		return nil
	}
}

// Context returns a detached Context value for diagnostics and resource
// binding. Config remains the Runtime-owned typed configuration and must be
// treated as read-only after initialization.
func (r *Runtime) Context(prefix string) (Context, bool) {
	if r == nil {
		return Context{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, entry := range r.entries {
		if entry.definition.Prefix == prefix {
			context := *entry.context
			if entry.context.Resource != nil {
				resource := *entry.context.Resource
				context.Resource = &resource
			}
			return context, true
		}
	}
	return Context{}, false
}

// Prefixes returns initialized extensions in deterministic order.
func (r *Runtime) Prefixes() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	prefixes := make([]string, 0, len(r.entries))
	for _, entry := range r.entries {
		prefixes = append(prefixes, entry.definition.Prefix)
	}
	return prefixes
}

// Close releases initialized extensions exactly once in reverse Init order.
func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	return r.closeInitialized()
}

func (r *Runtime) closeInitialized() error {
	var closeErrors []error
	for index := len(r.entries) - 1; index >= 0; index-- {
		entry := r.entries[index]
		if entry.definition.Close == nil {
			continue
		}
		if err := entry.definition.Close(entry.context); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("extension %q: close scope %d role %d: %w",
				entry.definition.Prefix, entry.context.Scope, entry.context.Role, err))
		}
	}
	return errors.Join(closeErrors...)
}
