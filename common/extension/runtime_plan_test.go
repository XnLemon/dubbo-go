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
	"context"
	"errors"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common"
	"dubbo.apache.org/dubbo-go/v3/filter"
	"dubbo.apache.org/dubbo-go/v3/protocol/base"
	"dubbo.apache.org/dubbo-go/v3/protocol/result"
)

type runtimeTestConfig struct {
	Value int
}

type runtimeTestFilter struct{}

func (*runtimeTestFilter) Invoke(ctx context.Context, invoker base.Invoker, invocation base.Invocation) result.Result {
	return invoker.Invoke(ctx, invocation)
}

func (*runtimeTestFilter) OnResponse(_ context.Context, response result.Result, _ base.Invoker, _ base.Invocation) result.Result {
	return response
}

type runtimeTestOption struct {
	prefix string
	value  int
	err    error
}

func (o runtimeTestOption) Prefix() string {
	return o.prefix
}

func (o runtimeTestOption) Apply(config any) error {
	if o.err != nil {
		return o.err
	}
	config.(*runtimeTestConfig).Value = o.value
	return nil
}

func registerRuntimeDefinition(t *testing.T, definition Definition) {
	t.Helper()
	Unregister(definition.Prefix)
	require.NoError(t, Register(definition))
	t.Cleanup(func() { Unregister(definition.Prefix) })
}

func TestPlanBuildAppliesDefaultRawAndOptionsInOrder(t *testing.T) {
	const prefix = "runtime-plan-precedence"
	registerRuntimeDefinition(t, Definition{
		Prefix: prefix,
		Scopes: ClientScope,
		NewConfig: func() any {
			return &runtimeTestConfig{Value: 1}
		},
		Decode: func(raw RawConfig, config any) error {
			value, ok := raw.Selected.Child("value")
			if !ok {
				return errors.New("value is missing")
			}
			config.(*runtimeTestConfig).Value = value.Value().(int)
			return nil
		},
	})

	root, err := NewRawNode(map[string]any{
		"consumer": map[string]any{"value": 2},
	})
	require.NoError(t, err)
	resolver := func(gotPrefix string, selectedKeys ...string) (RawConfig, bool, error) {
		assert.Equal(t, prefix, gotPrefix)
		assert.Equal(t, []string{"consumer"}, selectedKeys)
		return BuildRawConfig(root, selectedKeys...), true, nil
	}

	plan := NewPlan(resolver,
		runtimeTestOption{prefix: prefix, value: 3},
		runtimeTestOption{prefix: prefix, value: 4},
	)
	runtime, err := plan.Build(ClientScope, common.CONSUMER)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	context, ok := runtime.Context(prefix)
	require.True(t, ok)
	assert.Equal(t, 4, context.Config.(*runtimeTestConfig).Value)
}

func TestPlanBuildActivatesSelectedYAMLWithoutOptions(t *testing.T) {
	const prefix = "runtime-plan-yaml"
	initCalls := 0
	registerRuntimeDefinition(t, Definition{
		Prefix:    prefix,
		Scopes:    ClientScope | ServerScope,
		NewConfig: func() any { return &runtimeTestConfig{} },
		Init: func(context *Context) error {
			initCalls++
			return nil
		},
	})

	root, err := NewRawNode(map[string]any{
		"consumer": map[string]any{},
	})
	require.NoError(t, err)
	plan := NewPlan(func(_ string, selectedKeys ...string) (RawConfig, bool, error) {
		return BuildRawConfig(root, selectedKeys...), true, nil
	})

	clientRuntime, err := plan.Build(ClientScope, common.CONSUMER)
	require.NoError(t, err)
	require.NoError(t, clientRuntime.Close())
	assert.Equal(t, 1, initCalls)

	serverRuntime, err := plan.Build(ServerScope, common.PROVIDER)
	require.NoError(t, err)
	require.NoError(t, serverRuntime.Close())
	assert.Equal(t, 1, initCalls, "a missing provider branch must not activate the extension")
}

func TestPlanBuildCreatesIndependentConfigurations(t *testing.T) {
	const prefix = "runtime-plan-isolation"
	registerRuntimeDefinition(t, Definition{
		Prefix:    prefix,
		Scopes:    ClientScope,
		NewConfig: func() any { return &runtimeTestConfig{} },
	})
	plan := NewPlan(nil, runtimeTestOption{prefix: prefix, value: 7})

	first, err := plan.Build(ClientScope, common.CONSUMER)
	require.NoError(t, err)
	second, err := plan.Build(ClientScope, common.CONSUMER)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, first.Close())
		require.NoError(t, second.Close())
	})

	firstContext, _ := first.Context(prefix)
	secondContext, _ := second.Context(prefix)
	assert.NotSame(t, firstContext.Config, secondContext.Config)
}

func TestPlanBuildRollsBackInReverseOrder(t *testing.T) {
	order := make([]string, 0, 3)
	for _, prefix := range []string{"runtime-plan-a", "runtime-plan-b", "runtime-plan-c"} {
		registerRuntimeDefinition(t, Definition{
			Prefix:    prefix,
			Scopes:    ClientScope,
			NewConfig: func() any { return &runtimeTestConfig{} },
			Init: func(*Context) error {
				order = append(order, "init:"+prefix)
				if prefix == "runtime-plan-c" {
					return errors.New("init failed")
				}
				return nil
			},
			Close: func(*Context) error {
				order = append(order, "close:"+prefix)
				return nil
			},
		})
	}

	plan := NewPlan(nil,
		runtimeTestOption{prefix: "runtime-plan-c"},
		runtimeTestOption{prefix: "runtime-plan-a"},
		runtimeTestOption{prefix: "runtime-plan-b"},
	)
	_, err := plan.Build(ClientScope, common.CONSUMER)
	require.Error(t, err)
	assert.Equal(t, []string{
		"init:runtime-plan-a",
		"init:runtime-plan-b",
		"init:runtime-plan-c",
		"close:runtime-plan-b",
		"close:runtime-plan-a",
	}, order)
}

func TestRuntimeCloseIsIdempotent(t *testing.T) {
	const prefix = "runtime-plan-close"
	closeCalls := 0
	registerRuntimeDefinition(t, Definition{
		Prefix:    prefix,
		Scopes:    ClientScope,
		NewConfig: func() any { return &runtimeTestConfig{} },
		Close: func(*Context) error {
			closeCalls++
			return nil
		},
	})

	runtime, err := NewPlan(nil, runtimeTestOption{prefix: prefix}).Build(ClientScope, common.CONSUMER)
	require.NoError(t, err)
	require.NoError(t, runtime.Close())
	require.NoError(t, runtime.Close())
	assert.Equal(t, 1, closeCalls)
}

func TestPlanUsesDefinitionSnapshot(t *testing.T) {
	const prefix = "runtime-plan-snapshot"
	Unregister(prefix)
	plan := NewPlan(nil)
	registerRuntimeDefinition(t, Definition{
		Prefix:    prefix,
		Scopes:    ClientScope,
		NewConfig: func() any { return &runtimeTestConfig{} },
	})

	_, err := plan.Derive(runtimeTestOption{prefix: prefix}).Build(ClientScope, common.CONSUMER)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no definition registered")
}

func TestPlanRejectsConfiguredUnknownPrefix(t *testing.T) {
	root, err := NewRawNode(map[string]any{
		"consumer": map[string]any{},
	})
	require.NoError(t, err)
	plan := NewPlan(func(_ string, selectedKeys ...string) (RawConfig, bool, error) {
		return BuildRawConfig(root, selectedKeys...), true, nil
	}).WithRawConfigPrefixes("unknown-extension")

	_, err = plan.Build(ClientScope, common.CONSUMER)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown-extension")
	assert.Contains(t, err.Error(), "no definition")
}

func TestRuntimeBindResourceUsesContextCopyAndCachesSpecs(t *testing.T) {
	const prefix = "runtime-resource-cache"
	filterCalls := 0
	registerRuntimeDefinition(t, Definition{
		Prefix:    prefix,
		Scopes:    ClientScope,
		NewConfig: func() any { return &runtimeTestConfig{} },
		Filters: func(context *Context) ([]FilterSpec, error) {
			filterCalls++
			require.NotNil(t, context.Resource)
			assert.Equal(t, "group/example.Service:v1", context.Resource.ServiceKey)
			return []FilterSpec{
				{ID: "test:late", Order: 20, Factory: func() filter.Filter { return &runtimeTestFilter{} }},
				{ID: "test:early", Order: 10, Factory: func() filter.Filter { return &runtimeTestFilter{} }},
				{ID: "test:late", Order: 20, Factory: func() filter.Filter { return &runtimeTestFilter{} }},
			}, nil
		},
	})
	runtime, err := NewPlan(nil, runtimeTestOption{prefix: prefix}).Build(ClientScope, common.CONSUMER)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })
	resource := Resource{
		ServiceKey: "group/example.Service:v1",
		Interface:  "example.Service",
		Group:      "group",
		Version:    "v1",
	}

	first, err := runtime.BindResource(resource)
	require.NoError(t, err)
	second, err := runtime.BindResource(resource)
	require.NoError(t, err)
	assert.Equal(t, []string{"test:early", "test:late"}, []string{first[0].ID, first[1].ID})
	require.Len(t, second, 2)
	assert.Equal(t, []string{first[0].ID, first[1].ID}, []string{second[0].ID, second[1].ID})
	assert.Equal(t, []int{first[0].Order, first[1].Order}, []int{second[0].Order, second[1].Order})
	assert.Equal(t, 1, filterCalls)
	baseContext, ok := runtime.Context(prefix)
	require.True(t, ok)
	assert.Nil(t, baseContext.Resource)
}

func TestRuntimeBindResourceRejectsCrossExtensionIDConflict(t *testing.T) {
	prefixes := []string{"runtime-resource-conflict-a", "runtime-resource-conflict-b"}
	options := make([]Option, 0, len(prefixes))
	for _, prefix := range prefixes {
		registerRuntimeDefinition(t, Definition{
			Prefix:    prefix,
			Scopes:    ClientScope,
			NewConfig: func() any { return &runtimeTestConfig{} },
			Filters: func(*Context) ([]FilterSpec, error) {
				return []FilterSpec{{ID: "shared:id", Factory: func() filter.Filter { return &runtimeTestFilter{} }}}, nil
			},
		})
		options = append(options, runtimeTestOption{prefix: prefix})
	}
	runtime, err := NewPlan(nil, options...).Build(ClientScope, common.CONSUMER)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	_, err = runtime.BindResource(Resource{ServiceKey: "example.Service", Interface: "example.Service"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate ID")
}

func TestResourceValidateRequiresCanonicalServiceKey(t *testing.T) {
	err := (Resource{ServiceKey: "wrong", Interface: "example.Service", Group: "group"}).Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "canonical")
}
