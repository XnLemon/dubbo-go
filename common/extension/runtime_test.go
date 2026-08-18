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
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common"
)

func TestContextValidate(t *testing.T) {
	tests := []struct {
		name string
		ctx  Context
		pass bool
	}{
		{name: "instance", ctx: Context{Scope: InstanceScope, Role: RoleNone}, pass: true},
		{name: "client", ctx: Context{Scope: ClientScope, Role: common.CONSUMER}, pass: true},
		{name: "server", ctx: Context{Scope: ServerScope, Role: common.PROVIDER}, pass: true},
		{name: "instance consumer is invalid", ctx: Context{Scope: InstanceScope, Role: common.CONSUMER}},
		{name: "client provider is invalid", ctx: Context{Scope: ClientScope, Role: common.PROVIDER}},
		{name: "server consumer is invalid", ctx: Context{Scope: ServerScope, Role: common.CONSUMER}},
		{name: "combined scope is invalid", ctx: Context{Scope: ClientScope | ServerScope, Role: common.CONSUMER}},
		{name: "zero scope is invalid", ctx: Context{Role: RoleNone}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.ctx.Validate()
			if tt.pass {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestDefinitionRegistration(t *testing.T) {
	const prefix = "runtime-test"
	Unregister(prefix)
	t.Cleanup(func() { Unregister(prefix) })

	def := Definition{
		Prefix:    prefix,
		Scopes:    ClientScope | ServerScope,
		NewConfig: func() any { return map[string]any{"timeout": 1000} },
		Init: func(ctx *Context) error {
			if ctx == nil {
				return errors.New("context is nil")
			}
			return nil
		},
	}

	require.NoError(t, Register(def))
	assert.True(t, def.Supports(ClientScope))
	assert.True(t, def.Supports(ServerScope))
	assert.False(t, def.Supports(InstanceScope))

	got, ok := Lookup(prefix)
	require.True(t, ok)
	assert.Equal(t, prefix, got.Prefix)
	assert.NotNil(t, got.NewConfig)

	err := Register(def)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already registered")
}

func TestDefinitionValidation(t *testing.T) {
	tests := []struct {
		name string
		def  Definition
	}{
		{name: "missing prefix", def: Definition{Scopes: ClientScope, NewConfig: func() any { return nil }}},
		{name: "missing scopes", def: Definition{Prefix: "test", NewConfig: func() any { return nil }}},
		{name: "unknown scope bit", def: Definition{Prefix: "test", Scopes: Scope(8), NewConfig: func() any { return nil }}},
		{name: "missing constructor", def: Definition{Prefix: "test", Scopes: ClientScope}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, Register(tt.def))
		})
	}
}

func TestNewRawNodePreservesExactKeys(t *testing.T) {
	root, err := NewRawNode(map[string]any{
		"consumer": map[string]any{
			"greet.GreetService:::Greet": map[string]any{
				"timeout": 1000,
			},
		},
	})
	require.NoError(t, err)

	selected, ok := SelectRawNode(root, "consumer", "greet.GreetService:::Greet")
	require.True(t, ok)
	require.True(t, selected.Present())
	timeout, ok := selected.Child("timeout")
	require.True(t, ok)
	assert.Equal(t, 1000, timeout.Value())

	_, dottedPathLookup := SelectRawNode(root, "consumer", "greet", "GreetService:::Greet")
	assert.False(t, dottedPathLookup)
}

func TestRawNodeValueIsDetached(t *testing.T) {
	root, err := NewRawNode(map[string]any{
		"consumer": map[string]any{
			"timeout": 1000,
		},
	})
	require.NoError(t, err)

	value := root.Value().(map[string]any)
	value["consumer"].(map[string]any)["timeout"] = 2000

	consumer, ok := root.Child("consumer")
	require.True(t, ok)
	timeout, ok := consumer.Child("timeout")
	require.True(t, ok)
	assert.Equal(t, 1000, timeout.Value())
}

func TestBuildRawConfigSelectsScopeBranch(t *testing.T) {
	root, err := NewRawNode(map[string]any{
		"consumer": map[string]any{"timeout": 1000},
		"provider": map[string]any{"timeout": 1500},
	})
	require.NoError(t, err)

	config := BuildRawConfig(root, "consumer")
	assert.Same(t, root, config.Full)
	require.NotNil(t, config.Selected)
	timeout, ok := config.Selected.Child("timeout")
	require.True(t, ok)
	assert.Equal(t, 1000, timeout.Value())

	missing := BuildRawConfig(root, "instance")
	assert.Nil(t, missing.Selected)
}

func TestNewRawNodeRejectsNonStringMapKeys(t *testing.T) {
	_, err := NewRawNode(map[int]string{1: "invalid"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "map key type")
}
