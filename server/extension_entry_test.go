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

package server

import (
	"errors"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common/extension"
	"dubbo.apache.org/dubbo-go/v3/filter"
)

type serverEntryConfig struct {
	prefix        string
	Value         int `yaml:"value"`
	requiredScope extension.Scope
	initialized   extension.Scope
	onInit        func(*serverEntryConfig)
}

func (c *serverEntryConfig) Prefix() string {
	return c.prefix
}

func (c *serverEntryConfig) New() extension.Config {
	return &serverEntryConfig{
		prefix:        c.prefix,
		Value:         1,
		requiredScope: c.requiredScope,
		onInit:        c.onInit,
	}
}

func (c *serverEntryConfig) Init(scope extension.Scope) error {
	if c.requiredScope != 0 && scope != c.requiredScope {
		return errors.New("server scope is required")
	}
	c.initialized = scope
	if c.onInit != nil {
		c.onInit(c)
	}
	return nil
}

func (c *serverEntryConfig) FilterNames(extension.Scope) []string {
	return []string{"server-entry-filter"}
}

type serverEntryOption struct {
	prefix string
	value  int
}

func (o serverEntryOption) Prefix() string {
	return o.prefix
}

func (o serverEntryOption) Apply(config extension.Config) error {
	config.(*serverEntryConfig).Value = o.value
	return nil
}

func TestWithExtensionBuildsServerConfigAndMergesFilter(t *testing.T) {
	const prefix = "server-entry"
	const filterName = "server-entry-filter"

	extension.UnregisterConfig(prefix)
	extension.UnregisterFilter(filterName)
	t.Cleanup(func() {
		extension.UnregisterConfig(prefix)
		extension.UnregisterFilter(filterName)
	})

	var initialized *serverEntryConfig
	require.NoError(t, extension.RegisterConfig(&serverEntryConfig{
		prefix:        prefix,
		requiredScope: extension.ServerScope,
		onInit: func(config *serverEntryConfig) {
			initialized = config
		},
	}))
	extension.SetFilter(filterName, func() filter.Filter { return nil })

	srv, err := NewServer(
		SetServerExtensionConfigs(map[string]any{
			prefix: map[string]any{
				"provider": map[string]any{"value": 7},
			},
		}),
		WithServerFilter("explicit"),
		WithExtension(serverEntryOption{prefix: prefix, value: 9}),
	)
	require.NoError(t, err)
	require.NotNil(t, srv)
	require.NotNil(t, initialized)
	assert.Equal(t, 9, initialized.Value)
	assert.Equal(t, extension.ServerScope, initialized.initialized)
	assert.Equal(t, "explicit,"+filterName, srv.cfg.Provider.Filter)
}

func TestWithExtensionRejectsUnsupportedServerScope(t *testing.T) {
	const prefix = "server-entry-unsupported"
	extension.UnregisterConfig(prefix)
	t.Cleanup(func() { extension.UnregisterConfig(prefix) })

	require.NoError(t, extension.RegisterConfig(&serverEntryConfig{
		prefix:        prefix,
		requiredScope: extension.InstanceScope,
	}))
	extension.SetFilter("server-entry-filter", func() filter.Filter { return nil })
	t.Cleanup(func() { extension.UnregisterFilter("server-entry-filter") })
	_, err := NewServer(WithExtension(serverEntryOption{prefix: prefix, value: 1}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "server scope is required")
}
