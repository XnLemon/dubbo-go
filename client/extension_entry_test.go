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

package client

import (
	"errors"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"dubbo.apache.org/dubbo-go/v3/common/extension"
	"dubbo.apache.org/dubbo-go/v3/filter"
)

type clientEntryConfig struct {
	prefix        string
	Value         int                       `yaml:"value"`
	Commands      map[string]map[string]int `yaml:",remain"`
	requiredScope extension.Scope
	initialized   extension.Scope
	onInit        func(*clientEntryConfig)
}

func (c *clientEntryConfig) Prefix() string {
	return c.prefix
}

func (c *clientEntryConfig) New() extension.Config {
	return &clientEntryConfig{
		prefix:        c.prefix,
		Value:         1,
		Commands:      map[string]map[string]int{"default:::Run": {"timeout": 1}},
		requiredScope: c.requiredScope,
		onInit:        c.onInit,
	}
}

func (c *clientEntryConfig) Init(scope extension.Scope) error {
	if c.requiredScope != 0 && scope != c.requiredScope {
		return errors.New("client scope is required")
	}
	c.initialized = scope
	if c.onInit != nil {
		c.onInit(c)
	}
	return nil
}

func (c *clientEntryConfig) FilterNames(extension.Scope) []string {
	return []string{"client-entry-filter"}
}

type clientEntryOption struct {
	prefix string
	value  int
}

func (o clientEntryOption) Prefix() string {
	return o.prefix
}

func (o clientEntryOption) Apply(config extension.Config) error {
	config.(*clientEntryConfig).Value = o.value
	return nil
}

func TestWithExtensionBuildsClientConfigAndMergesFilter(t *testing.T) {
	const prefix = "client-entry"
	const filterName = "client-entry-filter"

	extension.UnregisterConfig(prefix)
	extension.UnregisterFilter(filterName)
	t.Cleanup(func() {
		extension.UnregisterConfig(prefix)
		extension.UnregisterFilter(filterName)
	})

	var initialized *clientEntryConfig
	require.NoError(t, extension.RegisterConfig(&clientEntryConfig{
		prefix:        prefix,
		requiredScope: extension.ClientScope,
		onInit: func(config *clientEntryConfig) {
			initialized = config
		},
	}))
	extension.SetFilter(filterName, func() filter.Filter { return nil })

	cli, err := NewClient(
		SetClientExtensionConfigs(map[string]any{
			prefix: map[string]any{
				"consumer": map[string]any{
					"value":                      7,
					"greet.GreetService:::Greet": map[string]any{"timeout": 3},
				},
			},
		}),
		WithClientFilter("explicit"),
		WithExtension(clientEntryOption{prefix: prefix, value: 9}),
	)
	require.NoError(t, err)
	require.NotNil(t, cli)
	require.NotNil(t, initialized)
	assert.Equal(t, 9, initialized.Value)
	assert.Equal(t, 3, initialized.Commands["greet.GreetService:::Greet"]["timeout"])
	assert.Equal(t, extension.ClientScope, initialized.initialized)
	assert.Equal(t, "explicit,"+filterName, cli.cliOpts.overallReference.Filter)
}

func TestWithExtensionRejectsUnsupportedClientScope(t *testing.T) {
	const prefix = "client-entry-unsupported"
	extension.UnregisterConfig(prefix)
	t.Cleanup(func() { extension.UnregisterConfig(prefix) })

	require.NoError(t, extension.RegisterConfig(&clientEntryConfig{
		prefix:        prefix,
		requiredScope: extension.InstanceScope,
	}))
	extension.SetFilter("client-entry-filter", func() filter.Filter { return nil })
	t.Cleanup(func() { extension.UnregisterFilter("client-entry-filter") })
	_, err := NewClient(WithExtension(clientEntryOption{prefix: prefix, value: 1}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "client scope is required")
}
