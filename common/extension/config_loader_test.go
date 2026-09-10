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

	"dubbo.apache.org/dubbo-go/v3/filter"
)

type loaderTestConfig struct {
	prefix       string
	Value        int               `yaml:"value"`
	CommandNames map[string]string `yaml:",remain"`
	initialized  Scope
	onInit       func(*loaderTestConfig)
}

func (c *loaderTestConfig) Prefix() string {
	return c.prefix
}

func (c *loaderTestConfig) New() Config {
	return &loaderTestConfig{
		prefix:       c.prefix,
		Value:        1,
		CommandNames: map[string]string{"default.command:::Run": "default"},
		onInit:       c.onInit,
	}
}

func (c *loaderTestConfig) Init(scope Scope) error {
	if !scope.valid() {
		return errors.New("invalid scope")
	}
	c.initialized = scope
	if c.onInit != nil {
		c.onInit(c)
	}
	return nil
}

func (c *loaderTestConfig) FilterNames(Scope) []string {
	return []string{"loader-test-filter", "loader-test-filter"}
}

type loaderTestOption struct {
	prefix string
	value  int
}

func (o loaderTestOption) Prefix() string {
	return o.prefix
}

func (o loaderTestOption) Apply(config Config) error {
	config.(*loaderTestConfig).Value = o.value
	return nil
}

func TestInitializeAppliesRoleYAMLThenOptions(t *testing.T) {
	const prefix = "loader-contract"
	const filterName = "loader-test-filter"

	UnregisterConfig(prefix)
	UnregisterFilter(filterName)
	t.Cleanup(func() {
		UnregisterConfig(prefix)
		UnregisterFilter(filterName)
	})

	var initialized *loaderTestConfig
	prototype := &loaderTestConfig{
		prefix: prefix,
		onInit: func(config *loaderTestConfig) {
			initialized = config
		},
	}
	require.NoError(t, RegisterConfig(prototype))
	SetFilter(filterName, func() filter.Filter { return nil })

	filters, err := Initialize(
		map[string]any{
			prefix: map[string]any{
				"consumer": map[string]any{
					"value":                      7,
					"greet.GreetService:::Greet": "greet",
				},
			},
		},
		[]Option{loaderTestOption{prefix: prefix, value: 9}},
		ClientScope,
	)
	require.NoError(t, err)
	assert.Equal(t, []string{filterName}, filters)
	assert.NotNil(t, initialized)
	assert.Equal(t, 9, initialized.Value)
	assert.Equal(t, "greet", initialized.CommandNames["greet.GreetService:::Greet"])
	assert.Equal(t, ClientScope, initialized.initialized)
}

func TestInitializeIgnoresOtherRoleYAML(t *testing.T) {
	const prefix = "loader-role-selection"
	UnregisterConfig(prefix)
	t.Cleanup(func() { UnregisterConfig(prefix) })

	initCount := 0
	config := &loaderTestConfig{
		prefix: prefix,
		onInit: func(*loaderTestConfig) {
			initCount++
		},
	}
	// A config with provider-only YAML is not active in a client lifecycle.
	require.NoError(t, RegisterConfig(config))
	filters, err := Initialize(map[string]any{
		prefix: map[string]any{
			"provider": map[string]any{"value": 11},
		},
	}, nil, ClientScope)
	require.NoError(t, err)
	assert.Empty(t, filters)
	assert.Equal(t, 0, initCount)
}

func TestMergeFilterNamesHonorsExplicitSuppression(t *testing.T) {
	assert.Equal(t, "-extension,a,b", MergeFilterNames("-extension,a,a", []string{"extension", "b", "b"}))
	assert.Equal(t, "", MergeFilterNames("", nil))
}
