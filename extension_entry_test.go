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

package dubbo

import (
	"errors"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

import "dubbo.apache.org/dubbo-go/v3/common/extension"

type instanceEntryConfig struct {
	prefix      string
	Value       int
	initialized extension.Scope
	onInit      func(*instanceEntryConfig)
}

func (c *instanceEntryConfig) Prefix() string {
	return c.prefix
}

func (c *instanceEntryConfig) New() extension.Config {
	return &instanceEntryConfig{
		prefix: c.prefix,
		Value:  1,
		onInit: c.onInit,
	}
}

func (c *instanceEntryConfig) Init(scope extension.Scope) error {
	if scope != extension.InstanceScope && scope != extension.ClientScope {
		return errors.New("instance or client scope is required")
	}
	c.initialized = scope
	if c.onInit != nil {
		c.onInit(c)
	}
	return nil
}

func (c *instanceEntryConfig) FilterNames(extension.Scope) []string {
	return nil
}

type instanceEntryOption struct {
	prefix string
	value  int
}

func (o instanceEntryOption) Prefix() string {
	return o.prefix
}

func (o instanceEntryOption) Apply(config extension.Config) error {
	config.(*instanceEntryConfig).Value = o.value
	return nil
}

func TestWithExtensionBuildsInstanceConfig(t *testing.T) {
	const prefix = "instance-entry"
	extension.UnregisterConfig(prefix)
	t.Cleanup(func() { extension.UnregisterConfig(prefix) })

	var initialized *instanceEntryConfig
	require.NoError(t, extension.RegisterConfig(&instanceEntryConfig{
		prefix: prefix,
		onInit: func(config *instanceEntryConfig) {
			initialized = config
		},
	}))

	_, err := NewInstance(WithExtension(instanceEntryOption{prefix: prefix, value: 9}))
	require.NoError(t, err)
	require.NotNil(t, initialized)
	assert.Equal(t, 9, initialized.Value)
	assert.Equal(t, extension.InstanceScope, initialized.initialized)
}

func TestInstancePropagatesRoleSpecificExtensionYAMLToClient(t *testing.T) {
	const prefix = "instance-to-client-entry"
	extension.UnregisterConfig(prefix)
	t.Cleanup(func() { extension.UnregisterConfig(prefix) })

	var initialized *instanceEntryConfig
	require.NoError(t, extension.RegisterConfig(&instanceEntryConfig{
		prefix: prefix,
		onInit: func(config *instanceEntryConfig) {
			initialized = config
		},
	}))

	instance, err := NewInstance(func(opts *InstanceOptions) {
		opts.extensionConfigs = map[string]any{
			prefix: map[string]any{
				"consumer": map[string]any{"value": 7},
			},
		}
	})
	require.NoError(t, err)
	_, err = instance.NewClient()
	require.NoError(t, err)
	require.NotNil(t, initialized)
	assert.Equal(t, 7, initialized.Value)
	assert.Equal(t, extension.ClientScope, initialized.initialized)
}

func TestExtensionConfigsFromKoanfPreservesDottedKeys(t *testing.T) {
	conf := NewLoaderConf(WithBytes([]byte(`dubbo:
  extensions:
    dotted:
      consumer:
        greet.GreetService:::Greet:
          timeout: 1000
`)))
	configs := extensionConfigsFromKoanf(GetConfigResolver(conf))
	require.NotNil(t, configs)
	dotted, ok := configs["dotted"].(map[string]any)
	require.True(t, ok)
	consumer, ok := dotted["consumer"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, consumer, "greet.GreetService:::Greet")
}
