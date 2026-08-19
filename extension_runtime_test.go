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
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common"
	"dubbo.apache.org/dubbo-go/v3/common/extension"
)

type instanceExtensionConfig struct {
	value int
}

type instanceExtensionOption struct {
	prefix string
	value  int
}

func (o instanceExtensionOption) Prefix() string {
	return o.prefix
}

func (o instanceExtensionOption) Apply(config any) error {
	config.(*instanceExtensionConfig).value = o.value
	return nil
}

func TestWithExtensionBuildsInstanceAndIndependentChildRuntimes(t *testing.T) {
	const prefix = "instance-runtime-entry"
	extension.Unregister(prefix)
	t.Cleanup(func() { extension.Unregister(prefix) })
	configs := make([]*instanceExtensionConfig, 0, 3)
	require.NoError(t, extension.Register(extension.Definition{
		Prefix: prefix,
		Scopes: extension.InstanceScope | extension.ClientScope | extension.ServerScope,
		NewConfig: func() any {
			config := &instanceExtensionConfig{}
			configs = append(configs, config)
			return config
		},
	}))

	instance, err := NewInstance(WithExtension(instanceExtensionOption{prefix: prefix, value: 5}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, instance.CloseExtensions()) })
	cli, err := instance.NewClient()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.CloseExtensions()) })
	srv, err := instance.NewServer()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, srv.CloseExtensions()) })

	require.Len(t, configs, 3)
	assert.NotSame(t, configs[0], configs[1])
	assert.NotSame(t, configs[0], configs[2])
	assert.Equal(t, 5, configs[0].value)
	assert.Equal(t, 5, configs[1].value)
	assert.Equal(t, 5, configs[2].value)
}

func TestWithExtensionRejectsUnsupportedInstanceScope(t *testing.T) {
	const prefix = "instance-runtime-unsupported"
	extension.Unregister(prefix)
	t.Cleanup(func() { extension.Unregister(prefix) })
	require.NoError(t, extension.Register(extension.Definition{
		Prefix:    prefix,
		Scopes:    extension.ClientScope,
		NewConfig: func() any { return &instanceExtensionConfig{} },
	}))

	_, err := NewInstance(WithExtension(instanceExtensionOption{prefix: prefix}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scope")
}

func TestExtensionRawConfigResolverActivatesOnlySelectedScope(t *testing.T) {
	const prefix = "loader-runtime-entry"
	extension.Unregister(prefix)
	t.Cleanup(func() { extension.Unregister(prefix) })
	require.NoError(t, extension.Register(extension.Definition{
		Prefix:    prefix,
		Scopes:    extension.ClientScope | extension.ServerScope,
		NewConfig: func() any { return &instanceExtensionConfig{} },
	}))

	conf := NewLoaderConf(WithBytes([]byte("dubbo:\n  extensions:\n    loader-runtime-entry:\n      consumer:\n        timeout: 1000\n")))
	plan := extension.NewPlan(extensionRawConfigResolver(GetConfigResolver(conf)))
	clientRuntime, err := plan.Build(extension.ClientScope, common.CONSUMER)
	require.NoError(t, err)
	assert.Equal(t, []string{prefix}, clientRuntime.Prefixes())
	require.NoError(t, clientRuntime.Close())

	serverRuntime, err := plan.Build(extension.ServerScope, common.PROVIDER)
	require.NoError(t, err)
	assert.Empty(t, serverRuntime.Prefixes())
	require.NoError(t, serverRuntime.Close())
}
