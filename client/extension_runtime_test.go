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

type clientExtensionConfig struct {
	value int
}

type clientExtensionOption struct {
	prefix string
	value  int
}

func (o clientExtensionOption) Prefix() string {
	return o.prefix
}

func (o clientExtensionOption) Apply(config any) error {
	config.(*clientExtensionConfig).value = o.value
	return nil
}

func TestWithExtensionBuildsClientRuntime(t *testing.T) {
	const prefix = "client-runtime-entry"
	extension.Unregister(prefix)
	t.Cleanup(func() { extension.Unregister(prefix) })
	require.NoError(t, extension.Register(extension.Definition{
		Prefix:    prefix,
		Scopes:    extension.ClientScope,
		NewConfig: func() any { return &clientExtensionConfig{} },
	}))

	cli, err := NewClient(WithExtension(clientExtensionOption{prefix: prefix, value: 9}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.CloseExtensions()) })

	context, ok := cli.extensionRuntime.Context(prefix)
	require.True(t, ok)
	assert.Equal(t, extension.ClientScope, context.Scope)
	assert.Equal(t, common.RoleType(common.CONSUMER), context.Role)
	assert.Equal(t, 9, context.Config.(*clientExtensionConfig).value)
}

func TestWithExtensionRejectsUnsupportedClientScope(t *testing.T) {
	const prefix = "client-runtime-unsupported"
	extension.Unregister(prefix)
	t.Cleanup(func() { extension.Unregister(prefix) })
	require.NoError(t, extension.Register(extension.Definition{
		Prefix:    prefix,
		Scopes:    extension.ServerScope,
		NewConfig: func() any { return &clientExtensionConfig{} },
	}))

	_, err := NewClient(WithExtension(clientExtensionOption{prefix: prefix}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scope")
}
