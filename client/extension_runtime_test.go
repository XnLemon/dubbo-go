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
	"context"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common"
	"dubbo.apache.org/dubbo-go/v3/common/extension"
	"dubbo.apache.org/dubbo-go/v3/filter"
	"dubbo.apache.org/dubbo-go/v3/protocol/base"
	"dubbo.apache.org/dubbo-go/v3/protocol/result"
)

type clientExtensionConfig struct {
	value int
}

type clientExtensionOption struct {
	prefix string
	value  int
}

type clientExtensionFilter struct{}

func (*clientExtensionFilter) Invoke(ctx context.Context, invoker base.Invoker, invocation base.Invocation) result.Result {
	return invoker.Invoke(ctx, invocation)
}

func (*clientExtensionFilter) OnResponse(_ context.Context, response result.Result, _ base.Invoker, _ base.Invocation) result.Result {
	return response
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

func TestDialBindsResourcesAndAutomaticallyBuildsExtensionFilters(t *testing.T) {
	const (
		prefix       = "client-runtime-resource"
		protocolName = "client-runtime-resource-protocol"
	)
	registerGenericResultProtocol(t, protocolName)
	extension.Unregister(prefix)
	t.Cleanup(func() { extension.Unregister(prefix) })

	resources := make([]extension.Resource, 0, 2)
	factoryCalls := 0
	require.NoError(t, extension.Register(extension.Definition{
		Prefix:    prefix,
		Scopes:    extension.ClientScope,
		NewConfig: func() any { return &clientExtensionConfig{} },
		Filters: func(ctx *extension.Context) ([]extension.FilterSpec, error) {
			require.NotNil(t, ctx.Resource)
			resources = append(resources, *ctx.Resource)
			return []extension.FilterSpec{{
				ID:    prefix + ":filter",
				Order: 100,
				Factory: func() filter.Filter {
					factoryCalls++
					return &clientExtensionFilter{}
				},
			}}, nil
		},
	}))

	cli, err := NewClient(WithExtension(clientExtensionOption{prefix: prefix}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.CloseExtensions()) })

	for _, interfaceName := range []string{"payment.PaymentService", "user.UserService"} {
		connection, dialErr := cli.Dial(
			interfaceName,
			WithProtocol(protocolName),
			WithURL(protocolName+"://127.0.0.1:1"),
			WithClusterAvailable(),
			WithGroup("test"),
			WithVersion("v1"),
		)
		require.NoError(t, dialErr)
		require.NotNil(t, connection.refOpts.invoker)
	}

	require.Len(t, resources, 2)
	assert.Equal(t, extension.Resource{
		ServiceKey: "test/payment.PaymentService:v1",
		Interface:  "payment.PaymentService",
		Group:      "test",
		Version:    "v1",
	}, resources[0])
	assert.Equal(t, "test/user.UserService:v1", resources[1].ServiceKey)
	assert.Equal(t, 2, factoryCalls)
}
