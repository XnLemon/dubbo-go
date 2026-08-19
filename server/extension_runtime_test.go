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

type serverExtensionConfig struct {
	value int
}

type serverExtensionOption struct {
	prefix string
	value  int
}

type serverExtensionFilter struct{}

func (*serverExtensionFilter) Invoke(ctx context.Context, invoker base.Invoker, invocation base.Invocation) result.Result {
	return invoker.Invoke(ctx, invocation)
}

func (*serverExtensionFilter) OnResponse(_ context.Context, response result.Result, _ base.Invoker, _ base.Invocation) result.Result {
	return response
}

func (o serverExtensionOption) Prefix() string {
	return o.prefix
}

func (o serverExtensionOption) Apply(config any) error {
	config.(*serverExtensionConfig).value = o.value
	return nil
}

func TestWithExtensionBuildsServerRuntime(t *testing.T) {
	const prefix = "server-runtime-entry"
	extension.Unregister(prefix)
	t.Cleanup(func() { extension.Unregister(prefix) })
	require.NoError(t, extension.Register(extension.Definition{
		Prefix:    prefix,
		Scopes:    extension.ServerScope,
		NewConfig: func() any { return &serverExtensionConfig{} },
	}))

	srv, err := NewServer(WithExtension(serverExtensionOption{prefix: prefix, value: 11}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, srv.CloseExtensions()) })

	context, ok := srv.extensionRuntime.Context(prefix)
	require.True(t, ok)
	assert.Equal(t, extension.ServerScope, context.Scope)
	assert.Equal(t, common.RoleType(common.PROVIDER), context.Role)
	assert.Equal(t, 11, context.Config.(*serverExtensionConfig).value)
}

func TestRegisterBindsResourcesAndAutomaticallyContributesExtensionFilters(t *testing.T) {
	const prefix = "server-runtime-resource"
	extension.Unregister(prefix)
	t.Cleanup(func() { extension.Unregister(prefix) })

	resources := make([]extension.Resource, 0, 2)
	require.NoError(t, extension.Register(extension.Definition{
		Prefix:    prefix,
		Scopes:    extension.ServerScope,
		NewConfig: func() any { return &serverExtensionConfig{} },
		Filters: func(ctx *extension.Context) ([]extension.FilterSpec, error) {
			require.NotNil(t, ctx.Resource)
			resources = append(resources, *ctx.Resource)
			return []extension.FilterSpec{{
				ID:      prefix + ":filter",
				Order:   100,
				Factory: func() filter.Filter { return &serverExtensionFilter{} },
			}}, nil
		},
	}))

	srv, err := NewServer(WithExtension(serverExtensionOption{prefix: prefix}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, srv.CloseExtensions()) })
	handler := &mockServerRPCService{}

	for _, interfaceName := range []string{"payment.PaymentService", "user.UserService"} {
		err = srv.Register(
			handler,
			&common.ServiceInfo{InterfaceName: interfaceName},
			WithGroup("test"),
			WithVersion("v1"),
		)
		require.NoError(t, err)
	}

	require.Len(t, resources, 2)
	assert.Equal(t, extension.Resource{
		ServiceKey: "test/payment.PaymentService:v1",
		Interface:  "payment.PaymentService",
		Group:      "test",
		Version:    "v1",
	}, resources[0])
	assert.Equal(t, "test/user.UserService:v1", resources[1].ServiceKey)
	require.Len(t, srv.GetServiceOptions(handler.Reference()).filterSpecs, 1)
	assert.Equal(t, prefix+":filter", srv.GetServiceOptions(handler.Reference()).filterSpecs[0].ID)
}
