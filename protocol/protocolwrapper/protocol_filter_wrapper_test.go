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

package protocolwrapper

import (
	"context"
	"net/url"
	"testing"
)

import (
	"github.com/dubbogo/gost/log/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common"
	"dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/common/extension"
	"dubbo.apache.org/dubbo-go/v3/filter"
	"dubbo.apache.org/dubbo-go/v3/protocol/base"
	"dubbo.apache.org/dubbo-go/v3/protocol/result"
)

const mockFilterKey = "mockEcho"

func TestProtocolFilterWrapperExport(t *testing.T) {
	filtProto := extension.GetProtocol(FILTER)
	filtProto.(*ProtocolFilterWrapper).protocol = &base.BaseProtocol{}

	u := common.NewURLWithOptions(
		common.WithParams(url.Values{}),
		common.WithAttribute(extension.FilterSpecsAttributeKey, []extension.FilterSpec{{
			ID:      mockFilterKey,
			Factory: newFilter,
		}}))
	exporter := filtProto.Export(base.NewBaseInvoker(u))
	_, ok := exporter.GetInvoker().(*FilterInvoker)
	assert.True(t, ok)
}

func TestProtocolFilterWrapperRefer(t *testing.T) {
	filtProto := extension.GetProtocol(FILTER)
	filtProto.(*ProtocolFilterWrapper).protocol = &base.BaseProtocol{}

	u := common.NewURLWithOptions(
		common.WithParams(url.Values{}),
		common.WithAttribute(extension.FilterSpecsAttributeKey, []extension.FilterSpec{{
			ID:      mockFilterKey,
			Factory: newFilter,
		}}))
	invoker := filtProto.Refer(u)
	_, ok := invoker.(*FilterInvoker)
	assert.True(t, ok)
}

func TestProtocolFilterWrapperIgnoresLegacyFilterNames(t *testing.T) {
	filtProto := extension.GetProtocol(FILTER)
	filtProto.(*ProtocolFilterWrapper).protocol = &base.BaseProtocol{}
	u := common.NewURLWithOptions(
		common.WithParams(url.Values{constant.ReferenceFilterKey: []string{mockFilterKey}}),
	)

	invoker := filtProto.Refer(u)
	_, wrapped := invoker.(*FilterInvoker)
	assert.False(t, wrapped)
}

func TestBuildInvokerChainRejectsNilFactoryResult(t *testing.T) {
	invoker, err := BuildInvokerChain(base.NewBaseInvoker(&common.URL{}), []extension.FilterSpec{{
		ID:      "test:nil",
		Factory: func() filter.Filter { return nil },
	}})

	require.Error(t, err)
	assert.Nil(t, invoker)
	assert.Contains(t, err.Error(), "returned nil")
}

type mockEchoFilter struct{}

func (ef *mockEchoFilter) Invoke(ctx context.Context, invoker base.Invoker, invocation base.Invocation) result.Result {
	logger.Infof("invoking echo filter.")
	logger.Debugf("%v,%v", invocation.MethodName(), len(invocation.Arguments()))
	if invocation.MethodName() == constant.Echo && len(invocation.Arguments()) == 1 {
		return &result.RPCResult{
			Rest: invocation.Arguments()[0],
		}
	}

	return invoker.Invoke(ctx, invocation)
}

func (ef *mockEchoFilter) OnResponse(ctx context.Context, result result.Result, invoker base.Invoker, invocation base.Invocation) result.Result {
	return result
}

func newFilter() filter.Filter {
	return &mockEchoFilter{}
}
