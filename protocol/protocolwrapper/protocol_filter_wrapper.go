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
	"fmt"
	"slices"
)

import (
	"github.com/dubbogo/gost/log/logger"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common"
	"dubbo.apache.org/dubbo-go/v3/common/extension"
	"dubbo.apache.org/dubbo-go/v3/filter"
	"dubbo.apache.org/dubbo-go/v3/protocol/base"
	"dubbo.apache.org/dubbo-go/v3/protocol/result"
)

const (
	// FILTER is protocol key.
	FILTER = "filter"
)

func init() {
	extension.SetProtocol(FILTER, GetProtocol)
}

// ProtocolFilterWrapper
// protocol in url decide who ProtocolFilterWrapper.protocol is
type ProtocolFilterWrapper struct {
	protocol base.Protocol
}

// Export service for remote invocation
func (pfw *ProtocolFilterWrapper) Export(invoker base.Invoker) base.Exporter {
	if pfw.protocol == nil {
		pfw.protocol = extension.GetProtocol(invoker.GetURL().Protocol)
	}
	var err error
	specs, err := filterSpecs(invoker.GetURL())
	if err == nil {
		invoker, err = BuildInvokerChain(invoker, specs)
	}
	if err != nil {
		logger.Errorf("[Protocol][Wrapper] build provider filter chain failed, err=%v", err)
		return nil
	}
	return pfw.protocol.Export(invoker)
}

// Refer a remote service
func (pfw *ProtocolFilterWrapper) Refer(url *common.URL) base.Invoker {
	if pfw.protocol == nil {
		pfw.protocol = extension.GetProtocol(url.Protocol)
	}
	invoker := pfw.protocol.Refer(url)
	if invoker == nil {
		return nil
	}
	specs, err := filterSpecs(url)
	if err == nil {
		invoker, err = BuildInvokerChain(invoker, specs)
	}
	if err != nil {
		logger.Errorf("[Protocol][Wrapper] build consumer filter chain failed, err=%v", err)
		return nil
	}
	return invoker
}

// Destroy will destroy all invoker and exporter.
func (pfw *ProtocolFilterWrapper) Destroy() {
	pfw.protocol.Destroy()
}

// BuildInvokerChain creates a chain from resolved FilterSpecs. It never reads
// filter names from URL parameters or the global extension registry.
func BuildInvokerChain(invoker base.Invoker, specs []extension.FilterSpec) (base.Invoker, error) {
	if len(specs) == 0 {
		return invoker, nil
	}
	validated, err := extension.MergeFilterSpecs(specs)
	if err != nil {
		return nil, err
	}

	// The order of filters is from left to right, so loading from right to left
	next := invoker
	ids := make([]string, 0, len(validated))
	for _, spec := range validated {
		ids = append(ids, spec.ID)
	}
	for _, spec := range slices.Backward(validated) {
		flt := spec.Factory()
		if flt == nil {
			return nil, fmt.Errorf("filter spec %q Factory returned nil", spec.ID)
		}
		fi := &FilterInvoker{next: next, invoker: invoker, filter: flt}
		next = fi
	}
	logger.Debugf("[Protocol][Wrapper] filter chain IDs=%v, invoker=%s", ids, invoker)
	return next, nil
}

func filterSpecs(url *common.URL) ([]extension.FilterSpec, error) {
	if url == nil {
		return nil, nil
	}
	value, ok := url.GetAttribute(extension.FilterSpecsAttributeKey)
	if !ok {
		return nil, nil
	}
	specs, ok := value.([]extension.FilterSpec)
	if !ok {
		return nil, fmt.Errorf("filter specs attribute has unexpected type %T", value)
	}
	return specs, nil
}

// GetProtocol returns a Protocol that applies filter chains around another protocol.
func GetProtocol() base.Protocol {
	return &ProtocolFilterWrapper{}
}

// FilterInvoker defines invoker and filter
type FilterInvoker struct {
	next    base.Invoker
	invoker base.Invoker
	filter  filter.Filter
}

// GetURL is used to get url from FilterInvoker
func (fi *FilterInvoker) GetURL() *common.URL {
	return fi.invoker.GetURL()
}

// IsAvailable is used to get available status
func (fi *FilterInvoker) IsAvailable() bool {
	return fi.invoker.IsAvailable()
}

// Invoke is used to call service method by invocation
func (fi *FilterInvoker) Invoke(ctx context.Context, invocation base.Invocation) result.Result {
	result := fi.filter.Invoke(ctx, fi.next, invocation)
	return fi.filter.OnResponse(ctx, result, fi.invoker, invocation)
}

// Destroy will destroy invoker
func (fi *FilterInvoker) Destroy() {
	fi.invoker.Destroy()
}
