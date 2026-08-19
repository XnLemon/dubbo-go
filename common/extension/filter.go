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
	"github.com/pkg/errors"
)

import (
	"dubbo.apache.org/dubbo-go/v3/filter"
)

var (
	frameworkFilters         = NewRegistry[func() filter.Filter]("framework filter")
	rejectedExecutionHandler = NewRegistry[func() filter.RejectedExecutionHandler]("rejected execution handler")
)

// RegisterFrameworkFilter registers an internal framework filter factory.
// External extensions contribute filters through Definition.Filters instead.
func RegisterFrameworkFilter(id string, factory func() filter.Filter) {
	frameworkFilters.Register(id, factory)
}

// NewFrameworkFilter creates one internal framework filter by ID.
func NewFrameworkFilter(id string) (filter.Filter, bool) {
	creator, ok := frameworkFilters.Get(id)
	if !ok {
		return nil, false
	}
	return creator(), true
}

// FrameworkFilterSpecs resolves internal framework IDs into FilterSpecs. The
// IDs are namespaced so they cannot be mistaken for user-facing filter names.
func FrameworkFilterSpecs(ids ...string) ([]FilterSpec, error) {
	specs := make([]FilterSpec, 0, len(ids))
	for order, id := range ids {
		factory, ok := frameworkFilters.Get(id)
		if !ok {
			return nil, errors.Errorf("framework filter %q is not registered", id)
		}
		specs = append(specs, FilterSpec{
			ID:      "framework:" + id,
			Factory: factory,
			Order:   order,
		})
	}
	return specs, nil
}

// SetRejectedExecutionHandler sets the RejectedExecutionHandler with @name
func SetRejectedExecutionHandler(name string, creator func() filter.RejectedExecutionHandler) {
	rejectedExecutionHandler.Register(name, creator)
}

// GetRejectedExecutionHandler finds the RejectedExecutionHandler with @name
func GetRejectedExecutionHandler(name string) (filter.RejectedExecutionHandler, error) {
	creator, ok := rejectedExecutionHandler.Get(name)
	if !ok {
		return nil, errors.New("RejectedExecutionHandler for " + name + " is not existing, make sure you have import the package " +
			"and you have register it by invoking extension.SetRejectedExecutionHandler.")
	}
	return creator(), nil
}

// UnregisterFrameworkFilter removes an internal framework filter factory.
func UnregisterFrameworkFilter(id string) {
	frameworkFilters.Unregister(id)
}

// UnregisterRejectedExecutionHandler removes the RejectedExecutionHandler with @name
func UnregisterRejectedExecutionHandler(name string) {
	rejectedExecutionHandler.Unregister(name)
}

// FrameworkFilterIDs returns registered internal framework filter IDs.
func FrameworkFilterIDs() []string {
	return frameworkFilters.Names()
}
