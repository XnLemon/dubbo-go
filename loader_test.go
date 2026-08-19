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
	"os"
	"path/filepath"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	return p
}

func TestHotUpdateConfig_AllowsLoggerLevelChange(t *testing.T) {
	// snapshot globals we touch and restore afterwards
	prevIns := instanceOptions
	defer func() { instanceOptions = prevIns }()

	tmp := t.TempDir()
	base := "dubbo:\n  logger:\n    level: info\n"
	updated := "dubbo:\n  logger:\n    level: debug\n"

	path := writeFile(t, tmp, "conf.yaml", base)
	conf := NewLoaderConf(WithPath(path))

	// overwrite the file with the updated content
	if err := os.WriteFile(path, []byte(updated), 0o600); err != nil {
		t.Fatalf("overwrite file: %v", err)
	}

	if err := hotUpdateConfig(conf); err != nil {
		t.Fatalf("hotUpdateConfig unexpected error: %v", err)
	}
	if got := instanceOptions.Logger.Level; got != "debug" {
		t.Fatalf("logger level not updated, want=debug got=%s", got)
	}
}

func TestHotUpdateConfig_DeniesDisallowedChange(t *testing.T) {
	// snapshot globals we touch and restore afterwards
	prevIns := instanceOptions
	defer func() { instanceOptions = prevIns }()

	tmp := t.TempDir()
	base := "dubbo:\n  application:\n    name: app1\n"
	updated := "dubbo:\n  application:\n    name: app2\n"

	path := writeFile(t, tmp, "conf.yaml", base)
	conf := NewLoaderConf(WithPath(path))

	// ensure a known baseline value in globals
	instanceOptions.Application.Name = "baseline"

	if err := os.WriteFile(path, []byte(updated), 0o600); err != nil {
		t.Fatalf("overwrite file: %v", err)
	}

	if err := hotUpdateConfig(conf); err == nil {
		t.Fatalf("expected error for disallowed change, got nil")
	}
	if got := instanceOptions.Application.Name; got != "baseline" {
		t.Fatalf("instanceOptions changed unexpectedly, want=baseline got=%s", got)
	}
}

func TestHotUpdateConfig_AllowsWithCustomPrefix(t *testing.T) {
	// snapshot globals and hot-reload predicates
	prevIns := instanceOptions
	prevPreds := hotReloadAllowedPredicates
	defer func() { instanceOptions = prevIns; hotReloadAllowedPredicates = prevPreds }()

	tmp := t.TempDir()
	base := "dubbo:\n  application:\n    name: app1\n"
	updated := "dubbo:\n  application:\n    name: app2\n"

	path := writeFile(t, tmp, "conf.yaml", base)
	conf := NewLoaderConf(WithPath(path))

	// allow changing any key under dubbo.application.*
	AllowHotReloadPrefix("dubbo.application.")

	if err := os.WriteFile(path, []byte(updated), 0o600); err != nil {
		t.Fatalf("overwrite file: %v", err)
	}

	if err := hotUpdateConfig(conf); err != nil {
		t.Fatalf("hotUpdateConfig unexpected error with allowed prefix: %v", err)
	}
}

func TestGetExtensionRawConfigPreservesResourceKeys(t *testing.T) {
	conf := NewLoaderConf(WithBytes([]byte("" +
		"dubbo:\n" +
		"  extensions:\n" +
		"    hystrix:\n" +
		"      consumer:\n" +
		"        'greet.GreetService:::Greet':\n" +
		"          timeout: 1000\n" +
		"      provider:\n" +
		"        'greet.GreetService:::Greet':\n" +
		"          timeout: 1500\n")))
	koan := GetConfigResolver(conf)

	raw, found, err := GetExtensionRawConfig(koan, "hystrix", "consumer")
	require.NoError(t, err)
	require.True(t, found)

	resource, ok := raw.Selected.Child("greet.GreetService:::Greet")
	require.True(t, ok)
	timeout, ok := resource.Child("timeout")
	require.True(t, ok)
	assert.Equal(t, 1000, timeout.Value())
}

func TestGetExtensionRawConfigResolvesPlaceholdersInResourceKeys(t *testing.T) {
	conf := NewLoaderConf(WithBytes([]byte("" +
		"dubbo:\n" +
		"  application:\n" +
		"    timeout: 2300\n" +
		"  extensions:\n" +
		"    hystrix:\n" +
		"      consumer:\n" +
		"        'greet.GreetService:::Greet':\n" +
		"          timeout: '${dubbo.application.timeout}'\n")))

	raw, found, err := GetExtensionRawConfig(GetConfigResolver(conf), "hystrix", "consumer")
	require.NoError(t, err)
	require.True(t, found)

	resource, ok := raw.Selected.Child("greet.GreetService:::Greet")
	require.True(t, ok)
	timeout, ok := resource.Child("timeout")
	require.True(t, ok)
	assert.Equal(t, 2300, timeout.Value())
	_, ok = raw.Selected.Child("greet")
	assert.False(t, ok)
}

func TestGetExtensionRawConfigIgnoresKoanfDelimiter(t *testing.T) {
	conf := NewLoaderConf(WithDelim("/"), WithBytes([]byte(""+
		"dubbo:\n"+
		"  extensions:\n"+
		"    hystrix:\n"+
		"      consumer:\n"+
		"        'greet.GreetService:::Greet':\n"+
		"          timeout: 1000\n")))

	raw, found, err := GetExtensionRawConfig(GetConfigResolver(conf), "hystrix", "consumer")
	require.NoError(t, err)
	require.True(t, found)

	resource, ok := raw.Selected.Child("greet.GreetService:::Greet")
	require.True(t, ok)
	timeout, ok := resource.Child("timeout")
	require.True(t, ok)
	assert.Equal(t, 1000, timeout.Value())
}

func TestGetExtensionRawConfigUsesMergedProfile(t *testing.T) {
	tmp := t.TempDir()
	basePath := writeFile(t, tmp, "dubbogo.yaml", "dubbo:\n  profiles:\n    active: dev\n  extensions:\n    hystrix:\n      consumer:\n        'greet.GreetService:::Greet':\n          timeout: 1000\n")
	writeFile(t, tmp, "dubbogo-dev.yaml", "dubbo:\n  extensions:\n    hystrix:\n      consumer:\n        'greet.GreetService:::Greet':\n          timeout: 2000\n")

	conf := NewLoaderConf(WithPath(basePath))
	koan := conf.MergeConfig(GetConfigResolver(conf))
	raw, found, err := GetExtensionRawConfig(koan, "hystrix", "consumer")
	require.NoError(t, err)
	require.True(t, found)

	resource, ok := raw.Selected.Child("greet.GreetService:::Greet")
	require.True(t, ok)
	timeout, ok := resource.Child("timeout")
	require.True(t, ok)
	assert.Equal(t, 2000, timeout.Value())
}

func TestGetExtensionRawConfigResolvesProfilePlaceholdersFromBase(t *testing.T) {
	tmp := t.TempDir()
	basePath := writeFile(t, tmp, "dubbogo.yaml", "dubbo:\n  profiles:\n    active: dev\n  application:\n    timeout: 2300\n  extensions:\n    hystrix:\n      consumer:\n        'greet.GreetService:::Greet':\n          timeout: 1000\n")
	writeFile(t, tmp, "dubbogo-dev.yaml", "dubbo:\n  extensions:\n    hystrix:\n      consumer:\n        'greet.GreetService:::Greet':\n          timeout: '${dubbo.application.timeout}'\n")

	conf := NewLoaderConf(WithPath(basePath))
	raw, found, err := GetExtensionRawConfig(conf.MergeConfig(GetConfigResolver(conf)), "hystrix", "consumer")
	require.NoError(t, err)
	require.True(t, found)

	resource, ok := raw.Selected.Child("greet.GreetService:::Greet")
	require.True(t, ok)
	timeout, ok := resource.Child("timeout")
	require.True(t, ok)
	assert.Equal(t, 2300, timeout.Value())
}
