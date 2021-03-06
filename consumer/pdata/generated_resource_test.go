// Copyright 2020 OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by "internal/data_generator/main.go". DO NOT EDIT.
// To regenerate this file run "go run internal/data_generator/main.go".

package pdata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResource_InitEmpty(t *testing.T) {
	ms := NewResource()
	assert.True(t, ms.IsNil())
	ms.InitEmpty()
	assert.False(t, ms.IsNil())
}

func TestResource_CopyTo(t *testing.T) {
	ms := NewResource()
	NewResource().CopyTo(ms)
	assert.True(t, ms.IsNil())
	generateTestResource().CopyTo(ms)
	assert.EqualValues(t, generateTestResource(), ms)
}

func TestResource_Attributes(t *testing.T) {
	ms := NewResource()
	ms.InitEmpty()
	assert.EqualValues(t, NewAttributeMap(), ms.Attributes())
	fillTestAttributeMap(ms.Attributes())
	testValAttributes := generateTestAttributeMap()
	assert.EqualValues(t, testValAttributes, ms.Attributes())
}

func generateTestResource() Resource {
	tv := NewResource()
	tv.InitEmpty()
	fillTestResource(tv)
	return tv
}

func fillTestResource(tv Resource) {
	fillTestAttributeMap(tv.Attributes())
}
