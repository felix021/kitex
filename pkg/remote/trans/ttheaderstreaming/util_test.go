/*
 * Copyright 2024 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ttheaderstreaming

import (
	"testing"

	"github.com/cloudwego/kitex/internal/test"
)

func Test_parsePackageServiceMethod(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		p, s, m, err := parsePackageServiceMethod("$package.$service/$method")
		test.Assert(t, err == nil, err)
		test.Assert(t, p == "$package", p)
		test.Assert(t, s == "$service", s)
		test.Assert(t, m == "$method", m)
	})
	t.Run("normal:dotted-pkg", func(t *testing.T) {
		p, s, m, err := parsePackageServiceMethod("a.b.c.$service/$method")
		test.Assert(t, err == nil, err)
		test.Assert(t, p == "a.b.c", p)
		test.Assert(t, s == "$service", s)
		test.Assert(t, m == "$method", m)
	})
	t.Run("normal:slash-prefixed", func(t *testing.T) {
		p, s, m, err := parsePackageServiceMethod("/$package.$service/$method")
		test.Assert(t, err == nil, err)
		test.Assert(t, p == "$package", p)
		test.Assert(t, s == "$service", s)
		test.Assert(t, m == "$method", m)
	})
	t.Run("normal:no-package", func(t *testing.T) {
		p, s, m, err := parsePackageServiceMethod("$service/$method")
		test.Assert(t, err == nil, err)
		test.Assert(t, p == "", p)
		test.Assert(t, s == "$service", s)
		test.Assert(t, m == "$method", m)
	})
	t.Run("error:no-service", func(t *testing.T) {
		_, _, _, err := parsePackageServiceMethod("$method")
		test.Assert(t, err != nil, err)
	})
	t.Run("error:empty-string", func(t *testing.T) {
		_, _, _, err := parsePackageServiceMethod("")
		test.Assert(t, err != nil, err)
	})
}
