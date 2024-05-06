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

package ttheader

import (
	"encoding/binary"
	"io"
	"reflect"
	"testing"

	"github.com/cloudwego/kitex/internal/test"
)

func TestNewBufReader(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		buf := []byte{1, 2, 3}
		br := newBytesReader(buf)
		test.Assert(t, reflect.DeepEqual(br.buf, buf))
		test.Assert(t, br.len == 3)
		test.Assert(t, br.idx == 0)
	})
}

func TestBytesReader_ReadByte(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		buf := []byte{1, 2}
		br := newBytesReader(buf)

		b, err := br.ReadByte()
		test.Assert(t, err == nil)
		test.Assert(t, b == 1)

		b, err = br.ReadByte()
		test.Assert(t, err == nil)
		test.Assert(t, b == 2)

		b, err = br.ReadByte()
		test.Assert(t, err == io.EOF)
		test.Assert(t, b == 0)
	})

	t.Run("eof", func(t *testing.T) {
		buf := []byte{}
		br := newBytesReader(buf)

		b, err := br.ReadByte()
		test.Assert(t, err == io.EOF)
		test.Assert(t, b == 0)
	})
}

func TestBytesReader_ReadBytes(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		buf := []byte{1, 2, 3}
		br := newBytesReader(buf)

		b, err := br.ReadBytes(2)
		test.Assert(t, err == nil)
		test.Assert(t, b[0] == 1)
		test.Assert(t, b[1] == 2)
	})

	t.Run("read-all", func(t *testing.T) {
		buf := []byte{1, 2, 3}
		br := newBytesReader(buf)

		b, err := br.ReadBytes(3)
		test.Assert(t, err == nil)
		test.Assert(t, b[0] == 1)
		test.Assert(t, b[1] == 2)
		test.Assert(t, b[2] == 3)
	})

	t.Run("read-more-than-rest", func(t *testing.T) {
		buf := []byte{1, 2, 3}
		br := newBytesReader(buf)

		b, err := br.ReadBytes(4)
		test.Assert(t, err == io.EOF)
		test.Assert(t, len(b) == 3)
		test.Assert(t, b[0] == 1)
		test.Assert(t, b[1] == 2)
		test.Assert(t, b[2] == 3)
	})

	t.Run("eof-and-read", func(t *testing.T) {
		buf := []byte{1, 2, 3}
		br := newBytesReader(buf)

		b, err := br.ReadBytes(4)
		test.Assert(t, err == io.EOF)
		test.Assert(t, len(b) == 3)

		b, err = br.ReadBytes(1)
		test.Assert(t, err == io.EOF)
		test.Assert(t, b == nil)
	})
}

func Test_bytesReader_ReadString(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		buf := []byte{'0', '1', '2', '3'}
		br := newBytesReader(buf)
		s, err := br.ReadString(4)
		test.Assert(t, err == nil)
		test.Assert(t, s == "0123")
	})
	t.Run("short-read", func(t *testing.T) {
		buf := []byte{'0', '1', '2', '3'}
		br := newBytesReader(buf)
		_, err := br.ReadString(5)
		test.Assert(t, err == io.EOF)
	})
}

func Test_bytesReader_ReadInt32(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, 0xffffffff)
		br := newBytesReader(buf)
		i, err := br.ReadInt32()
		test.Assert(t, err == nil)
		test.Assert(t, i == -1)
	})

	t.Run("eof", func(t *testing.T) {
		buf := []byte{1, 2, 3}
		br := newBytesReader(buf)
		_, err := br.ReadInt32()
		test.Assert(t, err == io.EOF)
	})
}

func Test_bytesReader_ReadUint32(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, 0x7fffffff)
		br := newBytesReader(buf)
		i, err := br.ReadUint32()
		test.Assert(t, err == nil)
		test.Assert(t, i == 0x7fffffff)
	})
	t.Run("eof", func(t *testing.T) {
		buf := []byte{1, 2, 3}
		br := newBytesReader(buf)
		_, err := br.ReadUint32()
		test.Assert(t, err == io.EOF)
	})
}

func Test_bytesReader_ReadUint16(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		buf := make([]byte, 2)
		binary.BigEndian.PutUint16(buf, 0x7fff)
		br := newBytesReader(buf)
		i, err := br.ReadUint16()
		test.Assert(t, err == nil)
		test.Assert(t, i == 0x7fff)
	})
	t.Run("eof", func(t *testing.T) {
		buf := []byte{1}
		br := newBytesReader(buf)
		_, err := br.ReadUint16()
		test.Assert(t, err == io.EOF)
	})
}
