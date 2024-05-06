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
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/cloudwego/kitex/internal/test"
)

func TestFrame_WriteWithSize(t *testing.T) {
	t.Run("too-long", func(t *testing.T) {
		f := NewFrame(nil, nil)
		err := f.WriteWithSize(nil, 0x80000000, 0x80000000)
		test.Assert(t, err != nil, err)
	})
	t.Run("insufficient-space-for-header", func(t *testing.T) {
		h := NewHeader()
		f := NewFrame(h, nil)
		headerSize, err := h.BytesLength()
		test.Assert(t, err == nil, err)
		buf := make([]byte, 4+headerSize-1)
		err = f.WriteWithSize(buf, headerSize, 0)
		test.Assert(t, err != nil, err)
	})
	t.Run("header:write-err", func(t *testing.T) {
		h := NewHeader()
		token := make([]byte, 65536)
		h.SetToken(string(token))
		f := NewFrame(h, nil)

		buf := make([]byte, 100)
		err := f.WriteWithSize(buf, 50, 0)
		test.Assert(t, err != nil, err)
	})
	t.Run("normal", func(t *testing.T) {
		h := NewHeader()
		headerSize, err := h.BytesLength()
		test.Assert(t, err == nil, err)

		payload := []byte{1, 2, 3, 4}
		payloadSize := len(payload)

		totalSize := 4 + headerSize + payloadSize
		buf := make([]byte, totalSize)

		f := NewFrame(h, payload)
		err = f.WriteWithSize(buf, headerSize, payloadSize)
		test.Assert(t, err == nil, err)

		test.Assert(t, binary.BigEndian.Uint32(buf) == uint32(totalSize)-4)

		headerBytes, err := h.Bytes()
		test.Assert(t, err == nil, err)

		test.Assert(t, reflect.DeepEqual(buf[4:4+headerSize], headerBytes), buf[4:4+headerSize])

		test.Assert(t, reflect.DeepEqual(buf[4+headerSize:], payload), buf[4+headerSize:])
	})
}

func TestFrame_Bytes(t *testing.T) {
	t.Run("header-length:err", func(t *testing.T) {
		h := NewHeader()
		token := make([]byte, 65536)
		h.SetToken(string(token))
		f := NewFrame(h, nil)

		_, err := f.Bytes()
		test.Assert(t, err != nil, err)
	})
	t.Run("normal", func(t *testing.T) {
		h := NewHeader()
		payload := []byte{1, 2, 3, 4}
		f := NewFrame(h, payload)

		buf, err := f.Bytes()
		test.Assert(t, err == nil, err)

		headerBuf, err := h.Bytes()
		test.Assert(t, err == nil, err)

		headerSize := len(headerBuf)
		payloadSize := len(payload)

		test.Assert(t, binary.BigEndian.Uint32(buf) == uint32(headerSize+payloadSize))
		test.Assert(t, reflect.DeepEqual(buf[4:4+headerSize], headerBuf), buf[4:4+headerSize])
		test.Assert(t, reflect.DeepEqual(buf[4+headerSize:], payload), buf[4+headerSize:])
	})
}

func TestFrame_ReadWithSize(t *testing.T) {
	t.Run("read-header-err", func(t *testing.T) {
		buf := make([]byte, 10)
		binary.BigEndian.PutUint32(buf, 6)
		f := NewFrame(nil, nil)
		err := f.ReadWithSize(buf, 6)
		test.Assert(t, err != nil, err)
	})
	t.Run("normal", func(t *testing.T) {
		h := NewHeader()
		h.SetSeqID(0x12345678)
		payload := []byte{1, 2, 3, 4}
		fw := NewFrame(h, payload)
		buf, err := fw.Bytes()
		test.Assert(t, err == nil, err)

		fr := NewFrame(nil, nil)
		err = fr.ReadWithSize(buf[4:], len(buf)-4)
		test.Assert(t, err == nil, err)
		test.Assert(t, fr.Header().SeqID() == 0x12345678, fr.Header().SeqID())
		test.Assert(t, reflect.DeepEqual(fr.Payload(), payload), fr.Payload())
	})
}

func TestFrame_Read(t *testing.T) {
	t.Run("invalid-frame:no-frame-size", func(t *testing.T) {
		buf := make([]byte, 3)
		_, err := ReadFrame(bytes.NewReader(buf))
		test.Assert(t, err != nil, err)
	})
	t.Run("invalid-frame:no-frame-data", func(t *testing.T) {
		buf := make([]byte, 5)
		binary.BigEndian.PutUint32(buf, 10)
		_, err := ReadFrame(bytes.NewReader(buf))
		test.Assert(t, err != nil, err)
	})
	t.Run("normal", func(t *testing.T) {
		h := NewHeader()
		h.SetToken("token")
		payload := []byte{1, 2, 3, 4}
		fw := NewFrame(h, payload)
		buf, err := fw.Bytes()
		test.Assert(t, err == nil, err)

		fr, err := ReadFrame(bytes.NewReader(buf))
		test.Assert(t, err == nil, err)
		test.Assert(t, fr.Header().Token() == "token", fr.Header().Token())
		test.Assert(t, reflect.DeepEqual(fr.Payload(), payload), fr.Payload())
	})
	t.Run("normal:read-into-header", func(t *testing.T) {
		h := NewHeader()
		h.SetToken("token")
		payload := []byte{1, 2, 3, 4}
		fw := NewFrame(h, payload)
		buf, err := fw.Bytes()
		test.Assert(t, err == nil, err)

		hr := NewHeader()
		fr := NewFrame(hr, nil)
		err = fr.Read(bytes.NewReader(buf))
		test.Assert(t, err == nil, err)
		test.Assert(t, fr.Header().Token() == "token", fr.Header().Token())
		test.Assert(t, hr.Token() == "token", fr.Header().Token()) // read into pre-allocated header obj
		test.Assert(t, reflect.DeepEqual(fr.Payload(), payload), fr.Payload())
	})
}
