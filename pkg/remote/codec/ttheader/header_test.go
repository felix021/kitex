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
	"errors"
	"reflect"
	"testing"

	"github.com/cloudwego/kitex/internal/test"
)

func TestNewHeaderWithInfo(t *testing.T) {
	intInfo := map[uint16]string{1: "a", 2: "b"}
	strInfo := map[string]string{"c": "d"}
	h := NewHeaderWithInfo(intInfo, strInfo)
	test.Assert(t, reflect.DeepEqual(h.IntInfo(), intInfo))
	test.Assert(t, reflect.DeepEqual(h.StrInfo(), strInfo))
}

func TestNewHeader(t *testing.T) {
	h := NewHeader()
	test.Assert(t, reflect.DeepEqual(h, &Header{}))
}

func TestHeader_Size(t *testing.T) {
	h := NewHeader()
	test.Assert(t, h.Size() == 0)
	h.SetSize(10)
	test.Assert(t, h.Size() == 10)

	defer func() {
		if err := recover(); err == nil {
			t.Errorf("should panic")
		} else {
			t.Logf("expected panic")
		}
	}()
	h.SetSize(uint16max + 1)
}

func TestHeader_Flags(t *testing.T) {
	h := NewHeader()
	test.Assert(t, h.Flags() == 0)
	h.SetFlags(10)
	test.Assert(t, h.Flags() == 10)
}

func TestHeader_SeqID(t *testing.T) {
	h := NewHeader()
	test.Assert(t, h.SeqID() == 0)
	h.SetSeqID(10)
	test.Assert(t, h.SeqID() == 10)
}

func TestHeader_ProtocolID(t *testing.T) {
	h := NewHeader()
	test.Assert(t, h.ProtocolID() == 0)
	h.SetProtocolID(10)
	test.Assert(t, h.ProtocolID() == 10)
}

func TestHeader_Token(t *testing.T) {
	h := NewHeader()
	test.Assert(t, h.Token() == "")
	h.SetToken("test")
	test.Assert(t, h.Token() == "test")
}

func TestHeader_IntInfo(t *testing.T) {
	h := NewHeader()
	test.Assert(t, h.IntInfo() == nil)
	intInfo := map[uint16]string{1: "a", 2: "b"}
	h.SetIntInfo(intInfo)
	test.Assert(t, reflect.DeepEqual(h.IntInfo(), intInfo))
}

func TestHeader_StrInfo(t *testing.T) {
	h := NewHeader()
	test.Assert(t, h.StrInfo() == nil)
	strInfo := map[string]string{"c": "d"}
	h.SetStrInfo(strInfo)
	test.Assert(t, reflect.DeepEqual(h.StrInfo(), strInfo))
}

func TestHeader_SetIsStreaming(t *testing.T) {
	h := NewHeader()
	test.Assert(t, h.IsStreaming() == false)
	h.SetIsStreaming()
	test.Assert(t, h.IsStreaming() == true)
}

func TestHeader_writeInfo(t *testing.T) {
	t.Run("int:short-write", func(t *testing.T) {
		intInfo := map[uint16]string{1: "a"}
		strInfo := map[string]string{"b": "c"}
		h := NewHeaderWithInfo(intInfo, strInfo)
		h.SetToken("token")
		buf := make([]byte, 0)
		err := h.writeInfo(buf)
		test.Assert(t, err != nil)
	})

	t.Run("str:short-write", func(t *testing.T) {
		intInfo := map[uint16]string{1: "a"}
		strInfo := map[string]string{"b": "c"}
		h := NewHeaderWithInfo(intInfo, strInfo)
		h.SetToken("token")
		buf := make([]byte, intInfoSize(intInfo))
		err := h.writeInfo(buf)
		test.Assert(t, err != nil)
		test.Assert(t, buf[0] == InfoIDIntKeyValue, buf[0])
	})

	t.Run("token:short-write", func(t *testing.T) {
		intInfo := map[uint16]string{1: "a"}
		strInfo := map[string]string{"b": "c"}
		h := NewHeaderWithInfo(intInfo, strInfo)
		h.SetToken("token")
		buf := make([]byte, intInfoSize(intInfo)+strInfoSize(strInfo))
		err := h.writeInfo(buf)
		test.Assert(t, err != nil)
		test.Assert(t, buf[0] == InfoIDIntKeyValue, buf[0])
		test.Assert(t, buf[intInfoSize(intInfo)] == InfoIDKeyValue, buf[intInfoSize(intInfo)])
	})

	t.Run("int+str+token", func(t *testing.T) {
		intInfo := map[uint16]string{1: "a"}
		strInfo := map[string]string{"b": "c"}
		h := NewHeaderWithInfo(intInfo, strInfo)
		h.SetToken("token")

		size := intInfoSize(intInfo) + strInfoSize(strInfo) + tokenSize("token")
		buf := make([]byte, size)
		err := h.writeInfo(buf)
		test.Assert(t, err == nil)

		test.Assert(t, buf[0] == InfoIDIntKeyValue, buf[0])
		test.Assert(t, buf[intInfoSize(intInfo)] == InfoIDKeyValue, buf[intInfoSize(intInfo)])
		test.Assert(t, buf[intInfoSize(intInfo)+strInfoSize(strInfo)] == InfoIDACLToken,
			buf[intInfoSize(intInfo)+strInfoSize(strInfo)])
	})

	t.Run("padding", func(t *testing.T) {
		h := NewHeader()
		size := 2
		buf := []byte{1, 2}
		err := h.writeInfo(buf[:size])
		test.Assert(t, err == nil, err)
		test.Assert(t, buf[0] == 0, buf[0])
		test.Assert(t, buf[1] == 0, buf[0])
	})

	t.Run("token+padding", func(t *testing.T) {
		h := NewHeader()
		h.SetToken("token")
		size := tokenSize(h.Token()) + 2
		buf := make([]byte, size)
		buf[size-2] = 1
		buf[size-1] = 2

		err := h.writeInfo(buf[:size])

		test.Assert(t, err == nil, err)
		test.Assert(t, buf[size-1] == 0, buf[0])
		test.Assert(t, buf[size-2] == 0, buf[0])
	})
}

func TestHeader_readInfo(t *testing.T) {
	intInfo := map[uint16]string{1: "a"}
	strInfo := map[string]string{"b": "c"}
	token := "token"

	t.Run("int+str+token", func(t *testing.T) {
		hw := NewHeaderWithInfo(intInfo, strInfo)
		hw.SetToken(token)
		buf := make([]byte, intInfoSize(intInfo)+strInfoSize(strInfo)+tokenSize(token))
		err := hw.writeInfo(buf)
		test.Assert(t, err == nil)

		h := NewHeader()
		err = h.readInfo(newBytesReader(buf))

		test.Assert(t, err == nil)
		test.Assert(t, h.IntInfo()[1] == "a")
		test.Assert(t, h.StrInfo()["b"] == "c")
		test.Assert(t, h.Token() == token)
	})

	t.Run("int+str", func(t *testing.T) {
		hw := NewHeaderWithInfo(intInfo, strInfo)
		buf := make([]byte, intInfoSize(intInfo)+strInfoSize(strInfo))
		err := hw.writeInfo(buf)
		test.Assert(t, err == nil)

		h := NewHeader()
		err = h.readInfo(newBytesReader(buf))

		test.Assert(t, err == nil)
		test.Assert(t, h.IntInfo()[1] == "a")
		test.Assert(t, h.StrInfo()["b"] == "c")
		test.Assert(t, h.token == "")
	})

	t.Run("int+token", func(t *testing.T) {
		hw := NewHeaderWithInfo(intInfo, nil)
		hw.SetToken(token)
		buf := make([]byte, intInfoSize(intInfo)+tokenSize(token))
		err := hw.writeInfo(buf)
		test.Assert(t, err == nil)

		h := NewHeader()
		err = h.readInfo(newBytesReader(buf))

		test.Assert(t, err == nil)
		test.Assert(t, h.IntInfo()[1] == "a")
		test.Assert(t, h.token == token)
		test.Assert(t, len(h.StrInfo()) == 0)
	})

	t.Run("str+token", func(t *testing.T) {
		hw := NewHeaderWithInfo(nil, strInfo)
		hw.SetToken(token)
		buf := make([]byte, strInfoSize(strInfo)+tokenSize(token))
		err := hw.writeInfo(buf)
		test.Assert(t, err == nil)

		h := NewHeader()
		err = h.readInfo(newBytesReader(buf))

		test.Assert(t, err == nil)
		test.Assert(t, len(h.IntInfo()) == 0)
		test.Assert(t, h.token == token)
		test.Assert(t, h.StrInfo()["b"] == "c")
	})

	t.Run("short-read", func(t *testing.T) {
		buf := make([]byte, 1)
		buf[0] = InfoIDIntKeyValue

		h := NewHeader()
		err := h.readInfo(newBytesReader(buf))

		test.Assert(t, err != nil)
	})

	t.Run("padding", func(t *testing.T) {
		buf := make([]byte, 1)
		buf[0] = InfoIDPadding

		h := NewHeader()
		err := h.readInfo(newBytesReader(buf))

		test.Assert(t, err == nil)
		test.Assert(t, h.token == "")
		test.Assert(t, len(h.IntInfo()) == 0)
		test.Assert(t, len(h.StrInfo()) == 0)
	})

	t.Run("invalid-info-id", func(t *testing.T) {
		buf := make([]byte, 1)
		buf[0] = 0xff

		h := NewHeader()
		err := h.readInfo(newBytesReader(buf))

		test.Assert(t, err != nil)
	})
}

func TestHeader_WriteWithSize(t *testing.T) {
	t.Run("fixed", func(t *testing.T) {
		h := NewHeader()
		h.SetIsStreaming()
		h.SetSeqID(0x12345678)
		h.SetProtocolID(0x1)

		bufSize, err := h.BytesLength()
		test.Assert(t, err == nil, err)
		test.Assert(t, bufSize == 14, bufSize)

		buf := make([]byte, bufSize)
		err = h.WriteWithSize(buf, bufSize)
		test.Assert(t, err == nil)

		test.Assert(t, binary.BigEndian.Uint16(buf[0:2]) == FrameHeaderMagic, buf[0:2])
		test.Assert(t, binary.BigEndian.Uint16(buf[2:4]) == h.flags, buf[2:4])
		test.Assert(t, binary.BigEndian.Uint32(buf[4:8]) == uint32(h.seqID), buf[4:8])
		test.Assert(t, binary.BigEndian.Uint16(buf[8:10]) == uint16(bufSize-10)/PaddingSize, buf[8:10])
		test.Assert(t, buf[OffsetProtocol] == h.protocolID, buf[OffsetProtocol])
		test.Assert(t, buf[OffsetNTransform] == 0x00, buf[OffsetNTransform:])                // nTransform: not supported yet
		test.Assert(t, buf[OffsetVariable] == 0x00 && buf[13] == 0x00, buf[OffsetVariable:]) // padding
	})

	t.Run("fixed:short-write", func(t *testing.T) {
		h := NewHeader()
		h.SetIsStreaming()
		h.SetSeqID(0x12345678)
		h.SetProtocolID(0x1)

		bufSize, err := h.BytesLength()
		test.Assert(t, err == nil, err)
		test.Assert(t, bufSize == 14, bufSize)

		buf := make([]byte, bufSize-1)
		err = h.WriteWithSize(buf, bufSize)
		test.Assert(t, err != nil)
	})

	t.Run("fixed+token+transforms", func(t *testing.T) {
		h := NewHeader()
		h.SetIsStreaming()
		h.SetSeqID(0x12345678)
		h.SetProtocolID(0x1)
		h.SetToken("test")
		transforms := []byte{0x1, 0x2, 0x3, 0x4}
		h.SetTransforms(transforms)

		bufSize, err := h.BytesLength()
		test.Assert(t, err == nil, err)
		test.Assert(t, bufSize == 22+len(transforms), bufSize)

		buf := make([]byte, bufSize)
		err = h.WriteWithSize(buf, bufSize)
		test.Assert(t, err == nil)

		test.Assert(t, binary.BigEndian.Uint16(buf[0:2]) == FrameHeaderMagic, buf[0:2])
		test.Assert(t, binary.BigEndian.Uint16(buf[2:4]) == h.flags, buf[2:4])
		test.Assert(t, binary.BigEndian.Uint32(buf[4:8]) == uint32(h.seqID), buf[4:8])
		test.Assert(t, binary.BigEndian.Uint16(buf[8:10]) == uint16(bufSize-10)/PaddingSize, buf[8:10])
		test.Assert(t, buf[OffsetProtocol] == h.protocolID, buf[OffsetProtocol])
		test.Assert(t, buf[OffsetNTransform] == byte(len(transforms)), buf[OffsetNTransform:])
		gotTransforms := buf[OffsetNTransform+1 : OffsetNTransform+1+len(transforms)]
		test.Assert(t, reflect.DeepEqual(gotTransforms, transforms), gotTransforms)
		offset := OffsetNTransform + 1 + len(transforms) // nTransform + transforms
		test.Assert(t, buf[offset] == InfoIDACLToken, buf[offset])
		offset += 1 // infoID
		test.Assert(t, binary.BigEndian.Uint16(buf[offset:offset+2]) == uint16(len("test")), buf[offset:offset+2])
		offset += 2 // size
		test.Assert(t, string(buf[offset:offset+4]) == "test", buf[offset+2:])
	})
}

func TestHeader_BytesLength(t *testing.T) {
	t.Run("fixed", func(t *testing.T) {
		h := NewHeader()
		length, err := h.BytesLength()
		test.Assert(t, err == nil, err)
		test.Assert(t, length == 14, length)
	})
	t.Run("fixed+token", func(t *testing.T) {
		h := NewHeader()
		h.SetToken("test")
		length, err := h.BytesLength()
		test.Assert(t, err == nil, err)
		test.Assert(t, length == 22, length) // fixed(12) + token(7) + padding(3)
	})
	t.Run("too-long", func(t *testing.T) {
		h := NewHeader()
		token := make([]byte, 65536)
		h.SetToken(string(token))
		_, err := h.BytesLength()
		test.Assert(t, err != nil, err)
	})
	t.Run("nTransform", func(t *testing.T) {
		h := NewHeader()
		h.SetTransforms([]byte{0x1, 0x2, 0x3, 0x4})
		size, err := h.BytesLength()
		test.Assert(t, err == nil, err)
		test.Assert(t, size == 18, size)
	})
}

func TestHeader_Bytes(t *testing.T) {
	t.Run("fixed", func(t *testing.T) {
		h := NewHeader()
		h.SetIsStreaming()
		h.SetSeqID(0x12345678)
		h.SetProtocolID(0x1)
		buf, err := h.Bytes()
		test.Assert(t, err == nil, err)
		test.Assert(t, len(buf) == 14, len(buf))
	})

	t.Run("fixed+token", func(t *testing.T) {
		h := NewHeader()
		h.SetIsStreaming()
		h.SetSeqID(0x12345678)
		h.SetProtocolID(0x1)
		h.SetToken("test")
		buf, err := h.Bytes()
		test.Assert(t, err == nil, err)
		test.Assert(t, len(buf) == 22, len(buf))
	})

	t.Run("too-long", func(t *testing.T) {
		h := NewHeader()
		token := make([]byte, 65536)
		h.SetToken(string(token))
		_, err := h.Bytes()
		test.Assert(t, err != nil, err)
	})
}

func TestHeader_Read(t *testing.T) {
	t.Run("fixed:short-read", func(t *testing.T) {
		h1 := NewHeader()
		err := h1.Read(nil)
		test.Assert(t, err != nil, err)
	})

	t.Run("invalid-magic", func(t *testing.T) {
		buf := make([]byte, 14)
		h1 := NewHeader()
		err := h1.Read(buf)
		test.Assert(t, errors.Is(err, ErrInvalidMagic), err)
	})

	t.Run("fixed+token+transforms", func(t *testing.T) {
		hw := NewHeader()
		hw.SetIsStreaming()
		hw.SetSeqID(0x12345678)
		hw.SetProtocolID(0x1)
		hw.SetToken("test")
		transforms := []byte{0x1, 0x2, 0x3, 0x4}
		hw.SetTransforms(transforms)
		buf, err := hw.Bytes()
		test.Assert(t, err == nil, err)

		hr := NewHeader()
		err = hr.Read(buf)
		test.Assert(t, err == nil, err)
		test.Assert(t, hr.flags == hw.flags, hr.flags)
		test.Assert(t, hr.seqID == hw.seqID, hr.seqID)
		test.Assert(t, hr.protocolID == hw.protocolID, hr.protocolID)
		test.Assert(t, hr.nTransform == hw.nTransform, hr.nTransform)
		test.Assert(t, hr.Token() == hw.Token(), hr.Token())
		test.Assert(t, hr.Size() == 12+len(transforms), hr.Size()) // protocol(1) + transform(5=1+len(transforms)) + token(7) + padding(3)
	})
}
