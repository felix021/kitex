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
	"fmt"
	"io"
)

const (
	uint16max = (1 << 16) - 1
	int32max  = (1 << 31) - 1

	FrameHeaderMagic uint16 = 0x1000

	BitMaskIsStreaming = 0b1000_0000_0000_0000

	PaddingSize = 4

	InfoIDPadding     byte = 0
	InfoIDKeyValue    byte = 0x01
	InfoIDIntKeyValue byte = 0x10
	InfoIDACLToken    byte = 0x11

	OffsetMagic      = 0
	OffsetFlags      = 2
	OffsetSeqID      = 4
	OffsetSize       = 8
	OffsetProtocol   = 10
	OffsetNTransform = 11
	OffsetVariable   = 12 // magic(2) + flags(2) + seq(4) + size(2) + protocol(1) + nTransform(1)

	FrameTypeMeta    = "1"
	FrameTypeHeader  = "2"
	FrameTypeData    = "3"
	FrameTypeTrailer = "4"

	StrKeyMetaData = "grpc-metadata"
)

var (
	ErrMetaSizeTooLarge = errors.New("meta size too large")
	ErrInvalidMagic     = errors.New("invalid ttheader magic")
)

type Header struct {
	size       uint16
	flags      uint16
	seqID      int32
	protocolID uint8
	nTransform uint8 // reserved for compression but not supported yet
	transforms []byte
	intInfo    map[uint16]string
	strInfo    map[string]string
	token      string
}

// NewHeader returns a new ttheader, with nil info maps
func NewHeader() *Header {
	return &Header{}
}

// NewHeaderWithInfo returns a new ttheader, with given info maps
func NewHeaderWithInfo(intInfo map[uint16]string, strInfo map[string]string) *Header {
	return &Header{
		intInfo: intInfo,
		strInfo: strInfo,
	}
}

// Size returns the size of ttheader, only valid for parsed ttheader
func (h *Header) Size() int {
	return int(h.size)
}

func (h *Header) SetSize(size int) {
	if size > uint16max {
		panic(ErrMetaSizeTooLarge)
	}
	h.size = uint16(size)
}

func (h *Header) Flags() uint16 {
	return h.flags
}

func (h *Header) SetFlags(flags uint16) {
	h.flags = flags
}

func (h *Header) SeqID() int32 {
	return h.seqID
}

func (h *Header) SetSeqID(seqID int32) {
	h.seqID = seqID
}

func (h *Header) ProtocolID() uint8 {
	return h.protocolID
}

func (h *Header) SetProtocolID(protocolID uint8) {
	h.protocolID = protocolID
}

func (h *Header) Transforms() []byte {
	return h.transforms
}

func (h *Header) NTransforms() uint8 {
	return h.nTransform
}

func (h *Header) SetTransforms(transforms []byte) {
	h.nTransform = uint8(len(transforms))
	h.transforms = transforms
}

func (h *Header) Token() string {
	return h.token
}

func (h *Header) SetToken(token string) {
	h.token = token
}

func (h *Header) IntInfo() map[uint16]string {
	return h.intInfo
}

func (h *Header) GetIntKey(key uint16) (string, bool) {
	if h.intInfo == nil {
		return "", false
	}
	val, ok := h.intInfo[key]
	return val, ok
}

func (h *Header) SetIntInfo(intInfo map[uint16]string) {
	h.intInfo = intInfo
}

func (h *Header) StrInfo() map[string]string {
	return h.strInfo
}

func (h *Header) GetStrKey(key string) (string, bool) {
	if h.strInfo == nil {
		return "", false
	}
	val, ok := h.strInfo[key]
	return val, ok
}

func (h *Header) SetStrInfo(strInfo map[string]string) {
	h.strInfo = strInfo
}

func (h *Header) SetIsStreaming() {
	h.flags |= BitMaskIsStreaming
}

func (h *Header) IsStreaming() bool {
	return h.flags&BitMaskIsStreaming != 0
}

// Bytes returns the bytes of ttheader
// Note: not including the 4-byte preceding Framed size (i.e. sizeof(ttheader) + sizeof(payload))
func (h *Header) Bytes() ([]byte, error) {
	size, err := h.BytesLength()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	err = h.WriteWithSize(buf, size)
	return buf, err
}

// BytesLength returns the byte size needed for serializing ttheader
// Note: not including the 4-byte preceding Framed size (i.e. sizeof(ttheader) + sizeof(payload))
func (h *Header) BytesLength() (int, error) {
	size := OffsetVariable
	if h.nTransform > 0 {
		size += int(h.nTransform)
	}
	size += tokenSize(h.token) + intInfoSize(h.intInfo) + strInfoSize(h.strInfo)
	size += paddingSize(size-OffsetProtocol, PaddingSize) // padding to multiple of 4, starting from protocolID
	if size > uint16max {
		return 0, ErrMetaSizeTooLarge
	}
	return size, nil
}

// WriteWithSize encodes the ttheader to bytes with given size
// It's useful when the size of the ttheader is already known
// Note:
// (1) Not including the 4-byte preceding Framed size (i.e. sizeof(ttheader) + sizeof(payload))
// (2) The caller is responsible for padding the size and allocate the buffer
func (h *Header) WriteWithSize(buf []byte, bufSize int) error {
	if len(buf) < 14 { // least size needed, including padding
		return io.ErrShortWrite
	}
	binary.BigEndian.PutUint16(buf[OffsetMagic:OffsetMagic+2], FrameHeaderMagic)
	binary.BigEndian.PutUint16(buf[OffsetFlags:OffsetFlags+2], h.flags)
	binary.BigEndian.PutUint32(buf[OffsetSeqID:OffsetSeqID+4], uint32(h.seqID))
	binary.BigEndian.PutUint16(buf[OffsetSize:OffsetSize+2],
		uint16(bufSize-OffsetProtocol)/PaddingSize) // not including fixed fields (10 bytes)
	buf[OffsetProtocol] = h.protocolID
	buf[OffsetNTransform] = h.nTransform
	offset := OffsetNTransform + 1
	if h.nTransform > 0 {
		offset += copy(buf[offset:], h.transforms)
	}
	return h.writeInfo(buf[offset:bufSize])
}

// Read decodes the ttheader from an io.Reader
// Note: DO NOT REUSE INPUT, since strings read from input will directly reference input to avoid copy
func (h *Header) Read(input []byte) (err error) {
	reader := newBytesReader(input)
	var buf []byte
	if buf, err = reader.ReadBytes(OffsetVariable); err != nil {
		return err
	}
	if !IsMagic(buf[OffsetMagic : OffsetMagic+2]) {
		return ErrInvalidMagic
	}
	h.flags = binary.BigEndian.Uint16(buf[OffsetFlags : OffsetFlags+2])
	h.seqID = int32(binary.BigEndian.Uint32(buf[OffsetSeqID : OffsetSeqID+4]))
	h.size = binary.BigEndian.Uint16(buf[OffsetSize:OffsetSize+2]) * PaddingSize // not including fixed fields
	h.protocolID = buf[OffsetProtocol]
	if err = h.readTransforms(buf[OffsetNTransform], reader); err != nil {
		return err
	}
	offset := OffsetNTransform + 1 + int(h.nTransform)
	varReader := newBytesReader(buf[offset : OffsetProtocol+h.size])
	return h.readInfo(varReader)
}

func (h *Header) readTransforms(nTransform byte, reader *bytesReader) (err error) {
	h.nTransform = nTransform
	h.transforms, err = reader.ReadBytes(int(nTransform))
	return err
}

func (h *Header) readInfo(reader *bytesReader) error {
	for {
		infoID, err := reader.ReadByte()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		switch infoID {
		case InfoIDPadding:
			continue
		case InfoIDKeyValue:
			h.strInfo, err = readStrKVInfo(reader)
			if err != nil {
				return err
			}
		case InfoIDIntKeyValue:
			h.intInfo, err = readIntKVInfo(reader)
			if err != nil {
				return err
			}
		case InfoIDACLToken:
			if token, err := readLengthPrefixedString(reader); err != nil {
				return err
			} else {
				h.token = token
			}
		default:
			return fmt.Errorf("invalid infoIDType[%#x]", infoID)
		}
	}
}

func (h *Header) writeInfo(buf []byte) error {
	intSize, err := writeIntKVInfo(buf, h.intInfo)
	if err != nil {
		return err
	}

	strSize, err := writeStrKVInfo(buf[intSize:], h.strInfo)
	if err != nil {
		return err
	}

	idx := intSize + strSize
	tokenLength, err := writeToken(buf[idx:], h.token)
	if err != nil {
		return err
	}

	// padding: buf might contain garbage data
	for i := idx + tokenLength; i < len(buf); i++ {
		buf[i] = 0
	}
	return nil
}
