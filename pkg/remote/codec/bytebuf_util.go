/*
 * Copyright 2021 CloudWeGo Authors
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

package codec

import (
	"github.com/cloudwego/kitex/pkg/remote"
	codecutils "github.com/cloudwego/kitex/pkg/remote/codec/utils"
)

// ReadUint32 ...
func ReadUint32(in remote.ByteBuffer) (uint32, error) {
	return codecutils.ReadUint32(in)
}

// PeekUint32 ...
func PeekUint32(in remote.ByteBuffer) (uint32, error) {
	return codecutils.PeekUint32(in)
}

// ReadUint16 ...
func ReadUint16(in remote.ByteBuffer) (uint16, error) {
	return codecutils.ReadUint16(in)
}

// WriteUint32 ...
func WriteUint32(val uint32, out remote.ByteBuffer) error {
	return codecutils.WriteUint32(val, out)
}

// WriteUint16 ...
func WriteUint16(val uint16, out remote.ByteBuffer) error {
	return codecutils.WriteUint16(val, out)
}

// WriteByte ...
func WriteByte(val byte, out remote.ByteBuffer) error {
	return codecutils.WriteByte(val, out)
}

// Bytes2Uint32NoCheck ...
func Bytes2Uint32NoCheck(bytes []byte) uint32 {
	return codecutils.Bytes2Uint32NoCheck(bytes)
}

// Bytes2Uint32 ...
func Bytes2Uint32(bytes []byte) (uint32, error) {
	return codecutils.Bytes2Uint32(bytes)
}

// Bytes2Uint16NoCheck ...
func Bytes2Uint16NoCheck(bytes []byte) uint16 {
	return codecutils.Bytes2Uint16NoCheck(bytes)
}

// Bytes2Uint16 ...
func Bytes2Uint16(bytes []byte, off int) (uint16, error) {
	return codecutils.Bytes2Uint16(bytes, off)
}

// Bytes2Uint8 ...
func Bytes2Uint8(bytes []byte, off int) (uint8, error) {
	return codecutils.Bytes2Uint8(bytes, off)
}

// ReadString ...
func ReadString(in remote.ByteBuffer) (string, int, error) {
	return codecutils.ReadString(in)
}

// WriteString ...
func WriteString(val string, out remote.ByteBuffer) (int, error) {
	return codecutils.WriteString(val, out)
}

// WriteString2BLen ...
func WriteString2BLen(val string, out remote.ByteBuffer) (int, error) {
	return codecutils.WriteString2BLen(val, out)
}

// ReadString2BLen ...
func ReadString2BLen(bytes []byte, off int) (string, int, error) {
	return codecutils.ReadString2BLen(bytes, off)
}
