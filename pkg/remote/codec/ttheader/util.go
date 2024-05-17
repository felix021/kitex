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
	"fmt"
	"io"
	"reflect"
	"strings"
	"unsafe"

	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/transmeta"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
)

func readIntKVInfo(reader *bytesReader) (map[uint16]string, error) {
	count, err := readUint16(reader)
	if err != nil {
		return nil, err
	}
	result := make(map[uint16]string, int(count))
	var key uint16
	var value string
	for i := 0; i < int(count); i++ {
		if key, err = readUint16(reader); err != nil {
			return nil, err
		}
		if value, err = readLengthPrefixedString(reader); err != nil {
			return nil, err
		}
		result[key] = value
	}
	return result, nil
}

func readStrKVInfo(reader *bytesReader) (map[string]string, error) {
	count, err := readUint16(reader)
	if err != nil {
		return nil, err
	}
	result := make(map[string]string, int(count))
	var key, value string
	for i := 0; i < int(count); i++ {
		if key, err = readLengthPrefixedString(reader); err != nil {
			return nil, err
		}
		if value, err = readLengthPrefixedString(reader); err != nil {
			return nil, err
		}
		result[key] = value
	}
	return result, nil
}

func readLengthPrefixedString(reader *bytesReader) (string, error) {
	length, err := readUint16(reader)
	if err != nil { // unlikely
		return "", err
	}
	return readString(reader, int(length))
}

func readUint16(reader *bytesReader) (uint16, error) {
	buf, err := reader.ReadBytes(2)
	if err != nil {
		return 0, err
	}
	if len(buf) < 2 {
		return 0, io.EOF
	}
	return binary.BigEndian.Uint16(buf), nil
}

func readString(reader *bytesReader, length int) (string, error) {
	str, err := reader.ReadBytes(length)
	if err != nil {
		return "", err
	}
	if len(str) < length {
		return "", io.EOF
	}
	return byteSliceToString(str), nil
}

// byteSliceToString converts a byte slice to a string without a copy.
func byteSliceToString(buf []byte) string {
	return *(*string)(unsafe.Pointer(&buf))
}

// stringToByteSlice converts a string to a byte slice without a copy.
func stringToByteSlice(str string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&reflect.SliceHeader{
			Data: (*reflect.StringHeader)(unsafe.Pointer(&str)).Data,
			Len:  len(str),
			Cap:  len(str),
		},
	))
}

func writeByte(buf []byte, value byte) error {
	if len(buf) < 1 {
		return io.ErrShortWrite
	}
	buf[0] = value
	return nil
}

func writeIntKVInfo(buf []byte, info map[uint16]string) (int, error) {
	if len(info) == 0 {
		return 0, nil
	}
	if err := writeByte(buf[0:], InfoIDIntKeyValue); err != nil {
		return 0, err
	}
	if err := writeUint16(buf[1:], uint16(len(info))); err != nil {
		return 0, err
	}
	size := 3
	for k, v := range info {
		if err := writeUint16(buf[size:], k); err != nil {
			return size, err
		}
		size += 2
		vSize, err := writeLengthPrefixedString(buf[size:], v)
		if err != nil {
			return size, err
		}
		size += vSize
	}
	return size, nil
}

func writeStrKVInfo(buf []byte, info map[string]string) (int, error) {
	if len(info) == 0 {
		return 0, nil
	}
	if err := writeByte(buf[0:], InfoIDKeyValue); err != nil {
		return 0, err
	}
	if err := writeUint16(buf[1:], uint16(len(info))); err != nil {
		return 0, err
	}
	size := 3
	for k, v := range info {
		kSize, err := writeLengthPrefixedString(buf[size:], k)
		if err != nil {
			return size, err
		}
		size += kSize
		vSize, err := writeLengthPrefixedString(buf[size:], v)
		if err != nil {
			return size, err
		}
		size += vSize
	}
	return size, nil
}

func writeToken(buf []byte, token string) (int, error) {
	if len(token) == 0 {
		return 0, nil
	}
	if err := writeByte(buf, InfoIDACLToken); err != nil {
		return 0, err
	}
	n, err := writeLengthPrefixedString(buf[1:], token)
	return n + 1, err
}

func writeLengthPrefixedString(buf []byte, token string) (int, error) {
	tokenLength := len(token)
	err := writeUint16(buf, uint16(tokenLength))
	if err != nil {
		return 0, err
	}
	return 2 + tokenLength, writeString(buf[2:], token)
}

func writeString(buf []byte, token string) error {
	if len(buf) < len(token) {
		return io.ErrShortWrite
	}
	copy(buf, token)
	return nil
}

func writeUint16(buf []byte, u uint16) error {
	if len(buf) < 2 {
		return io.ErrShortWrite
	}
	binary.BigEndian.PutUint16(buf, u)
	return nil
}

func paddingSize(size, padding int) int {
	if remain := size % padding; remain != 0 {
		return padding - remain
	}
	return 0
}

func strInfoSize(strInfo map[string]string) (size int) {
	if len(strInfo) > 0 {
		size += 1 + 2 // info_id(1) + strInfoLen(2)
		for k, v := range strInfo {
			size += 2 + len(k) + 2 + len(v) // kLen(2) + k + vLen(2) + v
		}
	}
	return size
}

func intInfoSize(intInfo map[uint16]string) (size int) {
	if len(intInfo) > 0 {
		size += 1 + 2 // info_id(1) + intInfoLen(2)
		for _, v := range intInfo {
			size += 2 + 2 + len(v) // k(2) + vLen(2) + v
		}
	}
	return size
}

func tokenSize(token string) int {
	if len(token) == 0 {
		return 0
	}
	return 1 + 2 + len(token) // info_id(1) + token_len(2) + token
}

// IsMagic checks whether the magic number is valid.
func IsMagic(buf []byte) bool {
	return len(buf) >= 2 && binary.BigEndian.Uint16(buf) == FrameHeaderMagic
}

// IsStreaming checks whether the streaming bit is set
func IsStreaming(buf []byte) bool {
	return len(buf) >= 2 && binary.BigEndian.Uint16(buf)&BitMaskIsStreaming != 0
}

func writeAll(writer func([]byte) (int, error), buf []byte) error {
	for idx := 0; idx < len(buf); {
		n, err := writer(buf[idx:])
		if err != nil {
			return err
		}
		idx += n
	}
	return nil
}

// FrameType returns the frame type of the header; the default value is "trailer" if the key is not found.
func FrameType(header *Header) string {
	return getFrameType(header.IntInfo())
}

// MessageFrameType returns the frame type of the message; the default value is "trailer" if the key is not found.
func MessageFrameType(message remote.Message) string {
	return getFrameType(message.TransInfo().TransIntInfo())
}

func getFrameType(intInfo map[uint16]string) string {
	if ft, exists := intInfo[transmeta.FrameType]; exists {
		return ft
	}
	// to be compatible with old ttheader frames containing TApplicationException
	return FrameTypeTrailer
}

// ParsePackageServiceMethod parses the "$package.$service/$method" format
func ParsePackageServiceMethod(s string) (string, string, string, error) {
	if s != "" && s[0] == '/' {
		s = s[1:]
	}

	pos := strings.LastIndex(s, "/")
	if pos == -1 {
		return "", "", "", fmt.Errorf("malformed $package.$service/$method format %q", s)
	}
	packageDotService, methodName := s[:pos], s[pos+1:]

	if pos = strings.LastIndex(packageDotService, "."); pos == -1 { // package is not necessary
		return "", packageDotService, methodName, nil
	}

	packageName, serviceName := packageDotService[:pos], packageDotService[pos+1:]
	return packageName, serviceName, methodName, nil
}

// ProtocolIDToPayloadCodec converts ProtocolID to PayloadCodec
func ProtocolIDToPayloadCodec(p ProtocolID) serviceinfo.PayloadCodec {
	switch p {
	case ProtocolIDThriftBinary, ProtocolIDThriftCompact, ProtocolIDThriftCompactV2:
		return serviceinfo.Thrift
	case ProtocolIDKitexProtobuf:
		return serviceinfo.Protobuf
	default:
		return serviceinfo.NotSpecified
	}
}

// PayloadCodecToProtocolID converts PayloadCodec to ProtocolID
func PayloadCodecToProtocolID(p serviceinfo.PayloadCodec) ProtocolID {
	switch p {
	case serviceinfo.Thrift:
		return ProtocolIDThriftBinary
	case serviceinfo.Protobuf:
		return ProtocolIDKitexProtobuf
	default:
		return ProtocolIDNotSpecified
	}
}

// SetOrCheckSeqID is used to check the sequence ID.
