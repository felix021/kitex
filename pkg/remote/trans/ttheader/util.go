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
	"io"
	"net"

	"github.com/cloudwego/netpoll"

	"github.com/cloudwego/kitex/pkg/streaming/metadata"
)

func getReader(conn net.Conn) netpoll.Reader {
	return conn.(interface{ Reader() netpoll.Reader }).Reader()
}

func sizeOfVarInt(l int) int {
	if l < (1 << 7 * 1) {
		return 1
	} else if l < (1 << 7 * 2) {
		return 2
	} else if l < (1 << 7 * 3) {
		return 3
	} else if l < (1 << 7 * 4) {
		return 4
	} else if l < (1 << 7 * 5) {
		return 5
	} else if l < (1 << 7 * 6) {
		return 6
	} else if l < (1 << 7 * 7) {
		return 7
	} else if l < (1 << 7 * 8) {
		return 8
	}
	return 9
}

// parseKeyValuePairs parses key-value pairs from buf
func parseKeyValuePairs(buf []byte) (meta map[string]string, err error) {
	meta = make(map[string]string)
	for idx := 0; idx < len(buf); {
		key, n, err := parseString(buf[idx:])
		if err != nil {
			return nil, err
		}
		idx += n
		value, n, err := parseString(buf[idx:])
		if err != nil {
			return nil, err
		}
		idx += n
		meta[key] = value
	}
	return meta, nil
}

// parseString reads a string with VarInt length prefix
func parseString(bytes []byte) (string, int, error) {
	size, n := binary.Uvarint(bytes)
	if n <= 0 {
		return "", 0, ErrInvalidMetaSize
	}
	if len(bytes) < n+int(size) {
		return "", 0, ErrInvalidMetaValue
	}
	// should not use the no-copy way since the buffer is returned by `reader.Next(n)`
	return string(bytes[n : n+int(size)]), n + int(size), nil
}

func metadataToMap(md metadata.MD) map[string]string {
	m := make(map[string]string, len(md))
	for k, v := range md {
		m[k] = v[0]
	}
	return m
}

func readIntKVInfo(reader *bytes.Reader) (uint16, string, error) {
	key, err := readUint16(reader)
	if err != nil {
		return 0, "", err
	}
	value, err := readLengthPrefixedString(reader)
	if err != nil {
		return 0, "", err
	}
	return key, value, nil
}

func readStrKVInfo(reader *bytes.Reader) (string, string, error) {
	key, err := readLengthPrefixedString(reader)
	if err != nil {
		return "", "", err
	}
	value, err := readLengthPrefixedString(reader)
	if err != nil {
		return "", "", err
	}
	return key, value, nil
}

func readLengthPrefixedString(reader *bytes.Reader) (string, error) {
	length, err := readUint16(reader)
	if err != nil { // unlikely
		return "", err
	}
	return readString(reader, int(length))
}

func readUint16(reader *bytes.Reader) (uint16, error) {
	buf := make([]byte, 2)
	if _, err := reader.Read(buf); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(buf), nil
}

func readString(reader *bytes.Reader, length int) (string, error) {
	value := make([]byte, length)
	if _, err := reader.Read(value); err != nil {
		return "", err
	}
	// TODO: no copy for better performance
	return string(value), nil
}

func writeIntKVInfo(buf []byte, info map[uint16]string) (int, error) {
	size := 0
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
	size := 0
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
