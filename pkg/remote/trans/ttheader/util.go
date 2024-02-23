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
	"net"

	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/metadata"
	"github.com/cloudwego/netpoll"
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
