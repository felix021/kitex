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
)

// bytesReader provides a no-copy way to read from []byte
type bytesReader struct {
	buf []byte
	len int
	idx int
}

// newBytesReader returns a new bytesReader
func newBytesReader(buf []byte) *bytesReader {
	return &bytesReader{buf: buf, len: len(buf), idx: 0}
}

// ReadByte reads a single byte
func (r *bytesReader) ReadByte() (byte, error) {
	if r.idx >= r.len {
		return 0, io.EOF
	}
	r.idx++
	return r.buf[r.idx-1], nil
}

// ReadBytes reads n bytes
// Note: if there are less than n bytes, it will return io.EOF, together with the remaining bytes
func (r *bytesReader) ReadBytes(n int) ([]byte, error) {
	if r.idx >= r.len {
		return nil, io.EOF
	}
	prevIndex := r.idx
	r.idx += n
	if r.idx > r.len {
		return r.buf[prevIndex:], io.EOF
	}
	return r.buf[prevIndex:r.idx], nil
}

func (r *bytesReader) ReadUint16() (uint16, error) {
	if r.idx+2 > r.len {
		return 0, io.EOF
	}
	v := binary.BigEndian.Uint16(r.buf[r.idx : r.idx+2])
	r.idx += 2
	return v, nil
}

func (r *bytesReader) ReadUint32() (uint32, error) {
	if r.idx+4 > r.len {
		return 0, io.EOF
	}
	v := binary.BigEndian.Uint32(r.buf[r.idx : r.idx+4])
	r.idx += 4
	return v, nil
}

func (r *bytesReader) ReadInt32() (int32, error) {
	v, err := r.ReadUint32()
	return int32(v), err
}

func (r *bytesReader) ReadString(size int) (string, error) {
	if r.idx+size > r.len {
		return "", io.EOF
	}
	v := string(r.buf[r.idx : r.idx+size])
	r.idx += size
	return v, nil
}
