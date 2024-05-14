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
	"io"
	"strconv"

	"github.com/cloudwego/kitex/pkg/remote"
)

const frameHeaderSize = 4

// Frame is a framed message, including size, header and payload
type Frame struct {
	size    int
	header  *Header
	payload []byte
}

// NewFrame creates a new framed message with ttheader and payload
func NewFrame(header *Header, payload []byte) *Frame {
	return &Frame{header: header, payload: payload}
}

// ReadFrame reads framed message from io.Reader
func ReadFrame(reader io.Reader) (*Frame, error) {
	f := &Frame{}
	if err := f.Read(reader); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *Frame) Header() *Header {
	return f.header
}

func (f *Frame) Payload() []byte {
	return f.payload
}

func (f *Frame) PayloadAsTransError() (*remote.TransError, error) {
	if len(f.payload) == 0 {
		return nil, nil
	}
	exc := &exception{}
	if err := exc.read(newBytesReader(f.payload)); err != nil {
		return nil, err
	}
	return remote.NewTransErrorWithMsg(int32(exc.ExceptionType), exc.Message), nil
}

// Bytes encodes the frame to bytes
func (f *Frame) Bytes() ([]byte, error) {
	headerSize, err := f.Header().BytesLength()
	if err != nil {
		return nil, err
	}
	payloadSize := len(f.payload)
	total := 4 + headerSize + payloadSize
	buf := make([]byte, total)
	err = f.WriteWithSize(buf, headerSize, payloadSize)
	return buf, err
}

// WriteWithSize encodes the frame to bytes with given header/payload size
// Note: there'll be a copy of payload to the buf
func (f *Frame) WriteWithSize(buf []byte, headerSize, payloadSize int) error {
	if err := f.WriteHeader(buf, headerSize, payloadSize); err != nil {
		return err
	}
	copy(buf[4+headerSize:], f.payload)
	return nil
}

// WriteHeader encodes the frame header to bytes, including the preceding 4-byte framed size
// It does not copy the payload, which is helpful to implement a no-copy payload transfer
func (f *Frame) WriteHeader(buf []byte, headerSize, payloadSize int) error {
	framedSize := headerSize + payloadSize
	if framedSize < 0 || framedSize > int32max+1 {
		return errors.New("invalid frame size: " + strconv.Itoa(framedSize))
	}
	if len(buf) < 4+headerSize {
		return errors.New("not enough buffer for ttheader")
	}
	binary.BigEndian.PutUint32(buf, uint32(framedSize))
	return f.header.WriteWithSize(buf[4:4+headerSize], headerSize)
}

// Read decodes the frame from io.Reader
// it reads 4 bytes first to get the frame size and then read the full frame
func (f *Frame) Read(reader io.Reader) error {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return err
	}
	size := int(binary.BigEndian.Uint32(buf))
	buf = make([]byte, size)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return err
	}
	return f.ReadWithSize(buf, size)
}

// ReadWithSize decodes the frame from bytes with given frame size
// Note: the given buf should starts after the 4-byte frame size
func (f *Frame) ReadWithSize(buf []byte, size int) error {
	f.size = size
	if f.header == nil {
		f.header = NewHeader()
	}
	if err := f.header.Read(buf); err != nil {
		return err
	}
	f.payload = buf[OffsetProtocol+f.header.Size():]
	return nil
}
