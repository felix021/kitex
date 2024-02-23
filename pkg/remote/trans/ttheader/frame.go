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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"strconv"

	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/utils"
	"github.com/cloudwego/netpoll"
)

const (
	uint16max = (1 << 16) - 1

	FrameHeaderMagic       uint16 = 0x1001
	FrameHeaderPrefaceSize        = 7

	BitMaskHasMeta     = 0x0000_0001
	BitMaskEndOfStream = 0x0000_0002
)

var (
	ErrInvalidMetaSize  = errors.New("invalid meta size")
	ErrInvalidMetaValue = errors.New("invalid meta value")
	ErrMetaSizeTooLarge = errors.New("meta size too large")
)

type frame struct {
	FrameSize   uint32
	Flags       byte
	MetaSize    uint16
	MetaData    map[string]string
	PayloadSize uint32
	Payload     []byte
}

// HasMeta returns true if the frame has meta
func (f *frame) HasMeta() bool {
	return f.Flags&BitMaskHasMeta != 0
}

// IsEndOfStream returns true if the frame is end of stream
func (f *frame) IsEndOfStream() bool {
	return f.Flags&BitMaskEndOfStream != 0
}

// ReadMeta reads meta data from reader
func (f *frame) ReadMeta(reader netpoll.Reader) error {
	metaSizeBuf, err := reader.Next(2)
	if err != nil {
		return err
	}
	f.MetaSize = binary.BigEndian.Uint16(metaSizeBuf)

	metaBuf, err := reader.Next(int(f.MetaSize))
	if err != nil {
		return err
	}

	f.MetaData, err = parseKeyValuePairs(metaBuf)
	if err != nil {
		return err
	}
	return nil
}

// SetEndOfStream sets the end-of-stream bit
func (f *frame) SetEndOfStream() {
	f.Flags |= BitMaskEndOfStream
}

// Encode encodes the frame to bytes
func (f *frame) Encode() (bytesArray [][]byte, err error) {
	var metaBuf []byte
	if len(f.MetaData) > 0 {
		f.Flags |= BitMaskHasMeta
		if metaBuf, err = EncodeMeta(f.MetaData); err != nil {
			return nil, err
		}
		f.MetaSize = uint16(len(metaBuf))
	}

	f.FrameSize = FrameHeaderPrefaceSize + uint32(f.MetaSize) + f.PayloadSize

	preMetaBuf := make([]byte, FrameHeaderPrefaceSize)
	// frame size
	binary.BigEndian.PutUint32(preMetaBuf[0:4], f.FrameSize)
	// magic
	binary.BigEndian.PutUint16(preMetaBuf[4:6], FrameHeaderMagic)
	// flags
	preMetaBuf[6] = f.Flags

	return [][]byte{preMetaBuf, metaBuf, f.Payload}, nil
}

// EncodeBytes encodes the frame to bytes with copy
func (f *frame) EncodeBytes() ([]byte, error) {
	bytesArray, err := f.Encode()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, f.FrameSize)
	copy(buf, bytesArray[0])
	copy(buf[FrameHeaderPrefaceSize:], bytesArray[1])
	copy(buf[FrameHeaderPrefaceSize+len(bytesArray[1]):], bytesArray[2])
	return buf, nil
}

// EncodeMeta encodes meta data to bytes
func EncodeMeta(data map[string]string) ([]byte, error) {
	// meta size first
	size := 0
	for k, v := range data {
		size += sizeOfVarInt(len(k)) + sizeOfVarInt(len(v)) + len(k) + len(v)
	}
	if size > 0 {
		size += 2
	}

	if size > uint16max {
		return nil, ErrMetaSizeTooLarge
	}

	buf := make([]byte, size)
	binary.BigEndian.PutUint16(buf[0:2], uint16(size))
	idx := 2
	for k, v := range data {
		idx += binary.PutVarint(buf[idx:], int64(len(k)))
		copy(buf[idx:], k)
		idx += len(k)

		idx += binary.PutVarint(buf[idx:], int64(len(v)))
		copy(buf[idx:], v)
		idx += len(v)
	}
	return buf, nil
}

// readFrame reads frame from reader
func readFrame(conn net.Conn) (*frame, error) {
	reader := getReader(conn)
	prefaceBuf, err := reader.Next(FrameHeaderPrefaceSize)
	if err != nil {
		return nil, err
	}

	// check magic to identify malformed frame
	if magic := binary.BigEndian.Uint16(prefaceBuf[4:6]); magic != FrameHeaderMagic {
		return nil, err
	}

	f := &frame{
		FrameSize: binary.BigEndian.Uint32(prefaceBuf[0:4]),
		Flags:     prefaceBuf[6],
	}
	f.PayloadSize = f.FrameSize - FrameHeaderPrefaceSize

	if f.HasMeta() {
		if err = f.ReadMeta(reader); err != nil {
			return nil, err
		}
		f.PayloadSize -= uint32(f.MetaSize)
	}

	if f.Payload, err = reader.Next(int(f.PayloadSize)); err != nil {
		return nil, err
	}
	return f, err
}

func writeFrame(ctx context.Context, conn net.Conn, f *frame) error {
	bytes, err := f.EncodeBytes()
	if err != nil {
		return err
	}
	finish := make(chan error)
	go func() {
		// TODO: a new goroutine for each Send call is expensive.
		// Maybe we should start a goroutine for each stream for sending
		var writeErr error
		defer func() {
			if r := recover(); r != nil {
				panicInfo := fmt.Sprintf("writeFrame panic: %v, stack=%s", r, string(debug.Stack()))
				finish <- kerrors.ErrPanic.WithCause(errors.New(panicInfo))
			}
		}()
		_, writeErr = conn.Write(bytes)
		finish <- writeErr
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case writeErr := <-finish:
		return writeErr
	}
}

func newEndOfStreamFrame(ctx context.Context, code int, message string, bizErr kerrors.BizStatusErrorIface) *frame {
	// TODO: backward values
	meta := map[string]string{
		KeyErrorCode: strconv.Itoa(code),
	}

	if len(message) > 0 {
		meta[KeyErrorMessage] = message
	}

	if bizErr != nil {
		meta[KeyBizErrorCode] = strconv.Itoa(int(bizErr.BizStatusCode()))
		meta[KeyBizErrorMessage] = bizErr.BizMessage()
		if extra := bizErr.BizExtra(); len(extra) > 0 {
			value, _ := utils.Map2JSONStr(extra)
			meta[KeyBizErrorExtra] = value
		}
	}

	f := &frame{
		MetaData: meta,
	}
	f.SetEndOfStream()
	return f
}
