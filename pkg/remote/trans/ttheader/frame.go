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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"strconv"

	"github.com/bytedance/gopkg/cloud/metainfo"
	"github.com/cloudwego/netpoll"

	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/remote/codec"
	"github.com/cloudwego/kitex/pkg/utils"
)

const (
	uint16max = (1 << 16) - 1

	FrameHeaderMagic uint16 = 0x1000

	BitMaskIsStreaming = uint16(1 << 15)
	BitMaskEndOfStream = uint16(1 << 14)
)

var (
	ErrInvalidMetaSize  = errors.New("invalid meta size")
	ErrInvalidMetaValue = errors.New("invalid meta value")
	ErrMetaSizeTooLarge = errors.New("meta size too large")
	ErrInvalidMagic     = errors.New("invalid ttheader magic")
)

var _ Header = (*ttheader)(nil)

func newTTHeader() *ttheader {
	return &ttheader{
		strInfo: make(map[string]string),
		intInfo: make(map[uint16]string),
	}
}

type ttheader struct {
	size       uint16
	flags      uint16
	seqNum     uint32
	protocolID uint8
	nTransform uint8
	transforms []byte
	intInfo    map[uint16]string
	strInfo    map[string]string
	token      string
}

// Size returns the size of ttheader, only valid for parsed ttheader
func (h *ttheader) Size() uint32 {
	return uint32(h.size)
}

// Bytes encodes the ttheader to bytes
func (h *ttheader) Bytes() ([]byte, error) {
	var size int = 12 // magic(2) + flags(2) + seq(4) + size(2) + protocol(1) + nTransform(1)
	if h.token != "" {
		size += 2 + len(h.token)
	}
	for _, v := range h.intInfo {
		size += 2 + 2 + len(v) // k(2) + vLen(2) + v
	}
	for k, v := range h.strInfo {
		size += 2 + len(k) + 2 + len(v) // kLen(2) + k + vLen(2) + v
	}
	if remain := size % 4; remain != 0 {
		size += 4 - remain // padding to multiple of 4
	}
	if size > uint16max {
		return nil, ErrMetaSizeTooLarge
	}

	buf := make([]byte, size)
	binary.BigEndian.PutUint16(buf[0:2], FrameHeaderMagic)
	binary.BigEndian.PutUint16(buf[2:4], h.flags)
	binary.BigEndian.PutUint32(buf[4:8], h.seqNum)
	binary.BigEndian.PutUint16(buf[8:12], uint16(h.size)/4)

	err := h.writeInfo(buf[12:])

	return buf, err
}

func (h *ttheader) readInfo(buf []byte) error {
	reader := bytes.NewReader(buf)
	h.strInfo = make(map[string]string)
	h.intInfo = make(map[uint16]string)
	for {
		infoID, err := reader.ReadByte()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		switch codec.InfoIDType(infoID) {
		case codec.InfoIDPadding:
			continue
		case codec.InfoIDKeyValue:
			k, v, err := readStrKVInfo(reader)
			if err != nil {
				return err
			}
			h.strInfo[k] = v
		case codec.InfoIDIntKeyValue:
			k, v, err := readIntKVInfo(reader)
			if err != nil {
				return err
			}
			h.intInfo[k] = v
		case codec.InfoIDACLToken:
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

func (h *ttheader) writeInfo(buf []byte) error {
	intSize, err := writeIntKVInfo(buf, h.intInfo)
	if err != nil {
		return err
	}
	strSize, err := writeStrKVInfo(buf[intSize:], h.strInfo)
	if err != nil {
		return err
	}
	if h.token == "" {
		return nil
	}
	_, err = writeLengthPrefixedString(buf[intSize+strSize:], h.token)
	return err
}

func (h *ttheader) setIsStreaming() {
	h.flags |= BitMaskIsStreaming
}

func (h *ttheader) setEndOfStream() {
	h.flags |= BitMaskEndOfStream
}

var _ Payload = (*payload)(nil)

func newPayload(bytes []byte) *payload {
	return &payload{
		size:  uint32(len(bytes)),
		bytes: bytes,
	}
}

type payload struct {
	size  uint32
	bytes []byte
}

func (p *payload) Size() uint32 {
	return p.size
}

func (p *payload) Bytes() ([]byte, error) {
	return p.bytes, nil
}

var _ Framer = (*frame)(nil)

type frame struct {
	FrameSize uint32
	header    *ttheader
	payload   *payload

	Flags       uint16
	MetaSize    uint16
	MetaData    map[string]string
	PayloadSize uint32
}

func (f *frame) Size() uint32 {
	return f.FrameSize
}

func (f *frame) Header() Header {
	return f.header
}

func (f *frame) Payload() Payload {
	return f.payload
}

// IsEndOfStream returns true if the frame is end of stream
func (f *frame) IsEndOfStream() bool {
	return f.header.flags&BitMaskEndOfStream != 0
}

// readHeader reads meta data from reader
// SetEndOfStream sets the end-of-stream bit
func (f *frame) SetEndOfStream() {
	f.header.flags |= BitMaskEndOfStream
}

// Encode encodes the frame to bytes
func (f *frame) Encode() (bytesArray [][]byte, err error) {
	headerBuf, err := f.header.Bytes()
	if err != nil {
		return nil, err
	}

	payloadBuf, err := f.Payload().Bytes()
	if err != nil {
		return nil, err
	}

	frameSize := uint32(len(headerBuf) + len(payloadBuf))
	frameSizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(frameSizeBuf, frameSize)

	return [][]byte{frameSizeBuf, headerBuf, payloadBuf}, nil
}

// Bytes encodes the frame to bytes with copy
func (f *frame) Bytes() ([]byte, error) {
	bytesArray, err := f.Encode()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, f.FrameSize)
	copy(buf, bytesArray[0])
	copy(buf[4:], bytesArray[1])
	copy(buf[4+len(bytesArray[1]):], bytesArray[2])
	return buf, nil
}

// readFrame reads frame from reader
func readFrame(conn net.Conn) (*frame, error) {
	reader := getReader(conn)
	prefaceBuf, err := reader.Peek(6) // size + magic
	if err != nil {
		return nil, err
	}

	frameSize := binary.BigEndian.Uint32(prefaceBuf[0:4])
	_ = reader.Skip(4)

	var hdr *ttheader
	if hdr, err = readHeader(reader); err != nil {
		return nil, err
	}

	payloadBuf, err := reader.Next(int(frameSize - hdr.Size()))
	if err != nil {
		return nil, err
	}

	return &frame{
		FrameSize: frameSize,
		header:    hdr,
		payload:   newPayload(payloadBuf),
	}, nil
}

func readHeader(reader netpoll.Reader) (*ttheader, error) {
	preface, err := reader.Next(10)
	if err != nil {
		return nil, err
	}

	// check magic to identify malformed frame
	if magic := binary.BigEndian.Uint16(preface[0:2]); magic != FrameHeaderMagic {
		return nil, ErrInvalidMagic
	}

	header := &ttheader{
		flags:  binary.BigEndian.Uint16(preface[2:4]),
		seqNum: binary.BigEndian.Uint32(preface[4:8]),
		size:   binary.BigEndian.Uint16(preface[8:10]) * 4,
	}

	headerBuf, err := reader.Next(int(header.size))
	if err != nil {
		return nil, err
	}

	header.protocolID = headerBuf[0]
	header.nTransform = headerBuf[1]
	header.transforms = headerBuf[2 : header.nTransform+2]

	err = header.readInfo(headerBuf[header.nTransform+2:])
	return header, err
}

func writeFrame(ctx context.Context, conn net.Conn, f *frame) error {
	bufList, err := f.Encode()
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
		for _, buf := range bufList {
			// TODO: netpoll append to avoid copy
			if _, writeErr = conn.Write(buf); writeErr != nil {
				break
			}
		}
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
	backwardValues := metainfo.AllBackwardValuesToSend(ctx)

	meta := make(map[string]string, len(backwardValues))
	for k, v := range backwardValues {
		meta[metainfo.PrefixBackward+k] = v
	}
	if len(message) > 0 {
		meta[KeyMessage] = message
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
