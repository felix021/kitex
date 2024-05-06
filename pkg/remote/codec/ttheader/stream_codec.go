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
	"strconv"

	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/codec"
	"github.com/cloudwego/kitex/pkg/remote/transmeta"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	header_transmeta "github.com/cloudwego/kitex/pkg/transmeta"
)

var (
	_ remote.PayloadCodec = (*streamCodec)(nil)

	ErrInvalidPayload = errors.New("ttheader streaming: invalid payload")

	ttheaderCodec = codec.TTHeaderCodec()
)

const (
	exceptionBufferSize = 256
)

// streamCodec implements the ttheader codec
type streamCodec struct{}

// NewStreamCodec ttheader codec construction
func NewStreamCodec() remote.PayloadCodec {
	return &streamCodec{}
}

// Marshal implements the remote.PayloadCodec interface
func (c *streamCodec) Marshal(ctx context.Context, message remote.Message, out remote.ByteBuffer) (err error) {
	var payload []byte
	defer func() {
		// record send size, even when err != nil (0 is recorded to the lastSendSize)
		if rpcStats := rpcinfo.AsMutableRPCStats(message.RPCInfo().Stats()); rpcStats != nil {
			rpcStats.IncrSendSize(uint64(len(payload)))
		}
	}()

	if codec.MessageFrameType(message) == codec.FrameTypeData {
		if payload, err = c.marshalData(ctx, message); err != nil {
			return err
		}
	}

	framedSizeBuf, err := ttheaderCodec.EncodeHeader(ctx, message, out)
	if err != nil {
		return err
	}
	headerSize := out.MallocLen() - codec.Size32
	binary.BigEndian.PutUint32(framedSizeBuf, uint32(headerSize+len(payload)))
	return codec.WriteAll(out.WriteBinary, payload)
}

func (c *streamCodec) marshalData(ctx context.Context, message remote.Message) ([]byte, error) {
	if message.Data() == nil { // for non-data frames: MetaFrame, HeaderFrame, TrailerFrame(w/o err)
		return nil, nil
	}
	serDes, err := remote.GetSerDes(message)
	if err != nil {
		return nil, ErrInvalidPayload
	}
	return serDes.Serialize(ctx, message.Data())
}

// InjectTransError set an error to message
// if err is not a remote.TransError, the exceptionType will be set to remote.InternalError
func InjectTransError(message remote.Message, err error) {
	exceptionType := remote.InternalError
	var transErr remote.TransError
	if errors.As(err, &transErr) {
		exceptionType = int(transErr.TypeID())
	}
	message.TransInfo().PutTransIntInfo(map[uint16]string{
		transmeta.TransCode:    strconv.Itoa(int(exceptionType)),
		transmeta.TransMessage: err.Error(),
	})
}

// Unmarshal implements the remote.PayloadCodec interface
func (c *streamCodec) Unmarshal(ctx context.Context, message remote.Message, in remote.ByteBuffer) (err error) {
	var payload []byte
	defer func() {
		if rpcStats := rpcinfo.AsMutableRPCStats(message.RPCInfo().Stats()); rpcStats != nil {
			// record recv size, even when err != nil (0 is recorded to the lastRecvSize)
			rpcStats.IncrRecvSize(uint64(len(payload)))
		}
	}()
	if err = ttheaderCodec.DecodeHeader(ctx, message, in); err != nil {
		return err
	}
	if err = c.decodeIntoMessage(message, in); err != nil {
		return err
	}
	if codec.MessageFrameType(message) != codec.FrameTypeData {
		return nil
	} else if message.Data() == nil {
		return nil // necessary for discarding dirty frames
	}
	// only data frame need to decode payload into message.Data()
	if payload, err = in.Next(message.PayloadLen()); err != nil {
		return err
	}
	serDes, err := remote.GetSerDes(message)
	if err != nil {
		return ErrInvalidPayload
	}
	return serDes.Deserialize(ctx, message.Data(), payload)
}

func (c *streamCodec) decodeIntoMessage(message remote.Message, in remote.ByteBuffer) (err error) {
	switch codec.MessageFrameType(message) {
	case codec.FrameTypeHeader:
		return c.updateRPCInfoFromHeaderFrame(message)
	case codec.FrameTypeTrailer:
		return c.decodeErrorFromTrailerFrame(message, in)
	}
	return nil
}

func (c *streamCodec) decodeErrorFromTrailerFrame(message remote.Message, in remote.ByteBuffer) error {
	if message.RPCRole() == remote.Server {
		return nil // client will not send error in a trailer
	}

	if err := parseTransError(message); err != nil {
		return err
	}

	// BizStatusError from StrInfo keys
	return header_transmeta.ParseBizStatusErrorToRPCInfo(
		message.TransInfo().TransStrInfo(),
		message.RPCInfo().Invocation(),
	)
}

func parseTransError(message remote.Message) error {
	intInfo := message.TransInfo().TransIntInfo()
	if transCode, exists := intInfo[transmeta.TransCode]; exists {
		code, err := strconv.Atoi(transCode)
		if err != nil {
			return remote.NewTransError(remote.InternalError,
				fmt.Errorf("parse trans code failed, code: %s, err: %s", transCode, err))
		}
		transMsg := intInfo[transmeta.TransMessage]
		return remote.NewTransErrorWithMsg(int32(code), transMsg)
	}
	return nil
}

func (c *streamCodec) updateRPCInfoFromHeaderFrame(message remote.Message) error {
	if message.RPCRole() == remote.Server {
		method, exists := message.TransInfo().TransIntInfo()[transmeta.ToMethod]
		if !exists { // method surely exists in Header Frame
			return errors.New("missing method in ttheader streaming header frame")
		}
		packageName, serviceName, methodName, err := codec.ParsePackageServiceMethod(method)
		if err != nil {
			return err
		}
		ink := message.RPCInfo().Invocation().(rpcinfo.InvocationSetter)
		ink.SetPackageName(packageName)
		ink.SetServiceName(serviceName)
		ink.SetMethodName(methodName)
	}
	return nil
}

// Name implements the remote.PayloadCodec interface
func (c *streamCodec) Name() string {
	return "ttheader streaming"
}
