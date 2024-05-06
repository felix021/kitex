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

	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/codec"
	"github.com/cloudwego/kitex/pkg/remote/codec/protobuf"
	"github.com/cloudwego/kitex/pkg/remote/codec/thrift"
	"github.com/cloudwego/kitex/pkg/remote/transmeta"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	header_transmeta "github.com/cloudwego/kitex/pkg/transmeta"
	"github.com/cloudwego/kitex/transport"
)

var (
	ErrInvalidPayload                     = errors.New("grpc invalid payload")
	ErrInvalidSeqID                       = errors.New("ttheader streaming: client received frame with invalid seq id")
	_                 remote.PayloadCodec = (*streamCodec)(nil)
)

// streamCodec implements the ttheader codec
type streamCodec struct {
	thriftCodec remote.PayloadCodec
}

type codecOption func(c *streamCodec)

// WithThriftCodec set thriftCodec if the given codec is thrift.thriftCodec
func WithThriftCodec(pc remote.PayloadCodec) codecOption {
	return func(c *streamCodec) {
		if thrift.IsThriftCodec(pc) {
			c.thriftCodec = pc
		}
	}
}

// NewCodec ttheader codec construction
func NewCodec(opts ...codecOption) remote.PayloadCodec {
	c := &streamCodec{}
	for _, opt := range opts {
		opt(c)
	}
	return c
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

	if payload, err = c.marshalData(ctx, message); err != nil {
		return err
	}

	header := NewHeaderWithInfo(message.TransInfo().TransIntInfo(), message.TransInfo().TransStrInfo())
	header.SetIsStreaming()
	header.SetSeqID(message.RPCInfo().Invocation().SeqID())
	header.SetProtocolID(byte(codec.PayloadCodecToProtocolID(message.ProtocolInfo().CodecType)))
	if token, exists := message.TransInfo().TransStrInfo()[transmeta.GDPRToken]; exists {
		header.SetToken(token)
	}
	headerSize, err := header.BytesLength()
	if err != nil {
		return err
	}
	headerBuf, err := out.Malloc(frameHeaderSize + headerSize)
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint32(headerBuf, uint32(headerSize+len(payload)))
	if err = header.WriteWithSize(headerBuf[frameHeaderSize:], headerSize); err != nil {
		return err
	}
	return writeAll(out.WriteBinary, payload)
}

func (c *streamCodec) marshalData(ctx context.Context, message remote.Message) (payload []byte, err error) {
	if message.Data() == nil { // for non-data frames
		return nil, nil
	}
	if err, ok := message.Data().(error); ok {
		return marshalApplicationException(message.RPCInfo(), err)
	}
	switch message.ProtocolInfo().CodecType {
	case serviceinfo.Thrift:
		if payload, err = thrift.MarshalThriftData(ctx, c.thriftCodec, message.Data()); err != nil {
			return nil, err
		}
	case serviceinfo.Protobuf:
		if payload, err = protobuf.MarshalProtobufData(ctx, message.Data()); err != nil {
			return nil, err
		}
	default:
		return nil, ErrInvalidPayload
	}
	return
}

func marshalApplicationException(ri rpcinfo.RPCInfo, err error) ([]byte, error) {
	exceptionType := remote.InternalError
	var transErr remote.TransError
	if errors.As(err, &transErr) {
		exceptionType = int(transErr.TypeID())
	}
	exc := newException(ri.Invocation().MethodName(), ri.Invocation().SeqID(), err.Error(), exceptionType)
	return exc.Bytes()
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
	var f *Frame
	if f, err = ReadFrame(in); err != nil {
		return nil
	}
	if err = c.decodeIntoMessage(message, f); err != nil {
		return err
	}
	if getFrameType(message.TransInfo().TransIntInfo()) != FrameTypeData {
		return nil
	} else if message.Data() == nil {
		return nil // necessary for discarding dirty frames
	}
	// only data frame need to decode payload into message.Data()
	payload = f.Payload()
	switch message.ProtocolInfo().CodecType {
	case serviceinfo.Thrift:
		return thrift.UnmarshalThriftData(ctx, c.thriftCodec, "", payload, message.Data())
	case serviceinfo.Protobuf:
		return protobuf.UnmarshalProtobufData(ctx, message.Data(), payload)
	default:
		return ErrInvalidPayload
	}
}

func (c *streamCodec) decodeIntoMessage(message remote.Message, f *Frame) (err error) {
	header := f.Header()
	if clientCheckSeqID(message, header) {
		return ErrInvalidSeqID
	}
	message.SetPayloadLen(len(f.Payload()))
	message.SetProtocolInfo(remote.NewProtocolInfo(
		transport.TTHeader, // ttheader streaming
		codec.ProtocolIDToPayloadCodec(codec.ProtocolID(header.ProtocolID())),
	))
	message.TransInfo().PutTransIntInfo(header.IntInfo())
	message.TransInfo().PutTransStrInfo(header.StrInfo())
	if t := header.Token(); len(t) != 0 {
		message.TransInfo().TransStrInfo()[transmeta.GDPRToken] = t
	}
	codec.FillBasicInfoOfTTHeader(message)
	switch FrameType(header) {
	case FrameTypeHeader:
		return c.updateRPCInfoFromHeaderFrame(message, header)
	case FrameTypeTrailer:
		return c.decodeErrorFromTrailerFrame(message, f)
	}
	return nil
}

// frames with another seq id are considered from a different stream
func clientCheckSeqID(message remote.Message, header *Header) bool {
	return message.RPCRole() == remote.Client &&
		header.SeqID() != message.RPCInfo().Invocation().SeqID()
}

func (c *streamCodec) decodeErrorFromTrailerFrame(message remote.Message, f *Frame) error {
	if message.RPCRole() == remote.Server {
		return nil // client will not send error in a trailer
	}

	// TApplicationException from Payload
	if transErr, err := f.PayloadAsTransError(); err != nil {
		return err // decode TApplicationException failed
	} else if transErr != nil {
		return transErr
	}

	// BizStatusError from StrInfo keys
	return header_transmeta.ParseBizStatusErrorToRPCInfo(
		message.TransInfo().TransStrInfo(),
		message.RPCInfo().Invocation(),
	)
}

func (c *streamCodec) updateRPCInfoFromHeaderFrame(message remote.Message, header *Header) error {
	if message.RPCRole() == remote.Server {
		ink := message.RPCInfo().Invocation().(rpcinfo.InvocationSetter)
		ink.SetSeqID(header.SeqID())

		method, exists := header.GetIntKey(transmeta.ToMethod)
		if !exists { // method surely exists in Header Frame
			return errors.New("missing method in ttheader streaming header frame")
		}
		packageName, serviceName, methodName, err := ParsePackageServiceMethod(method)
		if err != nil {
			return err
		}
		ink.SetPackageName(packageName)
		ink.SetServiceName(serviceName)
		ink.SetMethodName(methodName)
	}
	return nil
}

// Name implements the remote.PayloadCodec interface
func (c *streamCodec) Name() string {
	return "ttheader"
}
