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

package grpc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/bytedance/gopkg/lang/mcache"
	"github.com/cloudwego/fastpb"
	"google.golang.org/protobuf/proto"

	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/codec/perrors"
	"github.com/cloudwego/kitex/pkg/remote/codec/protobuf"
	"github.com/cloudwego/kitex/pkg/remote/codec/thrift"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
)

const dataFrameHeaderLen = 5

var ErrInvalidPayload = errors.New("grpc invalid payload")

// gogoproto generate
type marshaler interface {
	MarshalTo(data []byte) (n int, err error)
	Size() int
}

type grpcCodec struct {
	ThriftCodec         remote.PayloadCodec
	thriftStructCodec   remote.StructCodec
	protobufStructCodec remote.StructCodec
}

type CodecOption func(c *grpcCodec)

func WithThriftCodec(t remote.PayloadCodec) CodecOption {
	return func(c *grpcCodec) {
		c.ThriftCodec = t
	}
}

// NewGRPCCodec create grpc and protobuf codec
func NewGRPCCodec(opts ...CodecOption) remote.Codec {
	codec := &grpcCodec{}
	for _, opt := range opts {
		opt(codec)
	}
	if !thrift.IsThriftCodec(codec.ThriftCodec) {
		if c, err := remote.GetPayloadCodecByCodecType(serviceinfo.Thrift); err == nil {
			codec.ThriftCodec = c
		} else {
			codec.ThriftCodec = thrift.NewThriftCodec()
		}
	}
	codec.thriftStructCodec = codec.ThriftCodec.(remote.StructCodec)
	if c, err := remote.GetStructCodecByCodecType(serviceinfo.Protobuf); err == nil {
		codec.protobufStructCodec = c
	} else {
		codec.protobufStructCodec = protobuf.NewProtobufCodec().(remote.StructCodec)
	}
	return codec
}

func mallocWithFirstByteZeroed(size int) []byte {
	data := mcache.Malloc(size)
	data[0] = 0 // compressed flag = false
	return data
}

func (c *grpcCodec) Encode(ctx context.Context, message remote.Message, out remote.ByteBuffer) (err error) {
	var payload []byte
	defer func() {
		// record send size, even when err != nil (0 is recorded to the lastSendSize)
		if rpcStats := rpcinfo.AsMutableRPCStats(message.RPCInfo().Stats()); rpcStats != nil {
			rpcStats.IncrSendSize(uint64(len(payload)))
		}
	}()

	writer, ok := out.(remote.FrameWrite)
	if !ok {
		return fmt.Errorf("output buffer must implement FrameWrite")
	}
	compressor, err := getSendCompressor(ctx)
	if err != nil {
		return err
	}
	isCompressed := compressor != nil

	switch message.ProtocolInfo().CodecType {
	case serviceinfo.Thrift:
		payload, err = c.thriftStructCodec.Serialize(ctx, message.Data())
	case serviceinfo.Protobuf:
		switch t := message.Data().(type) {
		case fastpb.Writer:
			size := t.Size()
			if !isCompressed {
				payload = mallocWithFirstByteZeroed(size + dataFrameHeaderLen)
				t.FastWrite(payload[dataFrameHeaderLen:])
				binary.BigEndian.PutUint32(payload[1:dataFrameHeaderLen], uint32(size))
				return writer.WriteData(payload)
			}
			payload = mcache.Malloc(size)
			t.FastWrite(payload)
		case marshaler:
			size := t.Size()
			if !isCompressed {
				payload = mallocWithFirstByteZeroed(size + dataFrameHeaderLen)
				if _, err = t.MarshalTo(payload[dataFrameHeaderLen:]); err != nil {
					return err
				}
				binary.BigEndian.PutUint32(payload[1:dataFrameHeaderLen], uint32(size))
				return writer.WriteData(payload)
			}
			payload = mcache.Malloc(size)
			if _, err = t.MarshalTo(payload); err != nil {
				return err
			}
		case protobuf.ProtobufV2MsgCodec:
			payload, err = t.XXX_Marshal(nil, true)
		case proto.Message:
			payload, err = proto.Marshal(t)
		case protobuf.ProtobufMsgCodec:
			payload, err = t.Marshal(nil)
		case protobuf.MessageWriterWithContext:
			methodName := message.RPCInfo().Invocation().MethodName()
			if methodName == "" {
				return errors.New("empty methodName in grpc Encode")
			}
			actualMsg, err := t.WritePb(ctx, methodName)
			if err != nil {
				return err
			}
			payload, ok = actualMsg.([]byte)
			if !ok {
				return perrors.NewProtocolErrorWithErrMsg(err, fmt.Sprintf("grpc marshal message failed: %s", err.Error()))
			}
		default:
			return ErrInvalidPayload
		}
	default:
		return ErrInvalidPayload
	}

	if err != nil {
		return err
	}
	var header [dataFrameHeaderLen]byte
	if isCompressed {
		payload, err = compress(compressor, payload)
		if err != nil {
			return err
		}
		header[0] = 1
	} else {
		header[0] = 0
	}
	binary.BigEndian.PutUint32(header[1:dataFrameHeaderLen], uint32(len(payload)))
	err = writer.WriteHeader(header[:])
	if err != nil {
		return err
	}
	return writer.WriteData(payload)
	// TODO: recycle payload?
}

func (c *grpcCodec) Decode(ctx context.Context, message remote.Message, in remote.ByteBuffer) (err error) {
	d, err := decodeGRPCFrame(ctx, in)
	if rpcStats := rpcinfo.AsMutableRPCStats(message.RPCInfo().Stats()); rpcStats != nil {
		// record recv size, even when err != nil (0 is recorded to the lastRecvSize)
		rpcStats.IncrRecvSize(uint64(len(d)))
	}
	if err != nil {
		return err
	}
	message.SetPayloadLen(len(d))
	switch message.ProtocolInfo().CodecType {
	case serviceinfo.Thrift:
		return c.thriftStructCodec.Deserialize(ctx, message.Data(), d)
	case serviceinfo.Protobuf:
		if t, ok := data.(fastpb.Reader); ok {
			if len(d) == 0 {
				// if all fields of a struct is default value, data will be nil
				// In the implementation of fastpb, if data is nil, then fastpb will skip creating this struct, as a result user will get a nil pointer which is not expected.
				// So, when data is nil, use default protobuf unmarshal method to decode the struct.
				// todo: fix fastpb
			} else {
				_, err = fastpb.ReadMessage(d, fastpb.SkipTypeCheck, t)
				return err
			}
		}
		switch t := data.(type) {
		case protobufV2MsgCodec:
			return t.XXX_Unmarshal(d)
		case proto.Message:
			return proto.Unmarshal(d, t)
		case protobuf.ProtobufMsgCodec:
			return t.Unmarshal(d)
		case protobuf.MessageReaderWithMethodWithContext:
			methodName := message.RPCInfo().Invocation().MethodName()
			if methodName == "" {
				return errors.New("empty methodName in grpc Decode")
			}
			return t.ReadPb(ctx, methodName, d)
		default:
			return ErrInvalidPayload
		}
	default:
		return ErrInvalidPayload
	}
}

func (c *grpcCodec) Name() string {
	return "grpc"
}
