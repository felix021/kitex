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
	ThriftCodec     remote.PayloadCodec
	thriftSerDes    remote.SerDes
	protobufSerDesc remote.SerDes
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
		codec.ThriftCodec = thrift.NewThriftCodec()
	}
	// A better approach should be using remote.GetDerDes(message), but:
	// 1. Get thriftCodec from client/server option with fastCodec/frugal config
	// 2. protobufCodec is not compatible with current implementation of compression
	codec.thriftSerDes = codec.ThriftCodec.(remote.SerDes)
	codec.protobufSerDesc = protobuf.NewProtobufCodec().(remote.SerDes)
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
		payload, err = c.thriftSerDes.Serialize(ctx, message.Data())
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
		return c.thriftSerDes.Deserialize(ctx, message.Data(), d)
	case serviceinfo.Protobuf:
		return c.protobufSerDesc.Deserialize(ctx, message.Data(), d)
	default:
		return ErrInvalidPayload
	}
}

func (c *grpcCodec) Name() string {
	return "grpc"
}
