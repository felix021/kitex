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
	"errors"

	"github.com/cloudwego/fastpb"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/codec/protobuf"
	"github.com/cloudwego/kitex/pkg/remote/codec/thrift"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"google.golang.org/protobuf/proto"
)

var (
	ErrInvalidPayload = errors.New("ttheader streaming invalid payload")
)

// ttheaderStreamCodec is only used by ttheader streaming and not implementing remote.Codec, so we just put it here.
type ttheaderStreamCodec struct {
	payloadCodec remote.PayloadCodec
}

// Marshaler allow custom type to be encoded
type Marshaler interface {
	Encode() ([]byte, error)
}

// Unmarshaler allow custom type to be decoded
type Unmarshaler interface {
	Decode([]byte) error
}

func (t *ttheaderStreamCodec) encode(ctx context.Context, msg interface{}) (payload []byte, err error) {
	ri := rpcinfo.GetRPCInfo(ctx)
	defer func() {
		// record send size, even when err != nil (0 is recorded to the lastSendSize)
		if rpcStats := rpcinfo.AsMutableRPCStats(ri.Stats()); rpcStats != nil {
			rpcStats.IncrSendSize(uint64(len(payload)))
		}
	}()

	if marshaler, ok := msg.(Marshaler); ok {
		return marshaler.Encode()
	}

	switch ri.Config().PayloadCodec() {
	case serviceinfo.Thrift:
		payload, err = thrift.MarshalThriftData(ctx, t.payloadCodec, msg)
	case serviceinfo.Protobuf:
		switch t := msg.(type) {
		case fastpb.Writer:
			size := t.Size()
			payload = make([]byte, size)
			t.FastWrite(payload)
		case protobufV2MsgCodec:
			payload, err = t.XXX_Marshal(nil, true)
		case proto.Message:
			payload, err = proto.Marshal(t)
		case protobuf.ProtobufMsgCodec:
			payload, err = t.Marshal(nil)
		default:
			return nil, ErrInvalidPayload
		}
	default:
		return nil, ErrInvalidPayload
	}
	return
}

type protobufV2MsgCodec interface {
	XXX_Unmarshal(b []byte) error
	XXX_Marshal(b []byte, deterministic bool) ([]byte, error)
}

func (t *ttheaderStreamCodec) decode(ctx context.Context, buf []byte, msg interface{}) (err error) {
	ri := rpcinfo.GetRPCInfo(ctx)
	if rpcStats := rpcinfo.AsMutableRPCStats(ri.Stats()); rpcStats != nil {
		// record recv size, even when err != nil (0 is recorded to the lastRecvSize)
		rpcStats.IncrRecvSize(uint64(len(buf)))
	}

	if unmarshaler, ok := msg.(Unmarshaler); ok {
		return unmarshaler.Decode(buf)
	}

	switch ri.Config().PayloadCodec() {
	case serviceinfo.Thrift:
		err = thrift.UnmarshalThriftData(ctx, t.payloadCodec, "", buf, msg)
	case serviceinfo.Protobuf:
		switch msg.(type) {

		}
		if t, ok := msg.(fastpb.Reader); ok {
			if len(buf) == 0 {
				// if all fields of a struct is default value, data will be nil
				// In the implementation of fastpb, if data is nil, then fastpb will skip creating this struct,
				// as a result, caller will get a nil pointer which is not expected.
				// So, when data is nil, use default protobuf unmarshal method to decode the struct.
				// todo: fix fastpb
			} else {
				_, err = fastpb.ReadMessage(buf, fastpb.SkipTypeCheck, t)
				return err
			}
		}
		switch t := msg.(type) {
		case protobufV2MsgCodec:
			return t.XXX_Unmarshal(buf)
		case proto.Message:
			return proto.Unmarshal(buf, t)
		case protobuf.ProtobufMsgCodec:
			return t.Unmarshal(buf)
		default:
			return ErrInvalidPayload
		}

	default:
		return ErrInvalidPayload
	}
	return
}
