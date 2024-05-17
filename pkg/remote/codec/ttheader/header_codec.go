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

package ttheader

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/codec/perrors"
	codecutils "github.com/cloudwego/kitex/pkg/remote/codec/utils"
	"github.com/cloudwego/kitex/pkg/remote/transmeta"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/rpcinfo/remoteinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/utils"
)

/**
 *  TTHeader Protocol
 *  +-------------2Byte--------------|-------------2Byte-------------+
 *	+----------------------------------------------------------------+
 *	| 0|                          LENGTH                             |
 *	+----------------------------------------------------------------+
 *	| 0|       HEADER MAGIC          |            FLAGS              |
 *	+----------------------------------------------------------------+
 *	|                         SEQUENCE NUMBER                        |
 *	+----------------------------------------------------------------+
 *	| 0|     Header Size(/32)        | ...
 *	+---------------------------------
 *
 *	Header is of variable size:
 *	(and starts at offset 14)
 *
 *	+----------------------------------------------------------------+
 *	| PROTOCOL ID  |NUM TRANSFORMS . |TRANSFORM 0 ID (uint8)|
 *	+----------------------------------------------------------------+
 *	|  TRANSFORM 0 DATA ...
 *	+----------------------------------------------------------------+
 *	|         ...                              ...                   |
 *	+----------------------------------------------------------------+
 *	|        INFO 0 ID (uint8)      |       INFO 0  DATA ...
 *	+----------------------------------------------------------------+
 *	|         ...                              ...                   |
 *	+----------------------------------------------------------------+
 *	|                                                                |
 *	|                              PAYLOAD                           |
 *	|                                                                |
 *	+----------------------------------------------------------------+
 */

const (
	Size32           int    = 4
	TTHeaderMetaSize        = 14
	HeaderFlagsKey   string = "HeaderFlags"
)

func NewTTHeader() *TTHeader {
	return &TTHeader{}
}

type TTHeader struct {
}

// Name returns the name of the protocol
func (t *TTHeader) Name() string {
	return "TTHeader"
}

// EncodeWithFrameSize encode message to buffer with prefixed 4-byte frame size for the caller to overwrite
func (t *TTHeader) EncodeWithFrameSize(ctx context.Context, message remote.Message, out remote.ByteBuffer) ([]byte, error) {
	header := t.newHeaderFromMessage(message)
	if buf, err := t.writeHeaderWithFrameSize(header, out); err != nil {
		return nil, err
	} else {
		// the first 4 bytes will be overwritten by the caller with the frame size
		return buf[0:Size32], nil
	}
}

func (t *TTHeader) Decode(ctx context.Context, message remote.Message, in remote.ByteBuffer) error {
	header, totalLen, err := t.readHeader(in)
	if err != nil {
		return err
	}
	return t.updateMessage(header, message, totalLen)
}

func (t *TTHeader) newHeaderFromMessage(message remote.Message) *Header {
	header := NewHeaderWithInfo(message.TransInfo().TransIntInfo(), message.TransInfo().TransStrInfo())
	header.SetFlags(uint16(GetFlags(message)))
	header.SetSeqID(message.RPCInfo().Invocation().SeqID())
	header.SetProtocolID(byte(GetProtocolID(message.ProtocolInfo())))
	return header
}

func (t *TTHeader) writeHeaderWithFrameSize(header *Header, out remote.ByteBuffer) ([]byte, error) {
	headerSize, err := header.BytesLength()
	if err != nil {
		return nil, perrors.NewProtocolErrorWithMsg(
			fmt.Sprintf("TTHeader get header bytes length failed, %s", err.Error()))
	}
	buf, err := out.Malloc(Size32 + headerSize)
	if err != nil {
		return nil, perrors.NewProtocolErrorWithMsg(
			fmt.Sprintf("TTHeader malloc header failed, %s", err.Error()))
	}
	if err = header.WriteWithSize(buf[Size32:], headerSize); err != nil {
		return nil, perrors.NewProtocolErrorWithMsg(
			fmt.Sprintf("TTHeader write header failed, %s", err.Error()))
	}
	return buf, nil
}

func (t *TTHeader) readHeader(in remote.ByteBuffer) (*Header, uint32, error) {
	headerMeta, err := in.Peek(TTHeaderMetaSize)
	if err != nil {
		return nil, 0, perrors.NewProtocolErrorWithMsg(fmt.Sprintf("TTHeader read header meta failed, %s", err.Error()))
	}
	headerSize := int(binary.BigEndian.Uint16(headerMeta[Size32+OffsetSize:]) * 4)
	buf, err := in.Next(Size32 + headerSize)
	if err != nil {
		return nil, 0, perrors.NewProtocolErrorWithMsg(fmt.Sprintf("TTHeader get header buf failed, %s", err.Error()))
	}
	header := NewHeader()
	if err = header.Read(buf); err != nil {
		return nil, 0, perrors.NewProtocolErrorWithMsg(fmt.Sprintf("TTHeader read header failed, %s", err.Error()))
	}
	return header, binary.BigEndian.Uint32(headerMeta[:Size32]), nil
}

func (t *TTHeader) updateMessage(header *Header, message remote.Message, totalLen uint32) error {
	SetFlags(header.flags, message)
	if err := codecutils.SetOrCheckSeqID(header.SeqID(), message); err != nil {
		klog.Warnf("the seqID in TTHeader check failed, error=%s", err.Error())
		// some framework doesn't write correct seqID in TTHeader, to ignore err only check it in payload
		// print log to push the downstream framework to refine it.
	}
	if err := checkProtocolID(header.ProtocolID(), message); err != nil {
		return err
	}
	message.TransInfo().PutTransIntInfo(header.IntInfo())
	message.TransInfo().PutTransStrInfo(header.StrInfo())
	if token := header.Token(); token != "" {
		message.TransInfo().PutTransStrInfo(map[string]string{transmeta.GDPRToken: token})
	}
	FillBasicInfoOfTTHeader(message)
	message.SetPayloadLen(int(totalLen - uint32(header.Size()) + uint32(Size32) - TTHeaderMetaSize))
	return nil
}

func GetFlags(message remote.Message) (headerFlags HeaderFlags) {
	if message.Tags() != nil && message.Tags()[HeaderFlagsKey] != nil {
		if hfs, ok := message.Tags()[HeaderFlagsKey].(HeaderFlags); ok {
			headerFlags = hfs
		} else {
			klog.Warnf("KITEX: the type of headerFlags is invalid, %T", message.Tags()[HeaderFlagsKey])
		}
	}
	return headerFlags
}

func SetFlags(flags uint16, message remote.Message) {
	if message.MessageType() == remote.Call {
		message.Tags()[HeaderFlagsKey] = HeaderFlags(flags)
	}
}

// GetProtocolID protoID just for TTHeader
func GetProtocolID(pi remote.ProtocolInfo) ProtocolID {
	switch pi.CodecType {
	case serviceinfo.Protobuf:
		// ProtocolIDKitexProtobuf is 0x03 at old version(<=v1.9.1) , but it conflicts with ThriftCompactV2.
		// Change the ProtocolIDKitexProtobuf to 0x04 from v1.9.2. But notice! that it is an incompatible change of protocol.
		// For keeping compatible, Kitex use ProtocolIDDefault send TTHeader+KitexProtobuf request to ignore the old version
		// check failed if use 0x04. It doesn't make sense, but it won't affect the correctness of RPC call because the actual
		// protocol check at checkPayload func which check payload with HEADER MAGIC bytes of payload.
		return ProtocolIDDefault
	}
	return ProtocolIDDefault
}

// protoID just for TTHeader
func checkProtocolID(protoID uint8, message remote.Message) error {
	switch protoID {
	case uint8(ProtocolIDThriftBinary):
	case uint8(ProtocolIDKitexProtobuf):
	case uint8(ProtocolIDThriftCompactV2):
		// just for compatibility
	default:
		return fmt.Errorf("unsupported ProtocolID[%d]", protoID)
	}
	return nil
}

// Fill basic from_info(from service, from address) which carried by ttheader to rpcinfo.
// It is better to fill rpcinfo in matahandlers in terms of design,
// but metahandlers are executed after payloadDecode, we don't know from_info when error happen in payloadDecode.
// So 'FillBasicInfoOfTTHeader' is just for getting more info to output log when decode error happen.
func FillBasicInfoOfTTHeader(msg remote.Message) {
	if msg.RPCRole() == remote.Server {
		fi := rpcinfo.AsMutableEndpointInfo(msg.RPCInfo().From())
		if fi != nil {
			if v := msg.TransInfo().TransStrInfo()[transmeta.HeaderTransRemoteAddr]; v != "" {
				fi.SetAddress(utils.NewNetAddr("tcp", v))
			}
			if v := msg.TransInfo().TransIntInfo()[transmeta.FromService]; v != "" {
				fi.SetServiceName(v)
			}
		}
		if ink, ok := msg.RPCInfo().Invocation().(rpcinfo.InvocationSetter); ok {
			if svcName, ok := msg.TransInfo().TransStrInfo()[transmeta.HeaderIDLServiceName]; ok {
				ink.SetServiceName(svcName)
			}
		}
	} else {
		ti := remoteinfo.AsRemoteInfo(msg.RPCInfo().To())
		if ti != nil {
			if v := msg.TransInfo().TransStrInfo()[transmeta.HeaderTransRemoteAddr]; v != "" {
				ti.SetRemoteAddr(utils.NewNetAddr("tcp", v))
			}
		}
	}
}
