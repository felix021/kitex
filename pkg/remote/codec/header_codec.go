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

package codec

import (
	"context"
	"fmt"

	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/codec/perrors"
	"github.com/cloudwego/kitex/pkg/remote/codec/ttheader"
	"github.com/cloudwego/kitex/pkg/remote/transmeta"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/rpcinfo/remoteinfo"
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

// Header keys
const (
	// Header Magics
	// 0 and 16th bits must be 0 to differentiate from framed & unframed
	TTHeaderMagic     uint32 = 0x10000000 // magic(2 bytes) + flags(2 bytes)
	MeshHeaderMagic   uint32 = 0xFFAF0000
	MeshHeaderLenMask uint32 = 0x0000FFFF

	// HeaderMask        uint32 = 0xFFFF0000
	FlagsMask     uint32 = 0x0000FFFF
	MethodMask    uint32 = 0x41000000 // method first byte [A-Za-z_]
	MaxFrameSize  uint32 = 0x3FFFFFFF
	MaxHeaderSize uint32 = 65536
)

type HeaderFlags = ttheader.HeaderFlags

const (
	HeaderFlagsKey              = ttheader.HeaderFlagsKey
	HeaderFlagSupportOutOfOrder = ttheader.HeaderFlagSupportOutOfOrder
	HeaderFlagDuplexReverse     = ttheader.HeaderFlagDuplexReverse
	HeaderFlagSASL              = ttheader.HeaderFlagSASL
)

const (
	TTHeaderMetaSize = 14
)

// ProtocolID is the wrapped protocol id used in THeader.
type ProtocolID = ttheader.ProtocolID

const (
	ProtocolIDThriftBinary    = ttheader.ProtocolIDThriftBinary
	ProtocolIDThriftCompact   = ttheader.ProtocolIDThriftCompact
	ProtocolIDThriftCompactV2 = ttheader.ProtocolIDThriftCompactV2
	ProtocolIDKitexProtobuf   = ttheader.ProtocolIDKitexProtobuf
	ProtocolIDNotSpecified    = ttheader.ProtocolIDNotSpecified
	ProtocolIDDefault         = ProtocolIDThriftBinary
)

type InfoIDType uint8

const (
	InfoIDPadding     = ttheader.InfoIDPadding
	InfoIDKeyValue    = ttheader.InfoIDKeyValue
	InfoIDIntKeyValue = ttheader.InfoIDIntKeyValue
	InfoIDACLToken    = ttheader.InfoIDACLToken
)

type ttHeader struct {
	codec *ttheader.TTHeader
}

func (t ttHeader) encode(ctx context.Context, message remote.Message, out remote.ByteBuffer) (totalLenField []byte, err error) {
	return t.codec.EncodeWithFrameSize(ctx, message, out)
}

func (t ttHeader) decode(ctx context.Context, message remote.Message, in remote.ByteBuffer) error {
	return t.codec.Decode(ctx, message, in)
}

/**
 * +-------------2Byte-------------|-------------2Byte--------------+
 * +----------------------------------------------------------------+
 * |       HEADER MAGIC            |      HEADER SIZE               |
 * +----------------------------------------------------------------+
 * |       HEADER MAP SIZE         |    HEADER MAP...               |
 * +----------------------------------------------------------------+
 * |                                                                |
 * |                            PAYLOAD                             |
 * |                                                                |
 * +----------------------------------------------------------------+
 */
type meshHeader struct{}

//lint:ignore U1000 until encode is used
func (m meshHeader) encode(ctx context.Context, message remote.Message, payloadBuf, out remote.ByteBuffer) error {
	// do nothing, kitex just support decode meshHeader, encode protocol depend on the payload
	return nil
}

func (m meshHeader) decode(ctx context.Context, message remote.Message, in remote.ByteBuffer) error {
	headerMeta, err := in.Next(Size32)
	if err != nil {
		return perrors.NewProtocolErrorWithMsg(fmt.Sprintf("meshHeader read header meta failed, %s", err.Error()))
	}
	if !isMeshHeader(headerMeta) {
		return perrors.NewProtocolErrorWithMsg("not MeshHeader protocol")
	}
	headerLen := Bytes2Uint16NoCheck(headerMeta[Size16:])
	var headerInfo []byte
	if headerInfo, err = in.Next(int(headerLen)); err != nil {
		return perrors.NewProtocolErrorWithMsg(fmt.Sprintf("meshHeader read header buf failed, %s", err.Error()))
	}
	mapInfo := message.TransInfo().TransStrInfo()
	idx := 0
	if _, err = readStrKVInfo(&idx, headerInfo, mapInfo); err != nil {
		return perrors.NewProtocolErrorWithMsg(fmt.Sprintf("meshHeader read kv info failed, %s", err.Error()))
	}
	FillBasicInfoOfTTHeader(message)
	return nil
}

func readStrKVInfo(idx *int, buf []byte, info map[string]string) (has bool, err error) {
	kvSize, err := Bytes2Uint16(buf, *idx)
	*idx += 2
	if err != nil {
		return false, fmt.Errorf("error reading str kv info size: %s", err.Error())
	}
	if kvSize <= 0 {
		return false, nil
	}
	for i := uint16(0); i < kvSize; i++ {
		key, n, err := ReadString2BLen(buf, *idx)
		*idx += n
		if err != nil {
			return false, fmt.Errorf("error reading str kv info: %s", err.Error())
		}
		val, n, err := ReadString2BLen(buf, *idx)
		*idx += n
		if err != nil {
			return false, fmt.Errorf("error reading str kv info: %s", err.Error())
		}
		info[key] = val
	}
	return true, nil
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
