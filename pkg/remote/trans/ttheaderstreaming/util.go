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

package ttheaderstreaming

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/codec"
	"github.com/cloudwego/kitex/pkg/remote/codec/ttheader"
	"github.com/cloudwego/kitex/pkg/remote/transmeta"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/streaming/metadata"
	"github.com/cloudwego/kitex/pkg/utils"
	"github.com/cloudwego/kitex/transport"
	"github.com/cloudwego/netpoll"
)

type netpollReader interface {
	Reader() netpoll.Reader
}

func getReader(conn net.Conn) netpoll.Reader {
	return conn.(netpollReader).Reader()
}

// parsePackageServiceMethod parses the "$package.$service/$method" format
func parsePackageServiceMethod(s string) (string, string, string, error) {
	if s != "" && s[0] == '/' {
		s = s[1:]
	}

	pos := strings.LastIndex(s, "/")
	if pos == -1 {
		return "", "", "", fmt.Errorf("malformed $package.$service/$method format %q", s)
	}
	packageDotService, methodName := s[:pos], s[pos+1:]

	if pos = strings.LastIndex(packageDotService, "."); pos == -1 { // package is not necessary
		return "", packageDotService, methodName, nil
	}

	packageName, serviceName := packageDotService[:pos], packageDotService[pos+1:]
	return packageName, serviceName, methodName, nil
}

func readHeaderFrame(ctx context.Context, conn net.Conn) (*ttheader.Frame, error) {
	frame, err := ttheader.ReadFrame(conn)
	if err != nil {
		return nil, err
	}
	frameType, ok := frame.Header().GetIntKey(ttheader.IntKeyFrameType)
	if !ok {
		return nil, fmt.Errorf("frame type not set, header frame expected")
	}
	if frameType != ttheader.FrameTypeHeader {
		return nil, fmt.Errorf("unexpected frame type (not header frame): %s(%v)", frameType, ok)
	}
	return frame, nil
}

func newMessage(ri rpcinfo.RPCInfo, hdr *ttheader.Header) remote.Message {
	msg := remote.NewMessage(nil, nil, ri, remote.Stream, remote.Server)

	msg.Tags()[codec.HeaderFlagsKey] = codec.HeaderFlags(hdr.Flags())

	payloadCodec := codec.ProtocolIDToPayloadCodec(codec.ProtocolID(hdr.ProtocolID()))
	msg.SetProtocolInfo(remote.NewProtocolInfo(transport.TTHeader, payloadCodec))

	if fi := rpcinfo.AsMutableEndpointInfo(msg.RPCInfo().From()); fi != nil {
		if v := msg.TransInfo().TransStrInfo()[transmeta.HeaderTransRemoteAddr]; v != "" {
			_ = fi.SetAddress(utils.NewNetAddr("tcp", v))
		}
		if v := msg.TransInfo().TransIntInfo()[transmeta.FromService]; v != "" {
			_ = fi.SetServiceName(v)
		}
	}

	msg.TransInfo().PutTransIntInfo(hdr.IntInfo())
	msg.TransInfo().PutTransStrInfo(hdr.StrInfo())

	if hdr.Token() != "" {
		msg.TransInfo().TransStrInfo()[transmeta.GDPRToken] = hdr.Token()
	}
	return msg
}

// processBizStatusError saves the BizStatusErr (if any) into the RPCInfo and returns nil since it's not an RPC error
// server.invokeHandleEndpoint already processed BizStatusError, so it's only used by unknownMethodHandler
func processBizStatusError(err error, ri rpcinfo.RPCInfo) error {
	var bizErr kerrors.BizStatusErrorIface
	if errors.As(err, &bizErr) {
		err = nil
		if setter, ok := ri.Invocation().(rpcinfo.InvocationSetter); ok {
			setter.SetBizStatusErr(bizErr)
		}
	}
	return err
}

// getRemoteAddr returns the remote address of the connection.
func getRemoteAddr(ctx context.Context, conn net.Conn) net.Addr {
	addr := conn.RemoteAddr()
	if addr != nil && addr.Network() != "unix" {
		return addr
	}
	// likely from service mesh: return the addr read from mesh ttheader
	ri := rpcinfo.GetRPCInfo(ctx)
	if ri == nil {
		return addr
	}
	if ri.From().Address() != nil {
		return ri.From().Address()
	}
	return addr
}

// injectGRPCMetadata parses grpc style metadata into ctx, helpful for projects migrating from kitex-grpc
func injectGRPCMetadata(ctx context.Context, header *ttheader.Header) (context.Context, error) {
	value, exists := header.GetStrKey(ttheader.StrKeyMetaData)
	if !exists {
		return ctx, nil
	}
	md := metadata.MD{}
	if err := sonic.Unmarshal([]byte(value), &md); err != nil {
		return ctx, err
	}
	return metadata.NewIncomingContext(ctx, md), nil
}

func frameType(frame *ttheader.Frame) string {
	if t, exists := frame.Header().GetIntKey(ttheader.IntKeyFrameType); !exists {
		// fallback to be compatible with old ttheader response
		return ttheader.FrameTypeTrailer
	} else {
		return t
	}
}
