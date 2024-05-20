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
	"net"

	"github.com/bytedance/gopkg/cloud/metainfo"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/codec"
	"github.com/cloudwego/kitex/pkg/remote/codec/ttheader"
	"github.com/cloudwego/kitex/pkg/remote/trans"
	"github.com/cloudwego/kitex/pkg/remote/transmeta"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	kitex "github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/streaming"
	header_transmeta "github.com/cloudwego/kitex/pkg/transmeta"
	"github.com/cloudwego/kitex/transport"
)

var (
	_ remote.ClientStreamAllocator = (*ttheaderStreamingClientTransHandler)(nil)

	clientMetaFrameHandler   remote.MetaHandler = nil
	clientHeaderFrameHandler remote.MetaHandler = header_transmeta.ClientTTHeaderHandler
)

// SetClientHeaderFrameHandler is used to specify an extended meta handler for processing HeaderFrame at client side.
func SetClientHeaderFrameHandler(h remote.MetaHandler) {
	clientHeaderFrameHandler = h
}

// SetClientMetaFrameHandler is used to specify an extended meta handler for processing MetaFrame at client side.
func SetClientMetaFrameHandler(h remote.MetaHandler) {
	clientMetaFrameHandler = h
}

func NewDefaultCliTransHandler(opt *remote.ClientOption, ext trans.Extension) (remote.ClientTransHandler, error) {
	return &ttheaderStreamingClientTransHandler{
		opt:   opt,
		codec: ttheader.NewStreamCodec(),
		ext:   ext,
	}, nil
}

type ttheaderStreamingClientTransHandler struct {
	opt   *remote.ClientOption
	codec remote.PayloadCodec
	ext   trans.Extension
}

func (t *ttheaderStreamingClientTransHandler) newHeaderMessage(
	ctx context.Context, svcInfo *kitex.ServiceInfo,
) (message remote.Message, err error) {
	ri := rpcinfo.GetRPCInfo(ctx)
	message = remote.NewMessage(nil, svcInfo, ri, remote.Stream, remote.Client)
	message.SetProtocolInfo(remote.NewProtocolInfo(transport.TTHeader, svcInfo.PayloadCodec))
	if _, err = clientHeaderFrameHandler.WriteMeta(ctx, message); err != nil {
		return nil, err
	}
	message.TransInfo().TransIntInfo()[transmeta.FrameType] = codec.FrameTypeHeader
	message.TransInfo().PutTransStrInfo(buildMetaInfo(ctx))
	return message, nil
}

func (t *ttheaderStreamingClientTransHandler) NewStream(
	ctx context.Context, svcInfo *kitex.ServiceInfo, conn net.Conn, handler remote.TransReadWriter,
) (streaming.Stream, error) {
	headerMessage, err := t.newHeaderMessage(ctx, svcInfo)
	if err != nil {
		return nil, err
	}

	rawStream := newTTHeaderStream(ctx, conn, headerMessage.RPCInfo(), t.codec, headerMessage, remote.Client, t.ext)
	if t.opt.TTHeaderStreamingGRPCCompatible {
		rawStream.loadGRPCMetadata()
	}

	if err = rawStream.sendHeaderFrame(); err != nil {
		return nil, err
	}

	if t.opt.TTHeaderStreamingWaitMetaFrame {
		// Not enabled by default for performance reason, and should only be enabled when a MetaFrame will
		// definitely be returned, otherwise it will cause an error or even a hang
		// For example, when service discovery is taken over by a proxy, the proxy should return a MetaFrame,
		// so that Kitex client can get the real address and set it into RPCInfo; otherwise the tracer may not be
		// able to get the right address (e.g. for Client Streaming send events)
		if err = rawStream.clientReadMetaFrame(); err != nil {
			return nil, err
		}
	}
	return rawStream, nil
}

func buildMetaInfo(ctx context.Context) map[string]string {
	tMap := metainfo.GetAllValues(ctx)
	pMap := metainfo.GetAllPersistentValues(ctx)
	count := len(tMap) + len(pMap)
	metaInfos := make(map[string]string, count)
	for k, v := range tMap {
		metaInfos[metainfo.PrefixTransient+k] = v
	}
	for k, v := range pMap {
		metaInfos[metainfo.PrefixPersistent+k] = v
	}
	return metaInfos
}

// Write is not used and should never be called
func (t *ttheaderStreamingClientTransHandler) Write(ctx context.Context, conn net.Conn, send remote.Message) (nctx context.Context, err error) {
	panic("not used")
}

// Read is not used and should never be called
func (t *ttheaderStreamingClientTransHandler) Read(ctx context.Context, conn net.Conn, msg remote.Message) (nctx context.Context, err error) {
	panic("not used")
}

// OnInactive is not used and should never be called
func (t *ttheaderStreamingClientTransHandler) OnInactive(ctx context.Context, conn net.Conn) {
	panic("not used")
}

// OnError is not used and should never be called
func (t *ttheaderStreamingClientTransHandler) OnError(ctx context.Context, err error, conn net.Conn) {
	panic("not used")
}

// OnMessage is not used and should never be called
func (t *ttheaderStreamingClientTransHandler) OnMessage(ctx context.Context, args, result remote.Message) (context.Context, error) {
	panic("not used")
}

// SetPipeline is not used and should never be called
func (t *ttheaderStreamingClientTransHandler) SetPipeline(pipeline *remote.TransPipeline) {
	panic("not used")
}
