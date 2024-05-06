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
	"fmt"
	"net"

	"github.com/bytedance/gopkg/cloud/metainfo"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/codec/ttheader"
	"github.com/cloudwego/kitex/pkg/remote/trans"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	kitex "github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/streaming"
	"github.com/cloudwego/kitex/pkg/streaming/metadata"
	header_transmeta "github.com/cloudwego/kitex/pkg/transmeta"
)

var (
	_ remote.ClientStreamAllocator = (*ttheaderStreamingClientTransHandler)(nil)
)

func NewDefaultCliTransHandler(opt *remote.ClientOption, ext trans.Extension) (remote.ClientTransHandler, error) {
	return &ttheaderStreamingClientTransHandler{
		opt:   opt,
		codec: ttheader.NewCodec(ttheader.WithThriftCodec(opt.PayloadCodec)),
		ext:   ext,
	}, nil

}

type ttheaderStreamingClientTransHandler struct {
	opt   *remote.ClientOption
	codec remote.PayloadCodec
	ext   trans.Extension
}

func (t *ttheaderStreamingClientTransHandler) NewStream(
	ctx context.Context, svcInfo *kitex.ServiceInfo, conn net.Conn, handler remote.TransReadWriter,
) (streaming.Stream, error) {
	ri := rpcinfo.GetRPCInfo(ctx)
	sendMsg := newMessage(ri, ttheader.NewHeader())
	ctx, err := header_transmeta.ClientTTHeaderHandler.WriteMeta(ctx, sendMsg)
	if err != nil {
		return nil, err
	}
	header := ttheader.NewHeaderWithInfo(sendMsg.TransInfo().TransIntInfo(), sendMsg.TransInfo().TransStrInfo())
	header.SetStrInfo(appendMetaInfo(ctx, header.StrInfo()))

	st := newTTHeaderStream(ctx, conn, ri, t.codec, header, remote.Client, t.ext)

	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		st.replaceHeader(md) // processed within ttheaderStreaming.sendHeader
	}

	// TODO: delay to the first SendMsg call? faster for creating a stream
	if err = st.sendHeader(); err != nil {
		return nil, err
	}

	if err = t.readMetaFrameIfNecessary(ctx, st, ri); err != nil {
		return nil, err
	}
	return st, nil
}

func (t *ttheaderStreamingClientTransHandler) readMetaFrameIfNecessary(ctx context.Context, st *ttheaderStream, ri rpcinfo.RPCInfo) (err error) {
	if t.opt.TTHeaderStreamingWaitMetaFrame {
		// not enabled by default for performance (not necessary when mesh egress is not enabled).
		return nil
	}
	f := ttheader.NewFrame(nil, nil)
	err = st.skipFrameUntil(f, func(f *ttheader.Frame) bool {
		// ignore possible dirty frames from previous stream on the same tcp connection
		return f.Header().SeqNum() == uint32(ri.Invocation().SeqID())
	})
	if ft := frameType(f); ft != ttheader.FrameTypeMeta {
		return fmt.Errorf("unexpected frame type: expected=%v, got=%v", ttheader.FrameTypeMeta, ft)
	}
	return st.updateRPCInfoByMetaFrame(ctx, ri, f)
}

func appendMetaInfo(ctx context.Context, metaInfos map[string]string) map[string]string {
	tMap := metainfo.GetAllValues(ctx)
	pMap := metainfo.GetAllPersistentValues(ctx)
	count := len(tMap) + len(pMap)
	if len(metaInfos) == 0 {
		metaInfos = make(map[string]string, count)
	}
	for k, v := range tMap {
		metaInfos[metainfo.PrefixTransient+k] = v
	}
	for k, v := range pMap {
		metaInfos[metainfo.PrefixPersistent+k] = v
	}
	return metaInfos
}

func (t *ttheaderStreamingClientTransHandler) Write(ctx context.Context, conn net.Conn, send remote.Message) (nctx context.Context, err error) {

	//TODO implement me
	panic("implement me")
}

func (t *ttheaderStreamingClientTransHandler) Read(ctx context.Context, conn net.Conn, msg remote.Message) (nctx context.Context, err error) {
	//TODO implement me
	panic("implement me")
}

func (t *ttheaderStreamingClientTransHandler) OnInactive(ctx context.Context, conn net.Conn) {
	//TODO implement me
	panic("implement me")
}

func (t *ttheaderStreamingClientTransHandler) OnError(ctx context.Context, err error, conn net.Conn) {
	//TODO implement me
	panic("implement me")
}

func (t *ttheaderStreamingClientTransHandler) OnMessage(ctx context.Context, args, result remote.Message) (context.Context, error) {
	//TODO implement me
	panic("implement me")
}

func (t *ttheaderStreamingClientTransHandler) SetPipeline(pipeline *remote.TransPipeline) {
	//TODO implement me
	panic("implement me")
}
