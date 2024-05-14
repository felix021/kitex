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
	"github.com/cloudwego/kitex/transport"
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
	sendMsg := remote.NewMessage(nil, svcInfo, ri, remote.Stream, remote.Server)
	sendMsg.SetProtocolInfo(remote.NewProtocolInfo(transport.TTHeader, svcInfo.PayloadCodec))
	ctx, err := header_transmeta.ClientTTHeaderHandler.WriteMeta(ctx, sendMsg)
	if err != nil {
		return nil, err
	}

	st := newTTHeaderStream(ctx, conn, ri, t.codec, sendMsg, remote.Client, t.ext)

	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		st.replaceHeader(md) // processed within ttheaderStreaming.sendHeader
	}

	// TODO: asynchronously send the header? faster for creating a stream
	if err = st.sendHeader(); err != nil {
		return nil, err
	}

	if err = t.readMetaFrameIfNecessary(st, ri); err != nil {
		return nil, err
	}
	return st, nil
}

func (t *ttheaderStreamingClientTransHandler) readMetaFrameIfNecessary(st *ttheaderStream, ri rpcinfo.RPCInfo) error {
	if t.opt.TTHeaderStreamingWaitMetaFrame {
		// not enabled by default for performance (not necessary when mesh egress is not enabled).
		return nil
	}
	message, err := st.skipFrameUntil(func(message remote.Message) bool {
		// ignore possible dirty frames from previous stream on the same tcp connection
		return getSeqID(message) == ri.Invocation().SeqID()
	})
	if err != nil {
		return err
	}
	if ft := getFrameType(message); ft != ttheader.FrameTypeMeta {
		return fmt.Errorf("unexpected frame type: expected=%v, got=%v", ttheader.FrameTypeMeta, ft)
	}
	return st.parseMetaFrame(message)
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
