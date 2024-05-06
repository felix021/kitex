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
	"io"
	"math"
	"net"
	"runtime/debug"
	"time"

	"github.com/bytedance/gopkg/cloud/metainfo"
	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/codec"
	"github.com/cloudwego/kitex/pkg/remote/codec/ttheader"
	"github.com/cloudwego/kitex/pkg/remote/trans"
	"github.com/cloudwego/kitex/pkg/remote/trans/detection"
	"github.com/cloudwego/kitex/pkg/remote/transmeta"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/stats"
	"github.com/cloudwego/kitex/pkg/streaming"
	header_transmeta "github.com/cloudwego/kitex/pkg/transmeta"
	"github.com/cloudwego/kitex/transport"
	"github.com/cloudwego/netpoll"
)

const (
	detectionSize   = 8
	offsetMagic     = 4
	offsetFlags     = 6
	InfinityTimeout = time.Duration(math.MaxInt64)
)

var (
	_ remote.ServerTransHandler              = (*ttheaderStreamingTransHandler)(nil)
	_ remote.InvokeHandleFuncSetter          = (*ttheaderStreamingTransHandler)(nil)
	_ detection.DetectableServerTransHandler = (*ttheaderStreamingTransHandler)(nil)

	metaFrameWriter MetaFrameWriter
)

type MetaFrameWriter func(ctx context.Context, msg remote.Message) error

// SetMetaFrameWriter sets MetaFrameWriter
func SetMetaFrameWriter(writer MetaFrameWriter) {
	metaFrameWriter = writer
}

// NewSvrTransHandler returns a new server transHandler with given extension (netpoll, gonet, etc.)
func NewSvrTransHandler(opt *remote.ServerOption, ext trans.Extension) (remote.ServerTransHandler, error) {
	return &ttheaderStreamingTransHandler{
		opt:   opt,
		codec: ttheader.NewCodec(ttheader.WithThriftCodec(opt.PayloadCodec)),
		ext:   ext,
	}, nil
}

type ttheaderStreamingTransHandler struct {
	opt           *remote.ServerOption
	codec         remote.PayloadCodec
	invokeHandler endpoint.Endpoint
	ext           trans.Extension
}

// ProtocolMatch implements the detection.DetectableServerTransHandler interface.
func (t *ttheaderStreamingTransHandler) ProtocolMatch(ctx context.Context, conn net.Conn) (err error) {
	preface, err := getReader(conn).Peek(detectionSize)
	if err != nil {
		return err
	}
	magic := preface[offsetMagic : offsetMagic+2]
	flags := preface[offsetFlags : offsetFlags+2]
	if ttheader.IsMagic(magic) && ttheader.IsStreaming(flags) {
		return nil
	}
	return errors.New("not matching ttheader streaming")
}

// SetInvokeHandleFunc implements the remote.InvokeHandleFuncSetter interface.
func (t *ttheaderStreamingTransHandler) SetInvokeHandleFunc(invokeHandler endpoint.Endpoint) {
	t.invokeHandler = invokeHandler
}

func (t *ttheaderStreamingTransHandler) OnError(ctx context.Context, err error, conn net.Conn) {
	var detailedError *kerrors.DetailedError
	if err == nil || err.Error() == "EOF peer close" {
		detailedError = nil
	}
	if ok := errors.As(err, &detailedError); ok && detailedError.Stack() != "" {
		klog.CtxErrorf(ctx, "KITEX: processing ttheader streaming request error, remoteAddr=%s, error=%s\nstack=%s",
			getRemoteAddr(ctx, conn), err.Error(), detailedError.Stack())
	} else {
		klog.CtxErrorf(ctx, "KITEX: processing ttheader streaming request error, remoteAddr=%s, error=%s",
			getRemoteAddr(ctx, conn), err.Error())
	}
}

// Write should not be called in ttheaderStreamingTransHandler
func (t *ttheaderStreamingTransHandler) Write(ctx context.Context, conn net.Conn, send remote.Message) (context.Context, error) {
	//TODO implement me
	panic("implement me")
}

// Read should not be called in ttheaderStreamingTransHandler
func (t *ttheaderStreamingTransHandler) Read(ctx context.Context, conn net.Conn, msg remote.Message) (context.Context, error) {
	//TODO implement me
	panic("implement me")
}

// OnMessage should not be called in ttheaderStreamingTransHandler
func (t *ttheaderStreamingTransHandler) OnMessage(ctx context.Context, args, result remote.Message) (context.Context, error) {
	//TODO implement me
	panic("implement me")
}

// SetPipeline is not used in ttheaderStreamingTransHandler
func (t *ttheaderStreamingTransHandler) SetPipeline(pipeline *remote.TransPipeline) {
	// not used
}

// OnActive sets the read timeout on the connection to infinity
func (t *ttheaderStreamingTransHandler) OnActive(ctx context.Context, conn net.Conn) (context.Context, error) {
	// set readTimeout to infinity to avoid streaming break
	// use keepalive to check the health of connection
	if npConn, ok := conn.(netpoll.Connection); ok {
		_ = npConn.SetReadTimeout(InfinityTimeout)
	} else {
		_ = conn.SetReadDeadline(time.Now().Add(InfinityTimeout))
	}
	return ctx, nil
}

// OnInactive is not used in ttheaderStreamingTransHandler
func (t *ttheaderStreamingTransHandler) OnInactive(ctx context.Context, conn net.Conn) {}

func (t *ttheaderStreamingTransHandler) startTracer(ctx context.Context, ri rpcinfo.RPCInfo) context.Context {
	c := t.opt.TracerCtl.DoStart(ctx, ri)
	return c
}

func (t *ttheaderStreamingTransHandler) finishTracer(ctx context.Context, ri rpcinfo.RPCInfo, err error, panicErr interface{}) {
	rpcStats := rpcinfo.AsMutableRPCStats(ri.Stats())
	if rpcStats == nil {
		return
	}
	if panicErr != nil {
		rpcStats.SetPanicked(panicErr)
	}
	t.opt.TracerCtl.DoFinish(ctx, ri, err)
	rpcStats.Reset()
}

// OnRead handles a new TTHeader Stream
func (t *ttheaderStreamingTransHandler) OnRead(ctx context.Context, conn net.Conn) (err error) {
	ctx, ri := t.newCtxWithRPCInfo(ctx, conn)
	headerFrame := ttheader.NewFrame(ttheader.NewHeader(), nil)
	rawStream := newTTHeaderStream(ctx, conn, ri, t.codec, headerFrame.Header(), remote.Server, t.ext)
	st := endpoint.NewStreamWithMiddleware(rawStream, t.opt.RecvEndpoint, t.opt.SendEndpoint)

	if err = rawStream.skipUntilTargetFrame(headerFrame, ttheader.FrameTypeHeader); err != nil {
		if errors.Is(err, netpoll.ErrEOF) || errors.Is(err, io.EOF) {
			// There could be dirty frames from the previous stream while the connection has been closed by the client.
			// Just ignore the error since we're not starting a new stream at this moment.
			return nil
		}
		t.OnError(ctx, err, conn)
		return fmt.Errorf("KITEX: TTHeader Streaming readHeaderFrame failed, error=%v", err)
	}

	ctx = rawStream.Context() // with cancel

	if ctx, err = t.readMeta(ctx, conn, ri, headerFrame); err != nil {
		t.OnError(ctx, err, conn)
		return fmt.Errorf("KITEX: TTHeader Streaming readMeta failed, error=%v", err)
	}

	if ctx, err = t.onReadStream(ctx); err != nil {
		t.OnError(ctx, err, conn)
		return fmt.Errorf("KITEX: TTHeader Streaming onReadStream failed, error=%v", err)
	}

	rawStream.setContext(ctx) // including meta info

	ctx = t.startTracer(ctx, ri)
	defer func() {
		panicErr := recover()
		if panicErr != nil {
			klog.CtxErrorf(ctx,
				"KITEX: TTHeader Streaming panic happened in %s, remoteAddress=%v, error=%s\nstack=%s",
				"ttheaderStreamingTransHandler.OnRead", getRemoteAddr(ctx, conn), panicErr, string(debug.Stack()),
			)
		}
		t.finishTracer(ctx, ri, err, panicErr)
	}()

	svcInfo, methodInfo := t.retrieveServiceMethodInfo(ri)

	// TODO: move to Mesh Egress Proxy? Kitex server may not always be able to get the real addr.
	if err = t.sendMetaFrame(ctx, svcInfo, ri, rawStream); err != nil {
		t.OnError(ctx, err, conn)
		return fmt.Errorf("KITEX: TTHeader Streaming sendMetaFrame failed, error=%v", err)
	}

	// bind stream into ctx, in order to let user set header and trailer by provided api in meta_api.go
	ctx = streaming.NewCtxWithStream(ctx, st)

	if methodInfo == nil {
		err = t.unknownMethod(ctx, ri, st, svcInfo)
	} else {
		err = t.invokeHandler(ctx, &streaming.Args{Stream: st}, nil)
	}

	if err != nil {
		t.OnError(ctx, err, conn)
		// not returning directly: need to try sending the trailer frame with the error
	}

	if sendErr := rawStream.sendTrailer(err); sendErr != nil {
		t.OnError(ctx, err, conn)
		return sendErr
	}

	// when err != nil (even if sendErr is nil), a non-nil error should be returned to close the connection;
	// otherwise the connection will be reused for the next request which might contain unexpected data.
	return err
}

func (t *ttheaderStreamingTransHandler) onReadStream(ctx context.Context) (context.Context, error) {
	var err error
	// StreamingMetaHandler.OnReadStream
	for _, handler := range t.opt.StreamingMetaHandlers {
		if ctx, err = handler.OnReadStream(ctx); err != nil {
			return nil, err
		}
	}
	return ctx, nil
}

func (t *ttheaderStreamingTransHandler) newCtxWithRPCInfo(ctx context.Context, conn net.Conn) (context.Context, rpcinfo.RPCInfo) {
	ri := t.opt.InitOrResetRPCInfoFunc(nil, conn.RemoteAddr())
	_ = rpcinfo.AsMutableRPCConfig(ri.Config()).SetTransportProtocol(transport.TTHeader)
	return rpcinfo.NewCtxWithRPCInfo(ctx, ri), ri
}

func (t *ttheaderStreamingTransHandler) readMeta(
	ctx context.Context, conn net.Conn, ri rpcinfo.RPCInfo, headerFrame *ttheader.Frame,
) (nCtx context.Context, err error) {
	message := newMessageFromHeaderFrame(ri, headerFrame)
	if err = t.updateRPCInfo(ri, conn, headerFrame.Header()); err != nil {
		return nil, err
	}

	// TODO: use MetaHandler.ReadMeta instead of calling directly ServerTTHeaderHandler.ReadMeta
	// also for ServerTTHeaderHandler.WriteMeta
	if ctx, err = header_transmeta.ServerTTHeaderHandler.ReadMeta(ctx, message); err != nil {
		return nil, fmt.Errorf("KITEX: TTHeader Streaming ServerTTHeaderHandler.ReadMeta failed, error=%v", err)
	}

	// metainfo
	ctx = metainfo.SetMetaInfoFromMap(ctx, headerFrame.Header().StrInfo())

	// grpc style metadata:
	if ctx, err = injectGRPCMetadata(ctx, headerFrame.Header()); err != nil {
		return nil, fmt.Errorf("KITEX: TTHeader Streaming newCtxWithMetaData failed, error=%v", err)
	}
	return ctx, err
}

func (t *ttheaderStreamingTransHandler) sendMetaFrame(
	ctx context.Context, svcInfo *serviceinfo.ServiceInfo, ri rpcinfo.RPCInfo, st *ttheaderStream,
) (err error) {
	meta := remote.NewMessage(nil, svcInfo, ri, remote.Stream, remote.Server)
	defer meta.Recycle()
	meta.TransInfo().TransIntInfo()[ttheader.IntKeyFrameType] = ttheader.FrameTypeMeta
	if metaFrameWriter != nil {
		if err = metaFrameWriter(ctx, meta); err != nil {
			return err
		}
	}
	return st.sendMessage(meta)
}

// TODO: implement it better?
func newMessageFromHeaderFrame(ri rpcinfo.RPCInfo, headerFrame *ttheader.Frame) remote.Message {
	message := remote.NewMessage(nil, nil, ri, remote.Stream, remote.Server)
	message.TransInfo().PutTransIntInfo(headerFrame.Header().IntInfo())
	message.TransInfo().PutTransStrInfo(headerFrame.Header().StrInfo())
	message.SetProtocolInfo(remote.ProtocolInfo{
		CodecType:  codec.ProtocolIDToPayloadCodec(codec.ProtocolID(headerFrame.Header().ProtocolID())),
		TransProto: transport.TTHeader,
	})
	return message
}

func (t *ttheaderStreamingTransHandler) unknownMethod(
	ctx context.Context, ri rpcinfo.RPCInfo, st streaming.Stream, svcInfo *serviceinfo.ServiceInfo,
) (err error) {
	if handler := t.opt.GRPCUnknownServiceHandler; handler != nil {
		rpcinfo.Record(ctx, ri, stats.ServerHandleStart, nil)
		err = handler(ctx, ri.Invocation().MethodName(), st)
		err = processBizStatusError(err, ri)
		if err != nil {
			err = kerrors.ErrBiz.WithCause(err)
		}
		return err
	}

	if svcInfo == nil {
		return remote.NewTransErrorWithMsg(remote.UnknownService,
			fmt.Sprintf("unknown service %s", ri.Invocation().ServiceName()))
	}
	return remote.NewTransErrorWithMsg(remote.UnknownMethod,
		fmt.Sprintf("unknown method %s", ri.Invocation().MethodName()))
}

func (t *ttheaderStreamingTransHandler) retrieveServiceMethodInfo(ri rpcinfo.RPCInfo) (*serviceinfo.ServiceInfo, serviceinfo.MethodInfo) {
	svc, method := ri.Invocation().ServiceName(), ri.Invocation().MethodName()
	svcInfo := t.opt.SvcSearchMap[remote.BuildMultiServiceKey(svc, method)]
	if svcInfo == nil {
		return nil, nil
	}
	return svcInfo, svcInfo.MethodInfo(method)
}

func (t *ttheaderStreamingTransHandler) updateRPCInfo(ri rpcinfo.RPCInfo, conn net.Conn, hdr *ttheader.Header) error {
	serviceAndMethod, exists := hdr.GetIntKey(transmeta.ToMethod)
	if !exists {
		return errors.New("ToMethod not available in meta header")
	}
	packageName, serviceName, methodName, err := parsePackageServiceMethod(serviceAndMethod)
	if err != nil {
		return err
	}

	ink := ri.Invocation().(rpcinfo.InvocationSetter)
	ink.SetSeqID(int32(hdr.SeqNum()))
	ink.SetPackageName(packageName)
	ink.SetServiceName(serviceName)
	ink.SetMethodName(methodName)
	return nil
}
