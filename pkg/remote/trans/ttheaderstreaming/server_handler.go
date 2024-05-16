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
	"math"
	"net"
	"runtime/debug"
	"time"

	"github.com/cloudwego/localsession/backup"
	"github.com/cloudwego/netpoll"

	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/codec/ttheader"
	"github.com/cloudwego/kitex/pkg/remote/trans"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/stats"
	"github.com/cloudwego/kitex/pkg/streaming"
	header_transmeta "github.com/cloudwego/kitex/pkg/transmeta"
	"github.com/cloudwego/kitex/transport"
)

const (
	detectionSize   = 8
	offsetMagic     = 4
	offsetFlags     = 6
	InfinityTimeout = time.Duration(math.MaxInt64)
)

var (
	_ remote.ServerTransHandler     = (*ttheaderStreamingTransHandler)(nil)
	_ remote.InvokeHandleFuncSetter = (*ttheaderStreamingTransHandler)(nil)

	ErrNotTTHeaderStreaming = errors.New("not ttheader streaming")

	serverHeaderFrameHandler remote.MetaHandler = header_transmeta.ServerTTHeaderHandler
)

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
	return ErrNotTTHeaderStreaming
}

// SetInvokeHandleFunc implements the remote.InvokeHandleFuncSetter interface.
func (t *ttheaderStreamingTransHandler) SetInvokeHandleFunc(invokeHandler endpoint.Endpoint) {
	t.invokeHandler = invokeHandler
}

func (t *ttheaderStreamingTransHandler) OnError(ctx context.Context, err error, conn net.Conn) {
	var detailedError *kerrors.DetailedError
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
	panic("not used")
}

// Read should not be called in ttheaderStreamingTransHandler
func (t *ttheaderStreamingTransHandler) Read(ctx context.Context, conn net.Conn, msg remote.Message) (context.Context, error) {
	panic("not used")
}

// OnMessage should not be called in ttheaderStreamingTransHandler
func (t *ttheaderStreamingTransHandler) OnMessage(ctx context.Context, args, result remote.Message) (context.Context, error) {
	panic("not used")
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
	rawStream := newTTHeaderStream(ctx, conn, ri, t.codec, nil, remote.Server, t.ext)

	if _, err = rawStream.readHeaderForServer(t.opt); err != nil {
		if errors.Is(err, ErrDirtyFrame) {
			// There could be dirty frames from the previous stream while the connection has been closed by the client.
			// Just ignore the error (and drop the frame) since we're not starting a new stream at this moment.
			return nil
		}
		t.OnError(ctx, err, conn)
		return fmt.Errorf("KITEX: TTHeader Streaming readHeaderFrame failed, error=%v", err)
	}

	// tracer is started in rawStream.readHeaderForServer()
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

	st := endpoint.NewStreamWithMiddleware(rawStream, t.opt.RecvEndpoint, t.opt.SendEndpoint)
	// bind stream into ctx, for user to set grpc style metadata(headers/trailers) by provided api in meta_api.go
	ctx = streaming.NewCtxWithStream(ctx, st)

	svcInfo, methodInfo := t.retrieveServiceMethodInfo(ri)
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
		t.OnError(ctx, sendErr, conn)
		return sendErr
	}

	// when err != nil (even if sendErr is nil), a non-nil error should be returned to close the connection;
	// otherwise the connection will be reused for the next request which might contain unexpected data.
	return err
}

func (t *ttheaderStreamingTransHandler) newCtxWithRPCInfo(ctx context.Context, conn net.Conn) (context.Context, rpcinfo.RPCInfo) {
	ri := t.opt.InitOrResetRPCInfoFunc(nil, conn.RemoteAddr())
	_ = rpcinfo.AsMutableRPCConfig(ri.Config()).SetTransportProtocol(transport.TTHeader)
	return rpcinfo.NewCtxWithRPCInfo(ctx, ri), ri
}

func (t *ttheaderStreamingTransHandler) unknownMethod(
	ctx context.Context, ri rpcinfo.RPCInfo, st streaming.Stream, svcInfo *serviceinfo.ServiceInfo,
) (err error) {
	if t.opt.GRPCUnknownServiceHandler != nil {
		rpcinfo.Record(ctx, ri, stats.ServerHandleStart, nil)
		err = t.unknownMethodWithRecover(ctx, ri, st)
		if err = coverBizStatusError(err, ri); err != nil {
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

func (t *ttheaderStreamingTransHandler) unknownMethodWithRecover(
	ctx context.Context, ri rpcinfo.RPCInfo, st streaming.Stream,
) (err error) {
	defer func() {
		if handlerErr := recover(); handlerErr != nil {
			err = kerrors.ErrPanic.WithCauseAndStack(
				fmt.Errorf(
					"[happened in biz handler, method=%s.%s, please check the panic at the server side] %s",
					ri.Invocation().ServiceName(), ri.Invocation().MethodName(), handlerErr),
				string(debug.Stack()))
			rpcStats := rpcinfo.AsMutableRPCStats(ri.Stats())
			rpcStats.SetPanicked(err)
		}
		rpcinfo.Record(ctx, ri, stats.ServerHandleFinish, err)
		// clear session
		backup.ClearCtx()
	}()
	return t.opt.GRPCUnknownServiceHandler(ctx, ri.Invocation().MethodName(), st)
}

func (t *ttheaderStreamingTransHandler) retrieveServiceMethodInfo(ri rpcinfo.RPCInfo) (*serviceinfo.ServiceInfo, serviceinfo.MethodInfo) {
	svc, method := ri.Invocation().ServiceName(), ri.Invocation().MethodName()
	svcInfo := t.opt.SvcSearchMap[remote.BuildMultiServiceKey(svc, method)]
	if svcInfo == nil {
		return nil, nil
	}
	return svcInfo, svcInfo.MethodInfo(method)
}
