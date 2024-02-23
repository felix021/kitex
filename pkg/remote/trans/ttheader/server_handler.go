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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"runtime/debug"
	"strings"
	"time"

	"github.com/bytedance/gopkg/cloud/metainfo"
	"github.com/cloudwego/netpoll"

	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/codec"
	"github.com/cloudwego/kitex/pkg/remote/transmeta"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/stats"
	"github.com/cloudwego/kitex/pkg/streaming"
	"github.com/cloudwego/kitex/transport"
)

const (
	detectionSize   = 8
	HeaderMagic     = uint16(codec.TTHeaderMagic >> 16) // 0x1000
	InfinityTimeout = time.Duration(math.MaxInt64)

	CodeSuccess     = 0
	CodeInternal    = 1
	CodeStreamClose = 2
	CodeBizError    = 3
)

// NewSvrTransHandlerFactory ...
func NewSvrTransHandlerFactory() remote.ServerTransHandlerFactory {
	return &ttheaderStreamingTransHandlerFactory{}
}

type ttheaderStreamingTransHandlerFactory struct{}

func (t *ttheaderStreamingTransHandlerFactory) NewTransHandler(opt *remote.ServerOption) (remote.ServerTransHandler, error) {
	return newSvrTransHandler(opt)
}

func newSvrTransHandler(opt *remote.ServerOption) (remote.ServerTransHandler, error) {
	return &ttheaderStreamingTransHandler{
		opt: opt,
		codec: &ttheaderStreamCodec{
			payloadCodec: opt.PayloadCodec,
		},
	}, nil
}

var _ remote.InvokeHandleFuncSetter = &ttheaderStreamingTransHandler{}

type ttheaderStreamingTransHandler struct {
	opt           *remote.ServerOption
	codec         *ttheaderStreamCodec
	invokeHandler endpoint.Endpoint
}

// SetInvokeHandleFunc implements the remote.InvokeHandleFuncSetter interface.
func (t *ttheaderStreamingTransHandler) SetInvokeHandleFunc(invokeHandler endpoint.Endpoint) {
	t.invokeHandler = invokeHandler
}

// ProtocolMatch implements the detection.DetectableServerTransHandler interface.
// If bytes[4:6] is HeaderMagic and FlagStreamingBitMask set, it means the protocol is Streaming over TTHeader
func (t *ttheaderStreamingTransHandler) ProtocolMatch(ctx context.Context, conn net.Conn) (err error) {
	preface, err := getReader(conn).Peek(detectionSize)
	if err != nil {
		return err
	}
	magic := binary.BigEndian.Uint16(preface[4:6])
	if magic == HeaderMagic {
		flags := binary.BigEndian.Uint16(preface[6:8])
		if flags&BitMaskIsStreaming != 0 {
			return nil
		}
	}
	return errors.New("not matching ttheader streaming")
}

// Write implements the remote.ServerTransHandler interface.
func (t *ttheaderStreamingTransHandler) Write(ctx context.Context, conn net.Conn, msg remote.Message) (nctx context.Context, err error) {
	panic("not applicable for TTHeader Streaming")
}

// Read implements the remote.ServerTransHandler interface.
func (t *ttheaderStreamingTransHandler) Read(ctx context.Context, conn net.Conn, msg remote.Message) (nctx context.Context, err error) {
	panic("not applicable for TTHeader Streaming")
}

// OnMessage implements the remote.ServerTransHandler interface.
func (t *ttheaderStreamingTransHandler) OnMessage(ctx context.Context, args, result remote.Message) (context.Context, error) {
	panic("not applicable for TTHeader Streaming")
}

// OnInactive implements the remote.ServerTransHandler interface.
func (t *ttheaderStreamingTransHandler) OnInactive(ctx context.Context, conn net.Conn) {
	// TODO: anything to do?
}

// OnError implements the remote.ServerTransHandler interface.
func (t *ttheaderStreamingTransHandler) OnError(ctx context.Context, err error, conn net.Conn) {
	var de *kerrors.DetailedError
	if ok := errors.As(err, &de); ok && de.Stack() != "" {
		klog.Errorf("KITEX: processing TTHeader Streaming error, remoteAddr=%s, error=%s\nstack=%s",
			conn.RemoteAddr(), err.Error(), de.Stack())
	} else {
		klog.Errorf("KITEX: processing TTHeader Streaming error, remoteAddr=%s, error=%s",
			conn.RemoteAddr(), err.Error())
	}
}

// SetPipeline implements the remote.ServerTransHandler interface.
func (t *ttheaderStreamingTransHandler) SetPipeline(pipeline *remote.TransPipeline) {
}

// OnActive implements the remote.ServerTransHandler interface.
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

// OnRead implements the remote.ServerTransHandler interface.
func (t *ttheaderStreamingTransHandler) OnRead(ctx context.Context, conn net.Conn) error {
	ri, ctx, msg, err := t.readHeaderFrame(ctx, conn)
	if err != nil {
		f := newEndOfStreamFrame(ctx, CodeInternal, err.Error(), nil)
		if sendErr := writeFrame(ctx, conn, f); sendErr != nil {
			klog.Errorf("KITEX: TTHeader Streaming readHeaderFrame-sendCloseFrame error, remoteAddress=%s, error=%s",
				conn.RemoteAddr(), err.Error())
		}
		t.OnError(ctx, err, conn)
		return fmt.Errorf("KITEX: TTHeader Streaming readHeaderFrame failed, error=%s", err.Error())
	}

	// Streaming over TTHeader requires the server returns a meta frame
	if err = t.sendMetaFrame(ctx, conn, msg); err != nil {
		return err
	}

	// StreamingMetaHandler.OnReadStream
	for _, handler := range t.opt.StreamingMetaHandlers {
		if ctx, err = handler.OnReadStream(ctx); err != nil {
			f := newEndOfStreamFrame(ctx, CodeInternal, err.Error(), nil)
			if sendErr := writeFrame(ctx, conn, f); sendErr != nil {
				klog.Errorf("KITEX: TTHeader Streaming OnReadStream-sendCloseFrame error, remoteAddress=%s, error=%s",
					conn.RemoteAddr(), err.Error())
			}
			t.OnError(ctx, err, conn)
			return fmt.Errorf("KITEX: TTHeader Streaming OnReadStream failed, error=%s", err.Error())
		}
	}

	ctx = t.startTracer(ctx, ri)
	defer func() {
		panicErr := recover()
		if panicErr != nil {
			if conn != nil {
				klog.CtxErrorf(ctx, "KITEX: TTHeader Streaming OnRead panic happened, close conn, remoteAddress=%s, error=%s\nstack=%s",
					conn.RemoteAddr(), panicErr, string(debug.Stack()))
			} else {
				klog.CtxErrorf(ctx, "KITEX: TTHeader Streaming OnRead panic happened, error=%v\nstack=%s",
					panicErr, string(debug.Stack()))
			}
		}
		t.finishTracer(ctx, ri, err, panicErr)
	}()

	serviceName, methodName := ri.Invocation().ServiceName(), ri.Invocation().MethodName()

	svcInfo := t.opt.SvcSearchMap[remote.BuildMultiServiceKey(serviceName, methodName)]
	var methodInfo serviceinfo.MethodInfo
	if svcInfo != nil {
		methodInfo = svcInfo.MethodInfo(methodName)
	}

	rawStream := newTTHeaderStream(ctx, conn, msg, t.codec)
	st := newStreamWithMW(rawStream, t.opt.RecvEndpoint, t.opt.SendEndpoint)

	// bind stream into ctx, in order to let user set header and trailer by provided api in meta_api.go
	ctx = streaming.NewCtxWithStream(ctx, st)

	if methodInfo == nil {
		err = t.unknownMethod(ctx, ri, st, svcInfo)
	} else {
		// TODO: support ServerStreamingGetRequest
		err = t.invokeHandler(ctx, &streaming.Args{Stream: st}, nil)
	}

	if err = t.finishStream(ctx, conn, ri, st, err); err != nil {
		t.OnError(ctx, err, conn)
	}
	return err
}

func (t *ttheaderStreamingTransHandler) sendMetaFrame(ctx context.Context, conn net.Conn, msg remote.Message) error {
	metaFrame := &frame{}
	if metaFrameGenerator != nil {
		hdr, err := metaFrameGenerator(ctx, msg)
		if err != nil {
			f := newEndOfStreamFrame(ctx, CodeInternal, err.Error(), nil)
			if sendErr := writeFrame(ctx, conn, f); sendErr != nil {
				klog.Errorf("KITEX: TTHeader Streaming OnReadStream-metaFrameGenerator error, remoteAddress=%s, error=%s",
					conn.RemoteAddr(), err.Error())
			}
			t.OnError(ctx, err, conn)
			return fmt.Errorf("KITEX: TTHeader Streaming metaFrameGenerator failed, error=%s", err.Error())
		}
		metaFrame.header = hdr
	} else {
		metaFrame.header = newTTHeader()
	}
	if sendErr := writeFrame(ctx, conn, metaFrame); sendErr != nil {
		klog.Errorf("KITEX: TTHeader Streaming OnReadStream-sendMetaFrame error, remoteAddress=%s, error=%s",
			conn.RemoteAddr(), sendErr.Error())
		t.OnError(ctx, sendErr, conn)
		return fmt.Errorf("KITEX: TTHeader Streaming sendMetaFrame failed, error=%s", sendErr.Error())
	}
	return nil
}

func (t *ttheaderStreamingTransHandler) finishStream(
	ctx context.Context, conn net.Conn, ri rpcinfo.RPCInfo, st streaming.Stream, err error,
) error {
	code := 0
	message := ""
	var bizStatusErr kerrors.BizStatusErrorIface
	if err != nil {
		err = kerrors.ErrBiz.WithCause(err)
		code = CodeBizError
		message = err.Error()
		t.OnError(ctx, err, conn)
	} else {
		bizStatusErr = ri.Invocation().BizStatusErr()
	}

	_ = st.Close() // always close the stream after server method handler

	f := newEndOfStreamFrame(ctx, code, message, bizStatusErr)
	f.MetaData = metadataToMap(st.Trailer())
	return writeFrame(ctx, conn, f)
}

func (t *ttheaderStreamingTransHandler) unknownMethod(ctx context.Context, ri rpcinfo.RPCInfo, st streaming.Stream, svcInfo *serviceinfo.ServiceInfo) (err error) {
	unknownServiceHandlerFunc := t.opt.GRPCUnknownServiceHandler
	if unknownServiceHandlerFunc != nil {
		rpcinfo.Record(ctx, ri, stats.ServerHandleStart, nil)
		err = unknownServiceHandlerFunc(ctx, ri.Invocation().MethodName(), st)
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

func (t *ttheaderStreamingTransHandler) readHeaderFrame(
	ctx context.Context, conn net.Conn,
) (rpcinfo.RPCInfo, context.Context, remote.Message, error) {
	ri := t.opt.InitOrResetRPCInfoFunc(nil, conn.RemoteAddr())
	_ = rpcinfo.AsMutableRPCConfig(ri.Config()).SetTransportProtocol(transport.TTHeader) // TODO: TTHeaderStreaming?

	message := remote.NewMessage(nil, nil, ri, remote.Stream, remote.Server)
	err := codec.GetTTHeaderCodec().Decode(ctx, message, conn.(remote.ByteBuffer))
	if err != nil {
		return nil, nil, nil, err
	}
	ink := ri.Invocation().(rpcinfo.InvocationSetter)

	methodName := message.TransInfo().TransIntInfo()[transmeta.ToMethod]
	ink.SetMethodName(methodName)
	if mutableTo := rpcinfo.AsMutableEndpointInfo(ri.To()); mutableTo != nil {
		if err = mutableTo.SetMethod(methodName); err != nil {
			return nil, nil, nil,
				fmt.Errorf("setMethod failed in streaming, method=%s, error=%s", methodName, err.Error())
		}
	}

	pkg, svc := rpcinfo.SplitPackageAndService(message.TransInfo().TransIntInfo()[transmeta.ToService])
	ink.SetPackageName(pkg)
	ink.SetServiceName(svc)

	ctx = rpcinfo.NewCtxWithRPCInfo(ctx, ri)

	// metainfo
	values := make([]string, 8) // todo: initial length?
	pValues := make([]string, 8)
	for k, v := range message.TransInfo().TransStrInfo() {
		if strings.HasPrefix(k, metainfo.PrefixTransient) {
			values = append(values, k[len(metainfo.PrefixTransient):])
			values = append(values, v)
		}
		if strings.HasPrefix(k, metainfo.PrefixPersistent) {
			pValues = append(pValues, k[len(metainfo.PrefixPersistent):])
			pValues = append(pValues, v)
		}
	}
	ctx = metainfo.WithValues(ctx, values...)
	ctx = metainfo.WithPersistentValues(ctx, pValues...)

	return ri, ctx, message, nil
}

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
