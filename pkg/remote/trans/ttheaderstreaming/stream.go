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
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/bytedance/gopkg/cloud/metainfo"
	"github.com/bytedance/sonic"

	"github.com/cloudwego/kitex/pkg/gofunc"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/codec/ttheader"
	"github.com/cloudwego/kitex/pkg/remote/trans"
	remote_transmeta "github.com/cloudwego/kitex/pkg/remote/transmeta"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/streaming/metadata"
	"github.com/cloudwego/kitex/pkg/transmeta"
)

const (
	sent     = 1
	received = 1
	closed   = 1
)

var (
	ErrIllegalHeaderWrite = errors.New("ttheader streaming: the stream is done or headers was already sent")
	ErrSendClosed         = errors.New("ttheader streaming: send closed")
	ErrDirtyFrame         = errors.New("ttheader streaming: dirty frame")
)

func newTTHeaderStream(
	ctx context.Context, conn net.Conn, ri rpcinfo.RPCInfo, codec remote.PayloadCodec,
	header remote.Message, role remote.RPCRole, ext trans.Extension,
) *ttheaderStream {
	ctx, cancel := context.WithCancel(ctx)
	st := &ttheaderStream{
		ctx:         ctx,
		cancel:      cancel,
		conn:        conn,
		ri:          ri,
		codec:       codec,
		headerFrame: header, // ttheader from/for HeaderFrame
		rpcRole:     role,
		ext:         ext,
	}
	return st
}

type ttheaderStream struct {
	ctx    context.Context
	cancel context.CancelFunc

	conn  net.Conn
	ri    rpcinfo.RPCInfo
	codec remote.PayloadCodec
	ext   trans.Extension

	rpcRole         remote.RPCRole
	headerFrame     remote.Message
	headerFrameSent uint32

	// grpc style metadata for compatibility: send (for server)
	metadataMutex sync.Mutex
	headersToSend metadata.MD
	trailerToSend metadata.MD

	// grpc style metadata for compatibility: recv (for client)
	headersRecv     metadata.MD
	headerReceived  uint32
	trailerRecv     metadata.MD
	trailerReceived uint32

	lastRecvError error

	recvClosed uint32
	sendClosed uint32
}

// SetHeader implements streaming.Stream
// If the grpc-style headers was already sent or the stream is done, ErrIllegalHeaderWrite is returned
func (t *ttheaderStream) SetHeader(md metadata.MD) error {
	if t.rpcRole != remote.Server {
		panic("this method should only be used in server side stream!")
	}
	if t.isHeaderSent() || t.isSendClosed() {
		return ErrIllegalHeaderWrite
	}
	return t.setHeader(md)
}

// setHeader appends md to headers without check
func (t *ttheaderStream) setHeader(md metadata.MD) error {
	if md.Len() == 0 {
		return nil
	}
	t.metadataMutex.Lock()
	defer t.metadataMutex.Unlock()
	t.headersToSend = metadata.AppendMD(t.headersToSend, md)
	return nil
}

func (t *ttheaderStream) loadGRPCMetadata() {
	if md, ok := metadata.FromOutgoingContext(t.ctx); ok {
		t.headersToSend = md
	}
}

// SendHeader implements streaming.Stream
func (t *ttheaderStream) SendHeader(md metadata.MD) (err error) {
	if t.rpcRole != remote.Server {
		panic("this method should only be used in server side stream!")
	}
	if err = t.setHeader(md); err != nil {
		return err
	}
	return t.sendHeaderFrame()
}

func (t *ttheaderStream) sendHeaderFrame() (err error) {
	if atomic.SwapUint32(&t.headerFrameSent, sent) == sent {
		return nil
	}
	return t.sendFrameWithMetaData(ttheader.FrameTypeHeader, nil)
}

func (t *ttheaderStream) sendTrailer(invokeErr error) error {
	if t.isSendClosed() {
		return nil
	}
	return t.sendFrameWithMetaData(ttheader.FrameTypeTrailer, invokeErr)
}

// sendFrameWithMetaData sends the HeaderFrame or TrailerFrame with grpc-style metadata
// HeaderFrame includes the grpc style metadata headers (in strInfo key "grpc-metadata")
// TrailerFrame includes:
// - the grpc style metadata trailers (in strInfo key "grpc-metadata")
// - BizStatusError (in strInfo keys "biz-status", "biz-message" and "biz-extra")
// - (data) the invokeErr returned by method handler (in payload, serialized as a TApplicationException)
func (t *ttheaderStream) sendFrameWithMetaData(frameType string, data interface{}) (err error) {
	message, needRecycle := t.newMessageToSend(data, frameType)
	if needRecycle {
		defer message.Recycle()
	}
	var metadataJSON []byte
	if frameType == ttheader.FrameTypeTrailer {
		transmeta.InjectBizStatusError(message.TransInfo().TransStrInfo(), t.ri.Invocation().BizStatusErr())
		metadataJSON, err = t.getMetaDataAsJSON(&t.trailerToSend)
	} else { // HeaderFrame
		metadataJSON, err = t.getMetaDataAsJSON(&t.headersToSend)
	}
	if err != nil {
		return fmt.Errorf("failed to get metadata as json: frameType=%s, err=%w", frameType, err)
	}
	if len(metadataJSON) > 0 {
		message.TransInfo().TransStrInfo()[ttheader.StrKeyMetaData] = string(metadataJSON)
	}
	return t.sendMessage(message)
}

func (t *ttheaderStream) sendMessage(message remote.Message) error {
	out := t.ext.NewWriteByteBuffer(t.ctx, t.conn, message)
	if err := t.codec.Marshal(t.ctx, message, out); err != nil {
		return err
	}
	return out.Flush()
}

// getMetaDataAsJSON minimizes critical section on metadataMutex
func (t *ttheaderStream) getMetaDataAsJSON(target *metadata.MD) ([]byte, error) {
	t.metadataMutex.Lock()
	defer t.metadataMutex.Unlock()
	if len(*target) == 0 {
		return nil, nil
	}
	return sonic.Marshal(target)
}

func (t *ttheaderStream) SetTrailer(md metadata.MD) {
	if t.rpcRole != remote.Server {
		panic("this method should only be used in server side stream!")
	}
	if md.Len() == 0 || t.isSendClosed() {
		return
	}
	t.metadataMutex.Lock()
	t.trailerToSend = metadata.AppendMD(t.trailerToSend, md)
	t.metadataMutex.Unlock()
}

func (t *ttheaderStream) Header() (metadata.MD, error) {
	if t.rpcRole == remote.Server {
		panic("this method should only be used in client side stream!")
	}
	if old := atomic.SwapUint32(&t.headerReceived, received); old != received {
		if _, err := t.readUntilTargetFrame(nil, ttheader.FrameTypeHeader); err != nil {
			return nil, err
		}
	}
	return t.headersRecv, nil
}

func (t *ttheaderStream) Trailer() metadata.MD {
	if t.rpcRole == remote.Server {
		panic("this method should only be used in client side stream!")
	}
	if old := atomic.SwapUint32(&t.trailerReceived, received); old != received {
		_, _ = t.readUntilTargetFrame(nil, ttheader.FrameTypeTrailer)
	}
	return t.trailerRecv
}

func (t *ttheaderStream) Context() context.Context {
	return t.ctx
}

func (t *ttheaderStream) readMessage(data interface{}) (message remote.Message, err error) {
	defer func() {
		if panicInfo := recover(); panicInfo != nil {
			err = fmt.Errorf("readMessage panic: %v, stack=%s", panicInfo, string(debug.Stack()))
		}
		if err != nil {
			t.closeRecv(err)
		}
	}()
	if t.isRecvClosed() {
		return nil, t.lastRecvError
	}
	for {
		message = remote.NewMessage(data, nil, t.ri, remote.Stream, t.rpcRole)
		bufReader := t.ext.NewReadByteBuffer(t.ctx, t.conn, message)
		err = t.codec.Unmarshal(t.ctx, message, bufReader)
		if t.shouldSkipDirtyFrameForClient(err) {
			// client: dirty frames from previous stream, try next frame
			continue
		}
		return message, err
	}
}

func (t *ttheaderStream) shouldSkipDirtyFrameForClient(err error) bool {
	return t.rpcRole == remote.Client && errors.Is(err, ttheader.ErrInvalidSeqID)
}

func (t *ttheaderStream) readUntilTargetFrame(data interface{}, targetType string) (message remote.Message, err error) {
	for {
		if message, err = t.readMessage(data); err != nil {
			// Note: if grpc-trailer is returned with a TApplicationException, it will be ignored
			return nil, err
		}
		ft := ttheader.MessageFrameType(message)
		switch ft {
		case ttheader.FrameTypeMeta:
			if err = t.parseMetaFrame(message); err != nil {
				return nil, err
			}
		case ttheader.FrameTypeHeader:
			if err = t.parseHeaderFrame(message); err != nil {
				return nil, err
			}
		case ttheader.FrameTypeData:
		case ttheader.FrameTypeTrailer:
			return message, t.parseTrailerFrame(message) // last frame of the stream
		default:
			return nil, fmt.Errorf("unexpected frame, type = %s", ft)
		}
		if ft == targetType {
			return message, nil
		}
	}
}

// skipFrameUntil discards frames until the given condition is met
func (t *ttheaderStream) skipFrameUntil(expected func(remote.Message) bool) (message remote.Message, err error) {
	for {
		message, err = t.readMessage(nil)
		if err != nil {
			return nil, err
		}
		if expected(message) {
			return message, nil
		}
	}
}

func (t *ttheaderStream) readHeaderForServer(opt *remote.ServerOption) (nCtx context.Context, err error) {
	if t.headerFrame, err = t.readMessage(nil); err != nil {
		return nil, err
	} else if ttheader.MessageFrameType(t.headerFrame) != ttheader.FrameTypeHeader {
		return nil, ErrDirtyFrame
	}

	if nCtx, err = t.readMetaFromHeaderFrame(opt); err != nil {
		return nil, err
	}

	nCtx = opt.TracerCtl.DoStart(nCtx, t.ri)

	t.ctx = nCtx
	return nCtx, err
}

func (t *ttheaderStream) readMetaFromHeaderFrame(opt *remote.ServerOption) (nCtx context.Context, err error) {
	nCtx = t.ctx // with cancel
	if serverHeaderFrameHandler != nil {
		if nCtx, err = serverHeaderFrameHandler.ReadMeta(nCtx, t.headerFrame); err != nil {
			return nil, err
		}
	}

	// metainfo
	nCtx = metainfo.SetMetaInfoFromMap(nCtx, t.headerFrame.TransInfo().TransStrInfo())

	// grpc style metadata:
	if nCtx, err = injectGRPCMetadata(nCtx, t.headerFrame.TransInfo().TransStrInfo()); err != nil {
		return nil, err
	}

	// StreamingMetaHandler.OnReadStream
	for _, handler := range opt.StreamingMetaHandlers {
		if nCtx, err = handler.OnReadStream(nCtx); err != nil {
			return nil, err
		}
	}
	return nCtx, nil
}

func (t *ttheaderStream) parseMetaFrame(message remote.Message) (err error) {
	defer func() {
		if err != nil {
			t.closeRecv(err)
		}
	}()
	if clientMetaFrameHandler == nil {
		return nil
	}
	_, err = clientMetaFrameHandler.ReadMeta(t.ctx, message)
	return err
}

func (t *ttheaderStream) parseHeaderFrame(message remote.Message) (err error) {
	defer func() {
		if err != nil {
			t.closeRecv(err)
		}
	}()
	if t.rpcRole == remote.Client {
		if _, err = clientHeaderFrameHandler.ReadMeta(t.ctx, message); err != nil {
			return err
		}
		return t.parseGRPCMetadata(message, &t.headersRecv)
	} else {
		// server: HeaderFrame is already processed in readHeaderForServer
		return nil
	}
}

func (t *ttheaderStream) parseTrailerFrame(message remote.Message) (err error) {
	defer func() {
		if err == nil {
			err = io.EOF // always return io.EOF when there's no error, since trailer is the last frame
		}
		t.closeRecv(err)
	}()
	return t.parseGRPCMetadata(message, &t.trailerRecv)
}

// parseGRPCMetadata parses grpc style metadata, helpful for projects migrating from kitex-grpc
func (t *ttheaderStream) parseGRPCMetadata(message remote.Message, target *metadata.MD) error {
	if metadataJSON, exists := message.TransInfo().TransStrInfo()[ttheader.StrKeyMetaData]; exists {
		md := metadata.MD{}
		if err := sonic.Unmarshal([]byte(metadataJSON), &md); err != nil {
			return fmt.Errorf("invalid metadata: json=%s, err = %w", metadataJSON, err)
		}
		t.metadataMutex.Lock()
		*target = md
		t.metadataMutex.Unlock()
	}
	return nil
}

func (t *ttheaderStream) RecvMsg(m interface{}) error {
	if t.isRecvClosed() {
		return t.lastRecvError
	}

	frameReceived := make(chan struct{}, 1)

	var readErr error
	gofunc.GoFunc(t.ctx, func() {
		_, readErr = t.readUntilTargetFrame(m, ttheader.FrameTypeData)
		frameReceived <- struct{}{}
	})

	select {
	case <-t.ctx.Done():
		ctxDoneErr := t.ctx.Err()
		t.closeRecv(ctxDoneErr)
		return ctxDoneErr
	case <-frameReceived:
		if readErr == io.EOF && t.ri.Invocation().BizStatusErr() != nil {
			return nil // same behavior as grpc streaming: return nil for biz status error
		}
		return readErr
	}
}

func (t *ttheaderStream) SendMsg(m interface{}) (err error) {
	if t.isSendClosed() {
		return io.ErrClosedPipe
	}
	if err = t.sendHeaderFrame(); err != nil {
		return err
	}

	message, needRecycle := t.newMessageToSend(m, ttheader.FrameTypeData)
	if needRecycle {
		defer message.Recycle()
	}
	return t.sendMessage(message)
}

func (t *ttheaderStream) newMessageToSend(m interface{}, frameType string) (msg remote.Message, needRecycle bool) {
	if t.rpcRole == remote.Client && frameType == ttheader.FrameTypeHeader {
		return t.headerFrame, false // prepared in client_handler.go
	}
	message := remote.NewMessage(m, nil, t.ri, remote.Stream, t.rpcRole)
	message.SetProtocolInfo(t.headerFrame.ProtocolInfo())
	message.TransInfo().TransIntInfo()[remote_transmeta.FrameType] = frameType
	return message, true
}

// Close implements streaming.Stream
// For client side, the stream can still be used to receive data
func (t *ttheaderStream) Close() error {
	if alreadyClosed := t.closeSend(); alreadyClosed {
		return nil
	}
	if t.rpcRole == remote.Server {
		t.closeRecv(ErrSendClosed) // TODO: compare with grpc streaming
		t.cancel()                 // release blocking RecvMsg call (if any)
	} else /* remote.Client */ {
		// inform the server the client's end of stream
		clientTrailer, needRecycle := t.newMessageToSend(nil, ttheader.FrameTypeTrailer)
		if needRecycle {
			defer clientTrailer.Recycle()
		}
		_ = t.sendMessage(clientTrailer)
	}
	return nil
}

func (t *ttheaderStream) isHeaderSent() bool {
	return atomic.LoadUint32(&t.headerFrameSent) == sent
}

func (t *ttheaderStream) isRecvClosed() bool {
	return atomic.LoadUint32(&t.recvClosed) == closed
}

func (t *ttheaderStream) closeRecv(err error) {
	if atomic.SwapUint32(&t.recvClosed, closed) == closed {
		return
	}
	if err == nil {
		err = io.EOF
	}
	t.lastRecvError = err
}

func (t *ttheaderStream) isSendClosed() bool {
	return atomic.LoadUint32(&t.sendClosed) == sent
}

func (t *ttheaderStream) closeSend() (alreadyClosed bool) {
	return atomic.SwapUint32(&t.sendClosed, closed) == closed
}
