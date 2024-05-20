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
	"encoding/binary"
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
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/codec"
	"github.com/cloudwego/kitex/pkg/remote/codec/ttheader"
	"github.com/cloudwego/kitex/pkg/remote/trans"
	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/metadata"
	remote_transmeta "github.com/cloudwego/kitex/pkg/remote/transmeta"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/transmeta"
	"github.com/cloudwego/netpoll"
)

const (
	sent     = 1
	received = 1
	closed   = 1

	initialReceivedChanSize = 16
	initialSendChanSize     = 16
)

var (
	ErrIllegalHeaderWrite = errors.New("ttheader streaming: the stream is done or headers was already sent")
	ErrInvalidFrame       = errors.New("ttheader streaming: invalid frame")
	ErrSendClosed         = errors.New("ttheader streaming: send closed")
	ErrDirtyFrame         = errors.New("ttheader streaming: dirty frame")

	idAllocator = uint64(0)
)

func newTTHeaderStream(
	ctx context.Context, conn net.Conn, ri rpcinfo.RPCInfo, codec remote.PayloadCodec,
	clientHeader remote.Message, role remote.RPCRole, ext trans.Extension,
) *ttheaderStream {
	ctx, cancel := context.WithCancel(ctx)
	st := &ttheaderStream{
		id:           atomic.AddUint64(&idAllocator, 1),
		ctx:          ctx,
		cancel:       cancel,
		conn:         conn,
		ri:           ri,
		codec:        codec,
		clientHeader: clientHeader, // ttheader from/for HeaderFrame
		rpcRole:      role,
		ext:          ext,

		receiveQueue: make(chan []byte, initialReceivedChanSize),
		sendQueue:    make(chan remote.Message, initialSendChanSize),
	}
	if clientHeader != nil {
		st.protocolInfo = clientHeader.ProtocolInfo() // for creating following frames
	}
	gofunc.GoFunc(ctx, st.frameReceiver)
	gofunc.GoFunc(ctx, st.messageSender)
	return st
}

type ttheaderStream struct {
	id     uint64
	ctx    context.Context
	cancel context.CancelFunc

	conn  net.Conn
	ri    rpcinfo.RPCInfo
	codec remote.PayloadCodec
	ext   trans.Extension

	rpcRole         remote.RPCRole
	clientHeader    remote.Message
	headerFrameSent uint32

	// grpc style metadata for compatibility: send (for server)
	metadataMutex sync.Mutex
	headersToSend metadata.MD
	trailerToSend metadata.MD

	// grpc style metadata for compatibility: recv (for client)
	headerReceived  uint32
	headersRecv     metadata.MD
	trailerReceived uint32
	trailerRecv     metadata.MD

	recvState state

	recvClosed    uint32
	lastRecvError error

	sendClosed    uint32
	lastSendError error

	// receiveQueue is a channel of byte slice since we need to decode the frame directly into the struct
	// created by the `xxxClient.Recv()` (in kitex_gen: xxx-service/client.go)
	receiveQueue chan []byte

	// sendQueue is a channel of remote.Message since we need to write the frame directly to the output stream,
	// to make full use of the no-copy feature provided by netpoll.
	sendQueue chan remote.Message

	protocolInfo remote.ProtocolInfo
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
	return t.sendFrameWithMetaData(codec.FrameTypeHeader, nil)
}

func (t *ttheaderStream) serverSendTrailer(invokeErr error) error {
	if t.isSendClosed() {
		return nil
	}
	if err := t.sendHeaderFrame(); err != nil {
		// make sure HeaderFrame is always sent before any frame
		return err
	}
	return t.sendFrameWithMetaData(codec.FrameTypeTrailer, invokeErr)
}

// sendFrameWithMetaData sends the HeaderFrame or TrailerFrame with grpc-style metadata
// HeaderFrame includes the grpc style metadata headers (in strInfo key "grpc-metadata")
// TrailerFrame includes:
// - the grpc style metadata trailers (in strInfo key "grpc-metadata")
// - BizStatusError (in strInfo keys "biz-status", "biz-message" and "biz-extra")
// - (data) the invokeErr returned by method handler (in payload, serialized as a TApplicationException)
func (t *ttheaderStream) sendFrameWithMetaData(frameType string, invokeErr error) (err error) {
	message := t.newMessageToSend(invokeErr, frameType)
	var metadataJSON []byte
	if frameType == codec.FrameTypeHeader {
		metadataJSON, err = t.getMetaDataAsJSON(&t.headersToSend)
	} else { // TrailerFrame
		if invokeErr != nil {
			ttheader.InjectTransError(message, invokeErr)
		} else {
			transmeta.InjectBizStatusError(message.TransInfo().TransStrInfo(), t.ri.Invocation().BizStatusErr())
		}
		message.Tags()[codec.HeaderFlagsKey] = codec.HeaderFlagsStreamingTrailer
		metadataJSON, err = t.getMetaDataAsJSON(&t.trailerToSend)
	}
	if err != nil {
		return fmt.Errorf("failed to get metadata as json: frameType=%s, err=%w", frameType, err)
	}
	if len(metadataJSON) > 0 {
		message.TransInfo().TransStrInfo()[codec.StrKeyMetaData] = string(metadataJSON)
	}
	return t.putMessageToSendQueue(message)
}

func (t *ttheaderStream) putMessageToSendQueue(message remote.Message) error {
	select {
	case <-t.ctx.Done():
		err := t.ctx.Err()
		t.closeSend(err)
		return err
	case t.sendQueue <- message:
		return nil
	}
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
	if t.rpcRole != remote.Client {
		panic("Header() should only be used in client side stream!")
	}
	if old := atomic.SwapUint32(&t.headerReceived, received); old != received {
		if _, err := t.readUntilTargetFrame(nil, codec.FrameTypeHeader); err != nil {
			return nil, err
		}
	}
	return t.headersRecv, nil
}

func (t *ttheaderStream) Trailer() metadata.MD {
	if t.rpcRole != remote.Client {
		panic("Trailer() should only be used in client side stream!")
	}
	if old := atomic.SwapUint32(&t.trailerReceived, received); old != received {
		_, _ = t.readUntilTargetFrame(nil, codec.FrameTypeTrailer)
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
	select {
	case <-t.ctx.Done():
		return nil, t.ctx.Err()
	case frame, ok := <-t.receiveQueue:
		if !ok {
			return nil, io.EOF
		}
		message = remote.NewMessage(data, nil, t.ri, remote.Stream, t.rpcRole)
		if err = t.codec.Unmarshal(t.ctx, message, remote.NewReaderBuffer(frame)); err != nil {
			message.Recycle()
			return nil, err
		}
		return message, nil
	}
}

func (t *ttheaderStream) readUntilTargetFrame(data interface{}, targetType string) (message remote.Message, err error) {
	for {
		if message, err = t.readMessage(data); err != nil {
			// Note: if grpc-trailer is returned with a TApplicationException, it will be ignored
			return nil, err
		}
		ft := codec.MessageFrameType(message)
		switch ft {
		case codec.FrameTypeMeta:
			if err = t.parseMetaFrame(message); err != nil {
				return nil, err
			}
		case codec.FrameTypeHeader:
			if err = t.parseHeaderFrame(message); err != nil {
				return nil, err
			}
		case codec.FrameTypeData:
			if oldState, allow := t.recvState.setDataReceived(); !allow {
				return nil, fmt.Errorf("unexpected data frame, old recvState = %v", oldState)
			}
		case codec.FrameTypeTrailer:
			return message, t.parseTrailerFrame(message) // last frame of the stream
		default:
			return nil, fmt.Errorf("unexpected frame, type = %s", ft)
		}
		if ft == targetType {
			return message, nil
		}
	}
}

func (t *ttheaderStream) clientReadMetaFrame() error {
	message, err := t.readUntilTargetFrame(nil, codec.FrameTypeMeta)
	message.Recycle()
	return err
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

func (t *ttheaderStream) serverReadHeader(opt *remote.ServerOption) (nCtx context.Context, err error) {
	if t.clientHeader, err = t.readMessage(nil); err != nil {
		return nil, err
	} else if codec.MessageFrameType(t.clientHeader) != codec.FrameTypeHeader {
		return nil, ErrDirtyFrame
	}
	if old, allow := t.recvState.setHeaderReceived(); !allow {
		return nil, fmt.Errorf("unexpected header frame, old recvState = %d", old)
	}

	if nCtx, err = t.readMetaFromHeaderFrame(opt); err != nil {
		return nil, err
	}

	t.protocolInfo = t.clientHeader.ProtocolInfo()

	nCtx = opt.TracerCtl.DoStart(nCtx, t.ri)

	t.ctx = nCtx
	return nCtx, err
}

func (t *ttheaderStream) readMetaFromHeaderFrame(opt *remote.ServerOption) (nCtx context.Context, err error) {
	nCtx = t.ctx // with cancel
	if serverHeaderFrameHandler != nil {
		if nCtx, err = serverHeaderFrameHandler.ReadMeta(nCtx, t.clientHeader); err != nil {
			return nil, err
		}
	}

	// metainfo
	nCtx = metainfo.SetMetaInfoFromMap(nCtx, t.clientHeader.TransInfo().TransStrInfo())

	// grpc style metadata:
	if nCtx, err = injectGRPCMetadata(nCtx, t.clientHeader.TransInfo().TransStrInfo()); err != nil {
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
	if oldState, allow := t.recvState.setMetaReceived(); !allow {
		return fmt.Errorf("unexpected meta frame, old recvState = %d", oldState)
	}

	if t.rpcRole != remote.Client {
		return ErrInvalidFrame
	}

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
	if oldState, allow := t.recvState.setHeaderReceived(); !allow {
		return fmt.Errorf("unexpected header frame, old recvState = %d", oldState)
	}
	if t.rpcRole == remote.Client {
		if _, err = clientHeaderFrameHandler.ReadMeta(t.ctx, message); err != nil {
			return err
		}
		return t.parseGRPCMetadata(message, &t.headersRecv)
	} else {
		// server: HeaderFrame is already processed in serverReadHeader
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
	if oldState, allow := t.recvState.setTrailerReceived(); !allow {
		return fmt.Errorf("unexpected trailer frame, old recvState = %d", oldState)
	}
	return t.parseGRPCMetadata(message, &t.trailerRecv)
}

// parseGRPCMetadata parses grpc style metadata, helpful for projects migrating from kitex-grpc
func (t *ttheaderStream) parseGRPCMetadata(message remote.Message, target *metadata.MD) error {
	if metadataJSON, exists := message.TransInfo().TransStrInfo()[codec.StrKeyMetaData]; exists {
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
	message, err := t.readUntilTargetFrame(m, codec.FrameTypeData)
	if message != nil {
		defer message.Recycle()
	}
	if err == io.EOF && t.ri.Invocation().BizStatusErr() != nil {
		return nil // same behavior as grpc streaming: return nil for biz status error
	}
	return err
}

func (t *ttheaderStream) SendMsg(m interface{}) (err error) {
	if t.isSendClosed() {
		return io.ErrClosedPipe
	}
	if err = t.sendHeaderFrame(); err != nil {
		return err
	}

	message := t.newMessageToSend(m, codec.FrameTypeData)
	return t.putMessageToSendQueue(message)
}

func (t *ttheaderStream) newMessageToSend(m interface{}, frameType string) (msg remote.Message) {
	if t.rpcRole == remote.Client && frameType == codec.FrameTypeHeader {
		return t.clientHeader // prepared in client_handler.go
	}
	message := remote.NewMessage(m, nil, t.ri, remote.Stream, t.rpcRole)
	message.SetProtocolInfo(t.protocolInfo)
	message.TransInfo().TransIntInfo()[remote_transmeta.FrameType] = frameType
	return message
}

// Close implements streaming.Stream
// For client side, the stream can still be used to receive data
func (t *ttheaderStream) Close() error {
	if alreadyClosed := t.closeSend(nil); alreadyClosed {
		return nil
	}
	if t.rpcRole == remote.Server {
		t.closeRecv(ErrSendClosed) // TODO: compare with grpc streaming
		t.cancel()                 // release blocking RecvMsg call (if any)
	}
	// inform the opposite end of stream
	trailerFrame := t.newMessageToSend(nil, codec.FrameTypeTrailer)
	_ = t.putMessageToSendQueue(trailerFrame)
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
	return atomic.LoadUint32(&t.sendClosed) == closed
}

func (t *ttheaderStream) closeSend(err error) (closedBefore bool) {
	if atomic.SwapUint32(&t.sendClosed, closed) == closed {
		return true
	}
	t.lastSendError = err
	return false
}

func (t *ttheaderStream) frameReceiver() {
	var err error
	defer func() {
		if panicInfo := recover(); panicInfo != nil {
			err = fmt.Errorf("frameReceiver panic: %v, stack=%s", panicInfo, string(debug.Stack()))
			klog.CtxWarnf(t.ctx, "%s", err.Error())
		}
		if err != nil {
			t.closeRecv(err)
		}
		close(t.receiveQueue) // only one writer in this func, safe to close it here
	}()
	for {
		var frame []byte
		if frame, err = t.readFrame(); err != nil {
			if errors.Is(err, netpoll.ErrEOF) {
				err = nil
			}
			return
		}
		klog.CtxWarnf(t.ctx, "[%v@%v->%v] frameReceiver: %v", t.id, t.conn.LocalAddr(), t.conn.RemoteAddr(), frame)
		select {
		case <-t.ctx.Done():
			err = t.ctx.Err()
			return
		case t.receiveQueue <- frame:
			if isTrailerFrame(frame) {
				return
			}
		}
	}
}

func isTrailerFrame(frame []byte) bool {
	if len(frame) < codec.Size32*2 {
		return false
	}
	flags := codec.HeaderFlags(binary.BigEndian.Uint16(frame[codec.Size32+codec.Size16:]))
	return flags&codec.HeaderFlagsStreamingTrailer != 0
}

func (t *ttheaderStream) readFrame() ([]byte, error) {
	in := t.ext.NewReadByteBuffer(t.ctx, t.conn, nil)
	preface, err := in.Peek(codec.Size32 * 4)
	if err != nil {
		return nil, err
	}
	if !codec.IsTTHeaderStreaming(preface) {
		return nil, ErrInvalidFrame
	}
	size := binary.BigEndian.Uint32(preface)
	return in.Next(codec.Size32 + int(size)) // framed size + content (ttheader + payload)
}

func (t *ttheaderStream) messageSender() {
	var err error
	defer func() {
		if panicInfo := recover(); panicInfo != nil {
			err = fmt.Errorf("messageSender panic: %v, stack=%s", panicInfo, string(debug.Stack()))
			klog.CtxWarnf(t.ctx, "%v", err.Error())
		}
		if err != nil {
			t.closeSend(err)
		}
	}()
	for {
		select {
		case <-t.ctx.Done():
			err = t.ctx.Err()
			return
		case message, ok := <-t.sendQueue:
			klog.CtxWarnf(t.ctx, "[%v@%v->%v] messageSender: msg=%v, %v", t.id, t.conn.LocalAddr(), t.conn.RemoteAddr(), message.TransInfo(), message.Data())
			if !ok { // closed: no more message to send
				return
			}
			frameType := codec.MessageFrameType(message)
			if err = t.writeMessage(message); err != nil {
				return
			}
			if frameType == codec.FrameTypeTrailer {
				// trailer is the last frame in the stream, so close it (will panic if there's new frame)
				close(t.sendQueue)
				return
			}
		}
	}
}

func (t *ttheaderStream) writeMessage(message remote.Message) error {
	defer message.Recycle() // not referenced after this, including the HeaderFrame
	out := t.ext.NewWriteByteBuffer(t.ctx, t.conn, message)
	if err := t.codec.Marshal(t.ctx, message, out); err != nil {
		return err
	}
	return out.Flush()
}
