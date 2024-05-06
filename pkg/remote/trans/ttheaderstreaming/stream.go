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

	"github.com/bytedance/sonic"
	"github.com/cloudwego/kitex/pkg/gofunc"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/codec"
	"github.com/cloudwego/kitex/pkg/remote/codec/ttheader"
	"github.com/cloudwego/kitex/pkg/remote/trans"
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
	ErrIllegalHeaderWrite = errors.New("ttheader: the stream is done or header was already sent")
	ErrSendClosed         = errors.New("send closed")

	metaFrameReader MetaFrameReader = nil
)

type MetaFrameReader func(ctx context.Context, ri rpcinfo.RPCInfo, h *ttheader.Header) error

// SetMetaFrameReader sets metaFrameReader for client
func SetMetaFrameReader(r MetaFrameReader) {
	metaFrameReader = r
}

func newTTHeaderStream(
	ctx context.Context, conn net.Conn, ri rpcinfo.RPCInfo, codec remote.PayloadCodec,
	header *ttheader.Header, role remote.RPCRole, ext trans.Extension,
) *ttheaderStream {
	ctx, cancel := context.WithCancel(ctx)
	st := &ttheaderStream{
		ctx:      ctx,
		cancel:   cancel,
		conn:     conn,
		ri:       ri,
		codec:    codec,
		ttheader: header, // ttheader from/for header frame
		rpcRole:  role,
		ext:      ext,
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

	ttheader *ttheader.Header

	// grpc style metadata for compatibility: send (for server)
	metadataMutex sync.Mutex
	headersToSend metadata.MD
	trailerToSend metadata.MD
	headerSent    uint32

	// grpc style metadata for compatibility: recv (for client)
	headersRecv     metadata.MD
	headerReceived  uint32
	trailerRecv     metadata.MD
	trailerReceived uint32

	lastRecvError error

	recvClosed uint32
	sendClosed uint32

	state   uint32
	rpcRole remote.RPCRole
}

func (t *ttheaderStream) setContext(ctx context.Context) {
	t.ctx = ctx
}

// SetHeader implements streaming.Stream
// If the header was already sent or the stream is done, ErrIllegalHeaderWrite is returned
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

func (t *ttheaderStream) replaceHeader(md metadata.MD) {
	t.headersToSend = md
}

// SendHeader implements streaming.Stream
func (t *ttheaderStream) SendHeader(md metadata.MD) (err error) {
	if t.rpcRole != remote.Server {
		panic("this method should only be used in server side stream!")
	}
	if err = t.setHeader(md); err != nil {
		return err
	}
	return t.sendHeader()
}

func (t *ttheaderStream) sendHeader() (err error) {
	if atomic.SwapUint32(&t.headerSent, sent) == sent {
		return nil
	}
	metadataJSON, err := t.getMetaDataAsJSON(&t.headersToSend)
	if err != nil {
		return err
	}
	return t.sendFrameWithMetaData(ttheader.FrameTypeHeader, metadataJSON, nil)
}

func (t *ttheaderStream) sendTrailer(invokeErr error) error {
	if t.isSendClosed() {
		return nil
	}
	metadataJSON, err := t.getMetaDataAsJSON(&t.trailerToSend)
	if err != nil {
		return err
	}
	return t.sendFrameWithMetaData(ttheader.FrameTypeTrailer, metadataJSON, invokeErr)
}

// sendFrameWithMetaData sends header or trailer frame
// header frame includes only the grpc style metadata header (in strInfo key "metadata")
// trailer frame includes:
// - the grpc style metadata header (in strInfo key "metadata")
// - BizStatusError (in strInfo keys "biz-status", "biz-message" and "biz-extra")
// - (data) the invokeErr returned by method handler (in payload, serialized as a TApplicationException)
func (t *ttheaderStream) sendFrameWithMetaData(frameType string, metadata []byte, data interface{}) error {
	message := t.newMessage(data, frameType)
	defer message.Recycle()
	if len(metadata) > 0 {
		message.TransInfo().TransStrInfo()[ttheader.StrKeyMetaData] = string(metadata)
	}
	if frameType == ttheader.FrameTypeTrailer {
		transmeta.InjectBizStatusError(message.TransInfo().TransStrInfo(), t.ri.Invocation().BizStatusErr()) // TODO
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
	defer t.metadataMutex.Unlock() // in case of panic, make sure to unlock metadataMutex
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
		if _, err := t.readUntilTargetFrame(ttheader.FrameTypeHeader); err != nil {
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
		_, _ = t.readUntilTargetFrame(ttheader.FrameTypeTrailer)
	}
	return t.trailerRecv
}

func (t *ttheaderStream) Context() context.Context {
	return t.ctx
}

func (t *ttheaderStream) readFrame(f *ttheader.Frame) (err error) {
	defer func() {
		if panicInfo := recover(); panicInfo != nil {
			err = fmt.Errorf("readFrame panic: %v, stack=%s", panicInfo, string(debug.Stack()))
		}
		if err != nil {
			t.closeRecv(err)
		}
	}()
	if t.isRecvClosed() {
		return t.lastRecvError
	}
	return f.Read(t.conn)
}

func (t *ttheaderStream) readUntilTargetFrame(targetType string) (f *ttheader.Frame, err error) {
	for {
		f = &ttheader.Frame{}
		if err = t.readFrame(f); err != nil {
			return nil, err
		}
		if t.rpcRole == remote.Client && f.Header().SeqNum() != uint32(t.ri.Invocation().SeqID()) {
			// ignore possible dirty frames from previous stream on the same tcp connection
			continue
		}
		ft := frameType(f)
		switch ft {
		case ttheader.FrameTypeMeta:
			if err = t.parseMetaFrame(f); err != nil {
				return nil, err
			}
		case ttheader.FrameTypeHeader:
			if err = t.parseHeaderFrame(f); err != nil {
				return nil, err
			}
		case ttheader.FrameTypeData:
		case ttheader.FrameTypeTrailer:
			return f, t.parseTrailerFrame(f) // last frame of the stream
		default:
			return nil, fmt.Errorf("unexpected frame, type = %s", ft)
		}
		if ft == targetType {
			return f, nil
		}
	}
}

// skipFrameUntil discards frames until the given condition is met
func (t *ttheaderStream) skipFrameUntil(f *ttheader.Frame, expected func(f *ttheader.Frame) bool) (err error) {
	for {
		if err = t.readFrame(f); err != nil {
			return err
		}
		if expected(f) {
			return nil
		}
	}
}

// skipUntilTargetFrame discards frames until it reaches the target frame
// It's used to skip frames belong to the previous stream
func (t *ttheaderStream) skipUntilTargetFrame(f *ttheader.Frame, targetType string) (err error) {
	return t.skipFrameUntil(f, func(f *ttheader.Frame) bool {
		return frameType(f) == targetType
	})
}

func (t *ttheaderStream) parseMetaFrame(f *ttheader.Frame) (err error) {
	defer func() {
		if err != nil {
			t.closeRecv(err)
		}
	}()
	return t.updateRPCInfoByMetaFrame(t.ctx, t.ri, f)
}

func (t *ttheaderStream) parseHeaderFrame(f *ttheader.Frame) (err error) {
	defer func() {
		if err != nil {
			t.closeRecv(err)
		}
	}()
	if t.rpcRole == remote.Client {
		return t.parseGRPCMetadata(f, &t.headersRecv)
	} else {
		// header from client is saved to ctx
		return nil
	}
}

func (t *ttheaderStream) parseTrailerFrame(f *ttheader.Frame) (err error) {
	defer func() {
		if err == nil {
			err = io.EOF // always return io.EOF when there's no error, since trailer is the last frame
		}
		t.closeRecv(err)
	}()

	if err = t.parseGRPCMetadata(f, &t.trailerRecv); err != nil {
		return err
	}

	if t.rpcRole == remote.Client {
		// TODO: metadata backwardValue support ?

		// payload: TApplicationException, if not empty.
		var transErr *remote.TransError
		if transErr, err = f.PayloadAsTransError(); err != nil {
			return err // parse TApplicationException failed
		} else if transErr != nil {
			return transErr
		}

		// no transErr: check for BizStatusError and set to rpcinfo.Invocation if any
		return transmeta.ParseBizStatusErrorToRPCInfo(f.Header().StrInfo(), t.ri.Invocation())
	} else {
		return nil
	}
}

// parseGRPCMetadata parses grpc style metadata, helpful for projects migrating from kitex-grpc
func (t *ttheaderStream) parseGRPCMetadata(f *ttheader.Frame, target *metadata.MD) error {
	if metadataJSON, exists := f.Header().GetStrKey(ttheader.StrKeyMetaData); exists {
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

func (t *ttheaderStream) RecvMsg(m interface{}) (err error) {
	if t.isRecvClosed() {
		return t.lastRecvError
	}

	var f *ttheader.Frame
	frameReceived := make(chan struct{})

	gofunc.GoFunc(t.ctx, func() {
		f, err = t.readUntilTargetFrame(ttheader.FrameTypeData)
		frameReceived <- struct{}{}
	})

	select {
	case <-t.ctx.Done():
		err = t.ctx.Err()
		t.closeRecv(err)
		return err
	case <-frameReceived:
		if err != nil {
			if err == io.EOF && t.ri.Invocation().BizStatusErr() != nil {
				return nil // same behavior as grpc streaming: return nil for biz status error
			}
			return err
		}
		message := t.newMessage(m, ttheader.FrameTypeMeta)
		defer message.Recycle()
		return t.codec.Unmarshal(t.ctx, message, ttheader.NewBytesBufferForRead(f.Payload()))
	}
}

func (t *ttheaderStream) SendMsg(m interface{}) (err error) {
	if t.isSendClosed() {
		return io.ErrClosedPipe
	}
	if err = t.sendHeader(); err != nil {
		return err
	}

	message := t.newMessage(m, ttheader.FrameTypeData)
	defer message.Recycle()
	return t.sendMessage(message)
}

func (t *ttheaderStream) newMessage(m interface{}, frameType string) remote.Message {
	message := remote.NewMessage(m, nil, t.ri, remote.Stream, t.rpcRole)
	message.SetProtocolInfo(remote.ProtocolInfo{
		CodecType: codec.ProtocolIDToPayloadCodec(codec.ProtocolID(t.ttheader.ProtocolID())),
	})
	if t.rpcRole == remote.Client && frameType == ttheader.FrameTypeHeader {
		message.TransInfo().PutTransIntInfo(t.ttheader.IntInfo())
		message.TransInfo().PutTransStrInfo(t.ttheader.StrInfo())
	}
	message.TransInfo().TransIntInfo()[ttheader.IntKeyFrameType] = frameType
	return message
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
	} else {
		// inform the server to end reading
		clientTrailer := t.newMessage(nil, ttheader.FrameTypeTrailer)
		defer clientTrailer.Recycle()
		_ = t.sendMessage(clientTrailer)
	}
	return nil
}

func (t *ttheaderStream) isHeaderSent() bool {
	return atomic.LoadUint32(&t.headerSent) == sent
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

func (t *ttheaderStream) updateRPCInfoByMetaFrame(ctx context.Context, ri rpcinfo.RPCInfo, f *ttheader.Frame) (err error) {
	if metaFrameReader == nil {
		return nil
	}
	// read from meta frame into rpcinfo
	return metaFrameReader(ctx, ri, f.Header())
}
