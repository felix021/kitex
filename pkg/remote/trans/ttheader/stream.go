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
	"errors"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"sync/atomic"

	"github.com/bytedance/gopkg/cloud/metainfo"

	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/streaming"
	"github.com/cloudwego/kitex/pkg/streaming/metadata"
)

const (
	KeyCode            = "_code"
	KeyMessage         = "_message"
	KeyBizErrorCode    = "biz-status"
	KeyBizErrorMessage = "biz-message"
	KeyBizErrorExtra   = "biz-extra"
)

var (
	_ streaming.Stream = (*ttheaderStream)(nil)
	_ streaming.Stream = (*streamWithMW)(nil)
)

// newStreamWithMW creates a new Stream with given recvEndpoint and sendEndpoint
func newStreamWithMW(st *ttheaderStream, recv endpoint.RecvEndpoint, send endpoint.SendEndpoint) streaming.Stream {
	return &streamWithMW{
		Stream:       st,
		recvEndpoint: recv,
		sendEndpoint: send,
	}
}

type streamWithMW struct {
	streaming.Stream
	recvEndpoint endpoint.RecvEndpoint
	sendEndpoint endpoint.SendEndpoint
}

func (t *streamWithMW) RecvMsg(m interface{}) error {
	return t.recvEndpoint(t, m)
}

func (t *streamWithMW) SendMsg(m interface{}) error {
	return t.sendEndpoint(t, m)
}

func newTTHeaderStream(ctx context.Context, conn net.Conn, msg remote.Message, codec *ttheaderStreamCodec) *ttheaderStream {
	return &ttheaderStream{
		ctx:     ctx,
		conn:    conn,
		codec:   codec,
		message: msg,
		headers: make(map[string]string),
	}
}

type ttheaderStream struct {
	conn  net.Conn
	ctx   context.Context
	codec *ttheaderStreamCodec

	message remote.Message

	// headers to be sent
	headers    map[string]string
	headerSent uint32

	trailer metadata.MD

	recvClosed uint32
	sendClosed uint32
}

// SetHeader implements the streaming.Stream interface.
func (t *ttheaderStream) SetHeader(md metadata.MD) error {
	if atomic.LoadUint32(&t.headerSent) == 1 {
		return errors.New("header already sent")
	}
	for k, v := range md {
		if len(v) > 1 {
			klog.Warnf("ttheader streaming: SetHeader got multiple values for key %v", k)
			continue
		}
		t.headers[k] = v[0]
	}
	return nil
}

// SendHeader implements the streaming.Stream interface.
func (t *ttheaderStream) SendHeader(md metadata.MD) error {
	if atomic.LoadUint32(&t.headerSent) == 1 {
		return errors.New("header already sent")
	}
	for k, v := range md {
		if len(v) > 1 {
			klog.Warnf("ttheader streaming: SendHeader got multiple values for key %v", k)
			continue
		}
		t.headers[k] = v[0]
	}
	atomic.StoreUint32(&t.headerSent, 1)
	return nil
}

// SetTrailer implements the streaming.Stream interface.
func (t *ttheaderStream) SetTrailer(md metadata.MD) {
	for k, v := range md {
		if len(v) > 1 {
			klog.Warnf("ttheader streaming: SetTrailer got multiple values for key %v", k)
			continue
		}
	}
	t.trailer = md.Copy()
}

// Header implements the streaming.Stream interface.
func (t *ttheaderStream) Header() (metadata.MD, error) {
	headers := t.message.TransInfo().TransStrInfo()
	md := make(metadata.MD, len(headers))
	for k, v := range headers {
		md[k] = []string{v}
	}
	return md, nil
}

// Trailer implements the streaming.Stream interface.
func (t *ttheaderStream) Trailer() metadata.MD {
	return t.trailer.Copy()
}

// Context implements the streaming.Stream interface.
func (t *ttheaderStream) Context() context.Context {
	return t.ctx
}

// RecvMsg implements the streaming.Stream interface.
func (t *ttheaderStream) RecvMsg(m interface{}) error {
	if atomic.LoadUint32(&t.recvClosed) == 1 {
		return io.EOF
	}
	finish := make(chan error)
	var f *frame
	go func() {
		// TODO: a new goroutine for each Recv call is expensive.
		// Maybe we should start a goroutine for each stream for receiving
		defer func() {
			if r := recover(); r != nil {
				panicInfo := fmt.Sprintf("readFrame panic: %v, stack=%s", r, string(debug.Stack()))
				finish <- kerrors.ErrPanic.WithCause(errors.New(panicInfo))
			}
		}()
		var readErr error
		f, readErr = readFrame(t.conn)
		if f.IsEndOfStream() {
			atomic.StoreUint32(&t.recvClosed, 1)
		}
		finish <- readErr
	}()
	select {
	case <-t.ctx.Done():
		return t.ctx.Err()
	case readErr := <-finish:
		if readErr != nil {
			return readErr
		}
		payloadBuf, err := f.Payload().Bytes()
		if err != nil {
			return err
		}
		return t.codec.decode(t.ctx, payloadBuf, m)
	}
}

// SendMsg implements the streaming.Stream interface.
func (t *ttheaderStream) SendMsg(m interface{}) error {
	if atomic.LoadUint32(&t.sendClosed) == 1 {
		return errors.New("stream closed")
	}
	buf, err := t.codec.encode(t.ctx, m)
	if err != nil {
		return err
	}
	f := &frame{payload: newPayload(buf)}
	if atomic.SwapUint32(&t.headerSent, 1) == 0 {
		f.MetaData = t.headers

		if t.message.RPCRole() == remote.Client {
			// transient and persistent values
			metainfo.RangeValues(t.ctx, func(k, v string) bool {
				f.MetaData[k] = v
				return true
			})
			metainfo.RangePersistentValues(t.ctx, func(k, v string) bool {
				f.MetaData[k] = v
				return true
			})
		} else {
			for k, v := range t.headers {
				f.MetaData[k] = v
			}
		}
	}
	return writeFrame(t.ctx, t.conn, f)
}

func (t *ttheaderStream) Close() error {
	if atomic.SwapUint32(&t.sendClosed, 1) == 1 {
		// already closed before
		return nil
	}

	if t.message.RPCRole() == remote.Client {
		f := newEndOfStreamFrame(t.ctx, 0, "", nil)
		return writeFrame(t.ctx, t.conn, f)
	}

	return nil
}
