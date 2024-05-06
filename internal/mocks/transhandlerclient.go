/*
 * Copyright 2021 CloudWeGo Authors
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

package mocks

import (
	"context"
	"net"

	mocksremote "github.com/cloudwego/kitex/internal/mocks/remote"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/remote"
	kitex "github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/streaming"
)

var (
	_                    remote.ClientStreamAllocator           = (*MockCliTransHandler)(nil)
	_                    remote.ClientTransHandlerFactory       = (*mockCliTransHandlerFactory)(nil)
	_                    remote.ClientStreamTransHandlerFactory = (*mockCliTransHandlerFactory)(nil)
	DefaultNewStreamFunc func(
		ctx context.Context, svcInfo *kitex.ServiceInfo, conn net.Conn, handler remote.TransReadWriter,
	) streaming.Stream = nil

	DefaultNewStreamTransHandlerFunc func(opt *remote.ClientOption) (remote.ClientTransHandler, error) = nil
)

type mockCliTransHandlerFactory struct {
	hdlr                      *mocksremote.MockClientTransHandler
	NewStreamTransHandlerFunc func(opt *remote.ClientOption) (remote.ClientTransHandler, error)
}

func (f *mockCliTransHandlerFactory) NewStreamTransHandler(opt *remote.ClientOption) (remote.ClientTransHandler, error) {
	if f.NewStreamTransHandlerFunc != nil {
		return f.NewStreamTransHandlerFunc(opt)
	}
	if DefaultNewStreamTransHandlerFunc != nil {
		return DefaultNewStreamTransHandlerFunc(opt)
	}
	panic("NewStreamTransHandlerFunc is not available in mocks.mockCliTransHandlerFactory")
}

// NewMockCliTransHandlerFactory .
func NewMockCliTransHandlerFactory(handler *mocksremote.MockClientTransHandler) remote.ClientTransHandlerFactory {
	return &mockCliTransHandlerFactory{
		hdlr: handler,
	}
}

func (f *mockCliTransHandlerFactory) NewTransHandler(opt *remote.ClientOption) (remote.ClientTransHandler, error) {
	return f.hdlr, nil
}

// MockCliTransHandler .
type MockCliTransHandler struct {
	transPipe *remote.TransPipeline

	WriteFunc func(ctx context.Context, conn net.Conn, send remote.Message) (nctx context.Context, err error)

	ReadFunc func(ctx context.Context, conn net.Conn, msg remote.Message) (nctx context.Context, err error)

	OnMessageFunc func(ctx context.Context, args, result remote.Message) (context.Context, error)

	NewStreamFunc func(ctx context.Context, svcInfo *kitex.ServiceInfo, conn net.Conn, handler remote.TransReadWriter) (streaming.Stream, error)
}

func (t *MockCliTransHandler) NewStream(ctx context.Context, svcInfo *kitex.ServiceInfo, conn net.Conn, handler remote.TransReadWriter) (streaming.Stream, error) {
	if t.NewStreamFunc != nil {
		return t.NewStreamFunc(ctx, svcInfo, conn, handler)
	}
	if DefaultNewStreamFunc != nil {
		return DefaultNewStreamFunc(ctx, svcInfo, conn, handler), nil
	}
	panic("NewStreamFunc is not available in mocks.MockCliTransHandler")
}

// Write implements the remote.TransHandler interface.
func (t *MockCliTransHandler) Write(ctx context.Context, conn net.Conn, send remote.Message) (nctx context.Context, err error) {
	if t.WriteFunc != nil {
		return t.WriteFunc(ctx, conn, send)
	}
	return ctx, nil
}

// Read implements the remote.TransHandler interface.
func (t *MockCliTransHandler) Read(ctx context.Context, conn net.Conn, msg remote.Message) (nctx context.Context, err error) {
	if t.ReadFunc != nil {
		return t.ReadFunc(ctx, conn, msg)
	}
	return ctx, nil
}

// OnMessage implements the remote.TransHandler interface.
func (t *MockCliTransHandler) OnMessage(ctx context.Context, args, result remote.Message) (context.Context, error) {
	if t.OnMessageFunc != nil {
		return t.OnMessageFunc(ctx, args, result)
	}
	return ctx, nil
}

// OnActive implements the remote.TransHandler interface.
func (t *MockCliTransHandler) OnActive(ctx context.Context, conn net.Conn) (context.Context, error) {
	// ineffective now and do nothing
	return ctx, nil
}

// OnInactive implements the remote.TransHandler interface.
func (t *MockCliTransHandler) OnInactive(ctx context.Context, conn net.Conn) {
	// ineffective now and do nothing
}

// OnError implements the remote.TransHandler interface.
func (t *MockCliTransHandler) OnError(ctx context.Context, err error, conn net.Conn) {
	if pe, ok := err.(*kerrors.DetailedError); ok {
		klog.CtxErrorf(ctx, "KITEX: send request error, remote=%s, error=%s\nstack=%s", conn.RemoteAddr(), err.Error(), pe.Stack())
	} else {
		klog.CtxErrorf(ctx, "KITEX: send request error, remote=%s, error=%s", conn.RemoteAddr(), err.Error())
	}
}

// SetPipeline implements the remote.TransHandler interface.
func (t *MockCliTransHandler) SetPipeline(p *remote.TransPipeline) {
	t.transPipe = p
}
