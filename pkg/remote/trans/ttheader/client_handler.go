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
	"net"

	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/streaming"
)

type ttheaderClientTransHandlerFactory struct{}

// NewCliTransHandlerFactory ...
func NewCliTransHandlerFactory() remote.ClientTransHandlerFactory {
	return &ttheaderClientTransHandlerFactory{}
}

// NewTransHandler implements the remote.ClientTransHandlerFactory interface.
func (t ttheaderClientTransHandlerFactory) NewTransHandler(opt *remote.ClientOption) (remote.ClientTransHandler, error) {
	return newCliTransHandler(opt)
}

func newCliTransHandler(opt *remote.ClientOption) (remote.ClientTransHandler, error) {
	return &cliTransHandler{
		opt: opt,
		codec: &ttheaderStreamCodec{
			payloadCodec: opt.PayloadCodec,
		},
	}, nil
}

var _ remote.TransReadWriter = (*cliTransHandler)(nil)
var _ remote.StreamAllocator = (*cliTransHandler)(nil)

type cliTransHandler struct {
	opt   *remote.ClientOption
	codec *ttheaderStreamCodec
}

func (c *cliTransHandler) NewStream(
	ctx context.Context, svcInfo *serviceinfo.ServiceInfo, conn net.Conn, handler remote.TransReadWriter,
) (streaming.Stream, error) {
	msg := remote.NewMessage(nil, svcInfo, nil, remote.Stream, remote.Client)
	return newTTHeaderStream(ctx, conn, msg, c.codec), nil
}

// Write implements the remote.ClientTransHandler interface.
func (c *cliTransHandler) Write(ctx context.Context, conn net.Conn, send remote.Message) (nctx context.Context, err error) {
	panic("not used in TTHeader Streaming")
}

// Read implements the remote.ClientTransHandler interface.
func (c *cliTransHandler) Read(ctx context.Context, conn net.Conn, msg remote.Message) (nctx context.Context, err error) {
	panic("not used in TTHeader Streaming")
}

// OnInactive implements the remote.ClientTransHandler interface.
func (c *cliTransHandler) OnInactive(ctx context.Context, conn net.Conn) {
	panic("not used in TTHeader Streaming")
}

// OnError implements the remote.ClientTransHandler interface.
func (c *cliTransHandler) OnError(ctx context.Context, err error, conn net.Conn) {}

// SetPipeline implements the remote.ClientTransHandler interface.
func (c *cliTransHandler) SetPipeline(pipeline *remote.TransPipeline) {}

// OnMessage implements the remote.ClientTransHandler interface.
func (c *cliTransHandler) OnMessage(ctx context.Context, args, result remote.Message) (context.Context, error) {
	// do nothing
	return ctx, nil
}
