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

package remotecli

import (
	"context"
	"net"
	"testing"

	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2"
	kitex "github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/streaming"
	"github.com/golang/mock/gomock"

	"github.com/cloudwego/kitex/internal/mocks"
	mock_remote "github.com/cloudwego/kitex/internal/mocks/remote"
	"github.com/cloudwego/kitex/internal/test"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/transmeta"
	"github.com/cloudwego/kitex/pkg/utils"
)

func TestNewStream(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	addr := utils.NewNetAddr("tcp", "to")
	ri := newMockRPCInfo(addr)
	ctx := rpcinfo.NewCtxWithRPCInfo(context.Background(), ri)

	hdlr := &mocks.MockCliTransHandler{}
	hdlr.NewStreamFunc = func(ctx context.Context, svcInfo *kitex.ServiceInfo, conn net.Conn, handler remote.TransReadWriter) (streaming.Stream, error) {
		return nphttp2.NewStream(ctx, svcInfo, conn, handler), nil
	}

	opt := newMockOption(ctrl, addr)
	opt.Option = remote.Option{
		StreamingMetaHandlers: []remote.StreamingMetaHandler{
			transmeta.ClientHTTP2Handler,
		},
	}

	_, _, err := NewStream(ctx, ri, hdlr, opt)
	test.Assert(t, err == nil, err)
}

func TestStreamConnManagerReleaseConn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cr := mock_remote.NewMockConnReleaser(ctrl)
	cr.EXPECT().ReleaseConn(gomock.Any(), gomock.Any()).Times(1)

	scr := NewStreamConnManager(cr)
	test.Assert(t, scr != nil)
	test.Assert(t, scr.ConnReleaser == cr)
	scr.ReleaseConn(nil, nil)
}
