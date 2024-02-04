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

package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/kitex/internal/test"
	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
)

func Test_initWorkerPool(t *testing.T) {
	initWorkerPool()
	test.Assert(t, timeoutWorkerPool != nil)
}

func Test_fromAddress(t *testing.T) {
	t.Run("nil-ri", func(t *testing.T) {
		test.Assert(t, fromAddress(nil) == "")
	})
	t.Run("nil-from", func(t *testing.T) {
		ri := rpcinfo.NewRPCInfo(nil, nil, nil, nil, nil)
		test.Assert(t, fromAddress(ri) == "")
	})
	t.Run("nil-from-addr", func(t *testing.T) {
		from := rpcinfo.NewEndpointInfo("", "", nil, nil)
		ri := rpcinfo.NewRPCInfo(from, nil, nil, nil, nil)
		test.Assert(t, fromAddress(ri) == "")
	})
	t.Run("from-addr", func(t *testing.T) {
		addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
		from := rpcinfo.NewEndpointInfo("", "", addr, nil)
		ri := rpcinfo.NewRPCInfo(from, nil, nil, nil, nil)
		test.Assert(t, fromAddress(ri) == "127.0.0.1:8080")
	})
}

var _ context.Context = (*mockCtx)(nil)

type mockCtx struct {
	err    error
	ddl    time.Time
	hasDDL bool
	done   chan struct{}
	data   map[interface{}]interface{}
}

func (m *mockCtx) Deadline() (deadline time.Time, ok bool) {
	return m.ddl, m.hasDDL
}

func (m *mockCtx) Done() <-chan struct{} {
	return m.done
}

func (m *mockCtx) Err() error {
	return m.err
}

func (m *mockCtx) Value(key interface{}) interface{} {
	return m.data[key]
}

func Test_makeServerTimeoutErr(t *testing.T) {
	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	from := rpcinfo.NewEndpointInfo("from_service", "from_method", addr, nil)
	to := rpcinfo.NewEndpointInfo("to_service", "to_method", nil, nil)

	t.Run("canceled", func(t *testing.T) {
		// prepare
		start := time.Now()
		timeout := time.Second
		ctx := &mockCtx{err: context.Canceled}
		ri := rpcinfo.NewRPCInfo(from, to, nil, nil, nil)

		// test
		gotErr := makeServerTimeoutErr(ctx, ri, start, timeout)

		// assert
		test.Assert(t, errors.Is(gotErr, kerrors.ErrServerProcessingTimeout), gotErr)
		test.Assert(t, strings.Contains(gotErr.Error(), "context canceled by business"), gotErr)
	})

	t.Run("no_deadline", func(t *testing.T) {
		// prepare
		start := time.Now()
		timeout := time.Second
		ctx := &mockCtx{hasDDL: false}
		ri := rpcinfo.NewRPCInfo(from, to, nil, nil, nil)

		// test
		gotErr := makeServerTimeoutErr(ctx, ri, start, timeout)

		// assert
		test.Assert(t, strings.Contains(gotErr.Error(), "unknown error: context deadline not set?"), gotErr)
	})

	t.Run("timeout_by_business", func(t *testing.T) {
		// prepare
		start := time.Now()
		timeout := time.Duration(0)
		ctx := &mockCtx{ddl: start.Add(time.Second), hasDDL: true}
		ri := rpcinfo.NewRPCInfo(from, to, nil, nil, nil)

		// test
		gotErr := makeServerTimeoutErr(ctx, ri, start, timeout)

		// assert
		test.Assert(t, errors.Is(gotErr, kerrors.ErrServerProcessingTimeout), gotErr)
		test.Assert(t, strings.Contains(gotErr.Error(), "timeout by business"), gotErr)
	})

	t.Run("ddl_before_expected_timeout", func(t *testing.T) {
		// prepare
		start := time.Now()
		timeout := time.Second * 2
		ctx := &mockCtx{ddl: start.Add(time.Second), hasDDL: true}
		ri := rpcinfo.NewRPCInfo(from, to, nil, nil, nil)

		// test
		gotErr := makeServerTimeoutErr(ctx, ri, start, timeout)

		// assert
		test.Assert(t, errors.Is(gotErr, kerrors.ErrServerProcessingTimeout), gotErr)
		test.Assert(t, strings.Contains(gotErr.Error(), "context deadline earlier than timeout"), gotErr)
	})

	t.Run("ddl_set_by_kitex", func(t *testing.T) {
		// prepare
		start := time.Now()
		timeout := time.Second
		ctx := &mockCtx{ddl: start.Add(time.Second), hasDDL: true}
		ri := rpcinfo.NewRPCInfo(from, to, nil, nil, nil)

		// test
		gotErr := makeServerTimeoutErr(ctx, ri, start, timeout)

		// assert
		test.Assert(t, errors.Is(gotErr, kerrors.ErrServerProcessingTimeout), gotErr)
		test.Assert(t, strings.HasSuffix(gotErr.Error(), "kitex.serverTimeoutMW"), gotErr) // no additional info
	})
}

func Test_serverTimeoutMW(t *testing.T) {
	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	from := rpcinfo.NewEndpointInfo("from_service", "from_method", addr, nil)
	to := rpcinfo.NewEndpointInfo("to_service", "to_method", nil, nil)
	getTimeoutMW := func(lastLog *atomic.Value) endpoint.Middleware {
		var logger loggerFunc = func(ctx context.Context, format string, v ...interface{}) {
			if lastLog != nil {
				lastLog.Store(fmt.Sprintf(format, v...))
			}
		}
		return serverTimeoutMW(WithTimeoutLoggerFunc(context.Background(), logger))
	}
	atomicStringContains := func(s *atomic.Value, sub string) bool {
		if s == nil {
			return false
		}
		return strings.Contains(s.Load().(string), sub)
	}

	t.Run("no_timeout(fastPath)", func(t *testing.T) {
		// prepare
		cfg := rpcinfo.NewRPCConfig()
		_ = rpcinfo.AsMutableRPCConfig(cfg).SetRPCTimeout(0)
		ri := rpcinfo.NewRPCInfo(from, to, nil, cfg, nil)
		ctx := rpcinfo.NewCtxWithRPCInfo(context.Background(), ri)
		timeoutMW := getTimeoutMW(nil)

		var panicInfo interface{}
		// test
		defer func() {
			// fastPath: panic will not be recovered
			panicInfo = recover()
		}()
		_ = timeoutMW(func(ctx context.Context, req, resp interface{}) (err error) {
			panic("fastPath")
		})(ctx, nil, nil)

		// assert
		test.Assert(t, panicInfo.(string) == "fastPath", panicInfo)
	})

	t.Run("finish_before_timeout_without_error", func(t *testing.T) {
		// prepare
		cfg := rpcinfo.NewRPCConfig()
		_ = rpcinfo.AsMutableRPCConfig(cfg).SetRPCTimeout(time.Second)
		ri := rpcinfo.NewRPCInfo(from, to, nil, cfg, nil)
		ctx := rpcinfo.NewCtxWithRPCInfo(context.Background(), ri)
		timeoutMW := getTimeoutMW(nil)

		// test
		err := timeoutMW(func(ctx context.Context, req, resp interface{}) (err error) {
			return nil
		})(ctx, nil, nil)

		// assert
		test.Assert(t, err == nil, err)
	})

	t.Run("finish_before_timeout_with_error", func(t *testing.T) {
		// prepare
		cfg := rpcinfo.NewRPCConfig()
		_ = rpcinfo.AsMutableRPCConfig(cfg).SetRPCTimeout(time.Second)
		ri := rpcinfo.NewRPCInfo(from, to, nil, cfg, nil)
		ctx := rpcinfo.NewCtxWithRPCInfo(context.Background(), ri)
		timeoutMW := getTimeoutMW(nil)

		// test
		err := timeoutMW(func(ctx context.Context, req, resp interface{}) (err error) {
			return errors.New("error")
		})(ctx, nil, nil)

		// assert
		test.Assert(t, err.Error() == "error", err)
	})

	t.Run("finish_after_timeout_without_error", func(t *testing.T) {
		// prepare
		cfg := rpcinfo.NewRPCConfig()
		_ = rpcinfo.AsMutableRPCConfig(cfg).SetRPCTimeout(time.Millisecond)
		ri := rpcinfo.NewRPCInfo(from, to, nil, cfg, nil)
		ctx := rpcinfo.NewCtxWithRPCInfo(context.Background(), ri)
		var lastLog atomic.Value
		timeoutMW := getTimeoutMW(&lastLog)

		// test
		err := timeoutMW(func(ctx context.Context, req, resp interface{}) (err error) {
			time.Sleep(time.Millisecond * 10)
			return nil
		})(ctx, nil, nil)

		// assert
		test.Assert(t, errors.Is(err, kerrors.ErrServerProcessingTimeout), err)
		test.Assert(t, lastLog.Load() == nil, lastLog)
	})

	t.Run("finish_after_timeout_with_error", func(t *testing.T) {
		// prepare
		cfg := rpcinfo.NewRPCConfig()
		_ = rpcinfo.AsMutableRPCConfig(cfg).SetRPCTimeout(time.Millisecond)
		ri := rpcinfo.NewRPCInfo(from, to, nil, cfg, nil)
		ctx := rpcinfo.NewCtxWithRPCInfo(context.Background(), ri)
		var lastLog atomic.Value
		timeoutMW := getTimeoutMW(&lastLog)

		// test
		err := timeoutMW(func(ctx context.Context, req, resp interface{}) (err error) {
			time.Sleep(time.Millisecond * 10)
			return errors.New("error")
		})(ctx, nil, nil)

		// assert
		test.Assert(t, errors.Is(err, kerrors.ErrServerProcessingTimeout), err)
		time.Sleep(time.Millisecond * 20)
		test.Assert(t, atomicStringContains(&lastLog, "error"), lastLog)
	})

	t.Run("panic_before_timeout", func(t *testing.T) {
		// prepare
		cfg := rpcinfo.NewRPCConfig()
		_ = rpcinfo.AsMutableRPCConfig(cfg).SetRPCTimeout(time.Second)
		stats := rpcinfo.NewRPCStats()
		ri := rpcinfo.NewRPCInfo(from, to, nil, cfg, stats)
		ctx := rpcinfo.NewCtxWithRPCInfo(context.Background(), ri)
		var lastLog atomic.Value
		timeoutMW := getTimeoutMW(&lastLog)

		// test
		err := timeoutMW(func(ctx context.Context, req, resp interface{}) (err error) {
			panic("panic")
		})(ctx, nil, nil)

		// assert
		test.Assert(t, !errors.Is(err, kerrors.ErrServerProcessingTimeout), err)
		test.Assert(t, strings.Contains(err.Error(), "KITEX: server panic"), err)
		test.Assert(t, atomicStringContains(&lastLog, "KITEX: server panic"), lastLog)
	})

	t.Run("panic_after_timeout", func(t *testing.T) {
		// prepare
		cfg := rpcinfo.NewRPCConfig()
		_ = rpcinfo.AsMutableRPCConfig(cfg).SetRPCTimeout(time.Millisecond)
		stats := rpcinfo.NewRPCStats()
		ri := rpcinfo.NewRPCInfo(from, to, nil, cfg, stats)
		ctx := rpcinfo.NewCtxWithRPCInfo(context.Background(), ri)
		var lastLog atomic.Value
		timeoutMW := getTimeoutMW(&lastLog)

		// test
		err := timeoutMW(func(ctx context.Context, req, resp interface{}) (err error) {
			time.Sleep(time.Millisecond * 10)
			panic("panic")
		})(ctx, nil, nil)

		// assert
		test.Assert(t, errors.Is(err, kerrors.ErrServerProcessingTimeout), err)
		test.Assert(t, strings.HasSuffix(err.Error(), "serverTimeoutMW"), err) // timeout before panic
		time.Sleep(time.Millisecond * 20)
		test.Assert(t, atomicStringContains(&lastLog, "KITEX: server panic"), lastLog)
	})
}
