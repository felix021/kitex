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
	"sync"
	"time"

	"github.com/cloudwego/kitex/internal/wpool"
	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
)

const maxIdleForTimeoutWorkerPool = 128

var (
	// timeoutWorkerPool is used to reduce the timeout goroutine overhead.
	// if timeout middleware is not enabled, it will not be initialized, so do not use it somewhere else.
	timeoutWorkerPool         *wpool.Pool
	onceInitTimeoutWorkerPool sync.Once

	timeoutLoggerFuncKey = loggerFuncCtxKey{}
)

func initWorkerPool() {
	onceInitTimeoutWorkerPool.Do(func() {
		timeoutWorkerPool = wpool.New(
			maxIdleForTimeoutWorkerPool,
			time.Minute,
		)
	})
}

type (
	loggerFunc       func(ctx context.Context, format string, v ...interface{})
	loggerFuncCtxKey struct{}
)

// WithTimeoutLoggerFunc returns a new context with the provided logger function.
func WithTimeoutLoggerFunc(ctx context.Context, f loggerFunc) context.Context {
	return context.WithValue(ctx, timeoutLoggerFuncKey, f)
}

// getLoggerFunc returns the logger function from the context.
func getLoggerFunc(ctx context.Context) loggerFunc {
	if logger, ok := ctx.Value(timeoutLoggerFuncKey).(loggerFunc); ok {
		return logger
	}
	return klog.CtxErrorf
}

func fromAddress(ri rpcinfo.RPCInfo) string {
	if ri == nil {
		return ""
	}
	source := ri.From()
	if source == nil {
		return ""
	}
	addr := source.Address()
	if addr == nil {
		return ""
	}
	return addr.String()
}

func makeServerTimeoutErr(ctx context.Context, ri rpcinfo.RPCInfo, start time.Time, timeout time.Duration) error {
	errMsg := fmt.Sprintf("timeout_config=%v, method=%s, from_service=%s, from_addr=%s, location=%s",
		timeout, ri.To().Method(), ri.From().ServiceName(), fromAddress(ri), "kitex.serverTimeoutMW")

	// cancel error
	if ctx.Err() == context.Canceled {
		return kerrors.ErrServerProcessingTimeout.WithCause(fmt.Errorf("%s, %w by business, actual=%v",
			errMsg, ctx.Err(), time.Since(start)))
	}

	if ddl, ok := ctx.Deadline(); !ok {
		errMsg = fmt.Sprintf("%s, %s", errMsg, "unknown error: context deadline not set?")
	} else {
		// Go's timer implementation is not so accurate,
		// so if we need to check ctx deadline earlier than our timeout, we should consider the accuracy
		if timeout <= 0 {
			errMsg = fmt.Sprintf("%s, timeout by business, actual=%s", errMsg, ddl.Sub(start))
		} else {
			expectedTimeout := timeout - time.Millisecond
			if expectedTimeout >= 0 && ddl.Before(start.Add(expectedTimeout)) {
				errMsg = fmt.Sprintf("%s, context deadline earlier than timeout, actual=%v", errMsg, ddl.Sub(start))
			}
		}
	}
	return kerrors.ErrServerProcessingTimeout.WithCause(errors.New(errMsg))
}

func serverTimeoutMW(initCtx context.Context) endpoint.Middleware {
	initWorkerPool()                 // called only when the middleware is enabled
	logger := getLoggerFunc(initCtx) // useful for testing

	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request, response interface{}) error {
			// Regardless of the underlying protocol, only by checking the RPCTimeout
			// For TTHeader, it will be set by transmeta.ServerTTHeaderHandler (not added by default though)
			// For GRPC/HTTP2, the timeout deadline is already set in the context, so no need to check it
			ri := rpcinfo.GetRPCInfo(ctx)
			timeout := ri.Config().RPCTimeout()
			if timeout <= 0 {
				// fastPath for no RPCTimeout
				return next(ctx, request, response)
			}
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()

			startTime := time.Now()
			chanResult := make(chan error, 1) // non-blocking to avoid goroutine leak
			timeoutWorkerPool.GoCtx(ctx, func() {
				var err error
				defer func() {
					if panicInfo := recover(); panicInfo != nil {
						panicErr := rpcinfo.ServerPanicToErr(ctx, panicInfo, ri, false)
						logger(ctx, "%s", panicErr)
						chanResult <- panicErr // panic is more important
					} else {
						chanResult <- err // finished without panic
					}
				}()
				err = next(ctx, request, response)
				if err != nil && ctx.Err() != nil {
					// error occurs after the wait goroutine returns(RPCTimeout happens),
					// we should log this error for troubleshooting, otherwise it will be discarded.
					errMsg := fmt.Sprintf("KITEX: method=%s, from_service=%s, from_addr=%s, biz_error=%s",
						ri.To().Method(), ri.From().ServiceName(), fromAddress(ri), err.Error())
					logger(ctx, "%s", errMsg)
				}
			})

			select {
			case resultError := <-chanResult:
				return resultError
			case <-ctx.Done():
				return makeServerTimeoutErr(ctx, ri, startTime, timeout)
			}
		}
	}
}
