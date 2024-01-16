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

package rpcinfo

import (
	"context"
	"runtime/debug"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/stats"
)

// StreamEventReporter should be implemented by any tracer that wants to report stream events
type StreamEventReporter interface {
	// ReportStreamEvent is for collecting Recv/Send events on stream
	// NOTE: The callee should NOT hold references to event, which may be recycled later
	ReportStreamEvent(ctx context.Context, ri RPCInfo, event Event)
}

// TraceController controls tracers.
type TraceController struct {
	tracers              []stats.Tracer
	streamEventReporters []StreamEventReporter
}

// Append appends a new tracer to the controller.
func (c *TraceController) Append(col stats.Tracer) {
	c.tracers = append(c.tracers, col)
	if reporter, ok := col.(StreamEventReporter); ok {
		c.streamEventReporters = append(c.streamEventReporters, reporter)
	}
}

// DoStart starts the tracers.
func (c *TraceController) DoStart(ctx context.Context, ri RPCInfo) context.Context {
	defer c.tryRecover(ctx)
	Record(ctx, ri, stats.RPCStart, nil)

	for _, col := range c.tracers {
		ctx = col.Start(ctx)
	}
	return ctx
}

// DoFinish calls the tracers in reversed order.
func (c *TraceController) DoFinish(ctx context.Context, ri RPCInfo, err error) {
	defer c.tryRecover(ctx)
	Record(ctx, ri, stats.RPCFinish, err)
	if err != nil {
		rpcStats := AsMutableRPCStats(ri.Stats())
		rpcStats.SetError(err)
	}

	// reverse the order
	for i := len(c.tracers) - 1; i >= 0; i-- {
		c.tracers[i].Finish(ctx)
	}
}

// ReportStreamEvent is for collecting Recv/Send events on stream
func (c *TraceController) ReportStreamEvent(ctx context.Context, event Event) {
	if !c.HasStreamEventReporter() {
		return
	}
	defer c.tryRecover(ctx)
	// RPCInfo is likely to be used by each reporter
	ri := GetRPCInfo(ctx)
	for i := len(c.streamEventReporters) - 1; i >= 0; i-- {
		c.streamEventReporters[i].ReportStreamEvent(ctx, ri, event)
	}
	if recyclable, ok := event.(Recyclable); ok {
		recyclable.Recycle()
	}
}

// GetStreamEventHandler returns the stream event handler
// If there's no StreamEventReporter, nil is returned for client/server to skip adding tracing middlewares
func (c *TraceController) GetStreamEventHandler() StreamEventHandler {
	if c.HasStreamEventReporter() {
		return c.ReportStreamEvent
	}
	return nil
}

// HasTracer reports whether there exists any tracer.
func (c *TraceController) HasTracer() bool {
	return c != nil && len(c.tracers) > 0
}

// HasStreamEventReporter reports whether there exists any StreamEventReporter.
func (c *TraceController) HasStreamEventReporter() bool {
	return c != nil && len(c.streamEventReporters) > 0
}

func (c *TraceController) tryRecover(ctx context.Context) {
	if err := recover(); err != nil {
		klog.CtxWarnf(ctx, "Panic happened during tracer call. This doesn't affect the rpc call, but may lead to lack of monitor data such as metrics and logs: error=%s, stack=%s", err, string(debug.Stack()))
	}
}
