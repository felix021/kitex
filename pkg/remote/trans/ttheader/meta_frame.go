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

	"github.com/cloudwego/kitex/pkg/remote"
)

// Streaming over TTHeader requires that a meta frame is sent by server before sending/receiving on the stream

// MetaFrameGenerator generates the meta frame for TTHeader Streaming
type MetaFrameGenerator func(ctx context.Context, msg remote.Message) (*ttheader, error)

// MetaFrameProcessor is for processing meta frame at the client side
type MetaFrameProcessor func(ctx context.Context, msg remote.Message, metaFrame *frame) (context.Context, error)

var (
	metaFrameGenerator MetaFrameGenerator
	metaFrameHandler   MetaFrameProcessor = nil
)

// RegisterMetaFrameGenerator registers a hook for generating the meta frame for TTHeader Streaming
func RegisterMetaFrameGenerator(g MetaFrameGenerator) {
	metaFrameGenerator = g
}

// RegisterMetaFrameHandler registers a hook for processing meta frame before returning the stream to the caller
// For example, the server may return some necessary info for the client before sending/receiving streaming data
func RegisterMetaFrameHandler(h MetaFrameProcessor) {
	metaFrameHandler = h
}
